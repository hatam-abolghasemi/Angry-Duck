package worker

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"angryduck/internal/blobship"
	"angryduck/internal/imageref"
	"angryduck/internal/logging"
	"angryduck/internal/metrics"
	"angryduck/internal/model"
	"angryduck/internal/sharedtoken"
)

var (
	spreadTransfersTotal = metrics.NewCounterVec(
		"angryduck_worker_spread_transfers_total",
		"Blobs this node fetched for a spread, by path (registry, peer) and result: ok, failed, or cancelled (the blob arrived by the other path first, or the job was dropped).",
		"node", "path", "result",
	)
	spreadBytesTotal = metrics.NewCounterVec(
		"angryduck_worker_spread_bytes_total",
		"Bytes moved for spreads, by path: registry and peer are received, served is sent to other nodes.",
		"node", "path",
	)
	spreadMillisTotal = metrics.NewCounterVec(
		"angryduck_worker_spread_transfer_milliseconds_total",
		"Time spent on spread transfers, by path (registry, peer). spread_bytes_total / this x 1000 is the average throughput in bytes per second.",
		"node", "path",
	)
	spreadActive = metrics.NewGaugeVec(
		"angryduck_worker_spread_active",
		"1 while this node is fetching a blob on that path (one at a time per path).",
		"node", "path",
	)
	spreadFinalizesTotal = metrics.NewCounterVec(
		"angryduck_worker_spread_finalizes_total",
		"Images registered from spread blobs, by result.",
		"node", "result",
	)
)

// spreadLeaseTTL is how long a job's blobs stay held without the image
// that will reference them. A crashed or abandoned job frees them on its
// own; a longer job moves them to a fresh lease before this runs out.
const spreadLeaseTTL = time.Hour

// spreadMaxBlob refuses absurd sizes in an order, so a broken controller
// can't make a node fill its disk with one fetch.
const spreadMaxBlob = 64 << 30

// spreadMaxMetadata caps the metadata a finalize carries (real images: a
// few KB).
const spreadMaxMetadata = 4 << 20

// SpreadStore is what spreading needs from containerd. ContainerdStore
// has it.
type SpreadStore interface {
	HasBlob(dgst string) (have, known bool)
	Digests(ctx context.Context) (map[string]bool, error)
	WriteBlob(ctx context.Context, lease, ref, dgst string, size int64, r io.Reader) (int64, error)
	CreateLease(ctx context.Context, id string, ttl time.Duration) error
	MoveLease(ctx context.Context, from, to string, ttl time.Duration) error
	DeleteLease(ctx context.Context, id string) error
	Import(ctx context.Context, r io.Reader, platform string) error
	Resolve(ctx context.Context, image string) (mediaType, digest string, err error)
	StreamContent(ctx context.Context, dgst string, w io.Writer) error
}

// BlobOpener streams one blob of an image's repository from its registry.
type BlobOpener interface {
	OpenBlob(ctx context.Context, image, digest string) (io.ReadCloser, int64, error)
}

// Spread is this node's part of spreading an image blob by blob.
//
// The controller assigns each node at most one blob from the registry and
// one from a peer at a time; the two slots run independently, so a slow
// registry or a slow peer never holds up the other path. Both may be
// fetching the same blob, a race the controller starts on purpose: when
// one commits it, the other is cancelled right here, from the goroutine
// that committed, without waiting on anyone. Every transfer ends with a
// SpreadDone to the controller.
//
// Blobs land in containerd's content store under the job's lease (see
// spreadLeaseTTL) and are served to peers by digest at /spread/content.
// When the node holds every layer, /spread/finalize registers the image
// from the metadata the controller sends.
//
// No goroutine runs while nothing is being transferred.
type Spread struct {
	store         SpreadStore
	registry      BlobOpener
	token         string
	nodeID        string
	platform      string
	controllerURL string
	epoch         string        // random per process: lease names never collide with a previous run's
	timeout       time.Duration // one transfer, start to commit
	peers         *http.Client
	ctl           *http.Client

	mu   sync.Mutex
	jobs map[string]*spreadJob
	slot [2]*spreadXfer // by pathIndex

	finalizing chan struct{} // one finalize at a time
	onFinalize func()
	wg         sync.WaitGroup // transfers, for tests
}

type spreadJob struct {
	mu       sync.Mutex // serializes this job's lease calls
	lease    string     // "" until the first transfer
	gen      int
	leasedAt time.Time
	touched  time.Time
}

type spreadXfer struct {
	job, digest, path string
	cancel            context.CancelFunc
	cancelled         atomic.Bool // cancelled on purpose: race lost or job dropped
}

func pathIndex(path string) int {
	if path == model.SpreadPathPeer {
		return 1
	}
	return 0
}

// NewSpread builds the worker's spread handlers.
func NewSpread(store SpreadStore, registry BlobOpener, token, nodeID, platform, controllerURL string, timeout time.Duration) *Spread {
	return &Spread{
		store: store, registry: registry, token: token, nodeID: nodeID, platform: platform,
		controllerURL: strings.TrimRight(controllerURL, "/"), timeout: timeout, epoch: randomSuffix()[:8],
		// Peers stream for as long as a blob takes; ctx bounds it.
		peers: &http.Client{Transport: &http.Transport{
			Proxy:                 nil,
			DialContext:           (&net.Dialer{Timeout: 5 * time.Second}).DialContext,
			ResponseHeaderTimeout: 30 * time.Second,
		}},
		ctl:        &http.Client{Timeout: 5 * time.Second},
		jobs:       map[string]*spreadJob{},
		finalizing: make(chan struct{}, 1),
	}
}

// OnFinalize registers fn to run after an image is registered (the
// reporter's Kick, so the controller sees the image right away).
func (s *Spread) OnFinalize(fn func()) { s.onFinalize = fn }

// Register mounts the spread endpoints, all behind the shared token.
func (s *Spread) Register(mux *http.ServeMux) {
	mux.HandleFunc("/spread/fetch", sharedtoken.Require(s.token, s.HandleFetch))
	mux.HandleFunc("/spread/finalize", sharedtoken.Require(s.token, s.HandleFinalize))
	mux.HandleFunc("/spread/drop", sharedtoken.Require(s.token, s.HandleDrop))
	mux.HandleFunc("/spread/content/", sharedtoken.Require(s.token, s.HandleContent))
}

// validJobID keeps job IDs safe inside lease and ingest names.
func validJobID(id string) bool {
	if id == "" || len(id) > 64 {
		return false
	}
	for _, c := range id {
		if !(c >= 'a' && c <= 'z' || c >= '0' && c <= '9' || c == '-') {
			return false
		}
	}
	return true
}

// have reports whether blob dgst is committed here.
func (s *Spread) have(ctx context.Context, dgst string) bool {
	if have, known := s.store.HasBlob(dgst); known {
		return have
	}
	all, err := s.store.Digests(ctx)
	return err == nil && all[dgst]
}

// HandleFetch starts one transfer and answers at once (see SpreadFetchAck).
func (s *Spread) HandleFetch(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var o model.SpreadFetch
	if err := json.NewDecoder(io.LimitReader(r.Body, 64<<10)).Decode(&o); err != nil {
		http.Error(w, "invalid fetch order", http.StatusBadRequest)
		return
	}
	switch {
	case !validJobID(o.Job), !validDigest(o.Digest), o.Size <= 0, o.Size > spreadMaxBlob:
		http.Error(w, "invalid fetch order", http.StatusBadRequest)
		return
	case o.Path == model.SpreadPathRegistry && o.Image == "",
		o.Path == model.SpreadPathPeer && (o.Peer == nil || o.Peer.Address == ""),
		o.Path != model.SpreadPathRegistry && o.Path != model.SpreadPathPeer:
		http.Error(w, "invalid fetch order", http.StatusBadRequest)
		return
	}
	if s.have(r.Context(), o.Digest) {
		writeJSON(w, http.StatusOK, model.SpreadFetchAck{Present: true})
		return
	}
	i := pathIndex(o.Path)
	ctx, cancel := context.WithTimeout(context.Background(), s.timeout)
	x := &spreadXfer{job: o.Job, digest: o.Digest, path: o.Path, cancel: cancel}
	s.mu.Lock()
	if cur := s.slot[i]; cur != nil {
		s.mu.Unlock()
		cancel()
		writeJSON(w, http.StatusConflict, model.SpreadFetchAck{Reason: "busy with " + cur.digest})
		return
	}
	s.slot[i] = x
	s.pruneJobsLocked(time.Now())
	s.mu.Unlock()

	s.wg.Add(1)
	go s.run(ctx, x, o)
	writeJSON(w, http.StatusAccepted, model.SpreadFetchAck{Accepted: true})
}

// pruneJobsLocked forgets jobs idle for longer than their lease lives:
// their lease has expired, so there is nothing left to hold or drop.
func (s *Spread) pruneJobsLocked(now time.Time) {
	for id, j := range s.jobs {
		if now.Sub(j.touched) > spreadLeaseTTL && !s.busyLocked(id) {
			delete(s.jobs, id)
		}
	}
}

func (s *Spread) busyLocked(job string) bool {
	for _, x := range s.slot {
		if x != nil && x.job == job {
			return true
		}
	}
	return false
}

// lease returns the lease holding job's blobs here, creating it, or moving
// its blobs to a fresh one when half its life is gone.
func (s *Spread) lease(ctx context.Context, job string) (string, error) {
	s.mu.Lock()
	j := s.jobs[job]
	if j == nil {
		j = &spreadJob{}
		s.jobs[job] = j
	}
	j.touched = time.Now()
	s.mu.Unlock()

	j.mu.Lock()
	defer j.mu.Unlock()
	switch {
	case j.lease == "":
		name := "angryduck-spread-" + job + "-" + s.epoch + "-0"
		if err := s.store.CreateLease(ctx, name, spreadLeaseTTL); err != nil {
			return "", fmt.Errorf("creating lease: %w", err)
		}
		j.lease, j.leasedAt = name, time.Now()
	case time.Since(j.leasedAt) > spreadLeaseTTL/2:
		name := "angryduck-spread-" + job + "-" + s.epoch + "-" + strconv.Itoa(j.gen+1)
		if err := s.store.MoveLease(ctx, j.lease, name, spreadLeaseTTL); err != nil {
			return "", fmt.Errorf("renewing lease: %w", err)
		}
		j.gen++
		j.lease, j.leasedAt = name, time.Now()
	}
	return j.lease, nil
}

// run does one transfer, then frees its slot and reports.
func (s *Spread) run(ctx context.Context, x *spreadXfer, o model.SpreadFetch) {
	defer s.wg.Done()
	start := time.Now()
	spreadActive.Set(1, s.nodeID, o.Path)
	n, err := s.fetch(ctx, x, o)
	x.cancel()
	took := time.Since(start)

	result := model.SpreadResultOK
	switch {
	case err == nil:
		// Won: stop the other path if it is fetching this same blob.
		s.mu.Lock()
		if other := s.slot[1-pathIndex(o.Path)]; other != nil && other.digest == o.Digest {
			other.cancelled.Store(true)
			other.cancel()
		}
		s.mu.Unlock()
	case x.cancelled.Load():
		result = model.SpreadResultCancelled
	default:
		result = model.SpreadResultFailed
	}

	s.mu.Lock()
	if s.slot[pathIndex(o.Path)] == x {
		s.slot[pathIndex(o.Path)] = nil
	}
	s.mu.Unlock()
	spreadActive.Set(0, s.nodeID, o.Path)
	spreadTransfersTotal.Inc(s.nodeID, o.Path, result)
	spreadBytesTotal.Add(n, s.nodeID, o.Path)
	spreadMillisTotal.Add(took.Milliseconds(), s.nodeID, o.Path)

	done := model.SpreadDone{Node: s.nodeID, Job: o.Job, Digest: o.Digest, Path: o.Path, Result: result, Bytes: n, Seconds: took.Seconds()}
	from := "registry"
	if o.Peer != nil {
		from = o.Peer.NodeID
	}
	switch result {
	case model.SpreadResultOK:
		logging.Infof("angryduck-worker[%s]: spread: got %s (%d bytes) from %s in %s", s.nodeID, o.Digest, n, from, took.Round(time.Millisecond))
	case model.SpreadResultCancelled:
		logging.Debugf("angryduck-worker[%s]: spread: stopped fetching %s from %s after %d bytes: it arrived another way first", s.nodeID, o.Digest, from, n)
	default:
		done.Error = err.Error()
		logging.Warnf("angryduck-worker[%s]: spread: fetching %s from %s failed after %d bytes: %v", s.nodeID, o.Digest, from, n, err)
	}
	s.report(done)
}

// fetch streams the blob from its source into the content store.
func (s *Spread) fetch(ctx context.Context, x *spreadXfer, o model.SpreadFetch) (int64, error) {
	lease, err := s.lease(ctx, o.Job)
	if err != nil {
		return 0, err
	}
	var body io.ReadCloser
	var length int64
	if o.Path == model.SpreadPathRegistry {
		body, length, err = s.registry.OpenBlob(ctx, imageref.Normalize(o.Image), o.Digest)
	} else {
		body, length, err = s.openPeer(ctx, o.Peer.Address, o.Digest)
	}
	if err != nil {
		return 0, err
	}
	defer body.Close()
	if length >= 0 && length != o.Size {
		return 0, fmt.Errorf("source announced %d bytes, want %d", length, o.Size)
	}
	ref := "angryduck-spread-" + o.Path + "-" + strings.TrimPrefix(o.Digest, "sha256:")
	return s.store.WriteBlob(ctx, lease, ref, o.Digest, o.Size, body)
}

func (s *Spread) openPeer(ctx context.Context, addr, dgst string) (io.ReadCloser, int64, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://"+addr+"/spread/content/"+dgst, nil)
	if err != nil {
		return nil, 0, err
	}
	sharedtoken.Set(req, s.token)
	resp, err := s.peers.Do(req)
	if err != nil {
		return nil, 0, err
	}
	if resp.StatusCode != http.StatusOK {
		msg := readError(resp)
		resp.Body.Close()
		return nil, 0, fmt.Errorf("peer %s: %s", addr, msg)
	}
	return resp.Body, resp.ContentLength, nil
}

// report tells the controller a transfer ended. A lost report only costs
// time: the controller also learns of the blob from this node's next layer
// inventory, and frees a silent slot after its transfer timeout.
func (s *Spread) report(done model.SpreadDone) {
	if s.controllerURL == "" {
		return
	}
	body, _ := json.Marshal(done)
	for attempt, wait := 0, time.Second; attempt < 3; attempt, wait = attempt+1, wait*2 {
		req, err := http.NewRequest(http.MethodPost, s.controllerURL+"/spread/done", bytes.NewReader(body))
		if err != nil {
			return
		}
		req.Header.Set("Content-Type", "application/json")
		sharedtoken.Set(req, s.token)
		resp, err := s.ctl.Do(req)
		if err == nil {
			resp.Body.Close()
			if resp.StatusCode < 500 {
				return
			}
		}
		time.Sleep(wait)
	}
	logging.Warnf("angryduck-worker[%s]: spread: couldn't report %s of %s to the controller", s.nodeID, done.Result, done.Digest)
}

// HandleContent serves one committed blob by digest to a peer.
func (s *Spread) HandleContent(w http.ResponseWriter, r *http.Request) {
	dgst := strings.TrimPrefix(r.URL.Path, "/spread/content/")
	if !validDigest(dgst) || (r.Method != http.MethodGet && r.Method != http.MethodHead) {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}
	if !s.have(r.Context(), dgst) {
		http.NotFound(w, r)
		return
	}
	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("Docker-Content-Digest", dgst)
	if r.Method == http.MethodHead {
		return
	}
	cw := &countWriter{w: w}
	err := s.store.StreamContent(r.Context(), dgst, cw)
	spreadBytesTotal.Add(cw.n, s.nodeID, "served")
	if err != nil {
		logging.Warnf("angryduck-worker[%s]: spread: serving %s to %s failed after %d bytes: %v", s.nodeID, dgst, r.RemoteAddr, cw.n, err)
		if cw.n > 0 {
			panic(http.ErrAbortHandler) // cut it: the receiver must see a short read
		}
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

// HandleDrop cancels a job's transfers here and releases its blobs.
func (s *Spread) HandleDrop(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var d model.SpreadDrop
	if err := json.NewDecoder(io.LimitReader(r.Body, 4<<10)).Decode(&d); err != nil || !validJobID(d.Job) {
		http.Error(w, "invalid drop", http.StatusBadRequest)
		return
	}
	s.mu.Lock()
	for _, x := range s.slot {
		if x != nil && x.job == d.Job {
			x.cancelled.Store(true)
			x.cancel()
		}
	}
	s.mu.Unlock()
	s.release(r.Context(), d.Job)
	w.WriteHeader(http.StatusNoContent)
}

// release deletes job's lease and forgets the job.
func (s *Spread) release(ctx context.Context, job string) {
	s.mu.Lock()
	j := s.jobs[job]
	if !s.busyLocked(job) {
		delete(s.jobs, job)
	}
	s.mu.Unlock()
	if j == nil {
		return
	}
	j.mu.Lock()
	defer j.mu.Unlock()
	if j.lease == "" {
		return
	}
	if err := s.store.DeleteLease(ctx, j.lease); err != nil {
		logging.Warnf("angryduck-worker[%s]: spread: releasing lease %s: %v (it expires on its own)", s.nodeID, j.lease, err)
		return
	}
	j.lease = ""
}

// HandleFinalize registers the image once every layer is here.
func (s *Spread) HandleFinalize(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var f model.SpreadFinalize
	// Metadata travels base64-encoded: allow for that on top of its cap.
	if err := json.NewDecoder(io.LimitReader(r.Body, 2*spreadMaxMetadata)).Decode(&f); err != nil {
		http.Error(w, "invalid finalize", http.StatusBadRequest)
		return
	}
	if err := checkFinalize(f); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	select {
	case s.finalizing <- struct{}{}:
		defer func() { <-s.finalizing }()
	case <-r.Context().Done():
		return
	}
	// Not cut short by the controller giving up: an unpack halfway is
	// wasted work, and the next report shows the controller the image.
	ctx, cancel := context.WithTimeout(context.WithoutCancel(r.Context()), 15*time.Minute)
	defer cancel()
	start := time.Now()
	image := imageref.Normalize(f.Image)
	err := s.finalize(ctx, f, image)
	if err != nil {
		spreadFinalizesTotal.Inc(s.nodeID, "failure")
		logging.Warnf("angryduck-worker[%s]: spread: registering image=%s failed: %v", s.nodeID, image, err)
		writeJSON(w, http.StatusBadGateway, model.SpreadFinalizeResult{Error: err.Error()})
		return
	}
	s.release(ctx, f.Job) // the image references its blobs now
	spreadFinalizesTotal.Inc(s.nodeID, "success")
	logging.Infof("angryduck-worker[%s]: spread: image=%s registered in %s", s.nodeID, image, time.Since(start).Round(time.Millisecond))
	if s.onFinalize != nil {
		s.onFinalize()
	}
	writeJSON(w, http.StatusOK, model.SpreadFinalizeResult{OK: true})
}

// checkFinalize validates a finalize order: every metadata blob must match
// its digest, and the top one must be among them.
func checkFinalize(f model.SpreadFinalize) error {
	if !validJobID(f.Job) || f.Image == "" || !validDigest(f.TopDig) || f.TopType == "" {
		return errors.New("invalid finalize")
	}
	var total int
	for d, b := range f.Metadata {
		total += len(b)
		sum := sha256.Sum256(b)
		if d != "sha256:"+hex.EncodeToString(sum[:]) {
			return fmt.Errorf("metadata blob %s doesn't match its digest", d)
		}
	}
	if total > spreadMaxMetadata {
		return errors.New("metadata too large")
	}
	if top, ok := f.Metadata[f.TopDig]; !ok || int64(len(top)) != f.TopSize {
		return errors.New("top blob missing from metadata")
	}
	return nil
}

// finalize imports an archive of just the metadata, naming the image: the
// layers are already in the content store (or unpacked), so containerd
// registers the name and unpacks what isn't unpacked yet.
func (s *Spread) finalize(ctx context.Context, f model.SpreadFinalize, image string) error {
	top := blobship.Descriptor{MediaType: f.TopType, Digest: f.TopDig, Size: f.TopSize}
	pr, pw := io.Pipe()
	go func() {
		pw.CloseWithError(func() error {
			aw, err := blobship.NewArchiveWriter(pw, top, image)
			if err != nil {
				return err
			}
			for _, d := range sortedKeys(f.Metadata) {
				if err := aw.AddBytes(d, f.Metadata[d]); err != nil {
					return err
				}
			}
			return aw.Close()
		}())
	}()
	err := s.store.Import(ctx, pr, s.platform)
	pr.CloseWithError(errImportEnded)
	if err != nil {
		return err
	}
	if _, got, err := s.store.Resolve(ctx, image); err != nil || got != f.TopDig {
		return fmt.Errorf("after import, %s resolves to %q (err %v), want %s", image, got, err, f.TopDig)
	}
	return nil
}

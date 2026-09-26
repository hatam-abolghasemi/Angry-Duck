package worker

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"

	"angryduck/internal/blobship"
	"angryduck/internal/imageref"
	"angryduck/internal/logging"
	"angryduck/internal/metrics"
	"angryduck/internal/model"
	"angryduck/internal/sharedtoken"
)

var (
	rescuesTotal = metrics.NewCounterVec(
		"angryduck_worker_rescues_total",
		"Images this node was ordered to fetch from a peer, by reason (\"rescue\" for a stuck pod, \"propagate\" after a push) and result.",
		"node", "reason", "result",
	)
	rescueBytesTotal = metrics.NewCounterVec(
		"angryduck_worker_rescue_bytes_total",
		"Bytes shipped for rescues (blobs and compressed snapshots). direction is \"served\" (this node was the source) or \"received\".",
		"node", "direction",
	)
	rescueLayersTotal = metrics.NewCounterVec(
		"angryduck_worker_rescue_layers_total",
		"Layers of rescued images by how this node got them: \"present\" (snapshot already here), \"blob\" or \"snapshot\".",
		"node", "method",
	)
)

const (
	// planTimeout bounds the /blobs/plan round trip. Planning reads only a
	// few small metadata blobs, so this is generous.
	planTimeout = 30 * time.Second
	// receiveTimeout bounds one whole rescue on this node, so a source
	// that stalls mid-stream can't hold a slot forever.
	receiveTimeout = 30 * time.Minute
)

// Rescue fixes ImagePullBackOff on this node by copying the missing blobs
// of an image from a peer worker that has it, over plain HTTP between
// workers, guarded by a shared bearer token. See package blobship for the
// transfer format.
//
// The same type serves both roles:
//   - receiver: POST /rescue, called by the controller with a list of
//     candidate source workers;
//   - source: POST /blobs/plan, /blobs/export and /snapshots/export,
//     called by the receiving peer.
type Rescue struct {
	store    blobship.Store
	pins     *Pins
	token    string
	nodeID   string
	platform string
	client   *http.Client
	sem      chan struct{} // bounds concurrent receives on this node

	mu       sync.Mutex
	inflight map[string]*rescueCall // image -> the rescue already running for it

	onSuccess func()
}

type rescueCall struct {
	done   chan struct{}
	result model.RescueResult
}

// NewRescue builds the worker's rescue handler set. platform is this
// node's "os/arch[/variant]"; maxConcurrent bounds how many images this
// node receives at once.
//
// pins may be nil (tests): shipped snapshots are then unpinned right after
// each attempt instead of being kept for a retry.
func NewRescue(store blobship.Store, pins *Pins, token, nodeID, platform string, maxConcurrent int) *Rescue {
	if maxConcurrent < 1 {
		maxConcurrent = 1
	}
	if pins == nil {
		pins = NewPins(store, nodeID, 0, "")
	}
	return &Rescue{
		store:    store,
		pins:     pins,
		token:    token,
		nodeID:   nodeID,
		platform: platform,
		// No client-wide timeout: an export streams for as long as the
		// image takes. Every request carries a context instead, and a
		// peer that never answers is caught by the header timeout.
		client: &http.Client{Transport: &http.Transport{
			Proxy:                 nil, // peers are on the node network, never via a proxy
			DialContext:           (&net.Dialer{Timeout: 5 * time.Second}).DialContext,
			ResponseHeaderTimeout: 2 * time.Minute,
		}},
		sem:      make(chan struct{}, maxConcurrent),
		inflight: make(map[string]*rescueCall),
	}
}

// OnSuccess registers fn to run after an image is imported (the reporter's
// Kick, so the controller learns right away that this node now has it).
func (rs *Rescue) OnSuccess(fn func()) { rs.onSuccess = fn }

// Register mounts the rescue endpoints on mux, all behind the token.
func (rs *Rescue) Register(mux *http.ServeMux) {
	mux.HandleFunc("/rescue", sharedtoken.Require(rs.token, rs.HandleRescue))
	mux.HandleFunc("/blobs/plan", sharedtoken.Require(rs.token, rs.HandlePlan))
	mux.HandleFunc("/blobs/export", sharedtoken.Require(rs.token, rs.HandleExport))
	mux.HandleFunc("/snapshots/export", sharedtoken.Require(rs.token, rs.HandleSnapshotExport))
}

// --- receiver side ---

// HandleRescue runs one rescue synchronously and answers with its result.
// Two orders for the same image while one is running share its result.
func (rs *Rescue) HandleRescue(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var order model.RescueOrder
	if err := json.NewDecoder(r.Body).Decode(&order); err != nil || order.Image == "" {
		http.Error(w, "invalid rescue order", http.StatusBadRequest)
		return
	}
	order.Image = imageref.Normalize(order.Image)
	if order.Reason == "" {
		order.Reason = "rescue"
	}

	rs.mu.Lock()
	call, running := rs.inflight[order.Image]
	if !running {
		call = &rescueCall{done: make(chan struct{})}
		rs.inflight[order.Image] = call
	}
	rs.mu.Unlock()

	if !running {
		go func() {
			// Detached from this request: if the controller gives up, the
			// transfer still finishes and the image still lands. It is
			// bounded by the node's own semaphore and receiveTimeout.
			ctx, cancel := context.WithTimeout(context.Background(), receiveTimeout)
			call.result = rs.receive(ctx, order)
			cancel()
			rs.mu.Lock()
			delete(rs.inflight, order.Image)
			rs.mu.Unlock()
			close(call.done)
		}()
	}

	select {
	case <-call.done:
	case <-r.Context().Done():
		return
	}
	status := http.StatusOK
	if !call.result.OK {
		status = http.StatusBadGateway
	}
	writeJSON(w, status, call.result)
}

func (rs *Rescue) receive(ctx context.Context, order model.RescueOrder) model.RescueResult {
	rs.sem <- struct{}{}
	defer func() { <-rs.sem }()

	start := time.Now()
	if _, _, err := rs.store.Resolve(ctx, order.Image); err == nil {
		logging.Infof("angryduck-worker[%s]: rescue image=%s: already present, nothing to ship", rs.nodeID, order.Image)
		rescuesTotal.Inc(rs.nodeID, order.Reason, "already_present")
		return model.RescueResult{OK: true}
	}
	if len(order.Sources) == 0 {
		rescuesTotal.Inc(rs.nodeID, order.Reason, "failure")
		return model.RescueResult{Error: "no sources in order"}
	}

	var errs []string
	for _, src := range order.Sources {
		res, err := rs.receiveFrom(ctx, order.Image, src)
		if err != nil {
			logging.Warnf("angryduck-worker[%s]: rescue image=%s from node=%s failed: %v", rs.nodeID, order.Image, src.NodeID, err)
			errs = append(errs, src.NodeID+": "+err.Error())
			continue
		}
		logging.Infof("angryduck-worker[%s]: %s: got image=%s from node=%s: %d blob(s), %d snapshot(s), %d bytes in %s",
			rs.nodeID, order.Reason, order.Image, src.NodeID, res.Blobs, res.Snapshots, res.Bytes, time.Since(start).Round(time.Millisecond))
		rescuesTotal.Inc(rs.nodeID, order.Reason, "success")
		if rs.onSuccess != nil {
			rs.onSuccess()
		}
		return res
	}
	rescuesTotal.Inc(rs.nodeID, order.Reason, "failure")
	return model.RescueResult{Error: strings.Join(errs, "; ")}
}

func (rs *Rescue) receiveFrom(ctx context.Context, image string, src model.RescueSource) (model.RescueResult, error) {
	plan, err := rs.fetchPlan(ctx, image, src)
	if err != nil {
		return model.RescueResult{}, fmt.Errorf("plan: %w", err)
	}
	haveBlobs, err := rs.store.Digests(ctx)
	if err != nil {
		return model.RescueResult{}, fmt.Errorf("listing local content: %w", err)
	}
	haveSnaps := map[string]bool{}
	if len(plan.Layers) > 0 {
		if haveSnaps, err = rs.store.Snapshots(ctx); err != nil {
			return model.RescueResult{}, fmt.Errorf("listing local snapshots: %w", err)
		}
	}
	dec, err := blobship.Decide(plan, haveBlobs, haveSnaps)
	if err != nil {
		return model.RescueResult{}, err
	}
	logging.Infof("angryduck-worker[%s]: rescue image=%s from node=%s: %d layer(s) already here, %d to ship as snapshot, %d blob(s) to ship (%d bytes)",
		rs.nodeID, image, src.NodeID, dec.Present, len(dec.Snapshots), len(dec.Blobs), sizeOf(dec.Blobs))

	// Every shipped snapshot stays pinned until the import makes the
	// image reference it. If this attempt fails later, the pins are kept
	// (until RESCUE_PIN_TTL_S) so the retry skips what already arrived.
	var received int64
	for _, l := range dec.Snapshots {
		n, err := rs.receiveSnapshot(ctx, plan, l, src)
		received += n
		if err != nil {
			rescueBytesTotal.Add(received, rs.nodeID, "received")
			return model.RescueResult{}, fmt.Errorf("snapshot %s: %w", l.ChainID, err)
		}
		rs.pins.Add(l.ChainID)
	}

	// The import runs even with nothing to ship: index.json alone is what
	// registers the image name.
	digests := make([]string, len(dec.Blobs))
	for i, b := range dec.Blobs {
		digests[i] = b.Digest
	}
	body, err := json.Marshal(model.BlobExportRequest{Image: image, Platform: rs.platform, Digests: digests})
	if err != nil {
		return model.RescueResult{}, err
	}
	resp, err := rs.post(ctx, src.Address, "/blobs/export", body)
	if err != nil {
		return model.RescueResult{}, fmt.Errorf("export: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return model.RescueResult{}, fmt.Errorf("export: %s", readError(resp))
	}
	counted := &countReader{r: resp.Body}
	importErr := rs.store.Import(ctx, counted, rs.platform)
	received += counted.n
	rescueBytesTotal.Add(received, rs.nodeID, "received")
	if importErr != nil {
		return model.RescueResult{}, fmt.Errorf("import: %w", importErr)
	}

	// Trust, but check: the name must now point at what the source has.
	if _, digest, err := rs.store.Resolve(ctx, image); err != nil || digest != plan.Top.Digest {
		return model.RescueResult{}, fmt.Errorf("after import, %s resolves to %q (err %v), want %s", image, digest, err, plan.Top.Digest)
	}
	// The image references its snapshots now; drop our pins, including
	// any left by earlier failed attempts.
	chains := make([]string, len(plan.Layers))
	for i, l := range plan.Layers {
		chains[i] = l.ChainID
	}
	rs.pins.Release(ctx, chains)

	rescueLayersTotal.Add(int64(dec.Present), rs.nodeID, "present")
	rescueLayersTotal.Add(int64(len(dec.Snapshots)), rs.nodeID, "snapshot")
	rescueLayersTotal.Add(int64(len(plan.Layers)-dec.Present-len(dec.Snapshots)), rs.nodeID, "blob")
	return model.RescueResult{OK: true, Source: src.NodeID, Blobs: len(dec.Blobs), Snapshots: len(dec.Snapshots), Bytes: received}, nil
}

// receiveSnapshot fetches one layer's snapshot directory from src and
// commits it locally under the layer's chainID. It returns the bytes read
// off the wire.
//
// Unlike blobs, a snapshot tar can't be checked against a digest: the
// bytes differ from the original layer even when the files are the same.
// Integrity rests on the authenticated peer, TCP, and gzip's CRC-32, which
// is checked (by reading the stream to its end) before the commit.
func (rs *Rescue) receiveSnapshot(ctx context.Context, plan blobship.Plan, l blobship.Layer, src model.RescueSource) (int64, error) {
	parent := ""
	if i := layerIndex(plan, l.ChainID); i > 0 {
		parent = plan.Layers[i-1].ChainID
	}
	body, err := json.Marshal(model.SnapshotExportRequest{Image: plan.Image, Platform: rs.platform, ChainID: l.ChainID})
	if err != nil {
		return 0, err
	}
	resp, err := rs.post(ctx, src.Address, "/snapshots/export", body)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return 0, fmt.Errorf("%s", readError(resp))
	}
	counted := &countReader{r: resp.Body}
	gz, err := gzip.NewReader(counted)
	if err != nil {
		return counted.n, fmt.Errorf("reading gzip header: %w", err)
	}
	verify := func() error {
		// tar stops at its end-of-archive marker; read the rest so gzip
		// checks its CRC and length trailer.
		if _, err := io.Copy(io.Discard, gz); err != nil {
			return fmt.Errorf("stream check: %w", err)
		}
		return nil
	}
	err = rs.store.ApplySnapshot(ctx, l.ChainID, parent, gz, verify)
	return counted.n, err
}

func layerIndex(plan blobship.Plan, chainID string) int {
	for i, l := range plan.Layers {
		if l.ChainID == chainID {
			return i
		}
	}
	return -1
}

func (rs *Rescue) fetchPlan(ctx context.Context, image string, src model.RescueSource) (blobship.Plan, error) {
	ctx, cancel := context.WithTimeout(ctx, planTimeout)
	defer cancel()
	body, err := json.Marshal(model.BlobPlanRequest{Image: image, Platform: rs.platform})
	if err != nil {
		return blobship.Plan{}, err
	}
	resp, err := rs.post(ctx, src.Address, "/blobs/plan", body)
	if err != nil {
		return blobship.Plan{}, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return blobship.Plan{}, fmt.Errorf("%s", readError(resp))
	}
	var plan blobship.Plan
	if err := json.NewDecoder(io.LimitReader(resp.Body, 1<<20)).Decode(&plan); err != nil {
		return blobship.Plan{}, fmt.Errorf("decoding plan: %w", err)
	}
	if plan.Image != image || plan.Top.Digest == "" || len(plan.Blobs) == 0 {
		return blobship.Plan{}, fmt.Errorf("source returned an unusable plan")
	}
	return plan, nil
}

func (rs *Rescue) post(ctx context.Context, addr, path string, body []byte) (*http.Response, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, "http://"+addr+path, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	sharedtoken.Set(req, rs.token)
	return rs.client.Do(req)
}

// --- source side ---

// HandlePlan answers with the image's blob list for the caller's platform,
// marking the ones this node lacks in Absent, and says for each layer
// whether this node has its blob and its snapshot. The receiver decides
// what it needs; this node can't know what the receiver has.
func (rs *Rescue) HandlePlan(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var req model.BlobPlanRequest
	if err := json.NewDecoder(io.LimitReader(r.Body, 1<<20)).Decode(&req); err != nil || req.Image == "" {
		http.Error(w, "invalid plan request", http.StatusBadRequest)
		return
	}
	plan, err := blobship.BuildPlan(r.Context(), rs.store, imageref.Normalize(req.Image), req.Platform)
	if err != nil {
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}
	have, err := rs.store.Digests(r.Context())
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	for _, b := range plan.Missing(have) {
		plan.Absent = append(plan.Absent, b.Digest)
	}
	if len(plan.Layers) > 0 {
		snaps, err := rs.store.Snapshots(r.Context())
		if err != nil {
			// Not fatal: without it, the receiver just won't ask this node
			// for snapshots.
			logging.Warnf("angryduck-worker[%s]: listing snapshots for plan of %s: %v", rs.nodeID, plan.Image, err)
		}
		for i := range plan.Layers {
			plan.Layers[i].BlobPresent = have[plan.Layers[i].Blob.Digest]
			plan.Layers[i].SnapshotPresent = snaps[plan.Layers[i].ChainID]
		}
	}
	writeJSON(w, http.StatusOK, plan)
}

// HandleExport streams a partial OCI archive holding only the requested
// blobs of one image.
func (rs *Rescue) HandleExport(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var req model.BlobExportRequest
	if err := json.NewDecoder(io.LimitReader(r.Body, 4<<20)).Decode(&req); err != nil || req.Image == "" {
		http.Error(w, "invalid export request", http.StatusBadRequest)
		return
	}
	plan, err := blobship.BuildPlan(r.Context(), rs.store, imageref.Normalize(req.Image), req.Platform)
	if err != nil {
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}
	blobs, err := blobship.Select(plan, req.Digests)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	// Refuse before the 200 goes out, rather than cutting the stream
	// halfway. Receivers from 1.6.0 don't read Absent and land here.
	have, err := rs.store.Digests(r.Context())
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	for _, b := range blobs {
		if !have[b.Digest] {
			http.Error(w, "this node lacks blob "+b.Digest, http.StatusConflict)
			return
		}
	}

	w.Header().Set("Content-Type", "application/x-tar")
	cw := &countWriter{w: w}
	err = blobship.WriteArchive(r.Context(), cw, rs.store, plan, blobs)
	rescueBytesTotal.Add(cw.n, rs.nodeID, "served")
	if err != nil {
		logging.Warnf("angryduck-worker[%s]: export image=%s to %s failed after %d bytes: %v", rs.nodeID, plan.Image, r.RemoteAddr, cw.n, err)
		// The 200 is already on the wire. Cut the connection so the
		// receiver sees a truncated body, never a clean end of archive.
		panic(http.ErrAbortHandler)
	}
	logging.Infof("angryduck-worker[%s]: exported %d blob(s) of image=%s to %s (%d bytes)", rs.nodeID, len(blobs), plan.Image, r.RemoteAddr, cw.n)
}

// HandleSnapshotExport streams one layer's snapshot directory as a
// gzip-compressed tar. Only layers of the named image can be requested.
func (rs *Rescue) HandleSnapshotExport(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var req model.SnapshotExportRequest
	if err := json.NewDecoder(io.LimitReader(r.Body, 1<<20)).Decode(&req); err != nil || req.Image == "" || req.ChainID == "" {
		http.Error(w, "invalid snapshot export request", http.StatusBadRequest)
		return
	}
	plan, err := blobship.BuildPlan(r.Context(), rs.store, imageref.Normalize(req.Image), req.Platform)
	if err != nil {
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}
	i := layerIndex(plan, req.ChainID)
	if i < 0 {
		http.Error(w, "chain_id is not a layer of this image", http.StatusBadRequest)
		return
	}
	dirs, err := rs.store.SnapshotDirs(r.Context(), req.ChainID, i+1)
	if err != nil {
		http.Error(w, err.Error(), http.StatusConflict)
		return
	}

	w.Header().Set("Content-Type", "application/gzip")
	cw := &countWriter{w: w}
	gz, _ := gzip.NewWriterLevel(cw, gzip.BestSpeed)
	err = rs.store.ExportSnapshot(r.Context(), dirs[i], gz)
	if err == nil {
		err = gz.Close()
	}
	rescueBytesTotal.Add(cw.n, rs.nodeID, "served")
	if err != nil {
		logging.Warnf("angryduck-worker[%s]: snapshot export %s of image=%s to %s failed after %d bytes: %v", rs.nodeID, req.ChainID, plan.Image, r.RemoteAddr, cw.n, err)
		panic(http.ErrAbortHandler) // truncate: the receiver's gzip check then fails
	}
	logging.Infof("angryduck-worker[%s]: exported snapshot %s (layer %d) of image=%s to %s (%d bytes compressed)", rs.nodeID, req.ChainID, i, plan.Image, r.RemoteAddr, cw.n)
}

func sizeOf(ds []blobship.Descriptor) int64 {
	var n int64
	for _, d := range ds {
		n += d.Size
	}
	return n
}

func readError(resp *http.Response) string {
	b, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
	return fmt.Sprintf("status %d: %s", resp.StatusCode, strings.TrimSpace(string(b)))
}

type countReader struct {
	r io.Reader
	n int64
}

func (c *countReader) Read(p []byte) (int, error) {
	n, err := c.r.Read(p)
	c.n += int64(n)
	return n, err
}

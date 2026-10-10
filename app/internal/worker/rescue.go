package worker

import (
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"sort"
	"strconv"
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
	for _, reason := range []string{"rescue", "propagate"} {
		for _, r := range []string{"success", "failure", "already_present"} {
			rescuesTotal.Add(0, nodeID, reason, r)
		}
	}
	rescueBytesTotal.Add(0, nodeID, "served")
	rescueBytesTotal.Add(0, nodeID, "received")
	for _, m := range []string{"present", "blob", "snapshot"} {
		rescueLayersTotal.Add(0, nodeID, m)
	}
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

	// Plans are fetched lazily: the first source alone usually covers
	// everything with blobs, and each plan costs the source a few
	// containerd calls.
	plans := make([]blobship.Plan, len(order.Sources))
	planErr := make([]error, len(order.Sources))
	fetched := make([]bool, len(order.Sources))
	getPlan := func(i int) error {
		if !fetched[i] {
			fetched[i] = true
			plans[i], planErr[i] = rs.fetchPlan(ctx, order.Image, order.Sources[i])
		}
		return planErr[i]
	}

	var errs []string
	for primary, src := range order.Sources {
		if err := getPlan(primary); err != nil {
			logging.Warnf("angryduck-worker[%s]: rescue image=%s: plan from node=%s failed: %v", rs.nodeID, order.Image, src.NodeID, err)
			errs = append(errs, src.NodeID+": plan: "+err.Error())
			continue
		}
		res, err := rs.receiveFrom(ctx, order, primary, plans, getPlan)
		if err != nil {
			logging.Warnf("angryduck-worker[%s]: rescue image=%s with node=%s as primary source failed: %v", rs.nodeID, order.Image, src.NodeID, err)
			errs = append(errs, src.NodeID+": "+err.Error())
			continue
		}
		logging.Infof("angryduck-worker[%s]: %s: got image=%s from %s: %d blob(s), %d snapshot(s), %d bytes in %s",
			rs.nodeID, order.Reason, order.Image, res.Source, res.Blobs, res.Snapshots, res.Bytes, time.Since(start).Round(time.Millisecond))
		rescuesTotal.Inc(rs.nodeID, order.Reason, "success")
		if rs.onSuccess != nil {
			rs.onSuccess()
		}
		return res
	}
	rescuesTotal.Inc(rs.nodeID, order.Reason, "failure")
	return model.RescueResult{Error: strings.Join(errs, "; ")}
}

// receiveFrom builds the image with order.Sources[primary] naming it, and
// the other sources as extra places to get blobs from. Snapshots are the
// last resort: only for a layer no source has the blob of.
func (rs *Rescue) receiveFrom(ctx context.Context, order model.RescueOrder, primary int, plans []blobship.Plan, getPlan func(int) error) (model.RescueResult, error) {
	plan := plans[primary]
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
	asm, err := blobship.Assemble(plans, primary, haveBlobs, haveSnaps)
	if (err != nil || asm.SnapshotCount() > 0) && len(order.Sources) > 1 {
		// Ask the other sources before settling for snapshots (or for
		// failing): one of them may hold the blobs.
		for i := range order.Sources {
			if i != primary {
				_ = getPlan(i)
			}
		}
		asm, err = blobship.Assemble(plans, primary, haveBlobs, haveSnaps)
	}
	if err != nil {
		return model.RescueResult{}, err
	}
	logging.Infof("angryduck-worker[%s]: rescue image=%s primary=%s: %d layer(s) already here, %d by blob, %d by snapshot (last resort), %d base import(s), %d blob(s) to ship",
		rs.nodeID, order.Image, order.Sources[primary].NodeID, asm.Present, asm.BlobLayers, asm.SnapshotLayers, len(asm.Steps)-asm.SnapshotCount(), asm.BlobCount())

	used := map[int]bool{primary: true}
	var received int64
	var temps []string
	defer func() {
		if len(temps) > 0 {
			cctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			if err := rs.store.DeleteImages(cctx, temps...); err != nil {
				logging.Warnf("angryduck-worker[%s]: removing temporary base image(s) %v: %v", rs.nodeID, temps, err)
			}
			cancel()
		}
	}()
	// Every shipped snapshot stays pinned until the import makes the
	// image reference it. If this attempt fails later, the pins are kept
	// (until RESCUE_PIN_TTL_S) so the retry skips what already arrived.
	for _, st := range asm.Steps {
		if st.Base {
			top, inline, err := blobship.BaseImage(plan, st.UpTo, rs.platform)
			if err != nil {
				return model.RescueResult{}, err
			}
			name := baseImagePrefix + randomSuffix() + ":layers-0-" + strconv.Itoa(st.UpTo)
			n, err := rs.importMerged(ctx, order.Image, top, name, inline, st.Blobs, order.Sources)
			received += n
			if err != nil {
				rescueBytesTotal.Add(received, rs.nodeID, "received")
				return model.RescueResult{}, fmt.Errorf("base import of layers 0-%d: %w", st.UpTo, err)
			}
			temps = append(temps, name)
			for i := range st.Blobs {
				used[i] = true
			}
			continue
		}
		n, err := rs.receiveSnapshot(ctx, plan, st.Layer, order.Sources[st.Source])
		received += n
		if err != nil {
			rescueBytesTotal.Add(received, rs.nodeID, "received")
			return model.RescueResult{}, fmt.Errorf("snapshot %s: %w", st.Layer.ChainID, err)
		}
		rs.pins.Add(st.Layer.ChainID)
		used[st.Source] = true
	}

	// The import runs even with nothing to ship: index.json alone is what
	// registers the image name.
	n, importErr := rs.importMerged(ctx, order.Image, plan.Top, order.Image, nil, asm.Final, order.Sources)
	received += n
	rescueBytesTotal.Add(received, rs.nodeID, "received")
	if importErr != nil {
		return model.RescueResult{}, fmt.Errorf("import: %w", importErr)
	}
	for i := range asm.Final {
		used[i] = true
	}

	// Trust, but check: the name must now point at what the source has.
	if _, digest, err := rs.store.Resolve(ctx, order.Image); err != nil || digest != plan.Top.Digest {
		return model.RescueResult{}, fmt.Errorf("after import, %s resolves to %q (err %v), want %s", order.Image, digest, err, plan.Top.Digest)
	}
	// The image references its snapshots now; drop our pins, including
	// any left by earlier failed attempts.
	chains := make([]string, len(plan.Layers))
	for i, l := range plan.Layers {
		chains[i] = l.ChainID
	}
	rs.pins.Release(ctx, chains)

	rescueLayersTotal.Add(int64(asm.Present), rs.nodeID, "present")
	rescueLayersTotal.Add(int64(asm.SnapshotLayers), rs.nodeID, "snapshot")
	rescueLayersTotal.Add(int64(asm.BlobLayers), rs.nodeID, "blob")
	var names []string
	for i, src := range order.Sources {
		if used[i] {
			names = append(names, src.NodeID)
		}
	}
	return model.RescueResult{OK: true, Source: strings.Join(names, ","), Blobs: asm.BlobCount(), Snapshots: asm.SnapshotCount(), Bytes: received}, nil
}

// baseImagePrefix names the temporary images a base step imports. They
// are removed right after the rescue, and at startup if a crash left any.
const baseImagePrefix = "angryduck.local/rescue-base/"

// importMerged pipes one archive into containerd's import: top named as
// name, the inline blobs, then the blobs each source exports (streamed
// straight through, never buffered). image is the real image the sources
// export from; their export refuses digests outside its plan. It returns
// the bytes read off the wire.
func (rs *Rescue) importMerged(ctx context.Context, image string, top blobship.Descriptor, name string, inline map[string][]byte, bySource map[int][]blobship.Descriptor, sources []model.RescueSource) (int64, error) {
	pr, pw := io.Pipe()
	var wire int64
	writeErr := make(chan error, 1)
	go func() {
		err := func() error {
			aw, err := blobship.NewArchiveWriter(pw, top, name)
			if err != nil {
				return err
			}
			for _, d := range sortedKeys(inline) {
				if err := aw.AddBytes(d, inline[d]); err != nil {
					return err
				}
			}
			for _, i := range sortedInts(bySource) {
				want := bySource[i]
				if len(want) == 0 {
					continue
				}
				digests := make([]string, len(want))
				for j, b := range want {
					digests[j] = b.Digest
				}
				body, err := json.Marshal(model.BlobExportRequest{Image: image, Platform: rs.platform, Digests: digests})
				if err != nil {
					return err
				}
				resp, err := rs.post(ctx, sources[i].Address, "/blobs/export", body)
				if err != nil {
					return fmt.Errorf("export from %s: %w", sources[i].NodeID, err)
				}
				if resp.StatusCode != http.StatusOK {
					msg := readError(resp)
					resp.Body.Close()
					return fmt.Errorf("export from %s: %s", sources[i].NodeID, msg)
				}
				counted := &countReader{r: resp.Body}
				err = aw.CopyBlobsFrom(counted, want)
				wire += counted.n
				resp.Body.Close()
				if err != nil {
					return fmt.Errorf("export from %s: %w", sources[i].NodeID, err)
				}
			}
			return aw.Close()
		}()
		pw.CloseWithError(err)
		writeErr <- err
	}()
	importErr := rs.store.Import(ctx, pr, rs.platform)
	pr.CloseWithError(errImportEnded)
	werr := <-writeErr
	if werr != nil && !errors.Is(werr, errImportEnded) {
		return wire, werr // the root cause; the import failure follows from it
	}
	return wire, importErr
}

// errImportEnded stops an archive writer whose import already returned.
var errImportEnded = errors.New("import ended before the archive did")

func sortedKeys(m map[string][]byte) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

func sortedInts(m map[int][]blobship.Descriptor) []int {
	out := make([]int, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Ints(out)
	return out
}

// Snapshot transfer headers. The digest travels as an HTTP trailer: the
// source only knows it once the whole layer has been written.
const (
	snapshotFormatHeader  = "X-Angryduck-Snapshot-Format"
	snapshotDigestTrailer = "X-Angryduck-Snapshot-Digest"
)

// receiveSnapshot fetches one layer's snapshot from src and commits it
// locally under the layer's chainID. It returns the bytes read off the
// wire.
//
// A snapshot can't be checked against the layer's own digest: its tar
// bytes differ from the original layer even when the files are the same.
// A 1.8.6+ source sends the SHA-256 of the exact tar it produced, checked
// here before the commit, on top of gzip's CRC-32. An older source sends
// its overlayfs directory format with only the CRC; it is converted here.
func (rs *Rescue) receiveSnapshot(ctx context.Context, plan blobship.Plan, l blobship.Layer, src model.RescueSource) (int64, error) {
	parent := ""
	if i := layerIndex(plan, l.ChainID); i > 0 {
		parent = plan.Layers[i-1].ChainID
	}
	body, err := json.Marshal(model.SnapshotExportRequest{Image: plan.Image, Platform: rs.platform, ChainID: l.ChainID, Format: blobship.FormatOCILayer})
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
	hash := sha256.New()
	stream := io.TeeReader(gz, hash)
	format := resp.Header.Get(snapshotFormatHeader)
	layer := io.NopCloser(stream)
	if format != blobship.FormatOCILayer {
		layer = blobship.OverlayDirToOCI(stream)
	}
	defer layer.Close()
	verify := func() error {
		// A tar ends at its end-of-archive marker; read the rest so gzip
		// checks its CRC and length trailer, and the body to its end so
		// the HTTP trailer arrives.
		if _, err := io.Copy(io.Discard, stream); err != nil {
			return fmt.Errorf("stream check: %w", err)
		}
		if _, err := io.Copy(io.Discard, counted); err != nil {
			return fmt.Errorf("stream check: %w", err)
		}
		if format != blobship.FormatOCILayer {
			return nil
		}
		want := resp.Trailer.Get(snapshotDigestTrailer)
		got := "sha256:" + hex.EncodeToString(hash.Sum(nil))
		if want != got {
			return fmt.Errorf("snapshot digest mismatch: source sent %q, received %s", want, got)
		}
		return nil
	}
	err = rs.store.ApplySnapshot(ctx, l.ChainID, parent, layer, verify)
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
	parent := ""
	if i > 0 {
		parent = plan.Layers[i-1].ChainID
	}
	if snaps, err := rs.store.Snapshots(r.Context()); err != nil || !snaps[req.ChainID] {
		http.Error(w, "snapshot "+req.ChainID+" not on this node", http.StatusConflict)
		return
	}

	oci := req.Format == blobship.FormatOCILayer
	if oci {
		w.Header().Set(snapshotFormatHeader, blobship.FormatOCILayer)
		w.Header().Set("Trailer", snapshotDigestTrailer)
	}
	w.Header().Set("Content-Type", "application/gzip")
	cw := &countWriter{w: w}
	gz, _ := gzip.NewWriterLevel(cw, gzip.BestSpeed)
	hash := sha256.New()
	if oci {
		err = rs.store.ExportSnapshot(r.Context(), req.ChainID, parent, io.MultiWriter(gz, hash))
	} else {
		// A pre-1.8.6 receiver: hand it the overlayfs directory format it
		// applies itself.
		pr, pw := io.Pipe()
		go func() { pw.CloseWithError(rs.store.ExportSnapshot(r.Context(), req.ChainID, parent, pw)) }()
		conv := blobship.OCIToOverlayDir(pr)
		_, err = io.Copy(gz, conv)
		conv.Close()
		pr.CloseWithError(io.ErrClosedPipe)
	}
	if err == nil {
		err = gz.Close()
	}
	if err == nil && oci {
		w.Header().Set(snapshotDigestTrailer, "sha256:"+hex.EncodeToString(hash.Sum(nil)))
	}
	rescueBytesTotal.Add(cw.n, rs.nodeID, "served")
	if err != nil {
		logging.Warnf("angryduck-worker[%s]: snapshot export %s of image=%s to %s failed after %d bytes: %v", rs.nodeID, req.ChainID, plan.Image, r.RemoteAddr, cw.n, err)
		panic(http.ErrAbortHandler) // truncate: the receiver's gzip check then fails
	}
	logging.Infof("angryduck-worker[%s]: exported snapshot %s (layer %d) of image=%s to %s (%d bytes compressed)", rs.nodeID, req.ChainID, i, plan.Image, r.RemoteAddr, cw.n)
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

// Busy reports whether any of names is being received right now.
func (rs *Rescue) Busy(names []string) bool {
	rs.mu.Lock()
	defer rs.mu.Unlock()
	for _, n := range names {
		if _, ok := rs.inflight[n]; ok {
			return true
		}
	}
	return false
}

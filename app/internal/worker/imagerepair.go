package worker

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"regexp"
	"sort"
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
	brokenImageGauge = metrics.NewGaugeVec(
		"angryduck_worker_broken_image",
		"1 while this image is registered on the node but can't start a container (a metadata blob, or a layer that is neither unpacked nor here, is gone or unusable). kubelet calls such an image present and never pulls it. The series is removed once the image is repaired or no longer registered.",
		"node", "image",
	)
	imageRepairsTotal = metrics.NewCounterVec(
		"angryduck_worker_image_repairs_total",
		"Repairs of broken images, by result: repaired (the image can start containers again, nothing was removed), failed (some missing piece came from no peer and not from the registry either; retried at the next check).",
		"node", "result",
	)
	imageRepairSeconds = metrics.NewRecentGaugeVec(
		"angryduck_worker_image_repair_seconds",
		"How long a repair of the image took, by result: repaired, failed. An event, shown for METRICS_EVENT_TTL_S.",
		"node", "image", "result",
	)
	imageRepairedAt = metrics.NewRecentGaugeVec(
		"angryduck_worker_image_repair_timestamp_seconds",
		"When a repair of the image ended, as Unix time, by result. An event, shown for METRICS_EVENT_TTL_S.",
		"node", "image", "result",
	)
	imageRepairBytes = metrics.NewRecentGaugeVec(
		"angryduck_worker_image_repair_bytes",
		"Bytes a repair of the image fetched, by source: peer, registry. An event, shown for METRICS_EVENT_TTL_S.",
		"node", "image", "source",
	)
	repairBytesTotal = metrics.NewCounterVec(
		"angryduck_worker_repair_bytes_total",
		"Bytes the repair fetched, by source: peer (another node) or registry.",
		"node", "source",
	)
	tempImagesRemovedTotal = metrics.NewCounterVec(
		"angryduck_worker_temp_images_removed_total",
		"Temporary image names an earlier import left behind (a worker killed mid-import), removed by the repair.",
		"node",
	)
)

// tempImageHost is the registry host of every image name Angry Duck makes
// up for itself: rescue base images and the names imports unpack under.
// They are never reported, cleaned up by the image cleanup, or repaired.
const tempImageHost = "angryduck.local/"

// RepairStore is what the repair needs from containerd. ContainerdStore
// has it.
type RepairStore interface {
	blobship.Store
	ImageTargets(ctx context.Context) (map[string]string, error)
	CreateLease(ctx context.Context, id string, ttl time.Duration) error
	DeleteLease(ctx context.Context, id string) error
	HoldContent(ctx context.Context, lease, dgst string) (bool, error)
	HoldSnapshot(ctx context.Context, lease, key string) (bool, error)
	WriteBlob(ctx context.Context, lease, ref, dgst string, size int64, r io.Reader) (int64, error)
	DeleteContent(ctx context.Context, dgst string) error
}

// RepairRegistry is where the repair goes when no peer has a piece.
// registryclient.Client has it.
type RepairRegistry interface {
	OpenBlob(ctx context.Context, image, digest string) (io.ReadCloser, int64, error)
	OpenManifest(ctx context.Context, image, digest string) (io.ReadCloser, int64, error)
}

// ImageRepair finds images that are registered here but can't start a
// container, and fixes them in place: only the pieces that are gone are
// fetched, peers first, the registry second, and the image keeps its name
// throughout.
//
// Such an image is the worst kind of missing: kubelet sees it as present
// ("already present on machine"), never pulls it, and every container
// create fails; Angry Duck's controller would count the node as holding
// it, so while it is broken the worker leaves it out of its reports (see
// Broken).
//
// What counts as broken (see blobship.Health): a metadata blob (index,
// manifest, config) gone or unusable, or the top layer not unpacked while
// some layer has neither its snapshot nor its blob. Nothing is hashed to
// find corruption: a metadata blob is unusable when it doesn't parse or
// has the wrong size, and a layer blob is corrupted only when a real
// unpack failed on it (the reason in a wake from the controller).
//
// A repair:
//  1. holds what is still here under a lease of its own: the highest
//     unpacked snapshot, and the blob of every layer above it;
//  2. removes a blob known to be corrupted (containerd keeps an existing
//     blob when the same digest is written again);
//  3. fetches each missing piece by digest from a peer that holds it (the
//     controller's layer index says which), else from the registry;
//     containerd checks every digest as it writes, so a bad copy fails and
//     the next source is tried;
//  4. imports the image's metadata under its own name, which unpacks it
//     and only then republishes the name (pointing where it already did).
//
// When every source fails, the image stays as it is, reported broken, and
// the next check tries again. Checks run every interval, and right away
// on a wake (the controller saw a container fail to start on it).
type ImageRepair struct {
	store         RepairStore
	registry      RepairRegistry // nil: peers only
	platform      string
	nodeID        string
	interval      time.Duration
	token         string
	controllerURL string
	busy          func(names []string) bool
	onChange      func()
	peers         *http.Client
	ctl           *http.Client
	transfer      time.Duration // one fetch, start to commit

	mu sync.Mutex // one check or repair at a time

	// known holds, per target digest of an image found fine, what its
	// metadata walk found: metadata is immutable for a digest, so later
	// checks only need the listings. Only the Pass goroutine touches it.
	known map[string]knownImage

	stateMu sync.Mutex
	broken  map[string]string   // target digest -> image label (its main name)
	names   map[string][]string // target digest -> every name, for Broken

	wakeMu sync.Mutex
	queue  map[string]string // image -> reason
	wake   chan struct{}
}

// NewImageRepair builds the repair. busy reports images being pulled or
// received right now (never touched); onChange runs after it repaired
// something (the reporter's Kick). registry may be nil.
func NewImageRepair(store RepairStore, registry RepairRegistry, platform, nodeID string, interval time.Duration, busy func([]string) bool, onChange func()) *ImageRepair {
	for _, r := range []string{"repaired", "failed"} {
		imageRepairsTotal.Add(0, nodeID, r)
	}
	repairBytesTotal.Add(0, nodeID, "peer")
	repairBytesTotal.Add(0, nodeID, "registry")
	tempImagesRemovedTotal.Add(0, nodeID)
	if busy == nil {
		busy = func([]string) bool { return false }
	}
	return &ImageRepair{
		store: store, registry: registry, platform: platform, nodeID: nodeID, interval: interval,
		busy: busy, onChange: onChange, transfer: 30 * time.Minute,
		peers: &http.Client{Transport: &http.Transport{
			Proxy:                 nil, // peers are on the node network
			DialContext:           (&net.Dialer{Timeout: 5 * time.Second}).DialContext,
			ResponseHeaderTimeout: 30 * time.Second,
		}},
		ctl:    &http.Client{Timeout: 5 * time.Second},
		broken: map[string]string{}, names: map[string][]string{}, known: map[string]knownImage{},
		queue: map[string]string{}, wake: make(chan struct{}, 1),
	}
}

// UsePeers lets the repair fetch from other nodes: it asks the controller
// which nodes hold a digest and takes it from their spread endpoint. Both
// need the shared token.
func (r *ImageRepair) UsePeers(token, controllerURL string) {
	r.token, r.controllerURL = token, strings.TrimRight(controllerURL, "/")
}

// Run checks every interval until ctx is done, the first time shortly
// after startup, and handles wakes as they come.
func (r *ImageRepair) Run(ctx context.Context) {
	logging.Infof("angryduck-worker[%s]: image repair started: interval=%s platform=%s", r.nodeID, r.interval, r.platform)
	first := 30 * time.Second
	if r.interval < first {
		first = r.interval
	}
	t := time.NewTimer(first)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-r.wake:
			r.handleWakes(ctx)
		case <-t.C:
			r.handleWakes(ctx)
			r.Pass(ctx)
			t.Reset(r.interval)
		}
	}
}

// Wake asks for image to be checked and repaired now. reason is the error
// a container create got, if any: it may name a layer whose blob is
// corrupted.
func (r *ImageRepair) Wake(image, reason string) {
	r.wakeMu.Lock()
	r.queue[imageref.Normalize(image)] = reason
	r.wakeMu.Unlock()
	select {
	case r.wake <- struct{}{}:
	default:
	}
}

// Broken returns every name of every image known to be broken right now,
// for the reporter to leave out: a node must not be counted as holding an
// image it can't run.
func (r *ImageRepair) Broken() map[string]bool {
	r.stateMu.Lock()
	defer r.stateMu.Unlock()
	out := map[string]bool{}
	for d := range r.broken {
		for _, n := range r.names[d] {
			out[n] = true
		}
	}
	return out
}

// HandleRepair is POST /repair: the controller saw a container fail to
// start on an image here. It answers at once; the repair runs in the
// background.
func (r *ImageRepair) HandleRepair(w http.ResponseWriter, req *http.Request) {
	if req.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var o model.RepairOrder
	if err := json.NewDecoder(io.LimitReader(req.Body, 64<<10)).Decode(&o); err != nil || o.Image == "" {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}
	r.Wake(o.Image, o.Reason)
	w.WriteHeader(http.StatusAccepted)
}

// Register mounts /repair behind the shared token.
func (r *ImageRepair) Register(mux *http.ServeMux) {
	mux.HandleFunc("/repair", sharedtoken.Require(r.token, r.HandleRepair))
}

func (r *ImageRepair) handleWakes(ctx context.Context) {
	r.wakeMu.Lock()
	queue := r.queue
	r.queue = map[string]string{}
	r.wakeMu.Unlock()
	if len(queue) == 0 {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	targets, err := r.store.ImageTargets(ctx)
	if err != nil {
		logging.Warnf("angryduck-worker[%s]: image repair: listing images: %v", r.nodeID, err)
		return
	}
	byTarget := groupByTarget(targets)
	for image, reason := range queue {
		d, ok := targets[image]
		if !ok {
			continue // not registered: kubelet pulls it, nothing to repair
		}
		r.repair(ctx, d, byTarget[d], corruptLayers(reason))
	}
}

// Pass is one periodic check of every image.
func (r *ImageRepair) Pass(ctx context.Context) {
	r.mu.Lock()
	defer r.mu.Unlock()
	targets, err := r.store.ImageTargets(ctx)
	if err != nil {
		logging.Warnf("angryduck-worker[%s]: image repair: listing images: %v", r.nodeID, err)
		return
	}
	changed := r.removeStaleTemps(ctx, targets)

	byTarget := groupByTarget(targets)
	digests := make([]string, 0, len(byTarget))
	for d := range byTarget {
		digests = append(digests, d)
	}
	sort.Strings(digests)
	// One listing of each for the whole pass (cached, shared with the
	// rest of the worker): images checked before cost no call of their own.
	blobs, err := r.store.Digests(ctx)
	if err != nil {
		logging.Warnf("angryduck-worker[%s]: image repair: listing content: %v", r.nodeID, err)
		return
	}
	snaps, err := r.store.Snapshots(ctx)
	if err != nil {
		logging.Warnf("angryduck-worker[%s]: image repair: listing snapshots: %v", r.nodeID, err)
		return
	}
	seen := map[string]bool{}
	for _, d := range digests {
		if ctx.Err() != nil {
			return
		}
		seen[d] = true
		h, ok := r.known[d].check(blobs, snaps)
		if !ok {
			var walk blobship.MetaWalk
			h, walk, err = blobship.CheckWalk(ctx, r.store, byTarget[d][0], r.platform)
			if err == nil && !h.Broken() {
				r.known[d] = newKnownImage(walk)
			}
		}
		if err != nil || !h.Broken() {
			r.setFine(d) // fine, another platform's, or can't tell: leave it
			continue
		}
		delete(r.known, d)
		r.repair(ctx, d, byTarget[d], nil)
	}
	for d := range r.known {
		if !seen[d] {
			delete(r.known, d) // no longer registered
		}
	}
	r.stateMu.Lock()
	for d, label := range r.broken {
		if !seen[d] {
			delete(r.broken, d) // no longer registered
			delete(r.names, d)
			brokenImageGauge.Delete(r.nodeID, label)
		}
	}
	r.stateMu.Unlock()
	if changed && r.onChange != nil {
		r.onChange()
	}
}

// knownImage is what an image's metadata walk found: the metadata blobs
// it needs, and its layers. A zero knownImage knows nothing.
type knownImage struct {
	meta   []string
	layers []blobship.Layer
	valid  bool
}

func newKnownImage(w blobship.MetaWalk) knownImage {
	k := knownImage{layers: w.Layers, valid: true}
	for d := range w.Metadata {
		k.meta = append(k.meta, d)
	}
	return k
}

// check works out the image's health from the listings alone. ok is false
// when it can't (nothing known, or a metadata blob gone): walk again.
func (k knownImage) check(blobs, snaps map[string]bool) (blobship.Health, bool) {
	if !k.valid {
		return blobship.Health{}, false
	}
	for _, d := range k.meta {
		if !blobs[d] {
			return blobship.Health{}, false
		}
	}
	return blobship.CheckLayers(k.layers, snaps, blobs), true
}

// groupByTarget maps each target digest to its names, sorted, without
// Angry Duck's own temporary ones.
func groupByTarget(targets map[string]string) map[string][]string {
	out := map[string][]string{}
	for name, d := range targets {
		if strings.HasPrefix(name, tempImageHost) {
			continue
		}
		out[d] = append(out[d], name)
	}
	for _, names := range out {
		sort.Strings(names)
	}
	return out
}

// mainName is the name an image is labeled, imported and fetched under:
// a tag if it has one, else a digest reference; never a bare image ID.
func mainName(names []string) string {
	best := ""
	for _, n := range names {
		switch {
		case strings.HasPrefix(n, "sha256:"):
		case !strings.Contains(n, "@"):
			return n
		case best == "":
			best = n
		}
	}
	if best == "" && len(names) > 0 {
		best = names[0]
	}
	return best
}

func (r *ImageRepair) setBroken(digest string, names []string) {
	label := mainName(names)
	r.stateMu.Lock()
	if old, ok := r.broken[digest]; ok && old != label {
		brokenImageGauge.Delete(r.nodeID, old)
	}
	r.broken[digest], r.names[digest] = label, names
	r.stateMu.Unlock()
	brokenImageGauge.Set(1, r.nodeID, label)
}

func (r *ImageRepair) setFine(digest string) {
	r.stateMu.Lock()
	label, ok := r.broken[digest]
	delete(r.broken, digest)
	delete(r.names, digest)
	r.stateMu.Unlock()
	if ok {
		brokenImageGauge.Delete(r.nodeID, label)
	}
}

// repair confirms against fresh listings that the image is broken (or
// that a layer of it is known corrupted), and fixes it.
func (r *ImageRepair) repair(ctx context.Context, digest string, names []string, corrupt map[string]bool) {
	if len(names) == 0 {
		return
	}
	if inv, ok := r.store.(interface{ Invalidate() }); ok {
		inv.Invalidate()
	}
	name := mainName(names)
	h, err := blobship.Check(ctx, r.store, name, r.platform)
	if err != nil || (!h.Broken() && len(corrupt) == 0) {
		r.setFine(digest)
		return
	}
	if !h.Broken() && h.Unpacked {
		r.setFine(digest) // a corrupted blob of an unpacked layer is never read
		return
	}
	r.setBroken(digest, names)
	if r.busy(names) {
		logging.Infof("angryduck-worker[%s]: image repair: image=%s is being pulled or received; checking again later", r.nodeID, name)
		return
	}
	what := h.String()
	if len(corrupt) > 0 {
		what += fmt.Sprintf("; %d layer(s) failed to unpack", len(corrupt))
	}
	logging.Warnf("angryduck-worker[%s]: image repair: image=%s can't start containers (%s): repairing it in place", r.nodeID, name, what)
	start := time.Now()
	res, err := r.fix(ctx, name, corrupt)
	if err == nil {
		if inv, ok := r.store.(interface{ Invalidate() }); ok {
			inv.Invalidate()
		}
		if h, cerr := blobship.Check(ctx, r.store, name, r.platform); cerr == nil && h.Broken() {
			err = fmt.Errorf("still broken after the repair: %s", h)
		}
	}
	result := "repaired"
	if err != nil {
		result = "failed"
	}
	imageRepairSeconds.Set(time.Since(start).Seconds(), r.nodeID, name, result)
	imageRepairedAt.Set(float64(time.Now().Unix()), r.nodeID, name, result)
	if res.peerB > 0 {
		imageRepairBytes.Set(float64(res.peerB), r.nodeID, name, "peer")
	}
	if res.registry > 0 {
		imageRepairBytes.Set(float64(res.registry), r.nodeID, name, "registry")
	}
	if err != nil {
		imageRepairsTotal.Inc(r.nodeID, "failed")
		logging.Warnf("angryduck-worker[%s]: image repair: image=%s not repaired: %v (it stays as it is; trying again at the next check)", r.nodeID, name, err)
		return
	}
	imageRepairsTotal.Inc(r.nodeID, "repaired")
	r.setFine(digest)
	logging.Infof("angryduck-worker[%s]: image repair: image=%s repaired in %s: %s", r.nodeID, name, time.Since(start).Round(time.Millisecond), res)
	if r.onChange != nil {
		r.onChange()
	}
}

// repairResult is what one repair fetched, for the log.
type repairResult struct {
	meta, blobs     int
	peerB, registry int64
}

func (res repairResult) String() string {
	return fmt.Sprintf("%d metadata blob(s) and %d layer blob(s) fetched, %d bytes from peers, %d from the registry", res.meta, res.blobs, res.peerB, res.registry)
}

// fix fetches what image is missing and re-imports its metadata (see
// ImageRepair).
func (r *ImageRepair) fix(ctx context.Context, image string, corrupt map[string]bool) (repairResult, error) {
	var res repairResult
	mediaType, topDigest, err := r.store.Resolve(ctx, image)
	if err != nil {
		return res, err
	}
	lease := "angryduck-repair-" + randomSuffix()
	if err := r.store.CreateLease(ctx, lease, time.Hour); err != nil {
		return res, fmt.Errorf("creating lease: %w", err)
	}
	defer func() {
		dctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 30*time.Second)
		if err := r.store.DeleteLease(dctx, lease); err != nil {
			logging.Debugf("angryduck-worker[%s]: image repair: deleting lease %s: %v (it expires on its own)", r.nodeID, lease, err)
		}
		cancel()
	}()

	// Metadata: read what is here, fetch what is gone, and replace what is
	// here but unusable. Each round fixes one blob; there are at most three.
	local := map[string]bool{}
	fetched := map[string]bool{}
	read := func(d blobship.Descriptor) ([]byte, error) {
		b, err := r.store.ReadBlob(ctx, d.Digest, maxRepairMetadata)
		if err == nil {
			local[d.Digest] = true
			return b, nil
		}
		if !blobship.IsNotFound(err) {
			return nil, err
		}
		b, from, err := r.fetchMeta(ctx, image, d)
		if err != nil {
			return nil, err
		}
		fetched[d.Digest] = true
		res.meta++
		if from == "peer" {
			res.peerB += int64(len(b))
		} else {
			res.registry += int64(len(b))
		}
		return b, nil
	}
	top := blobship.Descriptor{MediaType: mediaType, Digest: topDigest}
	var walk blobship.MetaWalk
	for round := 0; ; round++ {
		walk, err = blobship.WalkMetadata(top, r.platform, read)
		var mp *blobship.MetaProblem
		if round < 4 && errors.As(err, &mp) && mp.Corrupt && local[mp.Blob.Digest] {
			logging.Warnf("angryduck-worker[%s]: image repair: image=%s: removing unusable %s %s to fetch it again", r.nodeID, image, mp.Kind, mp.Blob.Digest)
			if err := r.store.DeleteContent(ctx, mp.Blob.Digest); err != nil {
				return res, fmt.Errorf("removing unusable %s %s: %w", mp.Kind, mp.Blob.Digest, err)
			}
			delete(local, mp.Blob.Digest)
			continue
		}
		break
	}
	if err != nil {
		return res, err
	}
	top.Size = int64(len(walk.Metadata[top.Digest]))

	// Layers: hold what is here, find what isn't.
	var missing []blobship.Layer
	if layers := walk.Layers; len(layers) > 0 {
		unpacked := -1
		for i := len(layers) - 1; i >= 0; i-- {
			ok, err := r.store.HoldSnapshot(ctx, lease, layers[i].ChainID)
			if err != nil {
				return res, fmt.Errorf("holding snapshot of layer %d: %w", i, err)
			}
			if ok {
				unpacked = i
				break
			}
		}
		for i := unpacked + 1; i < len(layers); i++ {
			l := layers[i]
			if corrupt[l.DiffID] {
				logging.Warnf("angryduck-worker[%s]: image repair: image=%s: removing layer blob %s, which failed to unpack, to fetch it again", r.nodeID, image, l.Blob.Digest)
				if err := r.store.DeleteContent(ctx, l.Blob.Digest); err != nil {
					return res, fmt.Errorf("removing corrupted layer blob %s: %w", l.Blob.Digest, err)
				}
				missing = append(missing, l)
				continue
			}
			ok, err := r.store.HoldContent(ctx, lease, l.Blob.Digest)
			if err != nil {
				return res, fmt.Errorf("holding blob of layer %d: %w", i, err)
			}
			if !ok {
				missing = append(missing, l)
			}
		}
		if unpacked == len(layers)-1 && len(fetched) == 0 {
			return res, nil // unpacked meanwhile: nothing to do
		}
	}
	for _, l := range missing {
		n, from, err := r.fetchBlob(ctx, lease, image, l.Blob)
		if err != nil {
			return res, err
		}
		res.blobs++
		if from == "peer" {
			res.peerB += n
		} else {
			res.registry += n
		}
	}

	// Import the metadata under the image's own name: containerd labels
	// the blobs as the image's, unpacks it, and only then points the name
	// at it again (see ContainerdStore.Import).
	pr, pw := io.Pipe()
	go func() {
		pw.CloseWithError(func() error {
			aw, err := blobship.NewArchiveWriter(pw, top, image)
			if err != nil {
				return err
			}
			for _, d := range sortedKeys(walk.Metadata) {
				if err := aw.AddBytes(d, walk.Metadata[d]); err != nil {
					return err
				}
			}
			return aw.Close()
		}())
	}()
	err = r.store.Import(ctx, pr, r.platform)
	pr.CloseWithError(errImportEnded)
	if err != nil {
		for diffID := range corruptLayers(err.Error()) {
			if corrupt[diffID] {
				// The replacement didn't take: containerd adopts the file it
				// already has for a digest, and keeps that file while any
				// other containerd namespace on this node references it.
				return res, fmt.Errorf("layer %s still fails to unpack after its blob was replaced; another containerd namespace on this node probably shares the corrupted file (remove the image there, or the blob by hand): %w", diffID, err)
			}
		}
		return res, fmt.Errorf("re-importing: %w", err)
	}
	if _, got, err := r.store.Resolve(ctx, image); err != nil || got != topDigest {
		return res, fmt.Errorf("after the repair, %s resolves to %q (err %v), want %s", image, got, err, topDigest)
	}
	return res, nil
}

// maxRepairMetadata caps a metadata blob read into memory.
const maxRepairMetadata = 4 << 20

// fetchMeta gets one metadata blob, peers first, registry second. A
// source's copy counts only if it matches its digest: a few KB, and the
// next source can be tried when one is bad.
func (r *ImageRepair) fetchMeta(ctx context.Context, image string, d blobship.Descriptor) ([]byte, string, error) {
	var errs []string
	readAll := func(body io.ReadCloser) ([]byte, error) {
		defer body.Close()
		b, err := io.ReadAll(io.LimitReader(body, maxRepairMetadata+1))
		if err != nil {
			return nil, err
		}
		if len(b) > maxRepairMetadata {
			return nil, errors.New("too large")
		}
		if sum := sha256.Sum256(b); "sha256:"+hex.EncodeToString(sum[:]) != d.Digest {
			return nil, errors.New("content doesn't match its digest")
		}
		return b, nil
	}
	for _, h := range r.holders(ctx, d.Digest) {
		body, _, err := r.openPeer(ctx, h.Address, d.Digest)
		if err == nil {
			var b []byte
			if b, err = readAll(body); err == nil {
				repairBytesTotal.Add(int64(len(b)), r.nodeID, "peer")
				return b, "peer", nil
			}
		}
		errs = append(errs, h.NodeID+": "+err.Error())
	}
	if r.registry != nil && !strings.HasPrefix(image, "sha256:") {
		open := r.registry.OpenBlob
		if d.MediaType != "" && d.MediaType != "application/vnd.oci.image.config.v1+json" && d.MediaType != "application/vnd.docker.container.image.v1+json" {
			open = r.registry.OpenManifest
		}
		body, _, err := open(ctx, image, d.Digest)
		if err == nil {
			var b []byte
			if b, err = readAll(body); err == nil {
				repairBytesTotal.Add(int64(len(b)), r.nodeID, "registry")
				return b, "registry", nil
			}
		}
		errs = append(errs, "registry: "+err.Error())
	}
	if len(errs) == 0 {
		errs = append(errs, "no peer holds it and no registry to ask")
	}
	return nil, "", fmt.Errorf("fetching %s: %s", d.Digest, strings.Join(errs, "; "))
}

// fetchBlob writes one layer blob into the content store under lease,
// peers first, registry second. containerd checks the digest as it
// writes, so a bad copy fails and the next source is tried.
func (r *ImageRepair) fetchBlob(ctx context.Context, lease, image string, d blobship.Descriptor) (int64, string, error) {
	var errs []string
	ref := "angryduck-repair-" + strings.TrimPrefix(d.Digest, "sha256:")
	write := func(body io.ReadCloser, length int64) (int64, error) {
		defer body.Close()
		if length >= 0 && length != d.Size {
			return 0, fmt.Errorf("source announced %d bytes, want %d", length, d.Size)
		}
		return r.store.WriteBlob(ctx, lease, ref, d.Digest, d.Size, body)
	}
	try := func(open func(context.Context) (io.ReadCloser, int64, error)) (int64, error) {
		tctx, cancel := context.WithTimeout(ctx, r.transfer)
		defer cancel()
		body, length, err := open(tctx)
		if err != nil {
			return 0, err
		}
		return write(body, length)
	}
	for _, h := range r.holders(ctx, d.Digest) {
		addr := h.Address
		n, err := try(func(c context.Context) (io.ReadCloser, int64, error) { return r.openPeer(c, addr, d.Digest) })
		if err == nil {
			repairBytesTotal.Add(n, r.nodeID, "peer")
			return n, "peer", nil
		}
		errs = append(errs, h.NodeID+": "+err.Error())
	}
	if r.registry != nil && !strings.HasPrefix(image, "sha256:") {
		n, err := try(func(c context.Context) (io.ReadCloser, int64, error) { return r.registry.OpenBlob(c, image, d.Digest) })
		if err == nil {
			repairBytesTotal.Add(n, r.nodeID, "registry")
			return n, "registry", nil
		}
		errs = append(errs, "registry: "+err.Error())
	}
	if len(errs) == 0 {
		errs = append(errs, "no peer holds it and no registry to ask")
	}
	return 0, "", fmt.Errorf("fetching layer blob %s: %s", d.Digest, strings.Join(errs, "; "))
}

// holders asks the controller which other nodes hold digest.
func (r *ImageRepair) holders(ctx context.Context, digest string) []model.Holder {
	if r.token == "" || r.controllerURL == "" {
		return nil
	}
	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	u := r.controllerURL + "/layers/holders?digest=" + url.QueryEscape(digest) + "&exclude=" + url.QueryEscape(r.nodeID)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return nil
	}
	sharedtoken.Set(req, r.token)
	resp, err := r.ctl.Do(req)
	if err != nil {
		logging.Debugf("angryduck-worker[%s]: image repair: asking the controller for holders of %s: %v", r.nodeID, digest, err)
		return nil
	}
	defer resp.Body.Close()
	var list []model.Holder
	if resp.StatusCode == http.StatusOK {
		_ = json.NewDecoder(io.LimitReader(resp.Body, 64<<10)).Decode(&list)
	}
	return list
}

// openPeer streams one blob from a peer's spread endpoint, which serves
// any blob it holds by digest.
func (r *ImageRepair) openPeer(ctx context.Context, addr, dgst string) (io.ReadCloser, int64, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://"+addr+"/spread/content/"+dgst, nil)
	if err != nil {
		return nil, 0, err
	}
	sharedtoken.Set(req, r.token)
	resp, err := r.peers.Do(req)
	if err != nil {
		return nil, 0, err
	}
	if resp.StatusCode != http.StatusOK {
		msg := readError(resp)
		resp.Body.Close()
		return nil, 0, errors.New(msg)
	}
	return resp.Body, resp.ContentLength, nil
}

// extractFailure matches containerd's unpack error for one layer:
// "failed to extract layer sha256:<diff_id>: <cause>".
var extractFailure = regexp.MustCompile(`failed to extract layer (sha256:[0-9a-f]{64}): (.*)`)

// corruptLayers reads a container create error and returns the diff_id of
// a layer whose blob is there but couldn't be unpacked: corrupted. A blob
// that is gone ("not found") is no corruption (the check finds it), and
// failures that say nothing about the blob (a full disk, a timeout) are
// left alone, so a good blob is never thrown away.
func corruptLayers(reason string) map[string]bool {
	m := extractFailure.FindStringSubmatch(reason)
	if m == nil {
		return nil
	}
	cause := strings.ToLower(m[2])
	for _, notTheBlob := range []string{"not found", "no space left", "deadline exceeded", "context canceled", "too many open files", "permission denied", "read-only file system"} {
		if strings.Contains(cause, notTheBlob) {
			return nil
		}
	}
	return map[string]bool{m[1]: true}
}

// removeStaleTemps removes temporary import names no import in this
// process is using: a previous worker was killed mid-import.
func (r *ImageRepair) removeStaleTemps(ctx context.Context, targets map[string]string) bool {
	inflight, ok := r.store.(interface{ ImportInFlight(name string) bool })
	if !ok {
		return false
	}
	var stale []string
	for n := range targets {
		if strings.HasPrefix(n, importTempPrefix) && !inflight.ImportInFlight(n) {
			stale = append(stale, n)
		}
	}
	if len(stale) == 0 {
		return false
	}
	sort.Strings(stale)
	if err := r.store.DeleteImages(ctx, stale...); err != nil {
		logging.Warnf("angryduck-worker[%s]: image repair: removing leftover temporary images %v: %v", r.nodeID, stale, err)
		return false
	}
	tempImagesRemovedTotal.Add(int64(len(stale)), r.nodeID)
	logging.Infof("angryduck-worker[%s]: image repair: removed %d leftover temporary image name(s) of an interrupted import", r.nodeID, len(stale))
	return true
}

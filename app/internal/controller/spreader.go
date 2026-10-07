package controller

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"sort"
	"strings"
	"sync"
	"time"

	"angryduck/internal/imageref"
	"angryduck/internal/layerindex"
	"angryduck/internal/logging"
	"angryduck/internal/metrics"
	"angryduck/internal/model"
	"angryduck/internal/registryclient"
	"angryduck/internal/sharedtoken"
)

var (
	spreadNodes = metrics.NewGaugeVec(
		"angryduck_controller_spread_nodes",
		"Nodes per image being spread, by state: done, finalizing (registering the image), transferring, waiting (needs blobs, nothing to fetch right now), skipped (excluded or disk too full). Only while the spread runs.",
		"image", "state",
	)
	spreadLayers = metrics.NewGaugeVec(
		"angryduck_controller_spread_layers",
		"Layers per image being spread, by state: held (some node has the blob, peers can fetch it), pulling (only coming from the registry right now), missing (nobody has it, nothing fetching it). Only while the spread runs.",
		"image", "state",
	)
	spreadNodeMissingBytes = metrics.NewGaugeVec(
		"angryduck_controller_spread_node_missing_bytes",
		"Bytes of the image each node still lacks. Only nodes not done, only while the spread runs.",
		"image", "node",
	)
	spreadNodeMissingLayers = metrics.NewGaugeVec(
		"angryduck_controller_spread_node_missing_layers",
		"Layers of the image each node still lacks. Only nodes not done, only while the spread runs.",
		"image", "node",
	)
	spreadTransfer = metrics.NewGaugeVec(
		"angryduck_controller_spread_transfer_bytes",
		"Transfers running right now, one series each: the blob's size, by image, receiving node, path (registry, peer), source (registry or the sending node) and blob (first 12 hex digits of its digest).",
		"image", "node", "path", "source", "blob",
	)
	spreadTransfersTotal = metrics.NewCounterVec(
		"angryduck_controller_spread_transfers_total",
		"Spread transfers that ended, by path (registry, peer, image) and result: ok, failed, cancelled (lost a race), present (the node already had it), rejected (the node was busy or unreachable).",
		"path", "result",
	)
	spreadBytesTotal = metrics.NewCounterVec(
		"angryduck_controller_spread_bytes_total",
		"Bytes nodes reported receiving for spreads, by path. path=registry is everything the registry served for spreads.",
		"path",
	)
	spreadFinalizesTotal = metrics.NewCounterVec(
		"angryduck_controller_spread_finalizes_total",
		"Nodes told to register a spread image once they held every layer, by result.",
		"result",
	)
	spreadsTotal = metrics.NewCounterVec(
		"angryduck_controller_spreads_total",
		"Finished spreads: complete (every eligible node has the image), superseded (a newer tag of the same repo was pushed) or expired (the window ran out).",
		"result",
	)
	spreadLastSeconds = metrics.NewGaugeVec(
		"angryduck_controller_spread_last_duration_seconds",
		"How long the last complete spread took, from the preheat to the last node registering the image.",
	)
)

// ImageResolver returns what an image is made of, from its registry.
type ImageResolver interface {
	Resolve(ctx context.Context, image, platform string) (registryclient.Resolved, error)
}

// SpreaderConfig holds the spreader's tunables.
type SpreaderConfig struct {
	Interval        time.Duration // safety tick; transfers ending drive it
	MaxRegistry     int           // registry pulls in flight across the cluster
	RacePerBlob     int           // registry pulls of one blob at once (a slow puller can be raced)
	MaxPeer         int           // peer transfers in flight across the cluster
	PerSource       int           // peer transfers one node serves at once
	MaxUtilization  float64       // nodes fuller than this get nothing new (0-1)
	Exclude         []string      // node name substrings that never receive
	RetryAfter      time.Duration // after a failure; doubles per failure
	BackoffMax      time.Duration
	TransferTimeout time.Duration // an in-flight transfer nobody reported on is freed after this
	ResolveTimeout  time.Duration
	Window          time.Duration // 0: until done or superseded
	Platform        string
}

// Spreader spreads a pushed image to every eligible node blob by blob.
//
// Each node has two slots: one blob from the registry and one from a peer,
// at a time. As soon as any node holds a blob, every node lacking it can
// fetch it from that node; the registry is only used for blobs no node
// has yet, by at most MaxRegistry nodes at once, so it serves each blob
// about once. No node waits on another's pull: a node whose registry pull
// is slow can be raced by an idle one (RacePerBlob), and a node pulling a
// blob from the registry still gets it pushed from a peer as soon as a
// peer has it. Whichever path finishes first wins; the worker cancels the
// other on the spot.
//
// Peer transfers go to the rarest layer first (fewest holders), so new
// sources appear where they are scarcest. Nodes with a pod waiting for the
// image come first, then the nodes that lack the fewest bytes.
//
// When a node holds every layer (its blob or its unpacked snapshot), it
// gets the image's metadata, a few KB read from the registry once, and
// registers the image. A layer the registry keeps failing to serve, held
// elsewhere only as a snapshot, falls back to the image transfer rescue
// uses, from a node that has the whole image.
type Spreader struct {
	registry *Registry
	resolver ImageResolver
	token    string
	cfg      SpreaderConfig
	orders   *http.Client
	finals   *http.Client
	waiting  func() map[string]map[string]bool

	mu        sync.Mutex
	runCtx    context.Context  // Run's: orders outlive the request that started a spread
	jobs      map[string]*sjob // by image
	byID      map[string]*sjob
	published bool
	wakeup    waker
	wg        sync.WaitGroup // order goroutines, for tests

	// Scratch reused across ticks (tick runs under mu).
	present, blob []bool
}

type sjob struct {
	id, image string
	res       registryclient.Resolved
	started   time.Time
	total     int64
	idx       map[string]int // layer digest -> index
	nodes     map[string]*snode
	got       []map[string]bool // per layer: nodes that reported it (inventories lag)
	regFails  []int             // per layer: failed registry pulls
}

type snode struct {
	have       []bool // per layer: reported received through us
	missLayers int    // as of the last tick, for /status
	missBytes  int64
	reg, peer  *sxfer
	finalizing bool
	done       bool
	touched    bool // was ordered something: has blobs to release if the job ends early
	fails      int
	next       time.Time // no new orders before this
}

type sxfer struct {
	layer int // -1: a whole-image transfer (fallback)
	path  string
	from  string // registry, or the sending node
	at    time.Time
}

// NewSpreader builds a spreader.
func NewSpreader(registry *Registry, resolver ImageResolver, token string, cfg SpreaderConfig) *Spreader {
	if cfg.MaxRegistry < 1 {
		cfg.MaxRegistry = 1
	}
	if cfg.RacePerBlob < 1 {
		cfg.RacePerBlob = 1
	}
	if cfg.MaxPeer < 1 {
		cfg.MaxPeer = 1
	}
	if cfg.PerSource < 1 {
		cfg.PerSource = 1
	}
	if cfg.BackoffMax < cfg.RetryAfter {
		cfg.BackoffMax = cfg.RetryAfter
	}
	if cfg.Platform == "" {
		cfg.Platform = "linux/amd64"
	}
	dial := (&net.Dialer{Timeout: 5 * time.Second}).DialContext
	return &Spreader{
		registry: registry, resolver: resolver, token: token, cfg: cfg,
		orders: &http.Client{Timeout: 10 * time.Second, Transport: &http.Transport{Proxy: nil, DialContext: dial}},
		finals: &http.Client{Timeout: 16 * time.Minute, Transport: &http.Transport{Proxy: nil, DialContext: dial}},
		jobs:   map[string]*sjob{},
		byID:   map[string]*sjob{},
		wakeup: newWaker(),
	}
}

// SetWaiting tells the spreader where pods wait for images.
func (s *Spreader) SetWaiting(f func() map[string]map[string]bool) { s.waiting = f }

// Start begins spreading image and returns the nodes it ordered to pull
// from the registry right away. An error means the image's layers can't be
// read from its registry; the caller falls back to whole-image seeding. A
// spread of another tag of the same repo is superseded.
func (s *Spreader) Start(ctx context.Context, image string) ([]string, error) {
	rctx, cancel := context.WithTimeout(ctx, s.cfg.ResolveTimeout)
	res, err := s.resolver.Resolve(rctx, image, s.cfg.Platform)
	cancel()
	if err != nil {
		return nil, err
	}
	var drops []dropOrder
	s.mu.Lock()
	if job, ok := s.jobs[image]; ok {
		job.started = time.Now()
		s.mu.Unlock()
		return nil, nil
	}
	repo := imageref.Repo(image)
	for other, job := range s.jobs {
		if repo != "" && imageref.Repo(other) == repo {
			logging.Infof("angryduck-controller: spread: image=%s superseded by image=%s", other, image)
			spreadsTotal.Inc("superseded")
			drops = append(drops, s.endLocked(job)...)
		}
	}
	job := &sjob{
		id: newJobID(), image: image, res: res, started: time.Now(),
		idx: make(map[string]int, len(res.Layers)), nodes: map[string]*snode{},
		got: make([]map[string]bool, len(res.Layers)), regFails: make([]int, len(res.Layers)),
	}
	for i, l := range res.Layers {
		job.idx[l.Digest] = i
		job.total += l.Size
	}
	s.jobs[image], s.byID[job.id] = job, job
	s.mu.Unlock()
	s.sendDrops(drops)
	logging.Infof("angryduck-controller: spread: image=%s job=%s: %d layer(s), %d bytes, blob by blob to every eligible node",
		image, job.id, len(res.Layers), job.total)

	s.tick(s.baseCtx())
	s.mu.Lock()
	defer s.mu.Unlock()
	var ordered []string
	for node, n := range job.nodes {
		if n.reg != nil {
			ordered = append(ordered, node)
		}
	}
	sort.Strings(ordered)
	return ordered, nil
}

type registryLayer = layerindex.Layer

// baseCtx is the context orders run under: Run's, so a spread started by
// a webhook request isn't cut off when that request ends.
func (s *Spreader) baseCtx() context.Context {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.runCtx != nil {
		return s.runCtx
	}
	return context.Background()
}

func newJobID() string {
	b := make([]byte, 8)
	_, _ = rand.Read(b)
	return "s" + hex.EncodeToString(b)
}

// Run blocks, ticking until ctx is done. Transfers ending wake it early.
func (s *Spreader) Run(ctx context.Context) {
	s.mu.Lock()
	s.runCtx = ctx
	s.mu.Unlock()
	t := time.NewTicker(s.cfg.Interval)
	defer t.Stop()
	logging.Infof("angryduck-controller: spreader started: max_registry=%d race_per_blob=%d max_peer=%d per_source=%d max_utilization=%.2f transfer_timeout=%s window=%s exclude=%v",
		s.cfg.MaxRegistry, s.cfg.RacePerBlob, s.cfg.MaxPeer, s.cfg.PerSource, s.cfg.MaxUtilization, s.cfg.TransferTimeout, s.cfg.Window, s.cfg.Exclude)
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			s.tick(ctx)
		case <-s.wakeup:
			if !s.wakeup.settle(ctx) {
				return
			}
			s.tick(ctx)
		}
	}
}

func (s *Spreader) excluded(node string) bool {
	for _, sub := range s.cfg.Exclude {
		if sub != "" && strings.Contains(node, sub) {
			return true
		}
	}
	return false
}

// order is one HTTP call a tick decided on, sent after the lock is
// released.
type order struct {
	job    *sjob
	node   string
	addr   string
	fetch  *model.SpreadFetch // a blob
	final  bool               // a finalize
	image  []model.RescueSource
	xfer   *sxfer
	layer  int
	digest string
}

type dropOrder struct{ addr, job string }

func (s *Spreader) tick(ctx context.Context) {
	s.mu.Lock()
	if len(s.jobs) == 0 {
		if s.published {
			s.resetMetricsLocked()
		}
		s.mu.Unlock()
		return
	}
	s.mu.Unlock()

	fresh := s.registry.FreshWorkers() // least-full first
	var waiting map[string]map[string]bool
	if s.waiting != nil {
		waiting = s.waiting()
	}
	byNode := make(map[string]*workerEntry, len(fresh))
	ids := make([]string, len(fresh))
	for i, w := range fresh {
		byNode[w.NodeID] = w
		ids[i] = w.NodeID
	}
	now := time.Now()

	s.mu.Lock()
	jobs := make([]*sjob, 0, len(s.jobs))
	for _, j := range s.jobs {
		jobs = append(jobs, j)
	}
	sort.Slice(jobs, func(i, k int) bool { return jobs[i].started.Before(jobs[k].started) })

	// Load across every job, recomputed: nothing to drift when a report
	// is lost.
	var regBusy, peerBusy int
	serving := map[string]int{}
	for _, j := range jobs {
		for _, n := range j.nodes {
			if n.reg != nil {
				regBusy++
			}
			if n.peer != nil {
				peerBusy++
				if n.peer.from != "" {
					serving[n.peer.from]++
				}
			}
		}
	}

	var orders []order
	var drops []dropOrder
	s.resetMetricsLocked()
	for _, j := range jobs {
		o, d := s.planLocked(j, fresh, byNode, ids, waiting[j.image], now, &regBusy, &peerBusy, serving)
		orders = append(orders, o...)
		drops = append(drops, d...)
	}
	s.published = len(s.jobs) > 0
	s.mu.Unlock()

	s.sendDrops(drops)
	for _, o := range orders {
		s.wg.Add(1)
		go s.send(ctx, o)
	}
}

// planLocked updates one job from the latest inventories and decides its
// next orders. Caller holds s.mu.
func (s *Spreader) planLocked(j *sjob, fresh []*workerEntry, byNode map[string]*workerEntry, ids []string, waiting map[string]bool,
	now time.Time, regBusy, peerBusy *int, serving map[string]int) ([]order, []dropOrder) {
	layers := j.res.Layers
	nl := len(layers)
	index := s.registry.Layers()

	// Who holds each blob: any fresh node, excluded or full ones included.
	holders := make([][]string, nl)
	for i, l := range layers {
		hs := index.Holders(l.Digest, ids)
		for n := range j.got[i] {
			if _, ok := byNode[n]; ok && !contains(hs, n) {
				hs = append(hs, n)
			}
		}
		holders[i] = hs
	}

	type need struct {
		w       *workerEntry
		n       *snode
		missing []int
		bytes   int64
	}
	var needs []need
	var done, skipped, excluded, finalizing int
	for _, w := range fresh {
		if s.excluded(w.NodeID) {
			excluded++
			continue
		}
		n := j.nodes[w.NodeID]
		if n == nil {
			n = &snode{have: make([]bool, nl)}
			j.nodes[w.NodeID] = n
		}
		s.expireLocked(j, w.NodeID, n, now)
		if n.done || w.HasImage(j.image) {
			n.done = true
			done++
			continue
		}
		s.present, s.blob, _ = index.Present(w.NodeID, layers, s.present, s.blob)
		var missing []int
		var bytes int64
		for i := range layers {
			if n.have[i] || s.present[i] {
				continue
			}
			missing = append(missing, i)
			bytes += layers[i].Size
		}
		n.missLayers, n.missBytes = len(missing), bytes
		spreadNodeMissingBytes.Set(float64(bytes), j.image, w.NodeID)
		spreadNodeMissingLayers.Set(float64(len(missing)), j.image, w.NodeID)
		if n.finalizing {
			finalizing++
			continue
		}
		if w.Utilization > s.cfg.MaxUtilization && n.reg == nil && n.peer == nil {
			skipped++
			continue
		}
		needs = append(needs, need{w: w, n: n, missing: missing, bytes: bytes})
	}

	// Finished?
	expired := s.cfg.Window > 0 && now.Sub(j.started) > s.cfg.Window
	busy := false
	for _, n := range j.nodes {
		if n.reg != nil || n.peer != nil || n.finalizing {
			busy = true
		}
	}
	if len(needs) == 0 && finalizing == 0 && skipped == 0 && !busy && done > 0 {
		took := now.Sub(j.started)
		logging.Infof("angryduck-controller: spread: image=%s is on every eligible node (%d) after %s", j.image, done, took.Round(time.Second))
		spreadsTotal.Inc("complete")
		spreadLastSeconds.Set(took.Seconds())
		return nil, s.endLocked(j)
	}
	if expired && !busy {
		logging.Warnf("angryduck-controller: spread: window for image=%s ran out with %d node(s) still missing it", j.image, len(needs)+skipped)
		spreadsTotal.Inc("expired")
		return nil, s.endLocked(j)
	}

	// Serving order: a waiting pod first, then fewest missing bytes.
	sort.SliceStable(needs, func(a, b int) bool {
		wa, wb := waiting[needs[a].w.NodeID], waiting[needs[b].w.NodeID]
		if wa != wb {
			return wa
		}
		return needs[a].bytes < needs[b].bytes
	})

	// A node that has the whole image can serve a layer the registry keeps
	// failing on, even if it holds that layer only as a snapshot.
	fullHolder := false
	for _, w := range fresh {
		if w.HasImage(j.image) {
			fullHolder = true
			break
		}
	}

	// Registry pulls per layer right now, and the oldest one's start.
	regPulls := make([]int, nl)
	regSince := make([]time.Time, nl)
	for _, n := range j.nodes {
		if n.reg != nil && n.reg.layer >= 0 {
			i := n.reg.layer
			regPulls[i]++
			if regSince[i].IsZero() || n.reg.at.Before(regSince[i]) {
				regSince[i] = n.reg.at
			}
		}
	}

	var orders []order
	transferring, waitingNodes := 0, 0
	for _, nd := range needs {
		n, w := nd.n, nd.w
		if now.Before(n.next) || expired {
			if n.reg != nil || n.peer != nil {
				transferring++
			} else {
				waitingNodes++
			}
			continue
		}
		if len(nd.missing) == 0 {
			if n.reg == nil && n.peer == nil {
				n.finalizing = true
				orders = append(orders, order{job: j, node: w.NodeID, addr: w.Address, final: true})
				finalizing++
			} else {
				transferring++ // the slower path of a race, about to be cancelled
			}
			continue
		}
		// East-west: rarest layer first, then largest.
		if n.peer == nil && *peerBusy < s.cfg.MaxPeer {
			best, src := -1, ""
			for _, i := range nd.missing {
				h := pickSource(holders[i], w.NodeID, serving, s.cfg.PerSource)
				if h == "" {
					continue
				}
				if best < 0 || len(holders[i]) < len(holders[best]) ||
					len(holders[i]) == len(holders[best]) && layers[i].Size > layers[best].Size {
					best, src = i, h
				}
			}
			if best >= 0 {
				x := &sxfer{layer: best, path: model.SpreadPathPeer, from: src, at: now}
				n.peer, n.touched = x, true
				*peerBusy++
				serving[src]++
				orders = append(orders, order{job: j, node: w.NodeID, addr: w.Address, xfer: x, layer: best, digest: layers[best].Digest,
					fetch: &model.SpreadFetch{Job: j.id, Digest: layers[best].Digest, Size: layers[best].Size, Path: model.SpreadPathPeer,
						Peer: &model.Holder{NodeID: src, Address: byNode[src].Address}}})
			}
		}
		// North-south: only blobs no node holds. Unstarted ones first,
		// largest first; otherwise race the pull that has run longest.
		if n.reg == nil && *regBusy < s.cfg.MaxRegistry {
			best := -1
			for _, i := range nd.missing {
				if len(holders[i]) > 0 || (fullHolder && j.regFails[i] >= 3) || regPulls[i] >= s.cfg.RacePerBlob {
					continue
				}
				if best < 0 || betterPull(i, best, regPulls, regSince, layers) {
					best = i
				}
			}
			if best >= 0 {
				x := &sxfer{layer: best, path: model.SpreadPathRegistry, from: model.SpreadPathRegistry, at: now}
				n.reg, n.touched = x, true
				*regBusy++
				regPulls[best]++
				if regSince[best].IsZero() {
					regSince[best] = now
				}
				orders = append(orders, order{job: j, node: w.NodeID, addr: w.Address, xfer: x, layer: best, digest: layers[best].Digest,
					fetch: &model.SpreadFetch{Job: j.id, Image: j.image, Digest: layers[best].Digest, Size: layers[best].Size, Path: model.SpreadPathRegistry}})
			}
		}
		// Last resort: a layer nobody has the blob of and the registry
		// keeps failing on. Get the whole image from a node that has it
		// (blobs or snapshots, as rescue does).
		if fullHolder && n.peer == nil && n.reg == nil && *peerBusy < s.cfg.MaxPeer {
			stuck := false
			for _, i := range nd.missing {
				if len(holders[i]) == 0 && j.regFails[i] >= 3 {
					stuck = true
					break
				}
			}
			if stuck {
				var sources []model.RescueSource
				for _, h := range fresh {
					if h.NodeID != w.NodeID && h.HasImage(j.image) && serving[h.NodeID] < s.cfg.PerSource {
						sources = append(sources, model.RescueSource{NodeID: h.NodeID, Address: h.Address})
						if len(sources) == 3 {
							break
						}
					}
				}
				if len(sources) > 0 {
					x := &sxfer{layer: -1, path: "image", from: sources[0].NodeID, at: now}
					n.peer, n.touched = x, true
					*peerBusy++
					serving[sources[0].NodeID]++
					orders = append(orders, order{job: j, node: w.NodeID, addr: w.Address, xfer: x, layer: -1, image: sources})
				}
			}
		}
		if n.reg != nil || n.peer != nil {
			transferring++
		} else {
			waitingNodes++
		}
	}

	// Gauges for this job.
	spreadNodes.Set(float64(done), j.image, "done")
	spreadNodes.Set(float64(finalizing), j.image, "finalizing")
	spreadNodes.Set(float64(transferring), j.image, "transferring")
	spreadNodes.Set(float64(waitingNodes), j.image, "waiting")
	spreadNodes.Set(float64(skipped+excluded), j.image, "skipped")
	var held, pulling, nobody int
	for i := range layers {
		switch {
		case len(holders[i]) > 0:
			held++
		case regPulls[i] > 0:
			pulling++
		default:
			nobody++
		}
	}
	spreadLayers.Set(float64(held), j.image, "held")
	spreadLayers.Set(float64(pulling), j.image, "pulling")
	spreadLayers.Set(float64(nobody), j.image, "missing")
	for node, n := range j.nodes {
		for _, x := range []*sxfer{n.reg, n.peer} {
			if x == nil {
				continue
			}
			blob, size := "image", j.total
			if x.layer >= 0 {
				blob = shortDigest(layers[x.layer].Digest)
				size = layers[x.layer].Size
			}
			spreadTransfer.Set(float64(size), j.image, node, x.path, x.from, blob)
		}
	}
	return orders, nil
}

// expireLocked frees a slot whose transfer has been silent too long (a
// lost report, a dead worker), or whose blob the node's inventory already
// shows (its report was lost).
func (s *Spreader) expireLocked(j *sjob, node string, n *snode, now time.Time) {
	for _, slot := range []**sxfer{&n.reg, &n.peer} {
		x := *slot
		if x == nil {
			continue
		}
		if now.Sub(x.at) > s.cfg.TransferTimeout {
			logging.Warnf("angryduck-controller: spread: no word from node=%s on %s transfer of image=%s for %s; freeing the slot",
				node, x.path, j.image, now.Sub(x.at).Round(time.Second))
			*slot = nil
		}
	}
	if n.finalizing && n.done {
		n.finalizing = false
	}
}

// betterPull reports whether layer a is a better registry pull than b: one
// nobody is pulling beats one being raced, the larger of two unstarted
// ones first (it takes longest), and of two being pulled, the one pulled
// longest (most likely stuck on a slow node).
func betterPull(a, b int, pulls []int, since []time.Time, layers []registryLayer) bool {
	switch {
	case (pulls[a] == 0) != (pulls[b] == 0):
		return pulls[a] == 0
	case pulls[a] == 0:
		return layers[a].Size > layers[b].Size
	case !since[a].Equal(since[b]):
		return since[a].Before(since[b])
	default:
		return layers[a].Size > layers[b].Size
	}
}

// pickSource returns the holder serving the fewest transfers, among those
// below perSource, never the receiver itself.
func pickSource(holders []string, target string, serving map[string]int, perSource int) string {
	best := ""
	for _, h := range holders {
		if h == target || serving[h] >= perSource {
			continue
		}
		if best == "" || serving[h] < serving[best] {
			best = h
		}
	}
	return best
}

func contains(list []string, s string) bool {
	for _, x := range list {
		if x == s {
			return true
		}
	}
	return false
}

func shortDigest(d string) string {
	d = strings.TrimPrefix(d, "sha256:")
	if len(d) > 12 {
		return d[:12]
	}
	return d
}

// endLocked forgets a job and returns drops for the nodes that may still
// hold its blobs. Caller holds s.mu.
func (s *Spreader) endLocked(j *sjob) []dropOrder {
	delete(s.jobs, j.image)
	delete(s.byID, j.id)
	var drops []dropOrder
	for node, n := range j.nodes {
		if n.touched && !n.done {
			if w := s.registry.worker(node); w != nil {
				drops = append(drops, dropOrder{addr: w.Address, job: j.id})
			}
		}
	}
	return drops
}

func (s *Spreader) sendDrops(drops []dropOrder) {
	for _, d := range drops {
		s.wg.Add(1)
		go func(d dropOrder) {
			defer s.wg.Done()
			body, _ := json.Marshal(model.SpreadDrop{Job: d.job})
			if resp, err := s.post(context.Background(), s.orders, d.addr, "/spread/drop", body); err == nil {
				resp.Body.Close()
			}
		}(d)
	}
}

func (s *Spreader) resetMetricsLocked() {
	spreadNodes.Reset()
	spreadLayers.Reset()
	spreadNodeMissingBytes.Reset()
	spreadNodeMissingLayers.Reset()
	spreadTransfer.Reset()
	s.published = false
}

func (s *Spreader) post(ctx context.Context, hc *http.Client, addr, path string, body []byte) (*http.Response, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, "http://"+addr+path, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	sharedtoken.Set(req, s.token)
	return hc.Do(req)
}

// send delivers one order and records what came of it.
func (s *Spreader) send(ctx context.Context, o order) {
	defer s.wg.Done()
	switch {
	case o.final:
		s.sendFinalize(ctx, o)
	case o.image != nil:
		s.sendImage(ctx, o)
	default:
		s.sendFetch(ctx, o)
	}
}

func (s *Spreader) sendFetch(ctx context.Context, o order) {
	body, _ := json.Marshal(o.fetch)
	resp, err := s.post(ctx, s.orders, o.addr, "/spread/fetch", body)
	var ack model.SpreadFetchAck
	status := 0
	if err == nil {
		status = resp.StatusCode
		_ = json.NewDecoder(io.LimitReader(resp.Body, 4<<10)).Decode(&ack)
		resp.Body.Close()
	}
	if err == nil && status == http.StatusAccepted && ack.Accepted {
		return // its SpreadDone follows
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	n := s.slotOwnerLocked(o)
	if n == nil {
		return
	}
	s.clearLocked(n, o.xfer)
	switch {
	case err == nil && ack.Present:
		n.have[o.layer] = true
		o.job.addGot(o.layer, o.node)
		spreadTransfersTotal.Inc(o.fetch.Path, "present")
	case err == nil && status == http.StatusConflict:
		// The worker is busy with a transfer we don't know about (a
		// controller restart, a cancelled job winding down). Look again
		// shortly.
		n.next = time.Now().Add(5 * time.Second)
		spreadTransfersTotal.Inc(o.fetch.Path, "rejected")
	default:
		if err == nil {
			err = fmt.Errorf("status %d %s", status, ack.Reason)
		}
		s.backoffLocked(n)
		spreadTransfersTotal.Inc(o.fetch.Path, "rejected")
		logging.Warnf("angryduck-controller: spread: ordering node=%s to fetch %s of image=%s failed: %v", o.node, o.digest, o.job.image, err)
	}
	s.wakeup.wake()
}

func (s *Spreader) sendFinalize(ctx context.Context, o order) {
	j := o.job
	body, _ := json.Marshal(model.SpreadFinalize{Job: j.id, Image: j.image, TopType: j.res.Top.MediaType, TopDig: j.res.Top.Digest, TopSize: j.res.Top.Size, Metadata: j.res.Metadata})
	resp, err := s.post(ctx, s.finals, o.addr, "/spread/finalize", body)
	var res model.SpreadFinalizeResult
	if err == nil {
		_ = json.NewDecoder(io.LimitReader(resp.Body, 64<<10)).Decode(&res)
		resp.Body.Close()
		if !res.OK {
			err = fmt.Errorf("status %d: %s", resp.StatusCode, res.Error)
		}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	defer s.wakeup.wake()
	n := j.nodes[o.node]
	if n == nil {
		return
	}
	n.finalizing = false
	if err != nil {
		// Something it was counted as holding is gone (a lease expired, a
		// cleanup): forget what it reported and trust its next inventory.
		for i := range n.have {
			n.have[i] = false
		}
		s.backoffLocked(n)
		spreadFinalizesTotal.Inc("failure")
		logging.Warnf("angryduck-controller: spread: node=%s couldn't register image=%s: %v", o.node, j.image, err)
		return
	}
	n.done, n.fails = true, 0
	spreadFinalizesTotal.Inc("success")
	logging.Infof("angryduck-controller: spread: node=%s registered image=%s", o.node, j.image)
}

func (s *Spreader) sendImage(ctx context.Context, o order) {
	tctx, cancel := context.WithTimeout(ctx, s.cfg.TransferTimeout)
	res, err := sendRescueOrder(tctx, s.finals, s.token, o.addr, model.RescueOrder{Image: o.job.image, Sources: o.image, Reason: "propagate"})
	cancel()
	s.mu.Lock()
	defer s.mu.Unlock()
	defer s.wakeup.wake()
	n := s.slotOwnerLocked(o)
	if n == nil {
		return
	}
	s.clearLocked(n, o.xfer)
	if err != nil {
		s.backoffLocked(n)
		spreadTransfersTotal.Inc("image", "failed")
		logging.Warnf("angryduck-controller: spread: node=%s couldn't get image=%s whole from %s: %v", o.node, o.job.image, o.image[0].NodeID, err)
		return
	}
	n.done, n.fails = true, 0
	spreadTransfersTotal.Inc("image", "ok")
	spreadBytesTotal.Add(res.Bytes, "image")
}

// slotOwnerLocked returns the node state o was for, if its job is still
// running. Caller holds s.mu.
func (s *Spreader) slotOwnerLocked(o order) *snode {
	if s.byID[o.job.id] != o.job {
		return nil
	}
	return o.job.nodes[o.node]
}

// clearLocked frees the slot x is in, if it still is.
func (s *Spreader) clearLocked(n *snode, x *sxfer) {
	if n.reg == x {
		n.reg = nil
	}
	if n.peer == x {
		n.peer = nil
	}
}

func (s *Spreader) backoffLocked(n *snode) {
	n.fails++
	wait := s.cfg.RetryAfter
	for i := 1; i < n.fails && wait < s.cfg.BackoffMax; i++ {
		wait *= 2
	}
	if wait > s.cfg.BackoffMax {
		wait = s.cfg.BackoffMax
	}
	n.next = time.Now().Add(wait)
}

func (j *sjob) addGot(i int, node string) {
	if j.got[i] == nil {
		j.got[i] = map[string]bool{}
	}
	j.got[i][node] = true
}

// HandleDone takes a worker's report that a transfer ended.
func (s *Spreader) HandleDone(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var d model.SpreadDone
	if err := json.NewDecoder(io.LimitReader(r.Body, 16<<10)).Decode(&d); err != nil || d.Node == "" || d.Job == "" {
		http.Error(w, "invalid report", http.StatusBadRequest)
		return
	}
	w.WriteHeader(http.StatusNoContent)
	s.done(d)
}

func (s *Spreader) done(d model.SpreadDone) {
	path := d.Path
	if path != model.SpreadPathPeer {
		path = model.SpreadPathRegistry
	}
	spreadTransfersTotal.Inc(path, d.Result)
	if d.Bytes > 0 {
		spreadBytesTotal.Add(d.Bytes, path)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	j := s.byID[d.Job]
	if j == nil {
		return // superseded or finished meanwhile
	}
	i, ok := j.idx[d.Digest]
	n := j.nodes[d.Node]
	if !ok || n == nil {
		return
	}
	slot := &n.reg
	if path == model.SpreadPathPeer {
		slot = &n.peer
	}
	if x := *slot; x != nil && x.layer == i {
		*slot = nil
	}
	switch d.Result {
	case model.SpreadResultOK:
		n.have[i] = true
		j.addGot(i, d.Node)
		if n.fails > 0 {
			n.fails = 0
		}
	case model.SpreadResultFailed:
		if path == model.SpreadPathRegistry {
			j.regFails[i]++
		}
		s.backoffLocked(n)
		logging.Warnf("angryduck-controller: spread: node=%s failed to fetch %s of image=%s from %s: %s", d.Node, d.Digest, j.image, path, d.Error)
	}
	s.wakeup.wake()
}

// Status returns every running spread, for /status.
func (s *Spreader) Status() []model.SpreadStatus {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]model.SpreadStatus, 0, len(s.jobs))
	for _, j := range s.jobs {
		st := model.SpreadStatus{Image: j.image, Job: j.id, StartedAt: j.started, Layers: len(j.res.Layers), Bytes: j.total, Done: []string{}}
		for node, n := range j.nodes {
			if n.done {
				st.Done = append(st.Done, node)
				continue
			}
			ns := model.SpreadNodeStatus{Node: node, Finalizing: n.finalizing, Failures: n.fails}
			ns.MissingLayers, ns.MissingBytes = n.missLayers, n.missBytes
			if n.reg != nil && n.reg.layer >= 0 {
				ns.Registry = j.res.Layers[n.reg.layer].Digest
			}
			if n.peer != nil {
				ns.PeerFrom = n.peer.from
				if n.peer.layer >= 0 {
					ns.Peer = j.res.Layers[n.peer.layer].Digest
				} else {
					ns.Peer = "image"
				}
			}
			st.Nodes = append(st.Nodes, ns)
		}
		sort.Strings(st.Done)
		sort.Slice(st.Nodes, func(a, b int) bool { return st.Nodes[a].Node < st.Nodes[b].Node })
		out = append(out, st)
	}
	sort.Slice(out, func(a, b int) bool { return out[a].Image < out[b].Image })
	return out
}

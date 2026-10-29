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
	"strconv"
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
		"Bytes of the image each node still lacks. Only nodes not done and not too full, only while the spread runs.",
		"image", "node",
	)
	spreadNodeMissingLayers = metrics.NewGaugeVec(
		"angryduck_controller_spread_node_missing_layers",
		"Layers of the image each node still lacks. Only nodes not done and not too full, only while the spread runs.",
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
	spreadStreamOrdersTotal = metrics.NewCounterVec(
		"angryduck_controller_spread_stream_orders_total",
		"Peer transfers ordered from a node still receiving the blob itself, which streams it on.",
	)
	spreadSlowTotal = metrics.NewCounterVec(
		"angryduck_controller_spread_slow_total",
		"What the slow-chain guard did about late transfers: race (a late streamed registry pull raced from the registry), replaced (a late transfer restarted from a holder), kept (the receiver judged it finishes sooner as it is).",
		"action",
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
	PerSource       int           // peer transfers one node serves at once (no cluster-wide cap)
	MaxUtilization  float64       // nodes fuller than this get nothing new (0-1)
	Exclude         []string      // node name substrings that never receive
	RetryAfter      time.Duration // after a failure; doubles per failure
	BackoffMax      time.Duration
	TransferTimeout time.Duration // an in-flight transfer nobody reported on is freed after this
	ResolveTimeout  time.Duration
	Window          time.Duration // 0: until done or superseded
	Platform        string
	// Streaming: with Stream on, a node still receiving a blob of at least
	// StreamMinBytes may serve it on, to nodes that have no transfer of it
	// yet. Chains of nodes receiving through each other are at most
	// StreamMaxDepth long (the node getting it from the registry or a full
	// copy is 1); 0 caps them only by the number of nodes.
	Stream         bool
	StreamMinBytes int64
	StreamMaxDepth int
	// ExpressMaxBytes: blobs up to this size have their own PerSource
	// budget on each source, so they don't wait behind large transfers.
	// 0: one budget for all.
	ExpressMaxBytes int64
	// SlowFactor: a transfer running more than this many times its
	// expected time (its size at the typical speed of its path, learned
	// from finished transfers) is late, and the slow-chain guard acts on
	// it. 0: no guard.
	SlowFactor float64
}

// Slow-chain guard timing.
const (
	slowFloor     = 5 * time.Second  // nothing is late before this
	slowNoRate    = 30 * time.Second // late without a typical speed to judge by
	replaceEvery  = 10 * time.Second // a late transfer is offered a restart at most this often
	rateMinBytes  = 4 << 20          // transfers smaller than this don't teach typical speeds
	rateSmoothing = 0.3              // weight of the newest transfer in a typical speed
)

// express reports whether a blob of size uses the express budget.
func (s *Spreader) express(size int64) bool {
	return s.cfg.ExpressMaxBytes > 0 && size <= s.cfg.ExpressMaxBytes
}

// streamDepth is the chain cap in effect: StreamMaxDepth, or with 0 one
// more than the nodes there are (no cap). Below 2 nothing is streamed.
func (s *Spreader) streamDepth(nodes int) int {
	if !s.cfg.Stream {
		return 0
	}
	if s.cfg.StreamMaxDepth > 0 {
		return s.cfg.StreamMaxDepth
	}
	return nodes + 1
}

// Spreader spreads a pushed image to every eligible node blob by blob.
//
// The blob is the unit. A node receives any number of different blobs at
// once, each at most once per path (registry, peer), and serves PerSource
// peers at once. That per-source limit is the only limit on east-west
// traffic: there is no cluster-wide cap and no per-receiver cap, so every
// holder's link is used. As soon as any node holds a blob, every node
// lacking it can fetch it from that node; the registry is only used for
// blobs no node has yet, by at most MaxRegistry transfers at once, so it
// serves each blob about once. No node waits on another's pull: a slow
// registry pull of a blob can be raced by another node (RacePerBlob), and
// a node pulling a blob from the registry still gets it pushed from a peer
// as soon as a peer has it. Whichever path finishes first wins; the worker
// cancels the other on the spot.
//
// Orders go out in rounds, one new transfer per node per round, so the
// first node in line doesn't take every free source before the others get
// one.
//
// Peer transfers go to the rarest layer first (fewest holders), so new
// sources appear where they are scarcest. Nodes with a pod waiting for the
// image come first, then the nodes that lack the fewest bytes.
//
// A node still receiving a large blob is a source for it too, if its
// worker streams (see SpreaderConfig.StreamMinBytes): the receiver gets the
// bytes as they arrive there instead of waiting a whole transfer per hop.
// Nodes that hold the blob come first. A stream only goes to a node with
// no transfer of that blob yet whose worker streams too (it tells a broken
// stream from its own failure), never around in a circle, and chains stay
// within StreamMaxDepth. When a stream breaks because its source's own
// transfer stopped, the receiver isn't backed off and that source streams
// no more for the job.
//
// When a node holds every layer (its blob or its unpacked snapshot), it
// gets the image's metadata, a few KB read from the registry once, and
// registers the image.
//
// Nodes fuller than MaxUtilization get nothing new. The spread counts as
// complete once every node with room has the image; it then stays open,
// quietly, for the full ones, which receive it once cleanup makes room. A layer the registry keeps failing to serve, held
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

	// Typical speed per path (registry, peer), bytes per second, from
	// finished transfers of at least rateMinBytes. Streams don't count:
	// their speed is their source's. Under mu.
	rates map[string]float64
	// lateTimer wakes the loop when the next running transfer turns late.
	lateTimer *time.Timer
}

type sjob struct {
	id, image string
	res       registryclient.Resolved
	started   time.Time
	completed bool // every node with room has it; open only for full ones
	total     int64
	idx       map[string]int // layer digest -> index
	nodes     map[string]*snode
	got       []map[string]bool // per layer: nodes that reported it (inventories lag)
	regFails  []int             // per layer: failed registry pulls
	noStream  map[string]bool   // nodes whose stream broke on their side: not streaming sources for this job
	seq       int               // transfer IDs
}

// newXfer returns a transfer of this job with a fresh ID.
func (j *sjob) newXfer(x sxfer) *sxfer {
	j.seq++
	x.id = j.id + "-" + strconv.Itoa(j.seq)
	return &x
}

type snode struct {
	have       []bool // per layer: reported received through us
	missLayers int    // as of the last tick, for /status
	missBytes  int64
	xfers      []*sxfer // running: at most one per layer and path
	finalizing bool
	skipped    bool // too full, as of the last tick
	skipLogged bool // the skip was logged (once per spread)
	done       bool
	touched    bool              // was ordered something: has blobs to release if the job ends early
	recorded   bool              // its delivery is in the image history
	paths      map[string]bool   // paths its blobs came over (registry, peer)
	declined   map[int]time.Time // per layer: a second copy declined (the running one finishes sooner) until then
	received   pathTotals        // bytes and time received per path, for the history
	fails      int
	next       time.Time // no new orders before this
}

type sxfer struct {
	layer    int // -1: a whole-image transfer (fallback)
	path     string
	from     string // registry, or the sending node
	at       time.Time
	stream   bool // from is still receiving the blob and streams it on
	accepted bool // the worker took the order: peers may stream from it
	id       string
	replaces *sxfer    // a restart offered for this late transfer (see sendFetch)
	offered  time.Time // last restart offered for it
}

func (n *snode) busy() bool { return len(n.xfers) > 0 }

// running reports whether layer is coming over path.
func (n *snode) running(layer int, path string) bool {
	for _, x := range n.xfers {
		if x.layer == layer && x.path == path {
			return true
		}
	}
	return false
}

func (n *snode) countPath(path string) int {
	c := 0
	for _, x := range n.xfers {
		if x.path == path {
			c++
		}
	}
	return c
}

// remove forgets x, if it is still running.
func (n *snode) remove(x *sxfer) bool {
	for i, y := range n.xfers {
		if y == x {
			n.xfers = append(n.xfers[:i], n.xfers[i+1:]...)
			return true
		}
	}
	return false
}

// NewSpreader builds a spreader.
func NewSpreader(registry *Registry, resolver ImageResolver, token string, cfg SpreaderConfig) *Spreader {
	for _, path := range []string{"registry", "peer"} {
		for _, r := range []string{"ok", "failed", "cancelled", "present", "rejected"} {
			spreadTransfersTotal.Add(0, path, r)
		}
	}
	spreadTransfersTotal.Add(0, "image", "ok")
	spreadTransfersTotal.Add(0, "image", "failed")
	for _, path := range []string{"registry", "peer", "image"} {
		spreadBytesTotal.Add(0, path)
	}
	for _, r := range []string{"success", "failure"} {
		spreadFinalizesTotal.Add(0, r)
	}
	for _, r := range []string{"complete", "superseded", "expired"} {
		spreadsTotal.Add(0, r)
	}
	if cfg.MaxRegistry < 1 {
		cfg.MaxRegistry = 1
	}
	if cfg.RacePerBlob < 1 {
		cfg.RacePerBlob = 1
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
		job.started, job.completed = time.Now(), false
		recordSpreadStart(image, job.started)
		s.mu.Unlock()
		return nil, nil
	}
	repo := imageref.Repo(image)
	for other, job := range s.jobs {
		if repo != "" && imageref.Repo(other) == repo {
			if !job.completed { // a complete one was already counted
				logging.Infof("angryduck-controller: spread: image=%s superseded by image=%s", other, image)
				spreadsTotal.Inc("superseded")
				recordSpreadEnd(job.image, "superseded", time.Since(job.started))
			}
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
	recordSpreadStart(image, job.started)
	s.mu.Unlock()
	s.sendDrops(drops)
	logging.Infof("angryduck-controller: spread: image=%s job=%s: %d layer(s), %d bytes, blob by blob to every eligible node",
		image, job.id, len(res.Layers), job.total)

	s.tick(s.baseCtx())
	s.mu.Lock()
	defer s.mu.Unlock()
	var ordered []string
	for node, n := range job.nodes {
		if n.countPath(model.SpreadPathRegistry) > 0 {
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
	logging.Infof("angryduck-controller: spreader started: max_registry=%d race_per_blob=%d per_source=%d max_utilization=%.2f transfer_timeout=%s window=%s exclude=%v",
		s.cfg.MaxRegistry, s.cfg.RacePerBlob, s.cfg.PerSource, s.cfg.MaxUtilization, s.cfg.TransferTimeout, s.cfg.Window, s.cfg.Exclude)
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
		if s.lateTimer != nil {
			s.lateTimer.Stop()
			s.lateTimer = nil
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
	var regBusy int
	serving := map[string]int{} // per source: transfers of large blobs (and whole images)
	express := map[string]int{} // per source: transfers of small blobs (see ExpressMaxBytes)
	for _, j := range jobs {
		for _, n := range j.nodes {
			for _, x := range n.xfers {
				switch {
				case x.path == model.SpreadPathRegistry:
					regBusy++
				case x.from == "":
				case x.layer >= 0 && s.express(j.res.Layers[x.layer].Size):
					express[x.from]++
				default:
					serving[x.from]++
				}
			}
		}
	}

	var orders []order
	var drops []dropOrder
	s.resetMetricsLocked()
	for _, j := range jobs {
		o, d := s.planLocked(j, fresh, byNode, ids, waiting[j.image], now, &regBusy, serving, express)
		orders = append(orders, o...)
		drops = append(drops, d...)
	}
	s.published = len(s.jobs) > 0
	s.wakeForLateLocked(jobs, now)
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
	now time.Time, regBusy *int, serving, express map[string]int) ([]order, []dropOrder) {
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
	var skippedNodes []string
	for _, w := range fresh {
		if s.excluded(w.NodeID) {
			excluded++
			continue
		}
		n := j.nodes[w.NodeID]
		isNew := n == nil
		if isNew {
			n = &snode{have: make([]bool, nl)}
			j.nodes[w.NodeID] = n
		}
		s.expireLocked(j, w.NodeID, n, now)
		n.skipped = false
		if n.done || w.HasImage(j.image) {
			if !n.recorded {
				via := "kubelet"
				if isNew {
					via = "present"
				}
				recordDelivery(j.image, w.NodeID, via, j.started, now)
				n.received.record(j.image, w.NodeID)
				n.recorded = true
			}
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
		if n.finalizing {
			finalizing++
			continue
		}
		if w.Utilization > s.cfg.MaxUtilization && !n.busy() {
			n.skipped = true
			skipped++
			skippedNodes = append(skippedNodes, w.NodeID)
			if !n.skipLogged {
				n.skipLogged = true
				logging.Infof("angryduck-controller: spread: node=%s gets nothing of image=%s for now: disk at %.1f%%, above %.0f%% (PROPAGATE_MAX_UTILIZATION); it receives the image once cleanup makes room",
					w.NodeID, j.image, w.Utilization*100, s.cfg.MaxUtilization*100)
			}
			continue
		}
		needs = append(needs, need{w: w, n: n, missing: missing, bytes: bytes})
	}

	// Finished? Complete once every node with room has it; the job stays
	// open, quietly, while full nodes may still receive it.
	expired := s.cfg.Window > 0 && now.Sub(j.started) > s.cfg.Window
	busy := false
	for _, n := range j.nodes {
		if n.busy() || n.finalizing {
			busy = true
		}
	}
	if len(needs) == 0 && finalizing == 0 && !busy && done > 0 {
		if !j.completed {
			j.completed = true
			took := now.Sub(j.started)
			if skipped == 0 {
				logging.Infof("angryduck-controller: spread: image=%s is on every eligible node (%d) after %s", j.image, done, took.Round(time.Second))
			} else {
				sort.Strings(skippedNodes)
				logging.Infof("angryduck-controller: spread: image=%s is on every node with room (%d) after %s; %d node(s) too full get it once cleanup makes room: %s",
					j.image, done, took.Round(time.Second), skipped, strings.Join(skippedNodes, ", "))
			}
			spreadsTotal.Inc("complete")
			spreadLastSeconds.Set(took.Seconds())
			recordSpreadEnd(j.image, "complete", took)
		}
		if skipped == 0 {
			return nil, s.endLocked(j)
		}
	}
	if expired && !busy {
		if !j.completed {
			logging.Warnf("angryduck-controller: spread: window for image=%s ran out with %d node(s) still missing it", j.image, len(needs)+skipped)
			spreadsTotal.Inc("expired")
			recordSpreadEnd(j.image, "expired", now.Sub(j.started))
		}
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
	// regStreamed: a node pulling the layer streams it on, so other nodes
	// join that stream instead of racing it for the registry's bandwidth,
	// unless the pull is late (regLate): then it is raced, by any node,
	// those streaming it included.
	regPulls := make([]int, nl)
	regSince := make([]time.Time, nl)
	regStreamed := make([]bool, nl)
	regLate := make([]bool, nl)
	for node, n := range j.nodes {
		for _, x := range n.xfers {
			if x.path != model.SpreadPathRegistry || x.layer < 0 {
				continue
			}
			i := x.layer
			regPulls[i]++
			if regSince[i].IsZero() || x.at.Before(regSince[i]) {
				regSince[i] = x.at
			}
			if s.cfg.Stream && layers[i].Size >= s.cfg.StreamMinBytes && !j.noStream[node] {
				if w := byNode[node]; w != nil && w.Streams {
					regStreamed[i] = true
				}
			}
			if s.lateLocked(x, layers[i].Size, now) {
				regLate[i] = true
			}
		}
	}

	var orders []order
	var active []need // nodes that may get new transfers this tick
	for _, nd := range needs {
		n, w := nd.n, nd.w
		if now.Before(n.next) || expired {
			continue
		}
		if len(nd.missing) == 0 {
			if !n.busy() {
				n.finalizing = true
				orders = append(orders, order{job: j, node: w.NodeID, addr: w.Address, final: true})
				finalizing++
			}
			// else: the slower path of a race, about to be cancelled
			continue
		}
		active = append(active, nd)
	}

	// East-west: rarest layer first, then largest, from the least busy
	// holder. One new transfer per node per round, until nothing more fits.
	maxDepth := s.streamDepth(len(fresh))
	streamers := s.streamersLocked(j, holders, byNode, maxDepth)
	// load is the budget a transfer of layer i counts against.
	load := func(i int) map[string]int {
		if s.express(layers[i].Size) {
			return express
		}
		return serving
	}
	peerOrder := func(nd need) bool {
		n, w := nd.n, nd.w
		best, src, stream := -1, "", false
		for _, i := range nd.missing {
			if n.running(i, model.SpreadPathPeer) || now.Before(n.declined[i]) {
				continue
			}
			h, st := pickSource(holders[i], w.NodeID, load(i), s.cfg.PerSource), false
			if h == "" && w.Streams && !n.hasLayer(i) {
				h = j.pickStreamer(streamers[i], i, w.NodeID, load(i), s.cfg.PerSource, maxDepth)
				st = h != ""
			}
			if h == "" {
				continue
			}
			if best < 0 || len(holders[i]) < len(holders[best]) ||
				len(holders[i]) == len(holders[best]) && layers[i].Size > layers[best].Size {
				best, src, stream = i, h, st
			}
		}
		if best < 0 {
			return false
		}
		x := j.newXfer(sxfer{layer: best, path: model.SpreadPathPeer, from: src, at: now, stream: stream})
		n.xfers, n.touched = append(n.xfers, x), true
		load(best)[src]++
		if stream {
			spreadStreamOrdersTotal.Inc()
		}
		orders = append(orders, order{job: j, node: w.NodeID, addr: w.Address, xfer: x, layer: best, digest: layers[best].Digest,
			fetch: &model.SpreadFetch{Job: j.id, Digest: layers[best].Digest, Size: layers[best].Size, Path: model.SpreadPathPeer,
				Peer: &model.Holder{NodeID: src, Address: byNode[src].Address}, Stream: stream, ID: x.id, ExpectRate: s.rates[model.SpreadPathPeer]}})
		return true
	}
	// North-south: only blobs no node holds. Unstarted ones first, largest
	// first; otherwise race the pull that has run longest.
	regOrder := func(nd need) bool {
		n, w := nd.n, nd.w
		if *regBusy >= s.cfg.MaxRegistry {
			return false
		}
		best := -1
		for _, i := range nd.missing {
			if len(holders[i]) > 0 || (fullHolder && j.regFails[i] >= 3) || regPulls[i] >= s.cfg.RacePerBlob ||
				regPulls[i] > 0 && regStreamed[i] && !regLate[i] || n.running(i, model.SpreadPathRegistry) ||
				n.streaming(i) && !regLate[i] {
				continue
			}
			if best < 0 || betterPull(i, best, regPulls, regSince, layers) {
				best = i
			}
		}
		if best < 0 {
			return false
		}
		x := j.newXfer(sxfer{layer: best, path: model.SpreadPathRegistry, from: model.SpreadPathRegistry, at: now})
		n.xfers, n.touched = append(n.xfers, x), true
		*regBusy++
		if regPulls[best] > 0 && regStreamed[best] {
			spreadSlowTotal.Inc("race")
			logging.Infof("angryduck-controller: spread: registry pull of %s of image=%s is late; node=%s races it", layers[best].Digest, j.image, w.NodeID)
		}
		regPulls[best]++
		if s.cfg.Stream && layers[best].Size >= s.cfg.StreamMinBytes && w.Streams {
			regStreamed[best] = true
		}
		if regSince[best].IsZero() {
			regSince[best] = now
		}
		orders = append(orders, order{job: j, node: w.NodeID, addr: w.Address, xfer: x, layer: best, digest: layers[best].Digest,
			fetch: &model.SpreadFetch{Job: j.id, Image: j.image, Digest: layers[best].Digest, Size: layers[best].Size, Path: model.SpreadPathRegistry, ID: x.id}})
		return true
	}
	for more := true; more; {
		more = false
		for _, nd := range active {
			if peerOrder(nd) {
				more = true
			}
			if regOrder(nd) {
				more = true
			}
		}
	}

	// Slow-chain guard: a late peer transfer (a stream behind a slow node,
	// or a copy from a slow holder) is offered a restart from another node
	// that holds the blob. The receiver decides (it knows how far and how
	// fast its transfer is going); see sendFetch.
	if s.cfg.SlowFactor > 0 && s.rates[model.SpreadPathPeer] > 0 {
		for _, nd := range needs {
			n, w := nd.n, nd.w
			if n.finalizing || !w.Streams { // workers before 1.9.0 can't take a restart offer
				continue
			}
			for _, x := range append([]*sxfer(nil), n.xfers...) {
				i := x.layer
				if i < 0 || x.path != model.SpreadPathPeer || x.replaces != nil || !x.accepted || now.Sub(x.offered) < replaceEvery || n.offering(i) ||
					!s.lateLocked(x, layers[i].Size, now) {
					continue
				}
				// Never the late transfer's own source; one slot beyond
				// PerSource: a holder busy feeding a slow receiver isn't
				// using its link, and a restart frees a slot elsewhere.
				var h string
				for _, c := range holders[i] {
					if c != x.from && c != w.NodeID && load(i)[c] < s.cfg.PerSource+1 && (h == "" || load(i)[c] < load(i)[h]) {
						h = c
					}
				}
				if h == "" {
					continue
				}
				x.offered = now
				y := j.newXfer(sxfer{layer: i, path: model.SpreadPathPeer, from: h, at: now, replaces: x})
				n.xfers, n.touched = append(n.xfers, y), true
				load(i)[h]++
				orders = append(orders, order{job: j, node: w.NodeID, addr: w.Address, xfer: y, layer: i, digest: layers[i].Digest,
					fetch: &model.SpreadFetch{Job: j.id, Digest: layers[i].Digest, Size: layers[i].Size, Path: model.SpreadPathPeer,
						Peer: &model.Holder{NodeID: h, Address: byNode[h].Address}, ID: y.id, Replace: true, ExpectRate: s.rates[model.SpreadPathPeer]}})
			}
		}
	}

	// Last resort: a layer nobody has the blob of and the registry keeps
	// failing on. Get the whole image from a node that has it (blobs or
	// snapshots, as rescue does).
	for _, nd := range active {
		n, w := nd.n, nd.w
		if !fullHolder || n.busy() {
			continue
		}
		stuck := false
		for _, i := range nd.missing {
			if len(holders[i]) == 0 && j.regFails[i] >= 3 {
				stuck = true
				break
			}
		}
		if !stuck {
			continue
		}
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
			x := j.newXfer(sxfer{layer: -1, path: "image", from: sources[0].NodeID, at: now})
			n.xfers, n.touched = append(n.xfers, x), true
			serving[sources[0].NodeID]++
			orders = append(orders, order{job: j, node: w.NodeID, addr: w.Address, xfer: x, layer: -1, image: sources})
		}
	}

	transferring, waitingNodes := 0, 0
	for _, nd := range needs {
		if nd.n.finalizing {
			continue // counted above
		}
		if nd.n.busy() {
			transferring++
		} else {
			waitingNodes++
		}
	}

	// Gauges for this job. A complete job waiting only on full nodes shows
	// nothing until one of them gets room and starts receiving.
	if j.completed && len(orders) == 0 && transferring == 0 && finalizing == 0 {
		return orders, nil
	}
	if !j.completed {
		imageSpreadSeconds.Hold(now.Sub(j.started).Seconds(), j.image, "running")
	}
	for _, nd := range needs {
		spreadNodeMissingBytes.Set(float64(nd.bytes), j.image, nd.w.NodeID)
		spreadNodeMissingLayers.Set(float64(len(nd.missing)), j.image, nd.w.NodeID)
	}
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
		for _, x := range n.xfers {
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

// expireLocked frees a transfer that has been silent too long (a lost
// report, a dead worker).
func (s *Spreader) expireLocked(j *sjob, node string, n *snode, now time.Time) {
	kept := n.xfers[:0]
	for _, x := range n.xfers {
		if now.Sub(x.at) > s.cfg.TransferTimeout {
			logging.Warnf("angryduck-controller: spread: no word from node=%s on %s transfer of image=%s for %s; forgetting it",
				node, x.path, j.image, now.Sub(x.at).Round(time.Second))
			continue
		}
		kept = append(kept, x)
	}
	n.xfers = kept
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

// lateLocked reports whether x, a transfer of size bytes, has run more
// than SlowFactor times its expected time: size at the typical speed of
// its path, or slowNoRate when that isn't known yet. Never before
// slowFloor. Caller holds s.mu.
func (s *Spreader) lateLocked(x *sxfer, size int64, now time.Time) bool {
	after, ok := s.lateAfterLocked(x, size)
	return ok && now.Sub(x.at) > after
}

// lateAfterLocked is how long x may run before it is late. ok is false
// when the guard is off or x is a whole-image transfer.
func (s *Spreader) lateAfterLocked(x *sxfer, size int64) (time.Duration, bool) {
	if s.cfg.SlowFactor <= 0 || x.layer < 0 {
		return 0, false
	}
	path := x.path
	if path != model.SpreadPathRegistry {
		path = model.SpreadPathPeer
	}
	rate := s.rates[path]
	if rate <= 0 {
		return slowNoRate, true
	}
	after := time.Duration(s.cfg.SlowFactor * float64(size) / rate * float64(time.Second))
	if after < slowFloor {
		after = slowFloor
	}
	return after, true
}

// wakeForLateLocked schedules a tick for when the next running transfer
// turns late (or a late one may be offered a restart again), so the guard
// acts then rather than at the next periodic tick. Caller holds s.mu.
func (s *Spreader) wakeForLateLocked(jobs []*sjob, now time.Time) {
	if s.lateTimer != nil {
		s.lateTimer.Stop()
		s.lateTimer = nil
	}
	if s.cfg.SlowFactor <= 0 {
		return
	}
	var next time.Time
	for _, j := range jobs {
		for _, n := range j.nodes {
			for _, x := range n.xfers {
				after, ok := s.lateAfterLocked(x, j.res.Layers[max(x.layer, 0)].Size)
				if !ok {
					continue
				}
				at := x.at.Add(after)
				if !at.After(now) {
					if x.path != model.SpreadPathPeer || x.offered.IsZero() {
						continue // late already: acted on this tick, or nothing to do
					}
					at = x.offered.Add(replaceEvery)
				}
				if at.After(now) && (next.IsZero() || at.Before(next)) {
					next = at
				}
			}
		}
	}
	if !next.IsZero() {
		s.lateTimer = time.AfterFunc(next.Sub(now)+10*time.Millisecond, s.wakeup.wake)
	}
}

// learnRateLocked folds a finished transfer into its path's typical speed.
// Caller holds s.mu.
func (s *Spreader) learnRateLocked(path string, bytes int64, seconds float64) {
	if bytes < rateMinBytes || seconds <= 0 {
		return
	}
	if s.rates == nil {
		s.rates = map[string]float64{}
	}
	r := float64(bytes) / seconds
	if old := s.rates[path]; old > 0 {
		r = old + rateSmoothing*(r-old)
	}
	s.rates[path] = r
}

// offering reports whether a restart of layer is being offered.
func (n *snode) offering(layer int) bool {
	for _, x := range n.xfers {
		if x.layer == layer && x.replaces != nil {
			return true
		}
	}
	return false
}

// streaming reports whether layer is coming as a stream.
func (n *snode) streaming(layer int) bool {
	for _, x := range n.xfers {
		if x.layer == layer && x.stream {
			return true
		}
	}
	return false
}

// hasLayer reports whether layer is coming over any path.
func (n *snode) hasLayer(layer int) bool {
	for _, x := range n.xfers {
		if x.layer == layer {
			return true
		}
	}
	return false
}

// streamer is a node receiving a layer that may stream it on.
type streamer struct {
	node  string
	depth int // 1: receiving it from the registry or a full copy
}

// streamersLocked lists, per layer, the nodes that may stream it on: still
// receiving it (an order their worker took), with a worker that streams,
// a layer of at least StreamMinBytes, and room in the chain below
// the chain cap. Nodes holding the layer aren't listed: they are holders.
// Caller holds s.mu.
func (s *Spreader) streamersLocked(j *sjob, holders [][]string, byNode map[string]*workerEntry, maxDepth int) [][]streamer {
	out := make([][]streamer, len(j.res.Layers))
	if maxDepth < 2 {
		return out
	}
	for node, n := range j.nodes {
		w := byNode[node]
		if w == nil || !w.Streams || j.noStream[node] {
			continue
		}
		for _, x := range n.xfers {
			i := x.layer
			if i < 0 || !x.accepted || j.res.Layers[i].Size < s.cfg.StreamMinBytes || contains(holders[i], node) {
				continue
			}
			if d := j.depth(node, i, holders, maxDepth); d < maxDepth && !hasStreamer(out[i], node) {
				out[i] = append(out[i], streamer{node: node, depth: d})
			}
		}
	}
	return out
}

func hasStreamer(list []streamer, node string) bool {
	for _, st := range list {
		if st.node == node {
			return true
		}
	}
	return false
}

// depth is how far node is from a full copy of layer i: 1 when it receives
// it from the registry or a holder, one more than its source when it
// receives a stream. Anything beyond max counts as max+1.
func (j *sjob) depth(node string, i int, holders [][]string, max int) int {
	return j.depthWithin(node, i, holders, max, max)
}

func (j *sjob) depthWithin(node string, i int, holders [][]string, max, budget int) int {
	n := j.nodes[node]
	if n == nil || budget <= 0 {
		return max + 1
	}
	best := max + 1
	for _, x := range n.xfers {
		if x.layer != i {
			continue
		}
		d := 1
		if x.path == model.SpreadPathPeer && x.stream && !contains(holders[i], x.from) {
			d = j.depthWithin(x.from, i, holders, max, budget-1) + 1
		}
		if d < best {
			best = d
		}
	}
	return best
}

// feeds reports whether target is upstream of node for layer i: node
// receives the layer, directly or through other streams, from target.
func (j *sjob) feeds(target, node string, i, hops int) bool {
	n := j.nodes[node]
	if n == nil || hops <= 0 {
		return false
	}
	for _, x := range n.xfers {
		if x.layer != i || x.path != model.SpreadPathPeer || !x.stream {
			continue
		}
		if x.from == target || j.feeds(target, x.from, i, hops-1) {
			return true
		}
	}
	return false
}

// pickStreamer returns the streamer of layer i for target serving the
// fewest transfers, the shortest chain first, among those below perSource
// and never one target feeds (a circle). Caller holds s.mu.
func (j *sjob) pickStreamer(list []streamer, i int, target string, serving map[string]int, perSource, maxDepth int) string {
	best := -1
	for k, st := range list {
		if st.node == target || serving[st.node] >= perSource || j.feeds(target, st.node, i, maxDepth) {
			continue
		}
		if best < 0 || st.depth < list[best].depth ||
			st.depth == list[best].depth && serving[st.node] < serving[list[best].node] {
			best = k
		}
	}
	if best < 0 {
		return ""
	}
	return list[best].node
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
		if !n.recorded {
			n.received.record(j.image, node)
		}
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
		// Its SpreadDone follows. From now on peers may stream from it: a
		// streamable blob gets a round right away rather than at the next.
		s.mu.Lock()
		o.xfer.accepted = true
		if old := o.xfer.replaces; old != nil {
			// The receiver restarts from the holder; the late transfer
			// reports its end (cancelled) under its own ID.
			if n := s.slotOwnerLocked(o); n != nil {
				n.remove(old)
			}
			o.xfer.replaces = nil
			spreadSlowTotal.Inc("replaced")
			logging.Infof("angryduck-controller: spread: node=%s restarts its late transfer of %s of image=%s from %s", o.node, o.digest, o.job.image, o.fetch.Peer.NodeID)
		}
		s.mu.Unlock()
		if s.cfg.Stream && o.fetch.Size >= s.cfg.StreamMinBytes {
			s.wakeup.wake()
		}
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	n := s.slotOwnerLocked(o)
	if n == nil {
		return
	}
	n.remove(o.xfer)
	switch {
	case err == nil && ack.Present:
		n.have[o.layer] = true
		o.job.addGot(o.layer, o.node)
		spreadTransfersTotal.Inc(o.fetch.Path, "present")
	case err == nil && ack.Kept && o.xfer.replaces == nil:
		// A second copy declined: the one already coming finishes sooner
		// (a node at its own link's limit). Not offered again for a while.
		if n.declined == nil {
			n.declined = map[int]time.Time{}
		}
		n.declined[o.layer] = time.Now().Add(replaceEvery)
		spreadSlowTotal.Inc("kept")
	case o.xfer.replaces != nil:
		// A restart offer declined: the running transfer goes on.
		if err == nil && ack.Kept {
			spreadSlowTotal.Inc("kept")
			logging.Debugf("angryduck-controller: spread: node=%s keeps its transfer of %s: %s", o.node, o.digest, ack.Reason)
		}
	case err == nil && status == http.StatusConflict:
		// The worker is already fetching this blob over this path, a
		// transfer we don't know about (a controller restart, a cancelled
		// job winding down). Look again shortly.
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
	if !n.recorded {
		recordDelivery(j.image, o.node, viaPaths(n.paths), j.started, time.Now())
		n.received.record(j.image, o.node)
		n.recorded = true
	}
	spreadFinalizesTotal.Inc("success")
	logging.Infof("angryduck-controller: spread: node=%s registered image=%s", o.node, j.image)
}

func (s *Spreader) sendImage(ctx context.Context, o order) {
	start := time.Now()
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
	n.remove(o.xfer)
	if err != nil {
		s.backoffLocked(n)
		spreadTransfersTotal.Inc("image", "failed")
		logging.Warnf("angryduck-controller: spread: node=%s couldn't get image=%s whole from %s: %v", o.node, o.job.image, o.image[0].NodeID, err)
		return
	}
	n.done, n.fails = true, 0
	spreadTransfersTotal.Inc("image", "ok")
	spreadBytesTotal.Add(res.Bytes, "image")
	n.received = n.received.add("image", res.Bytes, time.Since(start).Seconds())
	if !n.recorded {
		recordDelivery(o.job.image, o.node, "image", o.job.started, time.Now())
		n.received.record(o.job.image, o.node)
		n.recorded = true
	}
}

// slotOwnerLocked returns the node state o was for, if its job is still
// running. Caller holds s.mu.
func (s *Spreader) slotOwnerLocked(o order) *snode {
	if s.byID[o.job.id] != o.job {
		return nil
	}
	return o.job.nodes[o.node]
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

// Holders returns the nodes a running spread knows to hold blob digest:
// reported by a finished transfer, ahead of their next inventory.
func (s *Spreader) Holders(digest string) []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []string
	for _, j := range s.jobs {
		if i, ok := j.idx[digest]; ok {
			for n := range j.got[i] {
				out = append(out, n)
			}
		}
	}
	return out
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
	result := d.Result
	if result == model.SpreadResultFailed && d.Upstream {
		result = "upstream"
	}
	spreadTransfersTotal.Inc(path, result)
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
	if d.Bytes > 0 || d.Seconds > 0 {
		n.received = n.received.add(path, d.Bytes, d.Seconds)
	}
	// Workers that send IDs are matched by them: a restarted transfer's
	// old one ends after the new one started, over the same path.
	var ended *sxfer
	for _, x := range n.xfers {
		if d.ID != "" && x.id == d.ID || d.ID == "" && x.layer == i && x.path == path {
			ended = x
			n.remove(x)
			break
		}
	}
	if d.ID != "" && ended == nil && d.Result != model.SpreadResultOK {
		return // a transfer replaced or forgotten meanwhile: nothing to act on
	}
	if d.Result == model.SpreadResultOK && ended != nil && !ended.stream {
		s.learnRateLocked(path, d.Bytes, d.Seconds)
	}
	switch d.Result {
	case model.SpreadResultOK:
		n.have[i] = true
		j.addGot(i, d.Node)
		if n.paths == nil {
			n.paths = map[string]bool{}
		}
		n.paths[path] = true
		if n.fails > 0 {
			n.fails = 0
		}
	case model.SpreadResultFailed:
		if d.Upstream && ended != nil && ended.stream {
			// The source's own transfer stopped: not this node's fault.
			// Get it elsewhere right away; that source streams no more.
			if j.noStream == nil {
				j.noStream = map[string]bool{}
			}
			j.noStream[ended.from] = true
			logging.Infof("angryduck-controller: spread: node=%s lost its stream of %s of image=%s: %s stopped receiving it (%s); fetching it elsewhere", d.Node, d.Digest, j.image, ended.from, d.Error)
			break
		}
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
		st := model.SpreadStatus{Image: j.image, Job: j.id, StartedAt: j.started, Complete: j.completed, Layers: len(j.res.Layers), Bytes: j.total, Done: []string{}}
		for node, n := range j.nodes {
			if n.done {
				st.Done = append(st.Done, node)
				continue
			}
			if n.skipped {
				st.Skipped = append(st.Skipped, node)
			}
			ns := model.SpreadNodeStatus{Node: node, Finalizing: n.finalizing, Failures: n.fails}
			ns.MissingLayers, ns.MissingBytes = n.missLayers, n.missBytes
			for _, x := range n.xfers {
				blob := "image"
				if x.layer >= 0 {
					blob = j.res.Layers[x.layer].Digest
				}
				ns.Transfers = append(ns.Transfers, model.SpreadTransferStatus{Blob: blob, Path: x.path, From: x.from, Stream: x.stream})
				switch {
				case x.path == model.SpreadPathRegistry && ns.Registry == "":
					ns.Registry = blob
				case x.path != model.SpreadPathRegistry && ns.Peer == "":
					ns.Peer, ns.PeerFrom = blob, x.from
				}
			}
			st.Nodes = append(st.Nodes, ns)
		}
		sort.Strings(st.Done)
		sort.Strings(st.Skipped)
		sort.Slice(st.Nodes, func(a, b int) bool { return st.Nodes[a].Node < st.Nodes[b].Node })
		out = append(out, st)
	}
	sort.Slice(out, func(a, b int) bool { return out[a].Image < out[b].Image })
	return out
}

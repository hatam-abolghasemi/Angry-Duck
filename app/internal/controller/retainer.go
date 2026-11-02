package controller

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sort"
	"sync"
	"time"

	"angryduck/internal/logging"
	"angryduck/internal/memlimit"
	"angryduck/internal/metrics"
	"angryduck/internal/model"
	"angryduck/internal/sharedtoken"
)

var (
	retentionCopies = metrics.NewGaugeVec(
		"angryduck_controller_retention_copies",
		"Images nothing in the cluster runs anymore, kept as one retention copy, by state: held (a node keeps it), pending (no node picked yet: none has room, or none answered), copying (being copied to the node that will keep it).",
		"state",
	)
	retentionPaused = metrics.NewGaugeVec(
		"angryduck_controller_retention_paused",
		"1 while retention copies wait because every node's disk is at or above GC_RETENTION_MAX_UTILIZATION. They are picked as soon as a node is below it again, until their time runs out.",
	)
	retentionEventsTotal = metrics.NewCounterVec(
		"angryduck_controller_retention_events_total",
		"Retention events, by event: unused (an image went unused everywhere: a copy is due), kept (a node keeps the copy), copied (the copy was first copied to a node with room), used (the image runs again: no copy needed), lost (the node keeping it went away or dropped it: picked again), gone (no node holds the image anymore), expired.",
		"event",
	)
)

// RetainerConfig holds the retention tunables.
type RetainerConfig struct {
	TTL            time.Duration // how long a copy is kept after the image went unused
	MaxUtilization float64       // never pick a node whose disk is at or above this
	Interval       time.Duration // how often copies are checked without a wake
	OrderTimeout   time.Duration // one /images/retain call
	CopyTimeout    time.Duration // one copy to a node that doesn't hold the image
	Copy           bool          // may copy to a node with room that doesn't hold the image (needs rescue)
	Platform       string        // for reading an image's layers, to prefer nodes holding its blobs
}

// Retainer keeps one copy of every image nothing in the cluster runs
// anymore, for GC_RETENTION_TTL_S after it went unused: a rollback to it
// then finds it on a peer (the mirror, rescue) instead of the registry.
//
// Workers report the images their containers use and the copies they
// keep (WorkerReport.InUse, Retained). When an image goes from used
// somewhere to used nowhere, the Retainer picks one node to keep it: one
// below MaxUtilization that holds it, preferring a node holding the
// image's blobs (it can serve the mirror) and then the most free disk.
// If no node holding it has room, the image is copied to the node with
// the most room first. The chosen worker marks the image (RetainLabel),
// and its cleanup leaves it alone until it expires, then removes it.
//
// Nothing is kept in the controller that a restart would lose: the marks
// are on the images, every worker reports its own, and the Retainer picks
// them up from the reports.
//
// It works from events (a report that changed a node's image state wakes
// it) and checks every Interval for what time alone changes: a copy
// expiring, a node going away, a node getting below MaxUtilization again.
type Retainer struct {
	registry *Registry
	token    string
	cfg      RetainerConfig
	client   *http.Client
	resolver LayerResolver
	wake     chan struct{}

	// send posts a retain order (tests replace it); copyTo copies an image
	// to a node (tests replace it).
	send   func(ctx context.Context, addr string, o model.RetainOrder) (int, error)
	copyTo func(ctx context.Context, target *workerEntry, image string, sources []model.RescueSource) error

	mu      sync.Mutex
	records map[string]*retention // digest -> its copy
	used    map[string]string     // digest -> name: used somewhere at the last look
	spare   map[string]string     // the previous used, reused for the next look
	known   map[string]knownUse   // node -> its last reported in-use images
	release map[string]time.Time  // node+digest -> when a release was sent, to send it once
}

// knownUse is a node's last reported in-use images. A worker that just
// restarted reports no image state until its cleanup's first look; until
// then its containers still count as using what they used.
type knownUse struct {
	inUse []model.ImageRef
	at    time.Time
}

// knownFor is how long a node's last in-use images count while it
// reports no image state.
const knownFor = 5 * time.Minute

type retention struct {
	digest   string
	name     string
	unusedAt time.Time
	until    time.Time
	node     string    // "": none picked yet
	ordered  time.Time // when node was ordered to keep it
	copying  bool
	failed   map[string]time.Time // node -> when it refused or failed, skipped for a while
}

// retainGrace is how long a node ordered to keep a copy has to show it
// in its reports before the copy counts as lost.
const retainGrace = 2 * time.Minute

// retainSkipFor is how long a node that refused or failed is skipped.
const retainSkipFor = 10 * time.Minute

// NewRetainer builds the retainer and wires it to the registry's reports.
func NewRetainer(registry *Registry, token string, cfg RetainerConfig) *Retainer {
	if cfg.Interval <= 0 {
		cfg.Interval = 30 * time.Second
	}
	if cfg.OrderTimeout <= 0 {
		cfg.OrderTimeout = 5 * time.Second
	}
	if cfg.CopyTimeout <= 0 {
		cfg.CopyTimeout = 10 * time.Minute
	}
	rt := &Retainer{
		registry: registry,
		token:    token,
		cfg:      cfg,
		client:   &http.Client{},
		wake:     make(chan struct{}, 1),
		records:  map[string]*retention{},
		release:  map[string]time.Time{},
		known:    map[string]knownUse{},
	}
	rt.send = rt.sendOrder
	rt.copyTo = rt.copyImage
	for _, s := range []string{"held", "pending", "copying"} {
		retentionCopies.Set(0, s)
	}
	retentionPaused.Set(0)
	for _, e := range []string{"unused", "kept", "copied", "used", "lost", "gone", "expired"} {
		retentionEventsTotal.Add(0, e)
	}
	registry.OnImageState(rt.Wake)
	return rt
}

// SetLayerResolver lets the retainer read an image's layers, to prefer
// nodes holding its blobs.
func (rt *Retainer) SetLayerResolver(r LayerResolver) { rt.resolver = r }

// Wake asks for a look now. Non-blocking.
func (rt *Retainer) Wake() {
	select {
	case rt.wake <- struct{}{}:
	default:
	}
}

// Run blocks until ctx is done.
func (rt *Retainer) Run(ctx context.Context) {
	logging.Infof("angryduck-controller: retention on: ttl=%s max_utilization=%.2f copy_to_nodes_with_room=%t", rt.cfg.TTL, rt.cfg.MaxUtilization, rt.cfg.Copy)
	t := time.NewTicker(rt.cfg.Interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-rt.wake:
			rt.Sweep(ctx)
		case <-t.C:
			rt.Sweep(ctx)
		}
		memlimit.Soon()
	}
}

// Sweep is one look: which images went unused or run again, which copies
// expired or were lost, and a node for every copy that has none.
func (rt *Retainer) Sweep(ctx context.Context) {
	now := time.Now()
	workers := rt.registry.FreshWorkers()
	byNode := make(map[string]*workerEntry, len(workers))
	for _, w := range workers {
		byNode[w.NodeID] = w
	}

	rt.mu.Lock()
	// Which images run anywhere. A node not reporting image state right
	// now (a restarted worker before its first cleanup look) counts with
	// what it last reported, for a while.
	used := rt.spare
	if used == nil {
		used = make(map[string]string, len(rt.used))
	}
	clear(used)
	for _, w := range workers {
		inUse := w.InUse
		if w.ImageState {
			rt.known[w.NodeID] = knownUse{inUse: w.InUse, at: now}
		} else if k, ok := rt.known[w.NodeID]; ok && now.Sub(k.at) < knownFor {
			inUse = k.inUse
		}
		for _, ref := range inUse {
			used[ref.Digest] = ref.Name
		}
	}
	for node, k := range rt.known {
		if _, fresh := byNode[node]; !fresh || now.Sub(k.at) >= knownFor {
			delete(rt.known, node)
		}
	}
	if rt.used != nil {
		for d, name := range rt.used {
			if _, still := used[d]; !still {
				rt.unusedLocked(d, name, now)
			}
		}
	}
	for d, name := range used {
		if rec := rt.records[d]; rec != nil {
			if rec.name == "" {
				rec.name = name // adopted after a restart: now it has one
			}
			rt.usedAgainLocked(rec)
		}
	}
	rt.spare, rt.used = rt.used, used
	releases := rt.reconcileLocked(workers, now)
	var picks []*retention
	for d, rec := range rt.records {
		switch {
		case !now.Before(rec.until):
			delete(rt.records, d)
			retentionEventsTotal.Inc("expired")
			recordRetention(rec.name, rec.node, "expired", now)
			continue
		case rec.copying:
			continue
		case rec.node != "":
			w := byNode[rec.node]
			lost := w == nil // gone or not reporting
			if !lost && w.ImageState && now.Sub(rec.ordered) > retainGrace {
				_, kept := w.Retained[d]
				lost = !kept // dropped (removed by hand, kubelet's GC, a new disk)
			}
			if !lost {
				continue
			}
			logging.Infof("angryduck-controller: retention: node=%s no longer keeps %s: picking another", rec.node, rec.label())
			retentionEventsTotal.Inc("lost")
			recordRetention(rec.name, rec.node, "lost", now)
			rec.node = ""
		}
		picks = append(picks, rec)
	}
	rt.mu.Unlock()

	for _, rel := range releases {
		rt.sendRelease(ctx, rel.addr, rel.node, rel.digest)
	}
	if len(picks) > 0 {
		sort.Slice(picks, func(i, j int) bool { return picks[i].unusedAt.Before(picks[j].unusedAt) })
		eligible := rt.eligible(workers)
		paused := len(eligible) == 0
		for _, rec := range picks {
			if paused {
				break
			}
			rt.pick(ctx, rec, workers, eligible, now)
		}
		if paused {
			retentionPaused.Set(1)
		} else {
			retentionPaused.Set(0)
		}
	} else {
		retentionPaused.Set(0)
	}
	rt.publish()
}

// unusedLocked starts a copy for an image nothing runs anymore.
func (rt *Retainer) unusedLocked(digest, name string, now time.Time) {
	if rt.records[digest] != nil {
		return
	}
	rt.records[digest] = &retention{digest: digest, name: name, unusedAt: now, until: now.Add(rt.cfg.TTL)}
	retentionEventsTotal.Inc("unused")
	recordRetention(name, "", "unused", now)
	logging.Infof("angryduck-controller: retention: %s went unused in the cluster: keeping one copy until %s", label(name, digest), now.Add(rt.cfg.TTL).UTC().Format(time.RFC3339))
}

// usedAgainLocked ends a copy for an image that runs again. The node
// keeping it is told to drop the mark by reconcile, from its report.
func (rt *Retainer) usedAgainLocked(rec *retention) {
	delete(rt.records, rec.digest)
	retentionEventsTotal.Inc("used")
	recordRetention(rec.name, rec.node, "used", time.Now())
	logging.Infof("angryduck-controller: retention: %s runs again: no copy needed", rec.label())
}

type releaseOrder struct{ node, addr, digest string }

// reconcileLocked matches the marks workers report with the records:
// adopts the marks it has no record of (a controller restart), and
// returns the marks to remove (an image running again, or a second copy).
func (rt *Retainer) reconcileLocked(workers []*workerEntry, now time.Time) []releaseOrder {
	var out []releaseOrder
	seen := map[string]bool{}
	for _, w := range workers {
		for d, until := range w.Retained {
			key := w.NodeID + "\x00" + d
			seen[key] = true
			_, runs := rt.used[d]
			rec := rt.records[d]
			switch {
			case runs, rec != nil && rec.node != "" && rec.node != w.NodeID:
				if t, ok := rt.release[key]; !ok || now.Sub(t) > retainGrace {
					rt.release[key] = now
					out = append(out, releaseOrder{node: w.NodeID, addr: w.Address, digest: d})
				}
			case rec == nil:
				exp := time.Unix(until, 0)
				if !now.Before(exp) {
					continue // the worker removes it at its next cleanup
				}
				rt.records[d] = &retention{digest: d, node: w.NodeID, until: exp, unusedAt: exp.Add(-rt.cfg.TTL), ordered: now}
				logging.Infof("angryduck-controller: retention: node=%s keeps %s until %s", w.NodeID, d, exp.UTC().Format(time.RFC3339))
			case rec.node == "" && !rec.copying:
				rec.node, rec.ordered = w.NodeID, now // an order whose answer was lost
			}
		}
	}
	for key := range rt.release {
		if !seen[key] {
			delete(rt.release, key) // the mark is gone
		}
	}
	return out
}

// eligible returns the workers that may keep a copy: reporting image
// state (retention on there) and below MaxUtilization, least full first.
func (rt *Retainer) eligible(workers []*workerEntry) []*workerEntry {
	var out []*workerEntry
	for _, w := range workers { // FreshWorkers sorts least full first
		if w.ImageState && w.Utilization < rt.cfg.MaxUtilization {
			out = append(out, w)
		}
	}
	return out
}

// pick finds a node to keep rec's copy and orders it to.
func (rt *Retainer) pick(ctx context.Context, rec *retention, workers, eligible []*workerEntry, now time.Time) {
	var candidates []*workerEntry
	for _, w := range eligible {
		if t, ok := rec.failed[w.NodeID]; ok && now.Sub(t) < retainSkipFor {
			continue
		}
		candidates = append(candidates, w)
	}
	if len(candidates) == 0 {
		return
	}
	ids := make([]string, len(candidates))
	for i, w := range candidates {
		ids[i] = w.NodeID
	}
	holding := rt.registry.Layers().Holders(rec.digest, ids)
	if len(holding) > 0 {
		holders := make([]*workerEntry, 0, len(holding))
		isHolder := make(map[string]bool, len(holding))
		for _, n := range holding {
			isHolder[n] = true
		}
		for _, w := range candidates {
			if isHolder[w.NodeID] {
				holders = append(holders, w)
			}
		}
		rt.preferBlobHolders(ctx, rec, holders)
		for _, w := range holders {
			if rt.order(ctx, rec, w, now) {
				return
			}
		}
		return
	}
	// No node with room holds it: copy it to the one with the most room,
	// from any node that still holds it.
	all := make([]string, len(workers))
	for i, w := range workers {
		all[i] = w.NodeID
	}
	sources := rt.registry.Layers().Holders(rec.digest, all)
	if len(sources) == 0 {
		rt.mu.Lock()
		if rt.records[rec.digest] == rec {
			delete(rt.records, rec.digest)
			retentionEventsTotal.Inc("gone")
			recordRetention(rec.name, "", "gone", time.Now())
			logging.Infof("angryduck-controller: retention: no node holds %s anymore: nothing to keep", rec.label())
		}
		rt.mu.Unlock()
		return
	}
	if !rt.cfg.Copy || rec.name == "" {
		return // waits for a holder to get below the limit
	}
	target := candidates[0]
	addr := map[string]string{}
	for _, w := range workers {
		addr[w.NodeID] = w.Address
	}
	var srcs []model.RescueSource
	for _, n := range sources {
		if len(srcs) == 3 {
			break
		}
		srcs = append(srcs, model.RescueSource{NodeID: n, Address: addr[n]})
	}
	rt.mu.Lock()
	rec.copying = true
	rt.mu.Unlock()
	go func() {
		cctx, cancel := context.WithTimeout(ctx, rt.cfg.CopyTimeout)
		err := rt.copyTo(cctx, target, rec.name, srcs)
		cancel()
		if err == nil {
			retentionEventsTotal.Inc("copied")
			recordRetention(rec.name, target.NodeID, "copied", time.Now())
			logging.Infof("angryduck-controller: retention: copied %s to node=%s, which has room to keep it", rec.label(), target.NodeID)
			rt.order(ctx, rec, target, time.Now())
		} else {
			logging.Warnf("angryduck-controller: retention: copying %s to node=%s: %v", rec.label(), target.NodeID, err)
			rt.mu.Lock()
			rec.fail(target.NodeID, time.Now())
			rt.mu.Unlock()
		}
		rt.mu.Lock()
		rec.copying = false
		rt.mu.Unlock()
		rt.Wake()
	}()
}

// order tells w to keep rec's copy. It reports whether w took it.
func (rt *Retainer) order(ctx context.Context, rec *retention, w *workerEntry, now time.Time) bool {
	rt.mu.Lock()
	until := rec.until
	rt.mu.Unlock()
	code, err := rt.send(ctx, w.Address, model.RetainOrder{Digest: rec.digest, Until: until.Unix()})
	rt.mu.Lock()
	defer rt.mu.Unlock()
	if err != nil || code != http.StatusOK {
		rec.fail(w.NodeID, now)
		logging.Debugf("angryduck-controller: retention: node=%s didn't take %s: status=%d err=%v", w.NodeID, rec.label(), code, err)
		return false
	}
	if rt.records[rec.digest] != rec {
		return true // ended meanwhile (runs again); the report will tell the node
	}
	rec.node, rec.ordered = w.NodeID, now
	retentionEventsTotal.Inc("kept")
	recordRetention(rec.name, w.NodeID, "kept", now)
	logging.Infof("angryduck-controller: retention: node=%s keeps %s until %s (disk %.0f%%)", w.NodeID, rec.label(), until.UTC().Format(time.RFC3339), w.Utilization*100)
	return true
}

func (rec *retention) fail(node string, now time.Time) {
	if rec.failed == nil {
		rec.failed = map[string]time.Time{}
	}
	rec.failed[node] = now
}

func (rec *retention) label() string { return label(rec.name, rec.digest) }

func label(name, digest string) string {
	if name == "" {
		return digest
	}
	return name
}

// preferBlobHolders moves nodes holding more of the image's blobs to the
// front, keeping the least-full order among equals: they can serve the
// mirror, where a node holding only snapshots can't.
func (rt *Retainer) preferBlobHolders(ctx context.Context, rec *retention, holders []*workerEntry) {
	if rt.resolver == nil || rec.name == "" || len(holders) < 2 {
		return
	}
	rctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	layers, err := rt.resolver.Layers(rctx, rec.name, rt.cfg.Platform)
	cancel()
	if err != nil || len(layers) == 0 {
		return // keep the least-full order
	}
	have := make(map[string]int64, len(holders))
	for _, w := range holders {
		have[w.NodeID] = rt.registry.Layers().BlobBytes(w.NodeID, layers)
	}
	sort.SliceStable(holders, func(i, j int) bool { return have[holders[i].NodeID] > have[holders[j].NodeID] })
}

func (rt *Retainer) sendRelease(ctx context.Context, addr, node, digest string) {
	code, err := rt.send(ctx, addr, model.RetainOrder{Digest: digest, Until: 0})
	if err != nil || code != http.StatusOK {
		logging.Debugf("angryduck-controller: retention: release of %s on node=%s: status=%d err=%v", digest, node, code, err)
		return
	}
	logging.Infof("angryduck-controller: retention: node=%s no longer keeps %s", node, digest)
}

// sendOrder posts o to the worker's /images/retain and returns the status.
func (rt *Retainer) sendOrder(ctx context.Context, addr string, o model.RetainOrder) (int, error) {
	body, err := json.Marshal(o)
	if err != nil {
		return 0, err
	}
	ctx, cancel := context.WithTimeout(ctx, rt.cfg.OrderTimeout)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, "http://"+addr+"/images/retain", bytes.NewReader(body))
	if err != nil {
		return 0, err
	}
	req.Header.Set("Content-Type", "application/json")
	sharedtoken.Set(req, rt.token)
	resp, err := rt.client.Do(req)
	if err != nil {
		return 0, err
	}
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 4<<10))
	resp.Body.Close()
	return resp.StatusCode, nil
}

// copyImage copies image to target from sources, the way rescue does.
func (rt *Retainer) copyImage(ctx context.Context, target *workerEntry, image string, sources []model.RescueSource) error {
	res, err := sendRescueOrder(ctx, rt.client, rt.token, target.Address, model.RescueOrder{Image: image, Sources: sources, Reason: "retain"})
	if err != nil {
		return err
	}
	if !res.OK {
		return fmt.Errorf("%s", res.Error)
	}
	return nil
}

// publish sets the copies gauge.
func (rt *Retainer) publish() {
	rt.mu.Lock()
	defer rt.mu.Unlock()
	var held, pending, copying int
	for _, rec := range rt.records {
		switch {
		case rec.copying:
			copying++
		case rec.node != "":
			held++
		default:
			pending++
		}
	}
	retentionCopies.Set(float64(held), "held")
	retentionCopies.Set(float64(pending), "pending")
	retentionCopies.Set(float64(copying), "copying")
}

// Status lists every copy, for /status.
func (rt *Retainer) Status() []model.RetentionStatus {
	rt.mu.Lock()
	defer rt.mu.Unlock()
	out := make([]model.RetentionStatus, 0, len(rt.records))
	for _, rec := range rt.records {
		out = append(out, model.RetentionStatus{Digest: rec.digest, Image: rec.name, Node: rec.node, UnusedAt: rec.unusedAt, Until: rec.until, Copying: rec.copying})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Digest < out[j].Digest })
	return out
}

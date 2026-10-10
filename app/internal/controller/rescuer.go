package controller

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strings"
	"sync"
	"time"

	"angryduck/internal/imageref"
	"angryduck/internal/kube"
	"angryduck/internal/logging"
	"angryduck/internal/metrics"
	"angryduck/internal/model"
	"angryduck/internal/sharedtoken"
)

// rescuesTotal counts rescue decisions per target node. result is one of:
// success, failure, no_source (no fresh worker has the image),
// no_target (no fresh worker on the stuck pod's node), pull_policy_always
// (kubelet would ignore a local copy anyway).
var (
	rescuesTotal = metrics.NewCounterVec(
		"angryduck_controller_rescues_total",
		"ImagePullBackOff rescues decided by the controller, by target node and result.",
		"node", "result",
	)
	rescueStuckImages = metrics.NewGaugeVec(
		"angryduck_controller_rescue_stuck_images",
		"Images each node currently can't pull (pods in ImagePullBackOff/ErrImagePull), as of the last rescuer tick.",
		"node",
	)
	rescueConsecutiveFailures = metrics.NewGaugeVec(
		"angryduck_controller_rescue_consecutive_failures",
		"Failed rescue attempts in a row for one node and image. Only stuck pairs with at least one failure are listed.",
		"node", "image",
	)
	rescueStuckPods = metrics.NewGaugeVec(
		"angryduck_controller_rescue_stuck_pods",
		"Pods stuck pulling an image on a node, per node and image, and whether a rescue is running for it (in_flight 1 or 0). Only pairs stuck as of the last rescuer tick.",
		"node", "image", "in_flight",
	)
	rescuesInFlight = metrics.NewGaugeVec(
		"angryduck_controller_rescues_in_flight",
		"Rescues currently running.",
	)
)

// PodLister is the Kubernetes API call the rescuer needs.
type PodLister interface {
	ListPods(ctx context.Context, fieldSelector string) ([]kube.Pod, error)
}

// RescuerConfig holds the rescuer's tunables.
type RescuerConfig struct {
	Interval      time.Duration // how often Pending pods are listed
	RetryAfter    time.Duration // wait before the next attempt; doubles after each failure
	BackoffMax    time.Duration // cap for that doubling
	Timeout       time.Duration // max wait for one rescue to finish
	MaxConcurrent int           // rescues in flight across the cluster
	MaxSources    int           // candidate sources sent per order
}

// Rescuer fixes pods stuck in ImagePullBackOff (or ErrImagePull) by having
// the stuck node copy the image from a peer that already has it, instead
// of from the registry. It exists for the cases where the registry can't
// serve the image to that node (a broken route, a firewall rule, an image
// deleted upstream) but the image is sitting on other nodes.
//
// Each tick it reads the Pending pods (watched, see kube.PodWatch), groups
// stuck containers by (node, image), and for each one sends the node's
// worker a RescueOrder naming a
// few fresh workers that reported the exact image. The worker does the
// transfer (see internal/worker/rescue.go). Nothing here restarts pods:
// kubelet's own backoff retry (at most 5 minutes) finds the image locally.
//
// A pair that keeps failing is retried with exponential backoff, from
// RetryAfter up to BackoffMax, so a permanent failure costs a few attempts
// an hour instead of one every RetryAfter. The backoff resets when the
// pair stops being stuck.
type Rescuer struct {
	registry *Registry
	pods     PodLister
	token    string
	cfg      RescuerConfig
	client   *http.Client
	sem      chan struct{}

	mu    sync.Mutex
	pairs map[string]*pairState
	// waiting is, per image, the nodes where a Pending pod needs it, as
	// of the last tick. The propagator serves those nodes first.
	waiting  map[string]map[string]bool
	resolver LayerResolver // nil: sources ordered by utilization only
	platform string
	wg       sync.WaitGroup // in-flight sends, for tests

	// backlog: the last tick stopped at the cluster-wide limit with
	// rescues still due. The next rescue to finish wakes the loop so they
	// don't wait for the next RESCUE_INTERVAL_S.
	backlog bool
	wakeup  waker
}

// SetLayerResolver orders rescue sources that hold the image's blobs
// first (a blob is digest-verified on arrival, a snapshot copy is not).
func (rs *Rescuer) SetLayerResolver(r LayerResolver, platform string) {
	rs.resolver, rs.platform = r, platform
}

// Waiting returns, per image, the nodes where a Pending pod needs it.
// Wake runs a check now (settled), instead of at the next tick: the pod
// watch calls it on every change.
func (rs *Rescuer) Wake() { rs.wakeup.wake() }

func (rs *Rescuer) Waiting() map[string]map[string]bool {
	rs.mu.Lock()
	defer rs.mu.Unlock()
	return rs.waiting
}

// StuckImages returns the images some node is stuck pulling right now;
// the image cleanup never removes them from anywhere.
func (rs *Rescuer) StuckImages() map[string]bool {
	rs.mu.Lock()
	defer rs.mu.Unlock()
	out := make(map[string]bool, len(rs.pairs))
	for _, st := range rs.pairs {
		out[st.Image] = true
	}
	return out
}

// pairState is what the rescuer remembers about one stuck node+image.
type pairState struct {
	stuckImage
	since      time.Time
	inFlight   bool
	failures   int
	next       time.Time
	lastResult string
	lastError  string
}

// NewRescuer builds a rescuer.
func NewRescuer(registry *Registry, pods PodLister, token string, cfg RescuerConfig) *Rescuer {
	rescuesInFlight.Set(0)
	registry.OnNewNode(func(node string) {
		for _, r := range []string{"success", "failure", "no_source", "no_target", "pull_policy_always"} {
			rescuesTotal.Add(0, node, r)
		}
	})
	if cfg.MaxConcurrent < 1 {
		cfg.MaxConcurrent = 1
	}
	if cfg.MaxSources < 1 {
		cfg.MaxSources = 1
	}
	if cfg.BackoffMax < cfg.RetryAfter {
		cfg.BackoffMax = cfg.RetryAfter
	}
	return &Rescuer{
		registry: registry,
		pods:     pods,
		token:    token,
		cfg:      cfg,
		client:   &http.Client{Timeout: cfg.Timeout},
		sem:      make(chan struct{}, cfg.MaxConcurrent),
		pairs:    make(map[string]*pairState),
		wakeup:   newWaker(),
	}
}

// Run blocks, ticking until ctx is done.
func (rs *Rescuer) Run(ctx context.Context) {
	ticker := time.NewTicker(rs.cfg.Interval)
	defer ticker.Stop()
	logging.Infof("angryduck-controller: rescuer started: interval=%s retry_after=%s backoff_max=%s timeout=%s max_concurrent=%d max_sources=%d",
		rs.cfg.Interval, rs.cfg.RetryAfter, rs.cfg.BackoffMax, rs.cfg.Timeout, rs.cfg.MaxConcurrent, rs.cfg.MaxSources)
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			rs.tick(ctx)
		case <-rs.wakeup:
			if !rs.wakeup.settle(ctx) {
				return
			}
			rs.tick(ctx)
		}
	}
}

// stuckImage is one image one node can't pull, and who is waiting on it.
type stuckImage struct {
	Node       string
	Image      string
	Pods       []string // namespace/name
	PullAlways bool
}

func (s stuckImage) key() string { return s.Node + "|" + s.Image }

// backoff is the wait after the given number of consecutive failures.
func (rs *Rescuer) backoff(failures int) time.Duration {
	d := rs.cfg.RetryAfter
	for i := 1; i < failures && d < rs.cfg.BackoffMax; i++ {
		d *= 2
	}
	if d > rs.cfg.BackoffMax {
		d = rs.cfg.BackoffMax
	}
	return d
}

func (rs *Rescuer) tick(ctx context.Context) {
	rs.mu.Lock()
	rs.backlog = false
	rs.mu.Unlock()
	listCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	pods, err := rs.pods.ListPods(listCtx, "status.phase=Pending")
	cancel()
	if err != nil {
		logging.Warnf("angryduck-controller: rescuer: listing pending pods failed: %v", err)
		return
	}
	stuck := findStuck(pods)
	now := time.Now()
	waiting := findWaiting(pods)

	rs.mu.Lock()
	rs.waiting = waiting
	current := make(map[string]bool, len(stuck))
	for _, s := range stuck {
		current[s.key()] = true
		st, ok := rs.pairs[s.key()]
		if !ok {
			st = &pairState{since: now}
			rs.pairs[s.key()] = st
		}
		st.stuckImage = s // pods may have changed
	}
	for key, st := range rs.pairs {
		if !current[key] && !st.inFlight {
			if st.failures > 0 || st.lastResult != "" {
				logging.Infof("angryduck-controller: rescuer: node=%s image=%s is no longer stuck", st.Node, st.Image)
			}
			delete(rs.pairs, key) // resolved (or the pod is gone): forget its backoff
		}
	}
	rs.publishMetricsLocked()
	rs.mu.Unlock()
	if len(stuck) == 0 {
		return
	}

	fresh := rs.registry.FreshWorkers() // sorted by utilization, lowest first
	byNode := make(map[string]*workerEntry, len(fresh))
	for _, w := range fresh {
		byNode[w.NodeID] = w
	}

	for _, s := range stuck {
		rs.mu.Lock()
		st := rs.pairs[s.key()]
		due := !st.inFlight && !now.Before(st.next)
		rs.mu.Unlock()
		if !due {
			continue
		}

		target := byNode[s.Node]
		switch {
		case s.PullAlways:
			rs.skip(st, "pull_policy_always", "imagePullPolicy is Always, so kubelet would go to the registry even with a local copy")
			continue
		case target == nil:
			rs.skip(st, "no_target", "no fresh worker on that node")
			continue
		case target.HasImage(s.Image):
			// Already rescued (or pulled); kubelet just hasn't retried yet.
			logging.Debugf("angryduck-controller: rescuer: node=%s already has image=%s, waiting for kubelet's next retry", s.Node, s.Image)
			continue
		}

		var sources []model.RescueSource
		for _, w := range fresh {
			if w.NodeID != s.Node && w.HasImage(s.Image) {
				sources = append(sources, model.RescueSource{NodeID: w.NodeID, Address: w.Address})
				if len(sources) == rs.cfg.MaxSources {
					break
				}
			}
		}
		if len(sources) == 0 {
			rs.skip(st, "no_source", "no fresh worker has that image")
			continue
		}
		rs.preferBlobHolders(ctx, s.Image, sources)

		select {
		case rs.sem <- struct{}{}:
		default:
			// Cluster-wide limit reached: the rest start as soon as a
			// running rescue finishes.
			rs.mu.Lock()
			rs.backlog = true
			rs.mu.Unlock()
			return
		}
		rs.mu.Lock()
		st.inFlight = true
		rs.mu.Unlock()
		rs.wg.Add(1)
		go rs.send(ctx, st, s, target, sources)
	}
}

// skip records a decision not to try. It doesn't count as a failure (the
// situation can change any moment, e.g. a source appears), so it is
// re-checked every RetryAfter.
func (rs *Rescuer) skip(st *pairState, result, why string) {
	rs.mu.Lock()
	changed := st.lastResult != result
	st.lastResult, st.lastError = result, why
	st.next = time.Now().Add(rs.cfg.RetryAfter)
	rs.mu.Unlock()
	rescuesTotal.Inc(st.Node, result)
	// Log on change only: an unrescuable pod would otherwise log every
	// RetryAfter for as long as it exists.
	if changed {
		logging.Infof("angryduck-controller: rescuer: not rescuing image=%s on node=%s for %s: %s", st.Image, st.Node, strings.Join(st.Pods, ","), why)
	}
}

func (rs *Rescuer) send(ctx context.Context, st *pairState, s stuckImage, target *workerEntry, sources []model.RescueSource) {
	defer rs.wg.Done()
	defer func() {
		<-rs.sem
		rs.mu.Lock()
		backlog := rs.backlog
		rs.mu.Unlock()
		if backlog {
			rs.wakeup.wake()
		}
	}()

	names := make([]string, len(sources))
	for i, src := range sources {
		names[i] = src.NodeID
	}
	logging.Infof("angryduck-controller: rescuer: ordering node=%s to fetch image=%s from peers [%s] for %s",
		s.Node, s.Image, strings.Join(names, ","), strings.Join(s.Pods, ","))

	start := time.Now()
	result, err := sendRescueOrder(ctx, rs.client, rs.token, target.Address, model.RescueOrder{Image: s.Image, Sources: sources, Reason: "rescue"})

	rs.mu.Lock()
	st.inFlight = false
	if err != nil {
		st.failures++
		st.lastResult, st.lastError = "failure", err.Error()
		wait := rs.backoff(st.failures)
		st.next = time.Now().Add(wait)
		failures := st.failures
		rs.mu.Unlock()
		logging.Warnf("angryduck-controller: rescuer: rescue of image=%s on node=%s failed after %s (%d in a row, next try in %s): %v",
			s.Image, s.Node, time.Since(start).Round(time.Millisecond), failures, wait, err)
		rescuesTotal.Inc(s.Node, "failure")
		return
	}
	st.failures = 0
	st.lastResult, st.lastError = "success", ""
	st.next = time.Now().Add(rs.cfg.RetryAfter)
	rs.mu.Unlock()
	logging.Infof("angryduck-controller: rescuer: node=%s now has image=%s (from %s: %d blob(s), %d snapshot(s), %d bytes, %s); kubelet picks it up on its next retry",
		s.Node, s.Image, orDash(result.Source), result.Blobs, result.Snapshots, result.Bytes, time.Since(start).Round(time.Millisecond))
	rescuesTotal.Inc(s.Node, "success")
}

// publishMetricsLocked refreshes the gauges from the pair table. Caller
// holds rs.mu.
func (rs *Rescuer) publishMetricsLocked() {
	rescueStuckImages.Reset()
	rescueConsecutiveFailures.Reset()
	rescueStuckPods.Reset()
	perNode := map[string]int{}
	inFlight := 0
	for _, st := range rs.pairs {
		perNode[st.Node]++
		if st.failures > 0 {
			rescueConsecutiveFailures.Set(float64(st.failures), st.Node, st.Image)
		}
		flight := "0"
		if st.inFlight {
			inFlight++
			flight = "1"
		}
		rescueStuckPods.Set(float64(len(st.Pods)), st.Node, st.Image, flight)
	}
	for node, n := range perNode {
		rescueStuckImages.Set(float64(n), node)
	}
	rescuesInFlight.Set(float64(inFlight))
}

// Status returns every stuck pair the rescuer knows about, for /status.
func (rs *Rescuer) Status() []model.RescueStatus {
	rs.mu.Lock()
	defer rs.mu.Unlock()
	out := make([]model.RescueStatus, 0, len(rs.pairs))
	for _, st := range rs.pairs {
		out = append(out, model.RescueStatus{
			Node: st.Node, Image: st.Image, Pods: append([]string(nil), st.Pods...),
			StuckSince: st.since, InFlight: st.inFlight,
			LastResult: st.lastResult, LastError: st.lastError,
			ConsecutiveFailures: st.failures, NextAttempt: st.next,
		})
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Node != out[j].Node {
			return out[i].Node < out[j].Node
		}
		return out[i].Image < out[j].Image
	})
	return out
}

// sendRescueOrder posts order to the worker at addr and waits for the
// result. Rescue and propagation both go through it.
func sendRescueOrder(ctx context.Context, client *http.Client, token, addr string, order model.RescueOrder) (model.RescueResult, error) {
	body, err := json.Marshal(order)
	if err != nil {
		return model.RescueResult{}, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, "http://"+addr+"/rescue", bytes.NewReader(body))
	if err != nil {
		return model.RescueResult{}, err
	}
	req.Header.Set("Content-Type", "application/json")
	sharedtoken.Set(req, token)
	resp, err := client.Do(req)
	if err != nil {
		return model.RescueResult{}, err
	}
	defer resp.Body.Close()
	var result model.RescueResult
	if err := json.NewDecoder(io.LimitReader(resp.Body, 64<<10)).Decode(&result); err != nil {
		return model.RescueResult{}, fmt.Errorf("status %d, unreadable body: %v", resp.StatusCode, err)
	}
	if resp.StatusCode != http.StatusOK || !result.OK {
		return result, fmt.Errorf("status %d: %s", resp.StatusCode, result.Error)
	}
	return result, nil
}

// preferBlobHolders moves sources holding more of the image's blobs to the
// front, keeping the utilization order among equals.
func (rs *Rescuer) preferBlobHolders(ctx context.Context, image string, sources []model.RescueSource) {
	if rs.resolver == nil || len(sources) < 2 {
		return
	}
	rctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	layers, err := rs.resolver.Layers(rctx, image, rs.platform)
	cancel()
	if err != nil {
		return // the registry may be the reason we're rescuing
	}
	blob := make(map[string]int64, len(sources))
	for _, src := range sources {
		blob[src.NodeID] = rs.registry.Layers().BlobBytes(src.NodeID, layers)
	}
	sort.SliceStable(sources, func(i, j int) bool { return blob[sources[i].NodeID] > blob[sources[j].NodeID] })
}

// findWaiting maps each image to the nodes where a scheduled, Pending pod
// uses it.
func findWaiting(pods []kube.Pod) map[string]map[string]bool {
	out := map[string]map[string]bool{}
	for _, p := range pods {
		node := p.Spec.NodeName
		if node == "" {
			continue
		}
		for _, cs := range [][]kube.Container{p.Spec.InitContainers, p.Spec.Containers} {
			for _, c := range cs {
				img := imageref.Normalize(c.Image)
				if out[img] == nil {
					out[img] = map[string]bool{}
				}
				out[img][node] = true
			}
		}
	}
	return out
}

// pullBackoffReasons are the waiting reasons kubelet sets while it can't
// pull. ErrImagePull is the first failure, ImagePullBackOff every one after.
var pullBackoffReasons = map[string]bool{"ImagePullBackOff": true, "ErrImagePull": true}

// findStuck extracts (node, image) pairs stuck on pulls, merging every pod
// and container waiting on the same pair. Sorted for stable ordering.
func findStuck(pods []kube.Pod) []stuckImage {
	byKey := make(map[string]*stuckImage)
	for _, p := range pods {
		node := p.Spec.NodeName
		if node == "" {
			continue // not scheduled yet: nothing to pull onto
		}
		specs := make(map[string]kube.Container)
		for _, c := range p.Spec.InitContainers {
			specs[c.Name] = c
		}
		for _, c := range p.Spec.Containers {
			specs[c.Name] = c
		}
		statuses := append(append([]kube.ContainerStatus{}, p.Status.InitContainerStatuses...), p.Status.ContainerStatuses...)
		for _, cs := range statuses {
			if cs.State.Waiting == nil || !pullBackoffReasons[cs.State.Waiting.Reason] {
				continue
			}
			spec := specs[cs.Name]
			image := spec.Image
			if image == "" {
				image = cs.Image
			}
			if image == "" {
				continue
			}
			image = imageref.Normalize(image)
			key := node + "|" + image
			s, ok := byKey[key]
			if !ok {
				s = &stuckImage{Node: node, Image: image}
				byKey[key] = s
			}
			s.Pods = appendUnique(s.Pods, p.Metadata.Namespace+"/"+p.Metadata.Name)
			// kubelet defaults imagePullPolicy to Always for :latest and
			// untagged images; the API server fills it in, so it is
			// always present on real pods.
			if spec.ImagePullPolicy == "Always" {
				s.PullAlways = true
			}
		}
	}
	out := make([]stuckImage, 0, len(byKey))
	for _, s := range byKey {
		out = append(out, *s)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].key() < out[j].key() })
	return out
}

func appendUnique(list []string, v string) []string {
	for _, x := range list {
		if x == v {
			return list
		}
	}
	return append(list, v)
}

func orDash(s string) string {
	if s == "" {
		return "-"
	}
	return s
}

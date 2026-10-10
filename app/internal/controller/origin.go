package controller

import (
	"context"
	"math"
	"sort"
	"strings"
	"time"

	"angryduck/internal/imageref"
	"angryduck/internal/logging"
	"angryduck/internal/metrics"
	"angryduck/internal/registryclient"
)

var (
	originImages = metrics.NewGaugeVec(
		"angryduck_controller_origin_images",
		"Distinct images held on fresh nodes, by registry and what that registry answered at the last origin check: ok, missing (no such manifest), denied (Angry Duck's credentials were refused, or none are configured and the registry wants some), unreachable (no usable answer) or unchecked (not checked yet).",
		"registry", "status",
	)
	originUnavailable = metrics.NewGaugeVec(
		"angryduck_controller_origin_unavailable",
		"Images held on nodes that their registry did not serve at the last origin check, by status (missing or denied). The value is how many fresh nodes still hold a copy: once the last one removes it, nothing can pull the image again.",
		"image", "status",
	)
	originChecksTotal = metrics.NewCounterVec(
		"angryduck_controller_origin_checks_total",
		"Origin checks (a HEAD request for the image's manifest at its registry, with Angry Duck's credentials), by result: ok, missing, denied, unreachable.",
		"status",
	)
)

var (
	originStatuses      = []string{"ok", "missing", "denied", "unreachable"}
	originImageStatuses = append(originStatuses[:len(originStatuses):len(originStatuses)], "unchecked")
)

// ManifestChecker asks an image's registry whether it still serves the
// image (registryclient.Client).
type ManifestChecker interface {
	CheckManifest(ctx context.Context, image string) error
}

// OriginConfig holds the origin checker's tunables.
type OriginConfig struct {
	Interval   time.Duration // each held image is checked this often; failing ones four times as often
	Delay      time.Duration // before the first check, so startup and the first reports come first
	Tick       time.Duration
	Timeout    time.Duration // per check
	Registries []string      // only registry hosts containing one of these; empty: all
}

// OriginChecker keeps asking the registries whether they still serve the
// images the nodes hold. Peers, the mirror, rescue and the cleanup can
// keep an image alive on the nodes long after its registry stopped
// serving it (the tag was deleted, the pull secret was rotated), and
// nothing else would notice until the last copy is removed. It only
// sends HEAD requests for manifests, one at a time, spread evenly over
// Interval, and only publishes metrics. It runs on its own goroutine with
// its own registry client and takes the Registry's read lock only to
// copy the inventory, so a slow or hanging registry delays nothing but
// the next check.
type OriginChecker struct {
	registry *Registry
	checker  ManifestChecker
	cfg      OriginConfig
	state    map[string]originState // by image; owned by the loop
}

type originState struct {
	status  string
	checked time.Time
}

// NewOriginChecker builds an origin checker.
func NewOriginChecker(registry *Registry, checker ManifestChecker, cfg OriginConfig) *OriginChecker {
	if cfg.Tick <= 0 {
		cfg.Tick = 30 * time.Second
	}
	if cfg.Timeout <= 0 {
		cfg.Timeout = 10 * time.Second
	}
	for _, s := range originStatuses {
		originChecksTotal.Add(0, s)
	}
	return &OriginChecker{registry: registry, checker: checker, cfg: cfg, state: map[string]originState{}}
}

// Run blocks, checking until ctx is done.
func (o *OriginChecker) Run(ctx context.Context) {
	logging.Infof("angryduck-controller: origin check starting in %s: interval=%s registries=%v", o.cfg.Delay, o.cfg.Interval, o.cfg.Registries)
	if o.cfg.Delay > 0 {
		d := time.NewTimer(o.cfg.Delay)
		select {
		case <-ctx.Done():
			d.Stop()
			return
		case <-d.C:
		}
	}
	o.tick(ctx, time.Now())
	t := time.NewTicker(o.cfg.Tick)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			o.tick(ctx, time.Now())
		}
	}
}

func (o *OriginChecker) wanted(image string) bool {
	if len(o.cfg.Registries) == 0 {
		return true
	}
	host := imageref.Host(image)
	for _, s := range o.cfg.Registries {
		if strings.Contains(host, s) {
			return true
		}
	}
	return false
}

func (o *OriginChecker) recheckAfter(status string) time.Duration {
	if status == "ok" {
		return o.cfg.Interval
	}
	if d := o.cfg.Interval / 4; d > o.cfg.Tick {
		return d
	}
	return o.cfg.Tick
}

// budget is how many checks one tick may send: enough to go through
// every held image twice per Interval, so new images are checked within
// minutes without a burst when the controller starts.
func (o *OriginChecker) budget(images int) int {
	n := int(math.Ceil(float64(images) * 2 * float64(o.cfg.Tick) / float64(o.cfg.Interval)))
	if n < 5 {
		n = 5
	}
	return n
}

func (o *OriginChecker) tick(ctx context.Context, now time.Time) {
	held := o.registry.ImageHolders()
	for img := range held {
		if !o.wanted(img) {
			delete(held, img)
		}
	}
	for img := range o.state {
		if _, ok := held[img]; !ok {
			delete(o.state, img)
		}
	}
	var due []string
	for img := range held {
		st, ok := o.state[img]
		if !ok || !now.Before(st.checked.Add(o.recheckAfter(st.status))) {
			due = append(due, img)
		}
	}
	sort.Slice(due, func(i, j int) bool {
		a, b := o.state[due[i]].checked, o.state[due[j]].checked
		if !a.Equal(b) {
			return a.Before(b)
		}
		return due[i] < due[j]
	})
	if n := o.budget(len(held)); len(due) > n {
		due = due[:n]
	}
	for _, img := range due {
		if ctx.Err() != nil {
			return
		}
		o.check(ctx, img, held[img])
	}
	o.publish(held)
}

func (o *OriginChecker) check(ctx context.Context, image string, holders int) {
	cctx, cancel := context.WithTimeout(ctx, o.cfg.Timeout)
	err := o.checker.CheckManifest(cctx, image)
	cancel()
	if ctx.Err() != nil {
		return
	}
	status := registryclient.Availability(err)
	originChecksTotal.Inc(status)
	prev, seen := o.state[image]
	o.state[image] = originState{status: status, checked: time.Now()}
	switch {
	case seen && prev.status == status, !seen && status == "ok":
	case status == "ok":
		logging.Infof("angryduck-controller: origin check: image=%s is served by its registry again", image)
	case status == "unreachable":
		logging.Debugf("angryduck-controller: origin check: image=%s: registry unreachable: %v", image, err)
	default:
		logging.Warnf("angryduck-controller: origin check: image=%s, held on %d node(s), is %s at its registry: %v", image, holders, status, err)
	}
}

func (o *OriginChecker) publish(held map[string]int) {
	counts := map[string]map[string]int{}
	originUnavailable.Reset()
	for img, n := range held {
		status := "unchecked"
		if st, ok := o.state[img]; ok {
			status = st.status
		}
		host := imageref.Host(img)
		if counts[host] == nil {
			counts[host] = map[string]int{}
		}
		counts[host][status]++
		if status == "missing" || status == "denied" {
			originUnavailable.Set(float64(n), img, status)
		}
	}
	originImages.Reset()
	for host, byStatus := range counts {
		for _, s := range originImageStatuses {
			originImages.Set(float64(byStatus[s]), host, s)
		}
	}
}

package worker

import (
	"context"
	"slices"
	"time"

	"github.com/containerd/containerd/events"
	"google.golang.org/protobuf/encoding/protowire"

	"angryduck/internal/logging"
	"angryduck/internal/metrics"
)

var (
	inventoryEventsTotal = metrics.NewCounterVec(
		"angryduck_worker_inventory_events_total",
		"containerd events that changed what this node can serve (images created, updated or deleted, content deleted), by topic. Each settles into one fresh inventory report.",
		"node", "topic",
	)
	inventoryWatchUp = metrics.NewGaugeVec(
		"angryduck_worker_inventory_watch_up",
		"1 while this node follows containerd's events (the inventory is event-driven and only resynced slowly), 0 while it falls back to periodic scans.",
		"node",
	)
)

// inventoryTopics are the containerd events that change which blobs this
// node can serve. Snapshot events are left out on purpose: they fire on
// every container start and stop, and a pull's snapshots come with its
// image event anyway.
var inventoryTopics = []string{"/images/create", "/images/update", "/images/delete", "/content/delete"}

// containerTopics are the events that change which images containers use
// (a sandbox becoming ready or not is its task starting or exiting). Only
// followed when someone asks (OnContainers): image cleanup.
//
// /tasks/start is only sent for a container's own (init) process, but
// /tasks/exit is sent for exec processes too: every exec probe (kubelet
// running a command in a container) ends in one, many times a minute on a
// node. Those exits change nothing about which images are used, so
// followContainerEvent leaves them out.
var containerTopics = []string{"/containers/create", "/containers/update", "/containers/delete", "/tasks/start", "/tasks/exit"}

// followContainerEvent says whether a container event can change which
// images containers use: all of them but the exit of an exec process. A
// TaskExit's id is the container's own id for its init process and the
// exec id for an exec; containerd's event filters can't compare two
// fields, so the two string fields are read here, straight from the
// event's protobuf bytes (container_id = 1, id = 2). Anything unreadable
// is followed, as before.
func followContainerEvent(env *events.Envelope) bool {
	if env.Topic != "/tasks/exit" || env.Event == nil {
		return true
	}
	containerID, id, ok := taskExitIDs(env.Event.GetValue())
	return !ok || id == "" || id == containerID
}

// taskExitIDs reads fields 1 (container_id) and 2 (id) of a TaskExit.
func taskExitIDs(b []byte) (containerID, id string, ok bool) {
	for len(b) > 0 {
		num, typ, n := protowire.ConsumeTag(b)
		if n < 0 {
			return "", "", false
		}
		b = b[n:]
		if (num == 1 || num == 2) && typ == protowire.BytesType {
			v, n := protowire.ConsumeBytes(b)
			if n < 0 {
				return "", "", false
			}
			if num == 1 {
				containerID = string(v)
			} else {
				id = string(v)
			}
			b = b[n:]
			continue
		}
		n = protowire.ConsumeFieldValue(num, typ, b)
		if n < 0 {
			return "", "", false
		}
		b = b[n:]
	}
	return containerID, id, true
}

// deleteTopics are the inventory events after which containerd's own
// garbage collection removes blobs a moment later, without an event.
var deleteTopics = map[string]bool{"/images/delete": true, "/content/delete": true}

// EventSubscriber is containerd's event stream (*containerd.Client has it).
type EventSubscriber interface {
	Subscribe(ctx context.Context, filters ...string) (ch <-chan *events.Envelope, errs <-chan error)
}

// InventoryWatch turns containerd's events into inventory reports, so the
// controller learns of a pull or a removal in about a second instead of at
// the next periodic scan: kubelet's pulls, which Angry Duck doesn't make,
// included. Bursts (a pull creates several image records) settle into one
// report.
//
// While subscribed it calls live(true) and periodic scans only resync;
// when the stream breaks it calls live(false), so scans fall back to their
// regular interval until it is back, and reports once more on reconnecting
// in case something changed meanwhile. One idle gRPC stream, no polling.
type InventoryWatch struct {
	sub         EventSubscriber
	namespace   string
	nodeID      string
	settle      time.Duration
	changed     func()
	live        func(bool)
	contChanged func() // nil: container events aren't followed
	deleted     func() // nil: nothing to do after deletions
}

// OnContainers follows container events too, calling fn (non-blocking) on
// each. Call before Run.
func (w *InventoryWatch) OnContainers(fn func()) { w.contChanged = fn }

// OnDeleted calls fn (non-blocking) after an image or content deletion.
// Call before Run.
func (w *InventoryWatch) OnDeleted(fn func()) { w.deleted = fn }

// NewInventoryWatch builds a watch; changed is the reporter's Kick, live
// switches the layer tracker between resync and regular scans.
func NewInventoryWatch(sub EventSubscriber, namespace, nodeID string, changed func(), live func(bool)) *InventoryWatch {
	for _, t := range inventoryTopics {
		inventoryEventsTotal.Add(0, nodeID, t)
	}
	inventoryWatchUp.Set(0, nodeID)
	return &InventoryWatch{sub: sub, namespace: namespace, nodeID: nodeID, settle: time.Second, changed: changed, live: live}
}

func (w *InventoryWatch) filters() []string {
	topics := inventoryTopics
	if w.contChanged != nil {
		topics = append(append([]string{}, inventoryTopics...), containerTopics...)
	}
	out := make([]string, 0, len(topics))
	for _, t := range topics {
		out = append(out, `namespace=="`+w.namespace+`",topic=="`+t+`"`)
	}
	return out
}

// Run follows the events until ctx is done, resubscribing with backoff.
func (w *InventoryWatch) Run(ctx context.Context) {
	backoff := time.Second
	first := true
	for ctx.Err() == nil {
		sctx, cancel := context.WithCancel(ctx)
		ch, errs := w.sub.Subscribe(sctx, w.filters()...)
		w.live(true)
		inventoryWatchUp.Set(1, w.nodeID)
		if !first {
			w.changed() // events may have been missed while disconnected
		}
		first = false
		start := time.Now()
		err := w.follow(sctx, ch, errs)
		cancel()
		w.live(false)
		inventoryWatchUp.Set(0, w.nodeID)
		if ctx.Err() != nil {
			return
		}
		if time.Since(start) > time.Minute {
			backoff = time.Second // it had been working: retry fast
		}
		logging.Warnf("angryduck-worker[%s]: containerd event stream ended (%v): periodic inventory scans until it's back, retrying in %s", w.nodeID, err, backoff)
		select {
		case <-ctx.Done():
			return
		case <-time.After(backoff):
		}
		backoff = min(backoff*2, 30*time.Second)
	}
}

// follow reads one subscription until it fails, settling bursts.
func (w *InventoryWatch) follow(ctx context.Context, ch <-chan *events.Envelope, errs <-chan error) error {
	var timer *time.Timer
	var fire <-chan time.Time
	defer func() {
		if timer != nil {
			timer.Stop()
		}
	}()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case err := <-errs:
			return err
		case env, ok := <-ch:
			if !ok {
				return nil
			}
			if slices.Contains(containerTopics, env.Topic) {
				if w.contChanged != nil && followContainerEvent(env) {
					w.contChanged()
				}
				continue
			}
			if deleteTopics[env.Topic] && w.deleted != nil {
				w.deleted()
			}
			inventoryEventsTotal.Inc(w.nodeID, env.Topic)
			if timer == nil {
				timer = time.NewTimer(w.settle)
				fire = timer.C
			}
		case <-fire:
			timer, fire = nil, nil
			w.changed()
		}
	}
}

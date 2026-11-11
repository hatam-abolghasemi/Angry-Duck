package controller

import (
	"context"
	"time"
)

// wakeCoalesce is how long an early tick waits after the first wake-up,
// so transfers that finish together share one tick. Short: a finished
// transfer frees a slot and makes a new holder, and every moment until
// the next order is a moment that link sits idle. Set with
// SetWakeCoalesce.
var wakeCoalesce = 50 * time.Millisecond

// SetWakeCoalesce sets how long every loop (spread, propagation, rescue)
// waits after a wake-up before it looks again. Zero or less keeps the
// default.
func SetWakeCoalesce(d time.Duration) {
	if d > 0 {
		wakeCoalesce = d
	}
}

// waker asks a ticking loop to look again before its next tick, when
// something it was waiting for (a free transfer slot, a new holder) has
// happened. Wake-ups never block and collapse into one.
type waker chan struct{}

func newWaker() waker { return make(waker, 1) }

func (w waker) wake() {
	select {
	case w <- struct{}{}:
	default:
	}
}

// settle waits wakeCoalesce, then drops wake-ups that arrived meanwhile.
// It returns false when ctx ends first.
func (w waker) settle(ctx context.Context) bool {
	t := time.NewTimer(wakeCoalesce)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
	}
	select {
	case <-w:
	default:
	}
	return true
}

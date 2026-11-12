package controller

import (
	"context"
	"errors"
	"time"

	"angryduck/internal/imageref"
	"angryduck/internal/logging"
	"angryduck/internal/metrics"
	"angryduck/internal/registryclient"
)

var spreadStartsTotal = metrics.NewCounterVec(
	"angryduck_controller_spread_starts_total",
	"Preheats, by how their spread started: first (the manifest was read at once), retried (read after retries), fallback (never read, or the registry refused: the image was seeded whole), superseded (a newer tag of the repo came first).",
	"result",
)

// errSuperseded cancels a start that a newer tag of the repo replaced.
var errSuperseded = errors.New("superseded by a newer tag")

// Begin starts spreading image in the background and returns at once, so
// the webhook answers CI without waiting on the registry. Reading the
// image's manifest is retried, backing off up to startBackoffMax, for
// StartDeadline; only then, or at once when the registry refuses Angry
// Duck's credentials, does fallback get the image (to seed it whole). A
// start for an image already being started is ignored; one for another tag
// of the same repo cancels it, without fallback.
func (s *Spreader) Begin(image string, fallback func(image string, err error)) {
	repo := imageref.Repo(image)
	s.mu.Lock()
	if _, ok := s.starting[image]; ok {
		s.mu.Unlock()
		return
	}
	for other, cancel := range s.starting {
		if repo != "" && imageref.Repo(other) == repo {
			cancel(errSuperseded)
		}
	}
	base := s.runCtx
	if base == nil {
		base = context.Background()
	}
	ctx, cancel := context.WithCancelCause(base)
	s.starting[image] = cancel
	s.mu.Unlock()

	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		defer func() {
			s.mu.Lock()
			delete(s.starting, image)
			s.mu.Unlock()
			cancel(nil)
		}()
		s.begin(ctx, image, fallback)
	}()
}

const (
	startBackoffMin = time.Second
	startBackoffMax = 15 * time.Second
)

func (s *Spreader) begin(ctx context.Context, image string, fallback func(string, error)) {
	deadline := time.Now().Add(s.cfg.StartDeadline)
	wait := startBackoffMin
	for attempt := 1; ; attempt++ {
		ordered, err := s.Start(ctx, image)
		if err == nil {
			if attempt == 1 {
				spreadStartsTotal.Inc("first")
				logging.Infof("angryduck-controller: accepted preheat for image=%s: spreading blob by blob, %d node(s) pulling from the registry first: %v", image, len(ordered), ordered)
			} else {
				spreadStartsTotal.Inc("retried")
				logging.Infof("angryduck-controller: accepted preheat for image=%s: manifest read on attempt %d; spreading blob by blob, %d node(s) pulling from the registry first: %v", image, attempt, len(ordered), ordered)
			}
			return
		}
		if errors.Is(context.Cause(ctx), errSuperseded) {
			spreadStartsTotal.Inc("superseded")
			logging.Infof("angryduck-controller: preheat of image=%s superseded by a newer tag before its manifest could be read", image)
			return
		}
		if ctx.Err() != nil {
			return // shutting down
		}
		var se *registryclient.StatusError
		refused := errors.As(err, &se) && (se.Code == 401 || se.Code == 403)
		if refused || !time.Now().Add(wait).Before(deadline) {
			spreadStartsTotal.Inc("fallback")
			logging.Warnf("angryduck-controller: can't spread image=%s blob by blob after %d attempt(s) (%v); seeding it whole instead", image, attempt, err)
			fallback(image, err)
			return
		}
		logging.Infof("angryduck-controller: reading the manifest of image=%s failed (attempt %d: %v); trying again in %s", image, attempt, err, wait)
		select {
		case <-ctx.Done():
			if errors.Is(context.Cause(ctx), errSuperseded) {
				spreadStartsTotal.Inc("superseded")
				logging.Infof("angryduck-controller: preheat of image=%s superseded by a newer tag before its manifest could be read", image)
			}
			return
		case <-time.After(wait):
		}
		if wait *= 2; wait > startBackoffMax {
			wait = startBackoffMax
		}
	}
}

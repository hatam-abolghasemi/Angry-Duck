package sharedtoken

import (
	"fmt"
	"net/http"
	"strings"
	"sync"
	"time"

	"angryduck/internal/logging"
)

// Mode decides what Auth.Guard does with a request that lacks a valid
// token.
type Mode int

const (
	// Enforce rejects it with 401. The default.
	Enforce Mode = iota
	// Warn lets it through and logs it. Only for rolling the token out
	// across controller and workers, which can't be upgraded at the same
	// instant: set AUTH_MODE=warn for the rollout, then remove it.
	Warn
)

// ParseMode reads AUTH_MODE. Empty means Enforce.
func ParseMode(s string) (Mode, error) {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "", "enforce":
		return Enforce, nil
	case "warn":
		return Warn, nil
	}
	return Enforce, fmt.Errorf("AUTH_MODE must be enforce or warn, got %q", s)
}

func (m Mode) String() string {
	if m == Warn {
		return "warn"
	}
	return "enforce"
}

// Auth guards the control-plane endpoints (/report on the controller,
// /pull and /pull/cancel on workers) with the shared token.
//
// With no token configured, Guard passes everything through: that is a
// deployment without the Secret, where rescue, propagation and the mirror
// are already off, so there is no token to leak and preheat keeps working
// as before.
type Auth struct {
	Token string
	Mode  Mode

	mu      sync.Mutex
	lastLog map[string]time.Time
}

// warnEvery rate-limits Warn-mode log lines per endpoint, since workers
// report on a short interval.
const warnEvery = time.Minute

// Guard wraps h for the endpoint called name.
func (a *Auth) Guard(name string, h http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if a == nil || a.Token == "" || Valid(r, a.Token) {
			h(w, r)
			return
		}
		if a.Mode == Warn {
			a.warn(name, r)
			h(w, r)
			return
		}
		logging.Warnf("angryduck: rejected unauthenticated %s %s from %s", r.Method, name, r.RemoteAddr)
		http.Error(w, "unauthorized", http.StatusUnauthorized)
	}
}

func (a *Auth) warn(name string, r *http.Request) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.lastLog == nil {
		a.lastLog = map[string]time.Time{}
	}
	if time.Since(a.lastLog[name]) < warnEvery {
		return
	}
	a.lastLog[name] = time.Now()
	logging.Warnf("angryduck: AUTH_MODE=warn: allowed unauthenticated %s %s from %s (would be rejected under enforce)", r.Method, name, r.RemoteAddr)
}

// SetIfAny adds the token to an outgoing request when one is configured.
func SetIfAny(r *http.Request, token string) {
	if token != "" {
		Set(r, token)
	}
}

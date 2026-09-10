package worker

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"time"

	"angryduck/internal/logging"
)

// hostsMarker identifies files this worker owns. A hosts.toml without it
// was written by someone else (an operator, Spegel, Ansible) and is never
// modified or removed.
const hostsMarker = "# managed-by: angryduck-worker"

// HostsTOML keeps /etc/containerd/certs.d/<registry>/hosts.toml pointing
// containerd at this node's worker for each configured registry.
//
// The file exists only while the worker runs: written at startup, removed
// on graceful shutdown. If the worker dies without cleaning up, containerd
// gets an instant connection-refused from 127.0.0.1 and falls through to
// origin — pulls get no slower, they just stop getting peer help.
//
// containerd re-reads hosts.toml on every pull, so none of this needs a
// containerd restart. It does need config_path set in containerd's own
// config (Spegel already required that on these clusters); Check warns
// if it isn't.
type HostsTOML struct {
	hx         *HostExec
	dir        string // node path, e.g. /etc/containerd/certs.d
	registries []string
	endpoint   string // e.g. http://127.0.0.1:18081

	mu     sync.Mutex
	warned map[string]bool
}

// NewHostsTOML builds a manager. registries are hostnames
// (registry.internal-registry.example.com, docker.io, host:5000); prefix one with http://
// for a plain-HTTP registry. https is assumed otherwise.
//
// "*" means every registry: it manages certs.d/_default/hosts.toml, which
// containerd uses for any registry that has no directory of its own. That
// file carries no `server` line, so containerd keeps its normal origin for
// each registry (https://<host>, registry-1.docker.io for docker.io) and
// only puts the worker in front of it.
func NewHostsTOML(hx *HostExec, dir string, registries []string, endpoint string) *HostsTOML {
	return &HostsTOML{hx: hx, dir: dir, registries: registries, endpoint: endpoint, warned: make(map[string]bool)}
}

// Check warns when containerd is not configured to read dir at all, in
// which case every file written here is dead text, and — when covering
// every registry via _default — names each registry whose own directory
// shadows _default and therefore bypasses the mirror.
func (h *HostsTOML) Check() {
	h.warnShadowing()
	cfg, err := os.ReadFile(h.hx.HostFile("/etc/containerd/config.toml"))
	if err != nil {
		logging.Warnf("angryduck-worker-mirror: could not read /etc/containerd/config.toml to verify config_path (%v); mirror only works if containerd's registry config_path = %q", err, h.dir)
		return
	}
	re := regexp.MustCompile(`(?m)^\s*config_path\s*=\s*["']` + regexp.QuoteMeta(h.dir) + `/?["']`)
	if !re.Match(cfg) {
		logging.Warnf("angryduck-worker-mirror: containerd config_path does not point at %s — containerd will ignore the mirror until it does (needs one containerd restart after setting it)", h.dir)
	}
}

// warnShadowing lists certs.d/<host>/ directories not written by this
// worker. containerd consults a registry's own directory INSTEAD of
// _default (never both), so each one is a registry the mirror can't see.
// Directories starting with "_" (_default, Spegel's _backup) aren't
// registries and are skipped.
func (h *HostsTOML) warnShadowing() {
	covered := false
	mine := map[string]bool{}
	for _, e := range h.registries {
		d, _ := splitRegistry(e)
		mine[d] = true
		if d == "_default" {
			covered = true
		}
	}
	if !covered {
		return
	}
	entries, err := os.ReadDir(h.hx.HostFile(h.dir))
	if err != nil {
		return
	}
	for _, e := range entries {
		name := e.Name()
		if !e.IsDir() || strings.HasPrefix(name, "_") || mine[name] {
			continue
		}
		b, err := os.ReadFile(h.hx.HostFile(filepath.Join(h.dir, name, "hosts.toml")))
		if err == nil && bytes.Contains(b, []byte(hostsMarker)) {
			continue // a per-registry file of ours from an earlier config; Ensure/Remove handle it
		}
		logging.Warnf("angryduck-worker-mirror: %s has its own hosts.toml config, which containerd uses instead of _default — pulls from %s bypass the mirror (delete or merge %s to cover it)",
			name, name, filepath.Join(h.dir, name))
	}
}

// Run re-asserts the files every interval. Cheap (one small read per
// registry) and needed: Spegel clears foreign files from certs.d whenever
// it restarts, for as long as the two run side by side.
func (h *HostsTOML) Run(ctx context.Context, interval time.Duration) {
	h.Ensure()
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			h.Ensure()
		}
	}
}

// Ensure writes each registry's file if missing or different. It only
// writes when content differs, so the steady state is read-only.
func (h *HostsTOML) Ensure() {
	for _, entry := range h.registries {
		reg, server := splitRegistry(entry)
		path := h.hx.HostFile(filepath.Join(h.dir, reg, "hosts.toml"))
		want := []byte(renderHostsTOML(server, h.endpoint))
		have, err := os.ReadFile(path)
		if err == nil && bytes.Equal(have, want) {
			continue
		}
		if err == nil && !bytes.Contains(have, []byte(hostsMarker)) {
			h.warnOnce(reg, "angryduck-worker-mirror: %s exists and was not written by angryduck — leaving it alone, so the mirror is inactive for %s", filepath.Join(h.dir, reg, "hosts.toml"), reg)
			continue
		}
		if err := writeAtomic(path, want); err != nil {
			logging.Errorf("angryduck-worker-mirror: writing hosts.toml for %s: %v", reg, err)
			continue
		}
		label := reg
		if reg == "_default" {
			label = "every registry without its own certs.d entry (_default)"
		}
		logging.Infof("angryduck-worker-mirror: containerd now tries %s before origin for %s", h.endpoint, label)
	}
}

// Remove deletes the files this worker owns. Called on shutdown, before
// the HTTP server stops, so containerd stops sending requests first.
func (h *HostsTOML) Remove() {
	for _, entry := range h.registries {
		reg, _ := splitRegistry(entry)
		path := h.hx.HostFile(filepath.Join(h.dir, reg, "hosts.toml"))
		have, err := os.ReadFile(path)
		if err != nil || !bytes.Contains(have, []byte(hostsMarker)) {
			continue
		}
		if err := os.Remove(path); err == nil {
			_ = os.Remove(filepath.Dir(path)) // only succeeds if empty
			logging.Infof("angryduck-worker-mirror: removed mirror entry for %s", reg)
		}
	}
}

func (h *HostsTOML) warnOnce(reg, format string, args ...interface{}) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if !h.warned[reg] {
		h.warned[reg] = true
		logging.Warnf(format, args...)
	}
}

// renderHostsTOML builds the file. capabilities = ["pull"] (no
// "resolve") is the important line: tags are always resolved by origin,
// so the worker only ever sees digests. dial_timeout keeps a dead worker
// from costing more than a blink.
func renderHostsTOML(server, endpoint string) string {
	var b strings.Builder
	b.WriteString(hostsMarker + "\n")
	b.WriteString("# Written at worker start, removed at worker stop. Do not edit.\n")
	if server != "" {
		b.WriteString(`server = "` + server + "\"\n")
	} else {
		b.WriteString("# No server line: containerd keeps each registry's usual origin.\n")
	}
	b.WriteString("\n")
	b.WriteString(`[host."` + endpoint + "\"]\n")
	b.WriteString("  capabilities = [\"pull\"]\n")
	b.WriteString("  dial_timeout = \"200ms\"\n")
	return b.String()
}

// splitRegistry turns a MIRROR_REGISTRIES entry into the certs.d
// directory name containerd looks up and the origin server URL.
func splitRegistry(entry string) (dir, server string) {
	if entry == "*" || entry == "_default" {
		return "_default", ""
	}
	if rest, ok := strings.CutPrefix(entry, "http://"); ok {
		return rest, entry
	}
	dir = strings.TrimPrefix(entry, "https://")
	if dir == "docker.io" {
		return dir, "https://registry-1.docker.io"
	}
	return dir, "https://" + dir
}

func writeAtomic(path string, data []byte) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	tmp := path + ".angryduck.tmp"
	if err := os.WriteFile(tmp, data, 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

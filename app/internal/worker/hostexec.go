package worker

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"
)

// hostPath is the search order used to find node binaries inside the host
// root. It covers both install styles seen on our fleet: Kubespray drops
// release tarballs into /usr/local/bin, while apt packages (containerd,
// cri-tools) install into /usr/bin. /usr/local wins, same as a login shell.
var hostPath = []string{"/usr/local/sbin", "/usr/local/bin", "/usr/sbin", "/usr/bin", "/sbin", "/bin"}

// HostExec runs the node's own CLI binaries (crictl, ctr, docker) instead
// of copies baked into the worker image.
//
// It does that by chrooting each child process into the node's root
// filesystem (by default /proc/1/root, which with hostPID: true is the
// node's real root with every live mount under it). Inside that chroot the
// binary runs exactly as it would from a node shell: the node's dynamic
// loader and libc, the node's /etc/crictl.yaml, the node's socket paths.
// That matters because apt's containerd package ships a dynamically
// linked ctr — bind-mounting that file into a distroless image would fail
// with a misleading "no such file or directory" (the missing file is the
// ELF interpreter, not ctr itself).
//
// Nothing is copied to or mounted over the node, so apt keeps sole
// ownership of these files: an upgrade replaces the file on disk and the
// very next exec here runs the new version, with no stale inode pinned by
// a single-file bind mount.
type HostExec struct {
	root string // "" means no chroot (local development)

	mu    sync.Mutex
	paths map[string]string // binary name -> path inside root

	// listGate lets one listing command (an image, container, content or
	// snapshot listing) run at a time. Each one is a separate ctr/crictl
	// process of 20-40 MB counted against this pod, and the periodic loops
	// (reporter, layer scan, cleanup, preheat monitor) would otherwise
	// line up on the same ticks. Transfers don't take it.
	listOnce sync.Once
	listGate chan struct{}
}

// AcquireListing waits for the listing gate; call the returned func to
// release it. It fails only when ctx ends first.
func (h *HostExec) AcquireListing(ctx context.Context) (func(), error) {
	h.listOnce.Do(func() { h.listGate = make(chan struct{}, 1) })
	select {
	case h.listGate <- struct{}{}:
		return func() { <-h.listGate }, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// listingTimeout bounds a runtime listing, so a wedged containerd can't
// hold the listing gate forever.
const listingTimeout = 2 * time.Minute

// List runs a listing command behind the gate, with listingTimeout.
func (h *HostExec) List(name string, args ...string) (string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), listingTimeout)
	defer cancel()
	return h.RunListing(ctx, name, args...)
}

// RunListing is RunContext behind the listing gate.
func (h *HostExec) RunListing(ctx context.Context, name string, args ...string) (string, error) {
	release, err := h.AcquireListing(ctx)
	if err != nil {
		return "", err
	}
	defer release()
	return h.RunContext(ctx, name, args...)
}

// NewHostExec builds a HostExec for the given root. An empty root or "/"
// disables the chroot and resolves binaries from this process's own PATH,
// which is what local non-Kubernetes runs want.
func NewHostExec(root string) (*HostExec, error) {
	root = strings.TrimRight(root, "/")
	if root != "" {
		if _, err := os.Stat(root + "/"); err != nil {
			return nil, fmt.Errorf("host root %q is not accessible (needs hostPID: true, CAP_SYS_CHROOT/CAP_SYS_PTRACE and an unconfined AppArmor profile): %w", root, err)
		}
	}
	return &HostExec{root: root, paths: make(map[string]string)}, nil
}

// Root returns the configured host root ("" when running without chroot).
func (h *HostExec) Root() string { return h.root }

// HostFile maps an absolute node path to the path this process must use to
// reach it (e.g. /etc/containerd/certs.d -> /proc/1/root/etc/containerd/certs.d).
func (h *HostExec) HostFile(p string) string { return h.root + p }

// Resolve returns the path of name inside the host root, caching the
// answer. It only lstat()s candidates rather than stat()ing them: an
// absolute symlink on the node (/usr/local/bin/ctr -> /opt/...) would be
// followed relative to THIS container's root when looked at through
// /proc/1/root and wrongly reported missing. The chrooted exec resolves
// the link correctly, so a link is accepted as-is.
func (h *HostExec) Resolve(name string) (string, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if p, ok := h.paths[name]; ok {
		return p, nil
	}
	if h.root == "" {
		p, err := exec.LookPath(name)
		if err != nil {
			return "", err
		}
		h.paths[name] = p
		return p, nil
	}
	for _, dir := range hostPath {
		candidate := dir + "/" + name
		fi, err := os.Lstat(h.root + candidate)
		if err != nil {
			continue
		}
		if fi.Mode()&fs.ModeSymlink != 0 || (fi.Mode().IsRegular() && fi.Mode()&0o111 != 0) {
			h.paths[name] = candidate
			return candidate, nil
		}
	}
	return "", fmt.Errorf("%s not found on the node in %s", name, strings.Join(hostPath, ":"))
}

func (h *HostExec) forget(name string) {
	h.mu.Lock()
	delete(h.paths, name)
	h.mu.Unlock()
}

// Command builds an *exec.Cmd for a node binary. The caller owns
// Stdin/Stdout/Stderr and Start/Wait. ctx cancellation SIGKILLs the child.
func (h *HostExec) Command(ctx context.Context, name string, args ...string) (*exec.Cmd, error) {
	p, err := h.Resolve(name)
	if err != nil {
		return nil, err
	}
	cmd := exec.CommandContext(ctx, p, args...)
	// A minimal, explicit environment: the child must not inherit this
	// pod's env, which carries CONTAINER_RUNTIME_ENDPOINT and friends that
	// crictl/ctr would silently pick up and prefer over the node's config.
	cmd.Env = []string{"PATH=" + strings.Join(hostPath, ":"), "HOME=/root"}
	// Pdeathsig: a transfer child must never outlive the worker — an
	// orphaned `ctr export` would keep streaming into a socket nobody reads.
	attr := &syscall.SysProcAttr{Pdeathsig: syscall.SIGKILL}
	if h.root != "" {
		attr.Chroot = h.root
		cmd.Dir = "/"
	}
	cmd.SysProcAttr = attr
	return cmd, nil
}

// Start starts cmd, re-resolving name once if the cached path vanished
// (e.g. an admin moved a binary between /usr/local/bin and /usr/bin).
func (h *HostExec) start(ctx context.Context, cmd *exec.Cmd, name string, args []string) (*exec.Cmd, error) {
	err := cmd.Start()
	if err == nil || !errors.Is(err, fs.ErrNotExist) {
		return cmd, err
	}
	h.forget(name)
	retry, rerr := h.Command(ctx, name, args...)
	if rerr != nil {
		return nil, rerr
	}
	retry.Stdin, retry.Stdout, retry.Stderr = cmd.Stdin, cmd.Stdout, cmd.Stderr
	return retry, retry.Start()
}

// Run executes a node binary to completion and returns its stdout.
func (h *HostExec) Run(name string, args ...string) (string, error) {
	return h.RunInput(nil, name, args...)
}

// RunInput is Run with bytes fed to stdin (used for docker's
// --password-stdin so credentials never appear in the process list).
func (h *HostExec) RunInput(stdin []byte, name string, args ...string) (string, error) {
	return h.RunInputContext(context.Background(), stdin, name, args...)
}

// RunContext is Run, killed when ctx ends.
func (h *HostExec) RunContext(ctx context.Context, name string, args ...string) (string, error) {
	return h.RunInputContext(ctx, nil, name, args...)
}

// RunInputContext is RunInput, killed when ctx ends.
func (h *HostExec) RunInputContext(ctx context.Context, stdin []byte, name string, args ...string) (string, error) {
	cmd, err := h.Command(ctx, name, args...)
	if err != nil {
		return "", err
	}
	var stdout bytes.Buffer
	stderr := &tailBuffer{max: 4096}
	cmd.Stdout = &stdout
	cmd.Stderr = stderr
	if stdin != nil {
		cmd.Stdin = bytes.NewReader(stdin)
	}
	cmd, err = h.start(ctx, cmd, name, args)
	if err == nil {
		err = cmd.Wait()
	}
	if err != nil {
		return stdout.String(), fmt.Errorf("%s %s: %w: %s", filepath.Base(name), redactArgs(args), err, strings.TrimSpace(stderr.String()))
	}
	return stdout.String(), nil
}

// redactArgs keeps credentials passed via --creds/--user out of logs.
func redactArgs(args []string) string {
	out := make([]string, len(args))
	copy(out, args)
	for i := 0; i < len(out)-1; i++ {
		if out[i] == "--creds" || out[i] == "--user" || out[i] == "-u" {
			out[i+1] = "<redacted>"
		}
	}
	return strings.Join(out, " ")
}

// tailBuffer keeps only the last max bytes written to it, so a chatty or
// misbehaving child can never grow the worker's memory through stderr.
type tailBuffer struct {
	max int
	buf []byte
}

func (t *tailBuffer) Write(p []byte) (int, error) {
	n := len(p)
	if len(p) >= t.max {
		t.buf = append(t.buf[:0], p[len(p)-t.max:]...)
		return n, nil
	}
	if over := len(t.buf) + len(p) - t.max; over > 0 {
		t.buf = t.buf[over:]
	}
	t.buf = append(t.buf, p...)
	return n, nil
}

func (t *tailBuffer) String() string { return string(t.buf) }

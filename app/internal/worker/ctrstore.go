package worker

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"io"
	"strings"
	"time"
)

// CtrStore is blobship.Store backed by the node's own `ctr`, run through
// HostExec like every other runtime call. It talks to containerd directly
// (not through CRI), because CRI has no API for reading or importing raw
// content.
//
// It works whatever CONTAINER_RUNTIME is set to, as long as the node runs
// containerd: crictl and ctr are both front-ends to the same daemon.
type CtrStore struct {
	hx        *HostExec
	namespace string // containerd namespace; kubelet's images live in k8s.io
	address   string // containerd socket path on the node; "" = ctr's default
}

// NewCtrStore builds a CtrStore. endpoint is CONTAINER_RUNTIME_ENDPOINT
// (e.g. unix:///run/containerd/containerd.sock); containerd serves CRI and
// its own API on the same socket.
func NewCtrStore(hx *HostExec, namespace, endpoint string) *CtrStore {
	return &CtrStore{hx: hx, namespace: namespace, address: strings.TrimPrefix(endpoint, "unix://")}
}

func (s *CtrStore) args(args ...string) []string {
	base := []string{}
	if s.address != "" {
		base = append(base, "--address", s.address)
	}
	base = append(base, "-n", s.namespace)
	return append(base, args...)
}

// Resolve finds image in `ctr images ls` and returns its TYPE and DIGEST
// columns.
func (s *CtrStore) Resolve(ctx context.Context, image string) (string, string, error) {
	out, err := s.run(ctx, nil, nil, s.args("images", "ls")...)
	if err != nil {
		return "", "", err
	}
	mediaType, digest, ok := parseCtrImageRow(out, image)
	if !ok {
		return "", "", fmt.Errorf("image %s not found in containerd namespace %s", image, s.namespace)
	}
	return mediaType, digest, nil
}

// parseCtrImageRow finds the row for ref in `ctr images ls` output:
//
//	REF  TYPE  DIGEST  SIZE  PLATFORMS  LABELS
//
// REF, TYPE and DIGEST never contain spaces, so the first three fields are
// reliable even though SIZE ("3.8 MiB") does.
func parseCtrImageRow(output, ref string) (mediaType, digest string, ok bool) {
	for _, line := range splitNonEmptyLines(output) {
		f := strings.Fields(line)
		if len(f) >= 3 && f[0] == ref && strings.HasPrefix(f[2], "sha256:") {
			return f[1], f[2], true
		}
	}
	return "", "", false
}

// ReadBlob runs `ctr content get` into memory, refusing anything above max.
func (s *CtrStore) ReadBlob(ctx context.Context, digest string, max int64) ([]byte, error) {
	var buf bytes.Buffer
	lw := &limitWriter{w: &buf, left: max}
	if _, err := s.run(ctx, nil, lw, s.args("content", "get", digest)...); err != nil {
		return nil, err
	}
	if lw.over {
		return nil, fmt.Errorf("blob %s is larger than %d bytes", digest, max)
	}
	return buf.Bytes(), nil
}

// StreamBlob runs `ctr content get` straight into w and checks the byte
// count, so a blob that doesn't match its descriptor never passes silently.
func (s *CtrStore) StreamBlob(ctx context.Context, digest string, size int64, w io.Writer) error {
	cw := &countWriter{w: w}
	if _, err := s.run(ctx, nil, cw, s.args("content", "get", digest)...); err != nil {
		return err
	}
	if cw.n != size {
		return fmt.Errorf("blob %s: got %d bytes, expected %d", digest, cw.n, size)
	}
	return nil
}

// Digests lists every blob digest in the namespace's content store.
func (s *CtrStore) Digests(ctx context.Context) (map[string]bool, error) {
	out, err := s.run(ctx, nil, nil, s.args("content", "ls", "-q")...)
	if err != nil {
		return nil, err
	}
	have := make(map[string]bool)
	for _, line := range splitNonEmptyLines(out) {
		if strings.HasPrefix(line, "sha256:") {
			have[line] = true
		}
	}
	return have, nil
}

// Import pipes an OCI archive into `ctr images import`. /dev/stdin is used
// rather than "-" because it works on every ctr version; containerd reads
// the tar sequentially, so a pipe is fine here (unlike `ctr content
// ingest`, which tries to seek its input when the blob already exists).
func (s *CtrStore) Import(ctx context.Context, r io.Reader, platform string) error {
	_, err := s.run(ctx, r, nil, s.args("images", "import", "--platform", platform, "/dev/stdin")...)
	return err
}

// run executes ctr. With stdout nil the output is captured and returned;
// otherwise it is streamed to stdout.
func (s *CtrStore) run(ctx context.Context, stdin io.Reader, stdout io.Writer, args ...string) (string, error) {
	cmd, err := s.hx.Command(ctx, "ctr", args...)
	if err != nil {
		return "", err
	}
	var captured bytes.Buffer
	if stdout == nil {
		stdout = &captured
	}
	stderr := &tailBuffer{max: 4096}
	cmd.Stdin, cmd.Stdout, cmd.Stderr = stdin, stdout, stderr
	cmd, err = s.hx.start(ctx, cmd, "ctr", args)
	if err == nil {
		err = cmd.Wait()
	}
	if err != nil {
		if ctx.Err() != nil {
			err = ctx.Err()
		}
		return "", fmt.Errorf("ctr %s: %w: %s", strings.Join(args, " "), err, strings.TrimSpace(stderr.String()))
	}
	return captured.String(), nil
}

type countWriter struct {
	w io.Writer
	n int64
}

func (c *countWriter) Write(p []byte) (int, error) {
	n, err := c.w.Write(p)
	c.n += int64(n)
	return n, err
}

// limitWriter stops writing after left bytes but keeps draining, so the
// child never blocks on a full pipe; over records that the cap was hit.
type limitWriter struct {
	w    io.Writer
	left int64
	over bool
}

func (l *limitWriter) Write(p []byte) (int, error) {
	if int64(len(p)) > l.left {
		l.over = true
		if l.left > 0 {
			_, _ = l.w.Write(p[:l.left])
			l.left = 0
		}
		return len(p), nil
	}
	l.left -= int64(len(p))
	return l.w.Write(p)
}

// --- snapshots ---
//
// Snapshot shipping copies overlayfs snapshot directories between nodes
// with the node's own GNU tar, which keeps what overlayfs needs: numeric
// ownership, the 0/0 character devices that mark deleted files, and the
// trusted.overlay.* attributes that mark replaced directories. Reading and
// writing those needs extra capabilities on the worker (see the DaemonSet).
// This assumes containerd's default overlayfs snapshotter, which is what
// `ctr snapshots` uses when no --snapshotter is given.

// gcRootLabel pins a snapshot against containerd's garbage collector while
// nothing references it yet: between its commit and the image import that
// makes the image point at it.
const gcRootLabel = "containerd.io/gc.root"

// Snapshots lists every snapshot key.
func (s *CtrStore) Snapshots(ctx context.Context) (map[string]bool, error) {
	out, err := s.run(ctx, nil, nil, s.args("snapshots", "ls")...)
	if err != nil {
		return nil, err
	}
	keys := make(map[string]bool)
	for _, line := range splitNonEmptyLines(out) {
		if f := strings.Fields(line); len(f) > 0 && f[0] != "KEY" {
			keys[f[0]] = true
		}
	}
	return keys, nil
}

// SnapshotDirs finds the directories behind chainID and its parents. The
// snapshotter doesn't expose that mapping directly, but a read-only view
// of chainID mounts all of them, top first, so one short-lived view gives
// the whole stack.
func (s *CtrStore) SnapshotDirs(ctx context.Context, chainID string, depth int) ([]string, error) {
	key := "angryduck-view-" + randomSuffix()
	if _, err := s.run(ctx, nil, nil, s.args("snapshots", "view", key, chainID)...); err != nil {
		return nil, err
	}
	defer s.removeSnapshot(key)
	out, err := s.run(ctx, nil, nil, s.args("snapshots", "mounts", "/mnt", key)...)
	if err != nil {
		return nil, err
	}
	m, err := parseMountCommand(out)
	if err != nil {
		return nil, err
	}
	var topFirst []string
	switch {
	case m.bindSource != "":
		topFirst = []string{m.bindSource}
	case len(m.lower) > 0:
		topFirst = m.lower
	default:
		return nil, fmt.Errorf("view of %s has no lower directories: %q", chainID, strings.TrimSpace(out))
	}
	if len(topFirst) != depth {
		return nil, fmt.Errorf("snapshot %s has %d levels, expected %d", chainID, len(topFirst), depth)
	}
	dirs := make([]string, len(topFirst))
	for i, d := range topFirst {
		dirs[len(topFirst)-1-i] = d
	}
	return dirs, nil
}

// ExportSnapshot streams a tar of dir to w.
func (s *CtrStore) ExportSnapshot(ctx context.Context, dir string, w io.Writer) error {
	return s.tar(ctx, nil, w, "-C", dir, "--xattrs", "--xattrs-include=*", "--numeric-owner", "-cf", "-", ".")
}

// ApplySnapshot prepares an empty snapshot on parent, unpacks r into it,
// runs verify, and commits it as chainID, pinned against GC.
func (s *CtrStore) ApplySnapshot(ctx context.Context, chainID, parent string, r io.Reader, verify func() error) (err error) {
	tmp := "angryduck-rescue-" + randomSuffix()
	args := []string{"snapshots", "prepare", tmp}
	if parent != "" {
		args = append(args, parent)
	}
	if _, err := s.run(ctx, nil, nil, s.args(args...)...); err != nil {
		return err
	}
	committed := false
	defer func() {
		if !committed {
			s.removeSnapshot(tmp)
		}
	}()
	// Pin the work-in-progress too: a large layer can take a while.
	if _, err := s.run(ctx, nil, nil, s.args("snapshots", "label", tmp, gcRootLabel+"=angryduck-rescue")...); err != nil {
		return err
	}
	out, err := s.run(ctx, nil, nil, s.args("snapshots", "mounts", "/mnt", tmp)...)
	if err != nil {
		return err
	}
	m, err := parseMountCommand(out)
	if err != nil {
		return err
	}
	dest := m.upper
	if dest == "" {
		dest = m.bindSource // a snapshot with no parent is a plain bind mount
	}
	if dest == "" {
		return fmt.Errorf("no writable directory in mounts of %s: %q", tmp, strings.TrimSpace(out))
	}
	if err := s.tar(ctx, r, nil, "-C", dest, "--xattrs", "--xattrs-include=*", "--numeric-owner", "-xpf", "-"); err != nil {
		return err
	}
	if err := verify(); err != nil {
		return err
	}
	if _, err := s.run(ctx, nil, nil, s.args("snapshots", "commit", chainID, tmp)...); err != nil {
		if !strings.Contains(err.Error(), "already exists") {
			return err
		}
		// Another rescue or pull created it meanwhile. Theirs is as good
		// as ours; drop ours (the deferred remove) and use it.
		return nil
	}
	committed = true
	_, err = s.run(ctx, nil, nil, s.args("snapshots", "label", chainID, gcRootLabel+"=angryduck-rescue")...)
	return err
}

// Unpin removes the GC pin ApplySnapshot set.
func (s *CtrStore) Unpin(ctx context.Context, chainID string) error {
	_, err := s.run(ctx, nil, nil, s.args("snapshots", "label", chainID, gcRootLabel+"=")...)
	return err
}

// RemoveSnapshot deletes a snapshot by key.
func (s *CtrStore) RemoveSnapshot(ctx context.Context, key string) error {
	_, err := s.run(ctx, nil, nil, s.args("snapshots", "rm", key)...)
	return err
}

// removeSnapshot is best-effort cleanup, on its own context so it still
// runs when the rescue's context was cancelled.
func (s *CtrStore) removeSnapshot(key string) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	_ = s.RemoveSnapshot(ctx, key)
}

// tar runs the node's tar. stdout nil means output is discarded.
func (s *CtrStore) tar(ctx context.Context, stdin io.Reader, stdout io.Writer, args ...string) error {
	cmd, err := s.hx.Command(ctx, "tar", args...)
	if err != nil {
		return err
	}
	if stdout == nil {
		stdout = io.Discard
	}
	stderr := &tailBuffer{max: 4096}
	cmd.Stdin, cmd.Stdout, cmd.Stderr = stdin, stdout, stderr
	cmd, err = s.hx.start(ctx, cmd, "tar", args)
	if err == nil {
		err = cmd.Wait()
	}
	if err != nil {
		if ctx.Err() != nil {
			err = ctx.Err()
		}
		return fmt.Errorf("tar %s: %w: %s", strings.Join(args, " "), err, strings.TrimSpace(stderr.String()))
	}
	return nil
}

type mountCommand struct {
	bindSource string
	upper      string
	lower      []string // top first, as overlayfs lists them
}

// parseMountCommand reads what `ctr snapshots mounts` prints, e.g.
//
//	mount -t bind /var/lib/.../snapshots/214/fs /mnt -o ro,rbind
//	mount -t overlay overlay /mnt -o index=off,workdir=.../work,upperdir=.../fs,lowerdir=A:B
func parseMountCommand(out string) (mountCommand, error) {
	var m mountCommand
	f := strings.Fields(strings.TrimSpace(out))
	if len(f) < 7 || f[0] != "mount" || f[1] != "-t" {
		return m, fmt.Errorf("unexpected mounts output: %q", strings.TrimSpace(out))
	}
	opts := ""
	for i := 0; i < len(f)-1; i++ {
		if f[i] == "-o" {
			opts = f[i+1]
		}
	}
	switch f[2] {
	case "bind":
		m.bindSource = f[3]
	case "overlay":
		for _, o := range strings.Split(opts, ",") {
			if v, ok := strings.CutPrefix(o, "upperdir="); ok {
				m.upper = v
			}
			if v, ok := strings.CutPrefix(o, "lowerdir="); ok {
				m.lower = strings.Split(v, ":")
			}
		}
	default:
		return m, fmt.Errorf("unsupported mount type %q (only overlayfs snapshots can be shipped)", f[2])
	}
	return m, nil
}

func randomSuffix() string {
	b := make([]byte, 6)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}

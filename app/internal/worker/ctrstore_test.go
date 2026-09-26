package worker

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
)

// fakeCtr puts a shell script named ctr first on PATH. It logs its args,
// serves `content get` from files and saves `images import` input.
func fakeCtr(t *testing.T) (dir string) {
	t.Helper()
	dir = t.TempDir()
	script := `#!/bin/sh
echo "$@" >> "` + dir + `/args"
while [ "$1" = "--address" ] || [ "$1" = "-n" ]; do shift 2; done
case "$1 $2" in
  "content get") cat "` + dir + `/blob-$3" ;;
  "content ls") printf 'sha256:aaa\nsha256:bbb\n' ;;
  "images import") cat "$5" > "` + dir + `/imported" ;;
  *) echo "unexpected: $*" >&2; exit 1 ;;
esac
`
	if err := os.WriteFile(filepath.Join(dir, "ctr"), []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", dir+":"+os.Getenv("PATH"))
	return dir
}

func TestCtrStore_RealExec(t *testing.T) {
	dir := fakeCtr(t)
	_ = os.WriteFile(filepath.Join(dir, "blob-sha256:aaa"), []byte("hello"), 0o644)
	hx, err := NewHostExec("")
	if err != nil {
		t.Fatal(err)
	}
	s := NewCtrStore(hx, "k8s.io", "unix:///run/containerd/containerd.sock")
	ctx := context.Background()

	var buf bytes.Buffer
	if err := s.StreamBlob(ctx, "sha256:aaa", 5, &buf); err != nil || buf.String() != "hello" {
		t.Fatalf("StreamBlob: %q %v", buf.String(), err)
	}
	if err := s.StreamBlob(ctx, "sha256:aaa", 6, &bytes.Buffer{}); err == nil {
		t.Fatal("StreamBlob must fail when the size doesn't match")
	}
	if _, err := s.ReadBlob(ctx, "sha256:aaa", 3); err == nil {
		t.Fatal("ReadBlob must refuse blobs above max")
	}
	have, err := s.Digests(ctx)
	if err != nil || !have["sha256:aaa"] || !have["sha256:bbb"] {
		t.Fatalf("Digests: %v %v", have, err)
	}
	if err := s.Import(ctx, strings.NewReader("tar bytes"), "linux/amd64"); err != nil {
		t.Fatal(err)
	}
	if b, _ := os.ReadFile(filepath.Join(dir, "imported")); string(b) != "tar bytes" {
		t.Fatalf("import got %q", b)
	}
	args, _ := os.ReadFile(filepath.Join(dir, "args"))
	if !strings.Contains(string(args), "--address /run/containerd/containerd.sock -n k8s.io images import --platform linux/amd64 /dev/stdin") {
		t.Fatalf("ctr args:\n%s", args)
	}
}

func TestParseMountCommand(t *testing.T) {
	// Real `ctr snapshots mounts` lines from a stg node.
	bind := "mount -t bind /var/lib/containerd/io.containerd.snapshotter.v1.overlayfs/snapshots/214/fs /x -o ro,rbind\n"
	m, err := parseMountCommand(bind)
	if err != nil || m.bindSource != "/var/lib/containerd/io.containerd.snapshotter.v1.overlayfs/snapshots/214/fs" {
		t.Fatalf("%+v %v", m, err)
	}
	ov := "mount -t overlay overlay /x -o index=off,lowerdir=/s/20308/fs:/s/20306/fs:/s/214/fs\n"
	m, err = parseMountCommand(ov)
	if err != nil || strings.Join(m.lower, ",") != "/s/20308/fs,/s/20306/fs,/s/214/fs" || m.upper != "" {
		t.Fatalf("%+v %v", m, err)
	}
	active := "mount -t overlay overlay /x -o index=off,workdir=/s/9/work,upperdir=/s/9/fs,lowerdir=/s/214/fs\n"
	if m, _ = parseMountCommand(active); m.upper != "/s/9/fs" {
		t.Fatalf("%+v", m)
	}
	if _, err := parseMountCommand("mount -t btrfs /dev/x /x -o subvol=1"); err == nil {
		t.Fatal("non-overlayfs snapshotters must be refused")
	}
}

// TestSnapshotRoundTrip_RealTar exports a directory with the host's real
// tar and applies it into a "prepared snapshot" through a fake ctr, the
// same exec path the worker uses on a node.
func TestSnapshotRoundTrip_RealTar(t *testing.T) {
	if os.Geteuid() != 0 {
		t.Skip("needs root for ownership and device files")
	}
	dir := t.TempDir()
	src := filepath.Join(dir, "src")
	dst := filepath.Join(dir, "dst")
	_ = os.MkdirAll(filepath.Join(src, "etc/opaque"), 0o755)
	_ = os.MkdirAll(dst, 0o755)
	_ = os.WriteFile(filepath.Join(src, "etc/shadow"), []byte("secret"), 0o600)
	_ = os.Chown(filepath.Join(src, "etc/shadow"), 0, 42)
	_ = os.Symlink("shadow", filepath.Join(src, "etc/link"))
	// An overlayfs whiteout (deleted file) is a 0/0 character device.
	if err := syscall.Mknod(filepath.Join(src, "etc/deleted"), syscall.S_IFCHR|0o000, 0); err != nil {
		t.Skipf("mknod not permitted here: %v", err)
	}
	opaqueOK := syscall.Setxattr(filepath.Join(src, "etc/opaque"), "trusted.overlay.opaque", []byte("y"), 0) == nil

	script := `#!/bin/sh
while [ "$1" = "--address" ] || [ "$1" = "-n" ]; do shift 2; done
case "$2" in
  view|prepare|label|rm) ;;
  commit) echo "$3" >> "` + dir + `/committed" ;;
  mounts)
    case "$4" in
      angryduck-view-*) echo "mount -t bind ` + src + ` /mnt -o ro,rbind" ;;
      *) echo "mount -t bind ` + dst + ` /mnt -o rw,rbind" ;;
    esac ;;
  *) echo "unexpected: $*" >&2; exit 1 ;;
esac
`
	bin := filepath.Join(dir, "bin")
	_ = os.MkdirAll(bin, 0o755)
	_ = os.WriteFile(filepath.Join(bin, "ctr"), []byte(script), 0o755)
	t.Setenv("PATH", bin+":"+os.Getenv("PATH"))

	hx, _ := NewHostExec("")
	s := NewCtrStore(hx, "k8s.io", "")
	ctx := context.Background()

	dirs, err := s.SnapshotDirs(ctx, "sha256:aaa", 1)
	if err != nil || dirs[0] != src {
		t.Fatalf("SnapshotDirs: %v %v", dirs, err)
	}
	var buf bytes.Buffer
	if err := s.ExportSnapshot(ctx, dirs[0], &buf); err != nil {
		t.Fatal(err)
	}
	verified := false
	if err := s.ApplySnapshot(ctx, "sha256:aaa", "", &buf, func() error { verified = true; return nil }); err != nil {
		t.Fatal(err)
	}
	if !verified {
		t.Fatal("verify not called")
	}
	if b, _ := os.ReadFile(filepath.Join(dir, "committed")); strings.TrimSpace(string(b)) != "sha256:aaa" {
		t.Fatalf("committed %q", b)
	}

	var st syscall.Stat_t
	if err := syscall.Lstat(filepath.Join(dst, "etc/deleted"), &st); err != nil || st.Mode&syscall.S_IFMT != syscall.S_IFCHR || st.Rdev != 0 {
		t.Fatalf("whiteout device not preserved: %v mode=%o", err, st.Mode)
	}
	if err := syscall.Lstat(filepath.Join(dst, "etc/shadow"), &st); err != nil || st.Gid != 42 || st.Mode&0o777 != 0o600 {
		t.Fatalf("ownership/mode not preserved: gid=%d mode=%o", st.Gid, st.Mode&0o777)
	}
	if l, _ := os.Readlink(filepath.Join(dst, "etc/link")); l != "shadow" {
		t.Fatalf("symlink = %q", l)
	}
	if opaqueOK {
		v := make([]byte, 8)
		n, err := syscall.Getxattr(filepath.Join(dst, "etc/opaque"), "trusted.overlay.opaque", v)
		if err != nil || string(v[:n]) != "y" {
			t.Fatalf("opaque xattr not preserved: %v", err)
		}
	} else {
		t.Log("trusted.* xattrs not permitted here; opaque-dir check skipped")
	}
}

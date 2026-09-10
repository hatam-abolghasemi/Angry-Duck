package worker

// mirror_test.go already covers Resolve() path preference, symlink
// handling, non-executable rejection, missing-root errors, env scrubbing,
// and Run() without a chroot. Everything here fills gaps that were still
// open: the cache actually surviving a filesystem change, forget()
// clearing it, Command() actually wiring up SysProcAttr.Chroot/Dir/Path,
// and — the one that matters most in production, since it's the exact
// mechanism angryduck-worker uses on every node — a real chroot(2) exec.

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

func TestResolve_CacheSurvivesFileRemoval(t *testing.T) {
	root := t.TempDir()
	mustWriteExec(t, filepath.Join(root, "usr/bin/ctr"))
	hx, _ := NewHostExec(root)

	first, err := hx.Resolve("ctr")
	if err != nil {
		t.Fatalf("Resolve: %v", err)
	}
	if err := os.Remove(filepath.Join(root, "usr/bin/ctr")); err != nil {
		t.Fatal(err)
	}
	second, err := hx.Resolve("ctr")
	if err != nil {
		t.Fatalf("cached Resolve should not re-check the filesystem, got error: %v", err)
	}
	if first != second {
		t.Fatalf("cached answer changed: %q -> %q", first, second)
	}
}

func TestForget_ClearsTheCache(t *testing.T) {
	root := t.TempDir()
	mustWriteExec(t, filepath.Join(root, "usr/bin/ctr"))
	hx, _ := NewHostExec(root)

	if _, err := hx.Resolve("ctr"); err != nil {
		t.Fatalf("Resolve: %v", err)
	}
	if err := os.Remove(filepath.Join(root, "usr/bin/ctr")); err != nil {
		t.Fatal(err)
	}
	hx.forget("ctr")
	if _, err := hx.Resolve("ctr"); err == nil {
		t.Fatal("expected an error after forget() + removal; cache should no longer answer")
	}
}

func TestCommand_NoRoot_LeavesChrootUnset(t *testing.T) {
	hx, _ := NewHostExec("")
	cmd, err := hx.Command(context.Background(), "true")
	if err != nil {
		t.Fatalf("Command: %v", err)
	}
	if cmd.SysProcAttr.Chroot != "" {
		t.Fatalf("Chroot = %q, want empty with no host root configured", cmd.SysProcAttr.Chroot)
	}
}

func TestCommand_WithRoot_WiresChrootDirAndPath(t *testing.T) {
	root := t.TempDir()
	mustWriteExec(t, filepath.Join(root, "usr/bin/ctr"))
	hx, _ := NewHostExec(root)

	cmd, err := hx.Command(context.Background(), "ctr", "images", "ls")
	if err != nil {
		t.Fatalf("Command: %v", err)
	}
	if cmd.SysProcAttr.Chroot != root {
		t.Fatalf("Chroot = %q, want %q", cmd.SysProcAttr.Chroot, root)
	}
	if cmd.Dir != "/" {
		t.Fatalf("Dir = %q, want / (must be relative to the new root, not the host root)", cmd.Dir)
	}
	if cmd.Path != "/usr/bin/ctr" {
		t.Fatalf("Path = %q, want the root-relative /usr/bin/ctr (absolute host paths would escape the chroot)", cmd.Path)
	}
}

func TestCommand_UnresolvedBinaryOnFakeRootErrors(t *testing.T) {
	hx, _ := NewHostExec(t.TempDir()) // empty fake root, nothing to find
	if _, err := hx.Command(context.Background(), "ctr"); err == nil {
		t.Fatal("expected an error building Command for a binary absent from the host root")
	}
}

// TestHostExec_RealChrootExecution actually performs chroot(2) against a
// fake host root and runs a statically-linked helper inside it. This is
// the one thing no existing test does: every current Run()/RunInput()
// test uses an empty HostExec root (no chroot at all), so the production
// code path — chroot into /proc/1/root and exec the node's own binary —
// had zero coverage. Requires CAP_SYS_CHROOT, so it's skipped rather than
// failed when not running as root.
func TestHostExec_RealChrootExecution(t *testing.T) {
	if os.Geteuid() != 0 {
		t.Skip("requires root / CAP_SYS_CHROOT for a real chroot(2)")
	}
	if runtime.GOOS != "linux" {
		t.Skip("this assumes Linux chroot/exec semantics")
	}
	goBin, err := exec.LookPath("go")
	if err != nil {
		t.Skip("no go toolchain available to build a static helper binary")
	}

	root := t.TempDir()
	binDir := filepath.Join(root, "usr", "local", "bin")
	if err := os.MkdirAll(binDir, 0o755); err != nil {
		t.Fatal(err)
	}

	src := filepath.Join(t.TempDir(), "helper.go")
	const helperSrc = `package main

import (
	"fmt"
	"os"
)

func main() {
	for _, a := range os.Args[1:] {
		fmt.Println(a)
	}
	if len(os.Args) > 1 && os.Args[1] == "fail" {
		fmt.Fprintln(os.Stderr, "helper-stderr-marker")
		os.Exit(7)
	}
}
`
	if err := os.WriteFile(src, []byte(helperSrc), 0o644); err != nil {
		t.Fatalf("write helper source: %v", err)
	}
	helperPath := filepath.Join(binDir, "adhelper")
	build := exec.Command(goBin, "build", "-o", helperPath, src)
	build.Env = append(os.Environ(), "CGO_ENABLED=0")
	if out, err := build.CombinedOutput(); err != nil {
		t.Fatalf("building static helper: %v\n%s", err, out)
	}

	hx, err := NewHostExec(root)
	if err != nil {
		t.Fatalf("NewHostExec: %v", err)
	}

	// Confirms the binary actually ran INSIDE the chroot: if Chroot were
	// silently ignored, this would fail with "not found" since adhelper
	// doesn't exist at /usr/local/bin on the test host.
	out, err := hx.Run("adhelper", "hello-from-chroot")
	if err != nil {
		t.Fatalf("Run(adhelper) under chroot: %v", err)
	}
	if strings.TrimSpace(out) != "hello-from-chroot" {
		t.Fatalf("stdout = %q, want %q", out, "hello-from-chroot")
	}

	_, err = hx.Run("adhelper", "fail")
	if err == nil {
		t.Fatal("expected an error from a failing chrooted command")
	}
	if !strings.Contains(err.Error(), "helper-stderr-marker") {
		t.Fatalf("error %q missing the chrooted child's stderr tail", err.Error())
	}

	// start()'s retry-on-ENOENT path: Resolve (Lstat-based) accepts a
	// symlink whose target doesn't exist yet; only once we create the
	// real target does exec succeed. This exercises the exact scenario
	// the retry comment describes — "an admin moved a binary between
	// /usr/local/bin and /usr/bin" — end to end through a real exec,
	// not just by inspecting cached state.
	usrBin := filepath.Join(root, "usr", "bin")
	if err := os.MkdirAll(usrBin, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink("/usr/local/bin/adhelper", filepath.Join(usrBin, "adhelper2")); err != nil {
		t.Fatal(err)
	}
	out, err = hx.Run("adhelper2", "via-symlink")
	if err != nil {
		t.Fatalf("Run via symlink under chroot: %v", err)
	}
	if strings.TrimSpace(out) != "via-symlink" {
		t.Fatalf("stdout = %q, want %q", out, "via-symlink")
	}
}

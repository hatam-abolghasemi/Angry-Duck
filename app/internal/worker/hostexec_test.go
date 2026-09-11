package worker

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// mustWriteExec creates an executable regular file at path (and its parent
// directories), for tests that need HostExec.Resolve to find a real,
// runnable binary inside a fake host root.
func mustWriteExec(t *testing.T, path string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	if err := os.WriteFile(path, []byte("#!/bin/sh\nexit 0\n"), 0o755); err != nil {
		t.Fatalf("write %s: %v", path, err)
	}
}

func TestNewHostExec_RejectsMissingRoot(t *testing.T) {
	_, err := NewHostExec(filepath.Join(t.TempDir(), "does-not-exist"))
	if err == nil {
		t.Fatal("expected an error for a non-existent host root")
	}
}

func TestNewHostExec_EmptyRootMeansNoChroot(t *testing.T) {
	hx, err := NewHostExec("")
	if err != nil {
		t.Fatalf("NewHostExec: %v", err)
	}
	if hx.Root() != "" {
		t.Fatalf("Root() = %q, want empty", hx.Root())
	}
}

func TestHostFile_PrependsRoot(t *testing.T) {
	root := t.TempDir()
	hx, _ := NewHostExec(root)
	if got, want := hx.HostFile("/etc/containerd/config.toml"), root+"/etc/containerd/config.toml"; got != want {
		t.Fatalf("HostFile = %q, want %q", got, want)
	}
}

func TestResolve_ResolvesFromHostRootPreferringUsrLocal(t *testing.T) {
	root := t.TempDir()
	// Both /usr/local/bin and /usr/bin have a "ctr" — /usr/local must win,
	// matching a login shell's own PATH precedence.
	mustWriteExec(t, filepath.Join(root, "usr/local/bin/ctr"))
	mustWriteExec(t, filepath.Join(root, "usr/bin/ctr"))
	hx, _ := NewHostExec(root)

	got, err := hx.Resolve("ctr")
	if err != nil {
		t.Fatalf("Resolve: %v", err)
	}
	if got != "/usr/local/bin/ctr" {
		t.Fatalf("Resolve = %q, want /usr/local/bin/ctr to win over /usr/bin", got)
	}
}

func TestResolve_AcceptsAbsoluteSymlinkWithoutFollowingIt(t *testing.T) {
	root := t.TempDir()
	if err := os.MkdirAll(filepath.Join(root, "usr/bin"), 0o755); err != nil {
		t.Fatal(err)
	}
	// Points somewhere that doesn't exist under THIS process's own root —
	// if Resolve used os.Stat (follows symlinks) rather than os.Lstat,
	// this would wrongly report "not found" for a valid on-node link.
	link := filepath.Join(root, "usr/bin/ctr")
	if err := os.Symlink("/opt/containerd/bin/ctr", link); err != nil {
		t.Fatal(err)
	}
	hx, _ := NewHostExec(root)

	got, err := hx.Resolve("ctr")
	if err != nil {
		t.Fatalf("Resolve with a dangling symlink: %v", err)
	}
	if got != "/usr/bin/ctr" {
		t.Fatalf("Resolve = %q, want /usr/bin/ctr", got)
	}
}

func TestResolve_IgnoresNonExecutableRegularFiles(t *testing.T) {
	root := t.TempDir()
	if err := os.MkdirAll(filepath.Join(root, "usr/bin"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "usr/bin/ctr"), []byte("data"), 0o644); err != nil {
		t.Fatal(err)
	}
	hx, _ := NewHostExec(root)
	if _, err := hx.Resolve("ctr"); err == nil {
		t.Fatal("expected an error resolving a non-executable regular file")
	}
}

func TestResolve_NotFoundOnEmptyRoot(t *testing.T) {
	hx, _ := NewHostExec(t.TempDir())
	if _, err := hx.Resolve("ctr"); err == nil {
		t.Fatal("expected an error, found nothing on an empty fake root")
	}
}

func TestResolve_NoRootUsesRealPath(t *testing.T) {
	hx, _ := NewHostExec("")
	if _, err := hx.Resolve("true"); err != nil {
		t.Fatalf("Resolve(true) via the real PATH: %v", err)
	}
	if _, err := hx.Resolve("angryduck-definitely-not-a-real-binary"); err == nil {
		t.Fatal("expected an error for a binary that doesn't exist")
	}
}

func TestHostExecChildDoesNotInheritPodEnv(t *testing.T) {
	hx, _ := NewHostExec("")
	t.Setenv("CONTAINER_RUNTIME_ENDPOINT", "unix:///should/not/leak")
	cmd, err := hx.Command(context.Background(), "true")
	if err != nil {
		t.Fatalf("Command: %v", err)
	}
	for _, e := range cmd.Env {
		if strings.HasPrefix(e, "CONTAINER_RUNTIME_ENDPOINT=") {
			t.Fatalf("child env leaked CONTAINER_RUNTIME_ENDPOINT: %v", cmd.Env)
		}
	}
}

func TestHostExecRunWithoutChroot(t *testing.T) {
	hx, _ := NewHostExec("")
	out, err := hx.Run("echo", "hello")
	if err != nil {
		t.Fatalf("Run(echo): %v", err)
	}
	if strings.TrimSpace(out) != "hello" {
		t.Fatalf("stdout = %q, want %q", out, "hello")
	}
}

func TestHostExecRunFailureIncludesStderr(t *testing.T) {
	hx, _ := NewHostExec("")
	_, err := hx.Run("sh", "-c", "echo boom 1>&2; exit 3")
	if err == nil {
		t.Fatal("expected an error from a failing command")
	}
	if !strings.Contains(err.Error(), "boom") {
		t.Fatalf("error %q missing the command's stderr", err.Error())
	}
}

func TestRedactArgs(t *testing.T) {
	cases := []struct {
		in   []string
		want string
	}{
		{[]string{"pull", "-u", "admin", "myimage"}, "pull -u <redacted> myimage"},
		{[]string{"login", "--creds", "admin:hunter2"}, "login --creds <redacted>"},
		{[]string{"login", "--user", "admin"}, "login --user <redacted>"},
		{[]string{"images", "ls"}, "images ls"},
	}
	for _, c := range cases {
		if got := redactArgs(c.in); got != c.want {
			t.Errorf("redactArgs(%v) = %q, want %q", c.in, got, c.want)
		}
	}
}

func TestTailBufferKeepsOnlyTheEnd(t *testing.T) {
	tb := &tailBuffer{max: 5}
	_, _ = tb.Write([]byte("abc"))
	_, _ = tb.Write([]byte("defgh"))
	if got := tb.String(); got != "defgh" {
		t.Fatalf("tailBuffer = %q, want %q", got, "defgh")
	}
}

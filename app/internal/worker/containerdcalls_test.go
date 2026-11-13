package worker

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/containerd/containerd/errdefs"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// removedFile returns a path that still opens a file whose name is gone
// from the filesystem (link count 0), as a socket mounted into a container
// looks once containerd has replaced it on the node.
func removedFile(t *testing.T) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), "containerd.sock")
	f, err := os.Create(p)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { f.Close() })
	if err := os.Remove(p); err != nil {
		t.Fatal(err)
	}
	return fmt.Sprintf("/proc/self/fd/%d", f.Fd())
}

func TestSocketRemoved(t *testing.T) {
	if socketRemoved(filepath.Join(t.TempDir(), "missing.sock")) {
		t.Error("a missing file must not count as removed")
	}
	live := filepath.Join(t.TempDir(), "live.sock")
	if err := os.WriteFile(live, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	if socketRemoved(live) {
		t.Error("a file with a name must not count as removed")
	}
	if !socketRemoved(removedFile(t)) {
		t.Error("a file with link count 0 must count as removed")
	}
}

func TestStaleAwareDialerCallsHookOnlyForRemovedSocket(t *testing.T) {
	t.Cleanup(func() { staleSocketHook.Store(nil) })
	var got []string
	OnStaleSocket(func(p string) { got = append(got, p) })
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	if _, err := staleAwareDialer(ctx, "unix://"+filepath.Join(t.TempDir(), "missing.sock")); err == nil {
		t.Fatal("dialing a missing socket must fail")
	}
	if len(got) != 0 {
		t.Fatalf("hook called for a missing socket: %v", got)
	}

	removed := removedFile(t)
	if _, err := staleAwareDialer(ctx, "unix://"+removed); err == nil {
		t.Fatal("dialing a removed socket must fail")
	}
	if len(got) != 1 || got[0] != removed {
		t.Fatalf("hook calls = %v, want [%s]", got, removed)
	}
}

func TestContainerdUnreachable(t *testing.T) {
	for _, c := range []struct {
		err  error
		want bool
	}{
		{nil, false},
		{errors.New("boom"), false},
		{status.Error(codes.Unavailable, "connection refused"), true},
		{fmt.Errorf("listing images: %w", status.Error(codes.Unavailable, "connection refused")), true},
		{fmt.Errorf("creating lease: %w", errdefs.ErrUnavailable), true},
		{status.Error(codes.NotFound, "no such image"), false},
	} {
		if got := containerdUnreachable(c.err); got != c.want {
			t.Errorf("containerdUnreachable(%v) = %v, want %v", c.err, got, c.want)
		}
	}
}

package worker

import (
	"context"
	"net"
	"os"
	"strings"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/containerd/containerd/errdefs"
	"github.com/containerd/containerd/pkg/dialer"
	"google.golang.org/grpc"
	"google.golang.org/grpc/backoff"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	"angryduck/internal/metrics"
)

var containerdCallsTotal = metrics.NewCounterVec(
	"angryduck_worker_containerd_calls_total",
	"gRPC calls this worker made to containerd (its API and CRI), by service and method.",
	"node", "method")

// ContainerdDialOptions is containerd's own default dial options (the
// client uses passed options instead of its defaults, not on top of them)
// plus interceptors counting every call made over the connection, so the
// load the worker puts on containerd is visible, and content writes
// encoded into pooled buffers (see grpcbuffers.go).
//
// gRPC deprecates WithBlock, FailOnNonTempDialError and
// WithReturnConnectionError for its newer NewClient. containerd 1.7's
// client still dials with grpc.DialContext, which honors them, and passes
// exactly these by default: dropping them would change how the worker
// connects (it would no longer wait for containerd, or fail fast, at
// startup).
func ContainerdDialOptions(nodeID string) []grpc.DialOption {
	bo := backoff.DefaultConfig
	bo.MaxDelay = 3 * time.Second
	name := func(full string) string {
		// "/containerd.services.content.v1.Content/Info" -> "Content/Info"
		full = strings.TrimPrefix(full, "/")
		if i := strings.LastIndex(full, "."); i >= 0 {
			full = full[i+1:]
		}
		return full
	}
	return append([]grpc.DialOption{
		//lint:ignore SA1019 containerd 1.7's own default, see above
		grpc.WithBlock(),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		//lint:ignore SA1019 containerd 1.7's own default, see above
		grpc.FailOnNonTempDialError(true),
		grpc.WithConnectParams(grpc.ConnectParams{Backoff: bo}),
		grpc.WithContextDialer(staleAwareDialer),
		//lint:ignore SA1019 containerd 1.7's own default, see above
		grpc.WithReturnConnectionError(),
		grpc.WithChainUnaryInterceptor(func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
			containerdCallsTotal.Inc(nodeID, name(method))
			return invoker(ctx, method, req, reply, cc, opts...)
		}),
		grpc.WithChainStreamInterceptor(func(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn, method string, streamer grpc.Streamer, opts ...grpc.CallOption) (grpc.ClientStream, error) {
			containerdCallsTotal.Inc(nodeID, name(method))
			return streamer(ctx, desc, cc, method, opts...)
		}),
	}, pooledWrites()...)
}

// staleSocketHook is called when containerd's socket, as this container
// sees it, has been removed from the node: containerd restarted (a
// package upgrade, needrestart, a config push) and made a new socket at
// the same path. A socket mounted as a single file stays bound to the old
// one, which refuses every connection, so nothing but a container restart
// (which mounts the path again) brings containerd back.
var staleSocketHook atomic.Pointer[func(path string)]

// OnStaleSocket sets what to do when containerd's socket turns out to be
// a removed one. Set it before connecting.
func OnStaleSocket(fn func(path string)) { staleSocketHook.Store(&fn) }

// staleAwareDialer is containerd's own dialer. Only when a dial fails does
// it look at the socket file (one stat, no containerd call): gRPC dials at
// startup and after the connection drops, so a healthy worker never pays
// for it.
func staleAwareDialer(ctx context.Context, addr string) (net.Conn, error) {
	conn, err := dialer.ContextDialer(ctx, addr)
	if err != nil {
		if fn := staleSocketHook.Load(); fn != nil {
			path := strings.TrimPrefix(addr, "unix://")
			if socketRemoved(path) {
				(*fn)(path)
			}
		}
	}
	return conn, err
}

// socketRemoved reports whether the file at path no longer has a name on
// the node (link count 0): it was deleted after being mounted here. A
// missing file or one that can't be read says nothing either way.
func socketRemoved(path string) bool {
	fi, err := os.Stat(path)
	if err != nil {
		return false
	}
	st, ok := fi.Sys().(*syscall.Stat_t)
	return ok && st.Nlink == 0
}

// containerdUnreachable reports whether err means containerd itself could
// not be reached (its socket refuses or is gone), as opposed to a call
// containerd answered with an error.
func containerdUnreachable(err error) bool {
	if err == nil {
		return false
	}
	if errdefs.IsUnavailable(err) {
		return true
	}
	if s, ok := status.FromError(err); ok {
		return s.Code() == codes.Unavailable
	}
	return false
}

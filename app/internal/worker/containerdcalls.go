package worker

import (
	"context"
	"strings"
	"time"

	"github.com/containerd/containerd/pkg/dialer"
	"google.golang.org/grpc"
	"google.golang.org/grpc/backoff"
	"google.golang.org/grpc/credentials/insecure"

	"angryduck/internal/metrics"
)

var containerdCallsTotal = metrics.NewCounterVec(
	"angryduck_worker_containerd_calls_total",
	"gRPC calls this worker made to containerd (its API and CRI), by service and method.",
	"node", "method")

// ContainerdDialOptions is containerd's own default dial options (the
// client uses passed options instead of its defaults, not on top of them)
// plus interceptors counting every call made over the connection, so the
// load the worker puts on containerd is visible.
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
	return []grpc.DialOption{
		grpc.WithBlock(),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.FailOnNonTempDialError(true),
		grpc.WithConnectParams(grpc.ConnectParams{Backoff: bo}),
		grpc.WithContextDialer(dialer.ContextDialer),
		grpc.WithReturnConnectionError(),
		grpc.WithChainUnaryInterceptor(func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
			containerdCallsTotal.Inc(nodeID, name(method))
			return invoker(ctx, method, req, reply, cc, opts...)
		}),
		grpc.WithChainStreamInterceptor(func(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn, method string, streamer grpc.Streamer, opts ...grpc.CallOption) (grpc.ClientStream, error) {
			containerdCallsTotal.Inc(nodeID, name(method))
			return streamer(ctx, desc, cc, method, opts...)
		}),
	}
}

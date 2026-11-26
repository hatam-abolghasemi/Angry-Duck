package worker

import (
	"context"
	"net"
	"net/http"
	"sync"
	"time"

	"angryduck/internal/metrics"
)

// Connections between workers.
//
// Spread, repair, rescue and the mirror talk to other workers' API
// (WORKER_LISTEN_ADDR), and the mirror to the controller too, over
// keep-alive connections, so the many requests of one job share a few
// connections. Once the work ends, an idle connection is only memory on
// both sides: two goroutines and their buffers in the client, one in the
// server, until something closes it. Before 1.15.1 nothing did, and every
// worker kept one to each peer it had ever talked to.
//
// They are now closed in two ways:
//
//   - When the work that used them ends (an event): spread when its last
//     job here ends, repair after each image it repairs, rescue when its
//     last rescue ends. Connections still in use are left alone.
//   - After IDLE_CONN_TIMEOUT_S idle, on both sides: a safety net for
//     what no event covers (the mirror's requests, which have no end of
//     their own; a peer before 1.15.1 or a dead one that never closes its
//     side; an event missed).

var peerConnections = metrics.NewGaugeVec(
	"angryduck_worker_peer_connections",
	"Open TCP connections between this worker and other Angry Duck processes, by direction: in (accepted by this worker's API, WORKER_LISTEN_ADDR, from peers and the controller), out (opened by its spread, repair, rescue and mirror to other workers' API, and by the mirror to the controller). Idle ones are closed when the work that used them ends, or after IDLE_CONN_TIMEOUT_S.",
	"node", "direction",
)

// connCount keeps one direction's count and its gauge in step.
type connCount struct {
	mu        sync.Mutex
	n         int64
	node, dir string
}

func (c *connCount) add(d int64) {
	if c == nil {
		return
	}
	c.mu.Lock()
	c.n += d
	peerConnections.Set(float64(c.n), c.node, c.dir)
	c.mu.Unlock()
}

// Set once at startup (SetPeerConns), before any peer client is built.
var (
	peerIdleTimeout time.Duration
	peerConnsOut    *connCount
	peerConnsIn     *connCount
)

// SetPeerConns sets how long an idle connection between workers is kept
// (IDLE_CONN_TIMEOUT_S; 0 keeps it until the work's end closes it) and
// starts counting this worker's peer connections. Call before building
// Spread, ImageRepair, Rescue and Mirror; it returns the hook counting the
// API server's side (http.Server.ConnState).
func SetPeerConns(nodeID string, idle time.Duration) func(net.Conn, http.ConnState) {
	peerIdleTimeout = idle
	peerConnsOut = &connCount{node: nodeID, dir: "out"}
	peerConnsIn = &connCount{node: nodeID, dir: "in"}
	in := peerConnsIn
	peerConnsOut.add(0)
	in.add(0)
	return func(_ net.Conn, st http.ConnState) {
		switch st {
		case http.StateNew:
			in.add(1)
		case http.StateClosed, http.StateHijacked:
			in.add(-1)
		}
	}
}

// peerTransport is a transport for requests to other workers: never
// through a proxy (they are on the node network), dial and response-header
// timeouts as given, idle connections closed after IDLE_CONN_TIMEOUT_S,
// and every connection counted.
func peerTransport(dial, header time.Duration) *http.Transport {
	d := (&net.Dialer{Timeout: dial}).DialContext
	count := peerConnsOut
	return &http.Transport{
		Proxy: nil,
		DialContext: func(ctx context.Context, network, addr string) (net.Conn, error) {
			c, err := d(ctx, network, addr)
			if err != nil || count == nil {
				return c, err
			}
			count.add(1)
			return &countedConn{Conn: c, count: count}, nil
		},
		ResponseHeaderTimeout: header,
		IdleConnTimeout:       peerIdleTimeout,
	}
}

// countedConn takes itself off its count when closed, once.
type countedConn struct {
	net.Conn
	count *connCount
	once  sync.Once
}

func (c *countedConn) Close() error {
	err := c.Conn.Close()
	c.once.Do(func() { c.count.add(-1) })
	return err
}

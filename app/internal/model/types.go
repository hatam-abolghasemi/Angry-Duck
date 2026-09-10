// Package model holds the wire-format types shared between the Angry Duck
// controller and worker processes.
package model

import "time"

// WorkerReport is pushed periodically (default every 15s) by every worker to
// the controller. It carries just enough information for the controller to
// rank nodes by disk utilization and image locality, and to know how to
// reach the worker back.
type WorkerReport struct {
	NodeID      string  `json:"node_id"`
	Address     string  `json:"address"`     // host:port the controller can reach this worker's HTTP API on
	Utilization float64 `json:"utilization"` // 0.0-1.0, root filesystem usage ratio
	// Repos is the deduplicated set of bare repository identities (see
	// imageref.Repo) present locally on this node, across every tag and
	// alias form the runtime reports. It lets the controller prefer
	// pre-pulling a newly-pushed tag onto a node that already has some
	// older tag of the same repo, since most layers are typically shared
	// between tags of the same image. Omitted (nil/empty) is always safe
	// — the ranker just falls back to utilization-only ranking for that
	// node, identical to its behavior before this field existed.
	Repos []string `json:"repos,omitempty"`
	// Digests is every manifest (or index) digest present locally and
	// exportable — the exact identity containerd asks a registry mirror
	// for. The controller answers /peers from this. Omitted is safe: the
	// node just never gets chosen as a transfer source.
	Digests []string `json:"digests,omitempty"`
	// Tags maps every tag-form local reference (e.g.
	// "repo-afra.internal-dev.example.com/rich-ubuntu:22.04") this node currently
	// holds to its manifest digest. Unlike Digests (an unordered set),
	// this preserves the name a pod's spec actually asks for, which is
	// exactly what /resolve needs to answer "what does this tag mean"
	// when origin itself can't be asked. Omitted is safe: this node's
	// tags simply never become a fallback answer for anyone.
	Tags      map[string]string `json:"tags,omitempty"`
	Timestamp time.Time         `json:"timestamp"`
}

// PeersResponse is the controller's answer to GET /peers?digest=...:
// addresses of fresh workers that reported having that digest, already
// shuffled so concurrent requesters spread across sources.
type PeersResponse struct {
	Peers []string `json:"peers"`
}

// ResolveResponse is the controller's answer to GET /resolve?tag=...: the
// fleet's current best-known digest for that tag, and how long that
// digest has been the answer. Used only by a worker's tag-fallback
// importer (see internal/worker's Mirror.FallbackImport), when a pod is
// stuck in ImagePullBackOff and origin itself cannot resolve the tag the
// normal way. ObservedAt is the age signal callers must log/surface —
// this is a trust decision, not a confirmation, and staleness is the
// whole risk.
type ResolveResponse struct {
	Digest     string    `json:"digest,omitempty"`
	ObservedAt time.Time `json:"observed_at,omitempty"`
}

// Announce is sent by a worker right after an image lands via peer
// transfer, so it becomes a source for others at once instead of at its
// next periodic report.
type Announce struct {
	NodeID string `json:"node_id"`
	Digest string `json:"digest"`
}

// ReportAck is returned to a worker after it submits a report.
type ReportAck struct {
	Accepted bool `json:"accepted"`
}

// PreheatRequest is the payload the CI/CD pipeline sends right after
// `docker push`, asking Angry Duck to pre-pull the freshly pushed image onto
// some low-utilization nodes ahead of / alongside the GitOps sync.
type PreheatRequest struct {
	Image string `json:"image"`
}

// PreheatResponse tells the caller what the controller decided to do.
type PreheatResponse struct {
	Accepted     bool     `json:"accepted"`
	Reason       string   `json:"reason,omitempty"`
	TargetImage  string   `json:"target_image,omitempty"`
	OrderedNodes []string `json:"ordered_nodes,omitempty"`
}

// PullOrder is what the controller sends to a specific worker's /pull
// endpoint, instructing it to pull an image locally.
type PullOrder struct {
	Image     string    `json:"image"`
	OrderedAt time.Time `json:"ordered_at"`
}

// PullAck is the worker's synchronous response to a pull order. The actual
// pull happens asynchronously; this just confirms the order was accepted.
type PullAck struct {
	Accepted bool   `json:"accepted"`
	Reason   string `json:"reason,omitempty"`
}

// WorkerStatus is a read-only snapshot of a worker's state, used by the
// controller's /status debug endpoint.
type WorkerStatus struct {
	NodeID      string    `json:"node_id"`
	Address     string    `json:"address"`
	Utilization float64   `json:"utilization"`
	Repos       []string  `json:"repos,omitempty"`
	DigestCount int       `json:"digest_count"`
	LastSeen    time.Time `json:"last_seen"`
	Fresh       bool      `json:"fresh"`
}

// ControllerStatus is the full debug snapshot served at /status.
type ControllerStatus struct {
	Workers     []WorkerStatus `json:"workers"`
	FreshCount  int            `json:"fresh_count"`
	TargetImage string         `json:"target_image,omitempty"`
	TargetSetAt *time.Time     `json:"target_set_at,omitempty"`
}

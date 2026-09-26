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
	// Images is every full image reference present locally (tag form and
	// repo@digest form; bare "sha256:..." image IDs are dropped). The
	// controller's rescuer uses it to find a node that holds the exact
	// image a stuck pod needs. Omitted means "unknown", which only makes
	// this node ineligible as a rescue source.
	Images    []string  `json:"images,omitempty"`
	Timestamp time.Time `json:"timestamp"`
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
	LastSeen    time.Time `json:"last_seen"`
	Fresh       bool      `json:"fresh"`
}

// ControllerStatus is the full debug snapshot served at /status.
type ControllerStatus struct {
	Workers      []WorkerStatus      `json:"workers"`
	FreshCount   int                 `json:"fresh_count"`
	TargetImage  string              `json:"target_image,omitempty"`
	TargetSetAt  *time.Time          `json:"target_set_at,omitempty"`
	Rescues      []RescueStatus      `json:"rescues,omitempty"`
	Propagations []PropagationStatus `json:"propagations,omitempty"`
}

// PropagationStatus is the propagator's view of one image being spread.
type PropagationStatus struct {
	Image     string    `json:"image"`
	StartedAt time.Time `json:"started_at"`
	EndsAt    time.Time `json:"ends_at"`
	Have      []string  `json:"have"`              // eligible nodes that have it
	Missing   []string  `json:"missing"`           // eligible nodes still without it
	InFlight  []string  `json:"in_flight"`         // nodes receiving it right now
	Skipped   []string  `json:"skipped,omitempty"` // nodes left out (excluded or too full)
	Failing   []string  `json:"failing,omitempty"` // nodes whose last attempt failed
}

// RescueStatus is the rescuer's view of one image one node can't pull.
type RescueStatus struct {
	Node                string    `json:"node"`
	Image               string    `json:"image"`
	Pods                []string  `json:"pods"`
	StuckSince          time.Time `json:"stuck_since"` // when the rescuer first saw it
	InFlight            bool      `json:"in_flight"`
	LastResult          string    `json:"last_result,omitempty"`
	LastError           string    `json:"last_error,omitempty"`
	ConsecutiveFailures int       `json:"consecutive_failures"`
	NextAttempt         time.Time `json:"next_attempt"`
}

// RescueSource is a peer worker that holds the image a rescue needs.
type RescueSource struct {
	NodeID  string `json:"node_id"`
	Address string `json:"address"`
}

// RescueOrder is what the controller sends to the /rescue endpoint of the
// worker on the node whose pod is stuck in ImagePullBackOff. The worker
// tries Sources in order until one of them ships the image.
type RescueOrder struct {
	Image   string         `json:"image"`
	Sources []RescueSource `json:"sources"`
	// Reason is "rescue" (a pod is stuck on this node) or "propagate" (the
	// image is being spread after a push). Only used for logs and metrics.
	Reason string `json:"reason,omitempty"`
}

// RescueResult is the target worker's answer to a RescueOrder. /rescue is
// synchronous: it returns once the image is imported, or every source has
// failed.
type RescueResult struct {
	OK        bool   `json:"ok"`
	Source    string `json:"source,omitempty"` // node that shipped the image
	Blobs     int    `json:"blobs"`            // blobs shipped (missing ones only)
	Snapshots int    `json:"snapshots"`        // layers shipped as snapshot directories
	Bytes     int64  `json:"bytes"`            // bytes received over the wire
	Error     string `json:"error,omitempty"`
}

// BlobPlanRequest asks a source worker which blobs make up image for one
// platform (served at /blobs/plan).
type BlobPlanRequest struct {
	Image    string `json:"image"`
	Platform string `json:"platform"` // "os/arch[/variant]", e.g. "linux/amd64"
}

// SnapshotExportRequest asks a source worker to stream one layer's
// snapshot directory as a gzip-compressed tar (served at /snapshots/export).
// ChainID must be a layer of the image's plan; anything else is refused.
type SnapshotExportRequest struct {
	Image    string `json:"image"`
	Platform string `json:"platform"`
	ChainID  string `json:"chain_id"`
}

// BlobExportRequest asks a source worker to stream a partial OCI archive
// holding only Digests (served at /blobs/export). Every digest must belong
// to the image's plan; anything else is refused.
type BlobExportRequest struct {
	Image    string   `json:"image"`
	Platform string   `json:"platform"`
	Digests  []string `json:"digests"`
}

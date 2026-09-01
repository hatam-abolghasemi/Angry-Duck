// Package model holds the wire-format types shared between the Angry Duck
// controller and worker processes.
package model

import "time"

// WorkerReport is pushed periodically (default every 15s) by every worker to
// the controller. It carries just enough information for the controller to
// rank nodes by disk utilization and to know how to reach the worker back.
type WorkerReport struct {
	NodeID      string    `json:"node_id"`
	Address     string    `json:"address"`     // host:port the controller can reach this worker's HTTP API on
	Utilization float64   `json:"utilization"` // 0.0-1.0, root filesystem usage ratio
	Timestamp   time.Time `json:"timestamp"`
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

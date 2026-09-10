package model

// PeerSourceRequest asks the controller for workers that may already have
// content for the requested repository. Exact manifest/blob availability is
// checked by the registry mirror on demand; the controller only narrows the
// candidate set using its cheap repo inventory.
type PeerSourceRequest struct {
	Image      string `json:"image"`
	TargetNode string `json:"target_node"`
}

type PeerSourceCandidate struct {
	NodeID  string `json:"node_id"`
	Address string `json:"address"`
}

type PeerSourceResponse struct {
	Candidates []PeerSourceCandidate `json:"candidates,omitempty"`
}

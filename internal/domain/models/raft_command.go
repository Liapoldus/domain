package models

import "encoding/json"

// RaftCommand contains only deterministic data. Authorization, time, random
// IDs and external lookups must be resolved before it is proposed to Raft.
type RaftCommand struct {
	Kind     string          `json:"kind"`
	Previous Model           `json:"previous,omitempty"`
	Next     Model           `json:"next,omitempty"`
	Tenant   string          `json:"tenant,omitempty"`
	Site     string          `json:"site,omitempty"`
	Entity   string          `json:"entity,omitempty"`
	ID       string          `json:"id,omitempty"`
	Row      json.RawMessage `json:"row,omitempty"`
	WriteID  string          `json:"writeId,omitempty"`
	Group    string          `json:"group,omitempty"`
	Ops      []RaftCommand   `json:"ops,omitempty"`
	Epoch    int64           `json:"epoch,omitempty"`
}

// ApplyResult reports the durable outcome of one applied FSM entry. It is
// returned for every success (including replays and writeId deduplication);
// failures are returned as errors instead.
type ApplyResult struct {
	Duplicate bool `json:"duplicate"`
}

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
}

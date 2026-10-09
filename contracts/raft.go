package contracts

type RPCMethod struct {
	Name                  string `json:"name,omitempty"`
	Mode                  string `json:"mode,omitempty"`
	MaxPayloadBytes       int    `json:"maxPayloadBytes,omitempty"`
	MaxStreamMessageBytes int    `json:"maxStreamMessageBytes,omitempty"`
	ChunkBytes            int    `json:"chunkBytes,omitempty"`
	MaxBytes              int64  `json:"maxBytes,omitempty"`
	TimeoutMilliseconds   int64  `json:"timeoutMs,omitempty"`
	Heartbeat             string `json:"heartbeat,omitempty"`
	FirstFrame            string `json:"firstFrame,omitempty"`
	RemainingFrames       string `json:"remainingFrames,omitempty"`
}
type RaftMethods struct {
	AppendEntries   RPCMethod `json:"appendEntries"`
	RequestVote     RPCMethod `json:"requestVote"`
	RequestPreVote  RPCMethod `json:"requestPreVote"`
	TimeoutNow      RPCMethod `json:"timeoutNow"`
	InstallSnapshot RPCMethod `json:"installSnapshot"`
}
type RaftContract struct {
	SchemaVersion         string      `json:"schemaVersion"`
	Security              string      `json:"security"`
	Methods               RaftMethods `json:"methods"`
	AppendEntriesPipeline string      `json:"appendEntriesPipeline"`
}

// Raft returns the typed peer contract; no file loading or parsing occurs.
func Raft() RaftContract {
	return RaftContract{SchemaVersion: "liapoldus.domain.raft-peer.v1", Security: "mutualTLS", AppendEntriesPipeline: "unsupported; callers fall back to unary appendEntries", Methods: RaftMethods{AppendEntries: RPCMethod{Name: "liapoldus.domain.raft.v1.append-entries", Mode: "call", MaxPayloadBytes: 4194304, TimeoutMilliseconds: 10000, Heartbeat: "appendEntries with no log entries"}, RequestVote: RPCMethod{Name: "liapoldus.domain.raft.v1.request-vote", Mode: "call", MaxPayloadBytes: 4194304, TimeoutMilliseconds: 10000}, RequestPreVote: RPCMethod{Name: "liapoldus.domain.raft.v1.request-pre-vote", Mode: "call", MaxPayloadBytes: 4194304, TimeoutMilliseconds: 10000}, TimeoutNow: RPCMethod{Name: "liapoldus.domain.raft.v1.timeout-now", Mode: "call", MaxPayloadBytes: 4194304, TimeoutMilliseconds: 10000}, InstallSnapshot: RPCMethod{Name: "liapoldus.domain.raft.v1.install-snapshot", Mode: "stream", FirstFrame: "InstallSnapshotRequest JSON", RemainingFrames: "snapshot bytes", ChunkBytes: 262144, MaxStreamMessageBytes: 1048576, MaxBytes: 536870912, TimeoutMilliseconds: 600000}}}
}

package contracts

// Capability defines a product method and its schemas and execution bounds.
type Capability struct {
	Name            string `json:"name"`
	Mode            string `json:"mode"`
	PayloadSchema   string `json:"payloadSchema"`
	ResponseSchema  string `json:"responseSchema"`
	ErrorSchema     string `json:"errorSchema"`
	MaxPayloadBytes int    `json:"maxPayloadBytes"`
	TimeoutMs       int    `json:"timeoutMs"`
}
type Configuration struct {
	SchemaVersion string `json:"schemaVersion"`
	Schema        string `json:"schema"`
}
type Compatibility struct {
	RaftLogCommandKinds      []string `json:"raftLogCommandKinds"`
	FSMSnapshotFormatVersion int      `json:"fsmSnapshotFormatVersion"`
	ERSchemaVersion          string   `json:"erSchemaVersion"`
	PeerContracts            []string `json:"peerContracts"`
}
type Manifest struct {
	Name          string        `json:"name"`
	Configuration Configuration `json:"configuration"`
	Capabilities  []Capability  `json:"capabilities"`
	Compatibility Compatibility `json:"compatibility"`
}

// Product returns an owned copy of the manifest, also used by runtime dispatch.
func Product() Manifest {
	return Manifest{
		Name: "domain", Configuration: Configuration{SchemaVersion: "1", Schema: "contracts/v1/model.schema.json"},
		Capabilities:  []Capability{{Name: "domain.create", Mode: "call", PayloadSchema: "contracts/v1/schemas/create.request.json", ResponseSchema: "contracts/v1/schemas/write.response.json", ErrorSchema: "contracts/v1/schemas/envelope.json", MaxPayloadBytes: 1048576, TimeoutMs: 15000}, {Name: "domain.get", Mode: "call", PayloadSchema: "contracts/v1/schemas/get.request.json", ResponseSchema: "contracts/v1/schemas/get.response.json", ErrorSchema: "contracts/v1/schemas/envelope.json", MaxPayloadBytes: 1048576, TimeoutMs: 10000}, {Name: "domain.update", Mode: "call", PayloadSchema: "contracts/v1/schemas/update.request.json", ResponseSchema: "contracts/v1/schemas/write.response.json", ErrorSchema: "contracts/v1/schemas/envelope.json", MaxPayloadBytes: 1048576, TimeoutMs: 15000}, {Name: "domain.delete", Mode: "call", PayloadSchema: "contracts/v1/schemas/delete.request.json", ResponseSchema: "contracts/v1/schemas/write.response.json", ErrorSchema: "contracts/v1/schemas/envelope.json", MaxPayloadBytes: 1048576, TimeoutMs: 15000}, {Name: "domain.batch", Mode: "call", PayloadSchema: "contracts/v1/schemas/batch.request.json", ResponseSchema: "contracts/v1/schemas/batch.response.json", ErrorSchema: "contracts/v1/schemas/envelope.json", MaxPayloadBytes: 2097152, TimeoutMs: 30000}, {Name: "domain.query", Mode: "call", PayloadSchema: "contracts/v1/schemas/query.request.json", ResponseSchema: "contracts/v1/schemas/query.response.json", ErrorSchema: "contracts/v1/schemas/envelope.json", MaxPayloadBytes: 1048576, TimeoutMs: 10000}, {Name: "domain.cluster.status", Mode: "call", PayloadSchema: "contracts/v1/schemas/cluster-status.request.json", ResponseSchema: "contracts/v1/schemas/status.response.json", ErrorSchema: "contracts/v1/schemas/envelope.json", MaxPayloadBytes: 1048576, TimeoutMs: 3000}, {Name: "domain.migration.plan", Mode: "call", PayloadSchema: "contracts/v1/schemas/migration-plan.request.json", ResponseSchema: "contracts/v1/schemas/migration-plan.response.json", ErrorSchema: "contracts/v1/schemas/envelope.json", MaxPayloadBytes: 1048576, TimeoutMs: 15000}, {Name: "domain.migration.status", Mode: "call", PayloadSchema: "contracts/v1/schemas/migration-status.request.json", ResponseSchema: "contracts/v1/schemas/migration-status.response.json", ErrorSchema: "contracts/v1/schemas/envelope.json", MaxPayloadBytes: 1048576, TimeoutMs: 10000}},
		Compatibility: Compatibility{RaftLogCommandKinds: []string{"init", "put", "migrate", "rollback", "create", "update", "delete", "batch"}, FSMSnapshotFormatVersion: 2, ERSchemaVersion: "1", PeerContracts: []string{"liapoldus.domain.raft-peer.v1", "liapoldus.domain.product-peer.v1"}}}
}

package models

// StoredRevision is one durable model_state document together with the epoch
// it was read at. The raw document bytes are the fingerprint source so the
// service never re-serializes a model it did not store itself.
type StoredRevision struct {
	Model    Model
	Epoch    int64
	Document []byte
}

// MigrationAudit is one durable record of an applied migrate or rollback.
type MigrationAudit struct {
	Kind              string `json:"kind"`
	WriteID           string `json:"writeId"`
	EpochBefore       int64  `json:"epochBefore"`
	EpochAfter        int64  `json:"epochAfter"`
	FingerprintBefore string `json:"fingerprintBefore"`
	FingerprintAfter  string `json:"fingerprintAfter"`
}

// PreflightFailure reports a row-level violation the migration preflight
// found before any Raft proposal. The token is a fixed contract identifier.
type PreflightFailure struct {
	Reason string
}

func (err *PreflightFailure) Error() string {
	if err == nil || err.Reason == "" {
		return "migration preflight failed"
	}
	return "migration preflight failed: " + err.Reason
}

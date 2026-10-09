package migration

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/Liapoldus/domain/internal/application/call"

	"github.com/Liapoldus/domain/internal/domain/interfaces"
	"github.com/Liapoldus/domain/internal/domain/models"
)

const migrationAuditLimit = 16

// Service exposes the domain migration lifecycle: plan, apply,
// rollback and durable status. It proposes deterministic commands through the
// raft-backed ports and reports the durable state re-read after each apply.
type Service struct {
	proposer interfaces.Proposer
	barrier  interfaces.ReadBarrier
	store    interfaces.MigrationStore
}

// New wires one service instance to its adapters.
func New(
	proposer interfaces.Proposer,
	barrier interfaces.ReadBarrier,
	store interfaces.MigrationStore,
) *Service {
	return &Service{
		proposer: proposer,
		barrier:  barrier,
		store:    store,
	}
}

type planResponse struct {
	Safe             bool                   `json:"safe"`
	PlanError        string                 `json:"planError,omitempty"`
	Steps            []models.MigrationStep `json:"steps,omitempty"`
	Epoch            int64                  `json:"epoch"`
	AppliedIndex     uint64                 `json:"appliedIndex"`
	Fingerprint      string                 `json:"fingerprint"`
	FingerprintAfter string                 `json:"fingerprintAfter"`
}

type applyResponse struct {
	WriteID           string `json:"writeId"`
	Duplicate         bool   `json:"duplicate"`
	Applied           bool   `json:"applied"`
	EpochBefore       int64  `json:"epochBefore"`
	EpochAfter        int64  `json:"epochAfter"`
	FingerprintBefore string `json:"fingerprintBefore"`
	FingerprintAfter  string `json:"fingerprintAfter"`
	AppliedIndex      uint64 `json:"appliedIndex"`
}

type statusResponse struct {
	Epoch               int64                   `json:"epoch"`
	AppliedIndex        uint64                  `json:"appliedIndex"`
	Fingerprint         string                  `json:"fingerprint"`
	SnapshotAvailable   bool                    `json:"snapshotAvailable"`
	SnapshotFingerprint string                  `json:"snapshotFingerprint,omitempty"`
	Audit               []models.MigrationAudit `json:"audit"`
}

// Plan describes how the domain would migrate to the next model. It never
// mutates state: a rejected plan only reports why it is unsafe.
func (service *Service) Plan(ctx context.Context, scope models.Scope, next models.Model, epoch int64) (json.RawMessage, *models.ProductError) {
	if err := service.checkScope(scope); err != nil {
		return nil, err
	}
	if !service.proposer.IsLeader() {
		return nil, service.notLeaderError()
	}
	appliedIndex, err := service.barrier.Barrier(ctx)
	if err != nil {
		return nil, call.FromPort(err)
	}
	revision, err := service.store.CurrentRevision(ctx)
	if err != nil {
		return nil, call.FromPort(err)
	}
	if err := checkMigrationEpoch(revision, epoch); err != nil {
		return nil, err
	}
	plan := Plan(revision.Model, next)
	fingerprint := fingerprintOf(revision.Document)
	fingerprintAfter := fingerprint
	if plan.Safe {
		encoded, marshalErr := json.Marshal(next)
		if marshalErr != nil {
			return nil, call.Failure(call.CodeInternal, false, false, "migration model serialization failed")
		}
		fingerprintAfter = fingerprintOf(encoded)
	}
	data := planResponse{
		Safe:             plan.Safe,
		PlanError:        plan.Error,
		Steps:            plan.Steps,
		Epoch:            revision.Epoch,
		AppliedIndex:     appliedIndex,
		Fingerprint:      fingerprint,
		FingerprintAfter: fingerprintAfter,
	}
	return call.Marshal(data)
}

// Apply migrates the domain to the next model under one writeId. The durable
// outcome is re-read after the apply so retries and replays report the state
// the cluster actually committed.
func (service *Service) Apply(ctx context.Context, scope models.Scope, next models.Model, writeID string, epoch int64) (json.RawMessage, *models.ProductError) {
	if err := service.checkScope(scope); err != nil {
		return nil, err
	}
	if !call.ValidWriteID(writeID) {
		return nil, call.Failure(call.CodeInvalidRequest, false, false, "writeId does not match the writeId schema")
	}
	if !service.proposer.IsLeader() {
		return nil, service.notLeaderError()
	}
	if _, err := service.barrier.Barrier(ctx); err != nil {
		return nil, call.FromPort(err)
	}
	revision, err := service.store.CurrentRevision(ctx)
	if err != nil {
		return nil, call.FromPort(err)
	}
	if err := checkMigrationEpoch(revision, epoch); err != nil {
		return nil, err
	}
	plan := Plan(revision.Model, next)
	if !plan.Safe {
		return nil, call.Failure(call.CodeInvalidRequest, false, false, "migration plan is not safe: "+plan.Error)
	}
	if err := service.store.PreflightMigration(ctx, revision, next); err != nil {
		var failure *models.PreflightFailure
		if errors.As(err, &failure) {
			return nil, call.Failure(call.CodeInvalidRequest, false, false, "migration preflight failed: "+failure.Reason)
		}
		return nil, call.FromPort(err)
	}
	command := models.RaftCommand{
		Kind:     "migrate",
		Previous: revision.Model,
		Next:     next,
		Tenant:   scope.Tenant,
		Site:     scope.Site,
		Group:    scope.Group,
		WriteID:  writeID,
		Epoch:    revision.Epoch,
	}
	result, err := service.proposer.Apply(ctx, command)
	if err != nil {
		return nil, call.FromPort(err)
	}
	return service.writeOutcome(ctx, writeID, revision, result)
}

// Rollback restores the exact pre-migration snapshot under one writeId. The
// command carries the parsed snapshot model so the FSM can verify the target
// byte-for-byte against the stored snapshot.
func (service *Service) Rollback(ctx context.Context, scope models.Scope, writeID string, epoch int64) (json.RawMessage, *models.ProductError) {
	if err := service.checkScope(scope); err != nil {
		return nil, err
	}
	if !call.ValidWriteID(writeID) {
		return nil, call.Failure(call.CodeInvalidRequest, false, false, "writeId does not match the writeId schema")
	}
	if !service.proposer.IsLeader() {
		return nil, service.notLeaderError()
	}
	if _, err := service.barrier.Barrier(ctx); err != nil {
		return nil, call.FromPort(err)
	}
	revision, err := service.store.CurrentRevision(ctx)
	if err != nil {
		return nil, call.FromPort(err)
	}
	if err := checkMigrationEpoch(revision, epoch); err != nil {
		return nil, err
	}
	snapshot, found, err := service.store.SnapshotRevision(ctx)
	if err != nil {
		return nil, call.FromPort(err)
	}
	target := models.Model{}
	if found {
		target = snapshot.Model
	}
	command := models.RaftCommand{
		Kind:    "rollback",
		Next:    target,
		Tenant:  scope.Tenant,
		Site:    scope.Site,
		Group:   scope.Group,
		WriteID: writeID,
		Epoch:   revision.Epoch,
	}
	result, err := service.proposer.Apply(ctx, command)
	if err != nil {
		return nil, call.FromPort(err)
	}
	return service.writeOutcome(ctx, writeID, revision, result)
}

// Status reports the durable migration state of this node. It runs on any
// node without a leader fence so followers can answer during an election.
func (service *Service) Status(ctx context.Context, scope models.Scope) (json.RawMessage, *models.ProductError) {
	if err := service.checkScope(scope); err != nil {
		return nil, err
	}
	revision, err := service.store.CurrentRevision(ctx)
	if err != nil {
		return nil, call.FromPort(err)
	}
	snapshot, found, err := service.store.SnapshotRevision(ctx)
	if err != nil {
		return nil, call.FromPort(err)
	}
	audit, err := service.store.RecentMigrations(ctx, scope.Tenant, scope.Site, migrationAuditLimit)
	if err != nil {
		return nil, call.FromPort(err)
	}
	if audit == nil {
		audit = []models.MigrationAudit{}
	}
	appliedIndex, err := service.store.AppliedRaftIndex(ctx)
	if err != nil {
		return nil, call.FromPort(err)
	}
	data := statusResponse{
		Epoch:             revision.Epoch,
		AppliedIndex:      appliedIndex,
		Fingerprint:       fingerprintOf(revision.Document),
		SnapshotAvailable: found,
		Audit:             audit,
	}
	if found {
		data.SnapshotFingerprint = fingerprintOf(snapshot.Document)
	}
	return call.Marshal(data)
}

// writeOutcome re-establishes a fresh barrier after one apply, re-reads the
// durable revision and reports both epochs and fingerprints around the change.
func (service *Service) writeOutcome(ctx context.Context, writeID string, before models.StoredRevision, result *models.ApplyResult) (json.RawMessage, *models.ProductError) {
	appliedIndex, err := service.barrier.Barrier(ctx)
	if err != nil {
		return nil, call.FromPort(err)
	}
	after, err := service.store.CurrentRevision(ctx)
	if err != nil {
		return nil, call.FromPort(err)
	}
	data := applyResponse{
		WriteID:           writeID,
		Duplicate:         result.Duplicate,
		Applied:           !result.Duplicate,
		EpochBefore:       before.Epoch,
		EpochAfter:        after.Epoch,
		FingerprintBefore: fingerprintOf(before.Document),
		FingerprintAfter:  fingerprintOf(after.Document),
		AppliedIndex:      appliedIndex,
	}
	return call.Marshal(data)
}

func (service *Service) checkScope(scope models.Scope) *models.ProductError {
	if scope.Tenant == "" || scope.Site == "" {
		return call.Failure(call.CodeForbidden, false, false, "the caller identity maps to no domain scope")
	}
	return nil
}

func (service *Service) notLeaderError() *models.ProductError {
	message := "this domain node is not the raft leader"
	if address := service.proposer.LeaderAddress(); address != "" {
		message = fmt.Sprintf("%s; leader: %s", message, address)
	}
	return call.Failure(call.CodeNotLeader, true, false, message)
}

func checkMigrationEpoch(revision models.StoredRevision, epoch int64) *models.ProductError {
	if epoch != 0 && epoch != revision.Epoch {
		return call.Failure(call.CodeEpochMismatch, true, false, "the client epoch does not match the current domain state")
	}
	return nil
}

func fingerprintOf(document []byte) string {
	sum := sha256.Sum256(document)
	return hex.EncodeToString(sum[:])
}

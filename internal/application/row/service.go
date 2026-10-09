// Package row validates and executes scoped row operations.
package row

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/Liapoldus/domain/internal/application/call"
	"github.com/Liapoldus/domain/internal/application/query"
	"regexp"

	"github.com/Liapoldus/domain/internal/domain/interfaces"
	"github.com/Liapoldus/domain/internal/domain/models"
)

var (
	identifierPattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9_.-]{0,127}$`)
)

// BatchOp is one nested operation of a domain.batch request.
type BatchOp struct {
	Op     string          `json:"op"`
	Entity string          `json:"entity"`
	ID     string          `json:"id"`
	Row    json.RawMessage `json:"row,omitempty"`
}

// Service implements the domain v1 product peer methods over the
// raft-backed ports. It performs no storage or raft operations itself.
type Service struct {
	proposer interfaces.Proposer
	barrier  interfaces.ReadBarrier
	reader   interfaces.RowReader
	status   interfaces.ClusterStatus
	revision interfaces.Revision
}

// New wires one service instance to its adapters.
func New(
	proposer interfaces.Proposer,
	barrier interfaces.ReadBarrier,
	reader interfaces.RowReader,
	status interfaces.ClusterStatus,
	revision interfaces.Revision,
) *Service {
	return &Service{
		proposer: proposer,
		barrier:  barrier,
		reader:   reader,
		status:   status,
		revision: revision,
	}
}

// Create inserts one row. It returns the write response data (duplicate,
// entity, id, appliedIndex, epoch).
func (service *Service) Create(ctx context.Context, scope models.Scope, entity, id string, writeID string, document []byte, epoch int64) (json.RawMessage, *models.ProductError) {
	return service.write(ctx, scope, "create", entity, id, writeID, document, epoch)
}

// Update replaces one row. It returns the write response data.
func (service *Service) Update(ctx context.Context, scope models.Scope, entity, id string, writeID string, document []byte, epoch int64) (json.RawMessage, *models.ProductError) {
	return service.write(ctx, scope, "update", entity, id, writeID, document, epoch)
}

// Delete removes one row. It returns the write response data.
func (service *Service) Delete(ctx context.Context, scope models.Scope, entity, id string, writeID string, epoch int64) (json.RawMessage, *models.ProductError) {
	return service.write(ctx, scope, "delete", entity, id, writeID, nil, epoch)
}

// Batch applies up to 100 nested operations atomically under one writeId.
func (service *Service) Batch(ctx context.Context, scope models.Scope, writeID string, ops []BatchOp, epoch int64) (json.RawMessage, *models.ProductError) {
	if err := service.checkScope(scope); err != nil {
		return nil, err
	}
	if writeID != "" && !call.ValidWriteID(writeID) {
		return nil, call.Failure(call.CodeInvalidRequest, false, false, "writeId does not match the writeId schema")
	}
	if len(ops) == 0 || len(ops) > 100 {
		return nil, call.Failure(call.CodeInvalidRequest, false, false, "batch requires between 1 and 100 operations")
	}
	revision := service.revision.Model(ctx)
	command := models.RaftCommand{
		Kind:    "batch",
		Tenant:  scope.Tenant,
		Site:    scope.Site,
		Group:   scope.Group,
		WriteID: writeID,
		Ops:     make([]models.RaftCommand, 0, len(ops)),
		Epoch:   epoch,
	}
	for _, operation := range ops {
		switch operation.Op {
		case "create", "update":
		case "delete":
		default:
			return nil, call.Failure(call.CodeInvalidRequest, false, false, "operation must be one of create, update, delete")
		}
		if !validDomainIdentifier(operation.Entity) || !validDomainIdentifier(operation.ID) {
			return nil, call.Failure(call.CodeInvalidRequest, false, false, "operation entity and id must match the identifier schema")
		}
		entityDef, ok := findDomainEntity(revision, operation.Entity)
		if !ok {
			return nil, call.Failure(call.CodeInvalidRequest, false, false, "unknown entity "+operation.Entity)
		}
		if entityDef.OwnerGroup != scope.Group {
			return nil, call.Failure(call.CodeForbidden, false, false, "entity "+operation.Entity+" is not owned by the caller scope")
		}
		var normalized []byte
		switch operation.Op {
		case "create", "update":
			if len(operation.Row) == 0 || !json.Valid(operation.Row) {
				return nil, call.Failure(call.CodeInvalidRequest, false, false, "operation row must be a valid JSON object")
			}
			normalized, ok = normalizeRowResult(revision, operation.Entity, operation.ID, operation.Row)
			if !ok {
				return nil, call.Failure(call.CodeInvalidRequest, false, false, "operation row does not match the entity schema")
			}
		}
		command.Ops = append(command.Ops, models.RaftCommand{
			Kind:   operation.Op,
			Entity: operation.Entity,
			ID:     operation.ID,
			Row:    normalized,
		})
	}
	return service.applyWrite(ctx, scope, &command, "batch")
}

// Get reads one row under a leader-confirmed fresh-read fence.
func (service *Service) Get(ctx context.Context, scope models.Scope, entity, id string, epoch int64) (json.RawMessage, *models.ProductError) {
	if err := service.checkScope(scope); err != nil {
		return nil, err
	}
	if !validDomainIdentifier(entity) || !validDomainIdentifier(id) {
		return nil, call.Failure(call.CodeInvalidRequest, false, false, "entity and id must match the identifier schema")
	}
	if _, ok := findDomainEntity(service.revision.Model(ctx), entity); !ok {
		return nil, call.Failure(call.CodeInvalidRequest, false, false, "unknown entity "+entity)
	}
	if !service.proposer.IsLeader() {
		return nil, service.notLeaderError()
	}
	if epoch != 0 && epoch != service.revision.Epoch(ctx) {
		return nil, call.Failure(call.CodeEpochMismatch, true, false, "the client epoch does not match the current domain state")
	}
	appliedIndex, err := service.barrier.Barrier(ctx)
	if err != nil {
		return nil, call.FromPort(err)
	}
	row, found, err := service.reader.ReadRow(ctx, scope.Tenant, scope.Site, entity, id)
	if err != nil {
		return nil, call.FromPort(err)
	}
	data := struct {
		Found        bool            `json:"found"`
		Row          json.RawMessage `json:"row,omitempty"`
		AppliedIndex uint64          `json:"appliedIndex"`
		Epoch        int64           `json:"epoch"`
	}{
		Found:        found,
		Row:          row,
		AppliedIndex: appliedIndex,
		Epoch:        service.revision.Epoch(ctx),
	}
	if !found {
		data.Row = nil
	}
	return call.Marshal(data)
}

// Query executes one SELECT over the configured entities after a
// leader-confirmed barrier.
func (service *Service) Query(ctx context.Context, scope models.Scope, sqlText string, params []json.RawMessage, maxRows int, epoch int64) (json.RawMessage, *models.ProductError) {
	if err := service.checkScope(scope); err != nil {
		return nil, err
	}
	if !service.proposer.IsLeader() {
		return nil, service.notLeaderError()
	}
	if epoch != 0 && epoch != service.revision.Epoch(ctx) {
		return nil, call.Failure(call.CodeEpochMismatch, true, false, "the client epoch does not match the current domain state")
	}
	appliedIndex, err := service.barrier.Barrier(ctx)
	if err != nil {
		return nil, call.FromPort(err)
	}
	revision := service.revision.Model(ctx)
	rows := make(map[string][]json.RawMessage, len(revision.Entities))
	for _, entity := range revision.Entities {
		scanned, scanErr := service.reader.ScanEntity(ctx, scope.Tenant, scope.Site, entity.Name)
		if scanErr != nil {
			return nil, call.FromPort(scanErr)
		}
		documents := make([]json.RawMessage, 0, len(scanned))
		for _, row := range scanned {
			documents = append(documents, row.Data)
		}
		rows[entity.Name] = documents
	}
	decoded, decodeErr := decodeQueryParams(params)
	if decodeErr != nil {
		return nil, call.Failure(call.CodeInvalidRequest, false, false, "query parameters must be JSON scalars")
	}
	result, queryErr := query.Execute(revision, rows, sqlText, decoded, maxRows)
	if queryErr != nil {
		var rejected *query.Error
		if errors.As(queryErr, &rejected) {
			return nil, call.Failure(call.CodeQueryRejected, false, false, rejected.Message)
		}
		return nil, call.Failure(call.CodeInternal, false, false, "query planner failed")
	}
	data := struct {
		Columns      []string `json:"columns"`
		Rows         [][]any  `json:"rows"`
		RowCount     int      `json:"rowCount"`
		AppliedIndex uint64   `json:"appliedIndex"`
		Epoch        int64    `json:"epoch"`
	}{
		Columns:      result.Columns,
		Rows:         result.Rows,
		RowCount:     len(result.Rows),
		AppliedIndex: appliedIndex,
		Epoch:        service.revision.Epoch(ctx),
	}
	return call.Marshal(data)
}

// Status reports the cluster status from any node.
func (service *Service) Status(ctx context.Context) (json.RawMessage, *models.ProductError) {
	status, err := service.status.Status(ctx)
	if err != nil {
		return nil, call.FromPort(err)
	}
	if len(status.Voters) == 0 {
		status.Voters = make([]string, 0)
	}
	data := struct {
		Ready         bool     `json:"ready"`
		Leader        string   `json:"leader"`
		LeaderAddress string   `json:"leaderAddress"`
		Term          uint64   `json:"term"`
		CommitIndex   uint64   `json:"commitIndex"`
		AppliedIndex  uint64   `json:"appliedIndex"`
		Voters        []string `json:"voters"`
		Quorum        int      `json:"quorum"`
		Epoch         int64    `json:"epoch"`
	}{
		Ready:         status.Ready,
		Leader:        status.Leader,
		LeaderAddress: status.LeaderAddress,
		Term:          status.Term,
		CommitIndex:   status.CommitIndex,
		AppliedIndex:  status.AppliedIndex,
		Voters:        status.Voters,
		Quorum:        status.Quorum,
		Epoch:         status.Epoch,
	}
	return call.Marshal(data)
}

// write is the shared create/update/delete flow: scope, schema, owner-group,
// normalization, leadership, propose, barrier, response.
func (service *Service) write(ctx context.Context, scope models.Scope, kind, entity, id, writeID string, document []byte, epoch int64) (json.RawMessage, *models.ProductError) {
	if err := service.checkScope(scope); err != nil {
		return nil, err
	}
	if !validDomainIdentifier(entity) || !validDomainIdentifier(id) {
		return nil, call.Failure(call.CodeInvalidRequest, false, false, "entity and id must match the identifier schema")
	}
	if writeID != "" && !call.ValidWriteID(writeID) {
		return nil, call.Failure(call.CodeInvalidRequest, false, false, "writeId does not match the writeId schema")
	}
	revision := service.revision.Model(ctx)
	entityDef, ok := findDomainEntity(revision, entity)
	if !ok {
		return nil, call.Failure(call.CodeInvalidRequest, false, false, "unknown entity "+entity)
	}
	if entityDef.OwnerGroup != scope.Group {
		return nil, call.Failure(call.CodeForbidden, false, false, "entity "+entity+" is not owned by the caller scope")
	}
	var normalized []byte
	switch kind {
	case "create", "update":
		if len(document) == 0 || !json.Valid(document) {
			return nil, call.Failure(call.CodeInvalidRequest, false, false, "row must be a valid JSON object")
		}
		normalized, ok = normalizeRowResult(revision, entity, id, document)
		if !ok {
			return nil, call.Failure(call.CodeInvalidRequest, false, false, "row does not match the entity schema")
		}
	case "delete":
	default:
		return nil, call.Failure(call.CodeInternal, false, false, "unknown write kind")
	}
	command := &models.RaftCommand{
		Kind:    kind,
		Tenant:  scope.Tenant,
		Site:    scope.Site,
		Entity:  entity,
		ID:      id,
		Row:     normalized,
		WriteID: writeID,
		Group:   scope.Group,
		Epoch:   epoch,
	}
	return service.applyWrite(ctx, scope, command, kind)
}

// applyWrite proposes one command, waits for the response, then re-establishes
// a fresh barrier to report the durable applied index.
func (service *Service) applyWrite(ctx context.Context, scope models.Scope, command *models.RaftCommand, kind string) (json.RawMessage, *models.ProductError) {
	if !service.proposer.IsLeader() {
		return nil, service.notLeaderError()
	}
	if command.Epoch != 0 && command.Epoch != service.revision.Epoch(ctx) {
		return nil, call.Failure(call.CodeEpochMismatch, true, false, "the client epoch does not match the current domain state")
	}
	result, err := service.proposer.Apply(ctx, *command)
	if err != nil {
		return nil, call.FromPort(err)
	}
	appliedIndex, barrierErr := service.barrier.Barrier(ctx)
	if barrierErr != nil {
		return nil, call.FromPort(barrierErr)
	}
	epoch := service.revision.Epoch(ctx)
	if kind == "batch" {
		applied := !result.Duplicate
		data := struct {
			WriteID      string `json:"writeId,omitempty"`
			Duplicate    bool   `json:"duplicate"`
			Applied      bool   `json:"applied"`
			AppliedIndex uint64 `json:"appliedIndex"`
			Epoch        int64  `json:"epoch"`
		}{
			WriteID:      command.WriteID,
			Duplicate:    result.Duplicate,
			Applied:      applied,
			AppliedIndex: appliedIndex,
			Epoch:        epoch,
		}
		return call.Marshal(data)
	}
	data := struct {
		WriteID      string `json:"writeId,omitempty"`
		Duplicate    bool   `json:"duplicate"`
		Entity       string `json:"entity"`
		ID           string `json:"id"`
		AppliedIndex uint64 `json:"appliedIndex"`
		Epoch        int64  `json:"epoch"`
	}{
		WriteID:      command.WriteID,
		Duplicate:    result.Duplicate,
		Entity:       command.Entity,
		ID:           command.ID,
		AppliedIndex: appliedIndex,
		Epoch:        epoch,
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

// LeaderAddress reports the current raft leader's peer address, or an empty
// string when no leader is known. The presentation layer uses it to route
// peer-forwarded read calls; it never exposes row or scope data.
func (service *Service) LeaderAddress() string {
	return service.proposer.LeaderAddress()
}

func validDomainIdentifier(value string) bool {
	return value != "" && identifierPattern.MatchString(value)
}

func findDomainEntity(model models.Model, name string) (models.Entity, bool) {
	for _, entity := range model.Entities {
		if entity.Name == name {
			return entity, true
		}
	}
	return models.Entity{}, false
}

// normalizeRowResult wraps application.Normalize for the product service.
func normalizeRowResult(model models.Model, entity, id string, document []byte) ([]byte, bool) {
	normalized, err := Normalize(model, entity, id, document)
	if err != nil {
		return nil, false
	}
	return normalized, true
}

func decodeQueryParams(params []json.RawMessage) ([]any, error) {
	decoded := make([]any, 0, len(params))
	for _, raw := range params {
		var value any
		decoder := json.NewDecoder(bytes.NewReader(raw))
		decoder.UseNumber()
		if err := decoder.Decode(&value); err != nil {
			return nil, err
		}
		decoded = append(decoded, value)
	}
	return decoded, nil
}

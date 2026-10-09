package main

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/Liapoldus/domain/tests/fixtures/check"
	"os"
	"path/filepath"
	"strings"

	"github.com/Liapoldus/domain/internal/domain/interfaces"
	"github.com/Liapoldus/domain/internal/domain/models"
	"github.com/Liapoldus/domain/internal/infrastructure/raftfsm"
	modelsqlite "github.com/Liapoldus/domain/internal/infrastructure/sqlite"
	"github.com/hashicorp/raft"
)

type step struct {
	Action      string          `json:"action"`
	Target      string          `json:"target"`
	Index       uint64          `json:"index,omitempty"`
	Command     json.RawMessage `json:"command,omitempty"`
	Tenant      string          `json:"tenant,omitempty"`
	Site        string          `json:"site,omitempty"`
	Entity      string          `json:"entity,omitempty"`
	ID          string          `json:"id,omitempty"`
	Mode        string          `json:"mode,omitempty"`
	Snapshot    string          `json:"snapshot,omitempty"`
	SnapshotRef *int            `json:"snapshotRef,omitempty"`
	Epoch       int64           `json:"epoch,omitempty"`
}

type request struct {
	Steps []step `json:"steps"`
}

type stepResult struct {
	OK           bool            `json:"ok"`
	Code         string          `json:"code,omitempty"`
	Duplicate    *bool           `json:"duplicate,omitempty"`
	Applied      *int            `json:"applied,omitempty"`
	Found        *bool           `json:"found,omitempty"`
	Row          json.RawMessage `json:"row,omitempty"`
	AppliedIndex *uint64         `json:"appliedIndex,omitempty"`
	Snapshot     string          `json:"snapshot,omitempty"`
	Epoch        *int64          `json:"epoch,omitempty"`
}

type response struct {
	Results []stepResult `json:"results"`
}

type probe struct {
	ctx        context.Context
	root       string
	stores     map[string]*modelsqlite.Store
	fsms       map[string]*raftfsm.FSM
	exports    map[int]string
	models     map[string]models.Model
	snapshots  map[string]models.Model
	migrations int
}

func main() {
	if err := run(); err != nil {
		check.Value(fmt.Fprintln(os.Stderr, "Domain product commands probe failed:", err))
		os.Exit(1)
	}
}

func run() error {
	var input request
	if err := json.NewDecoder(os.Stdin).Decode(&input); err != nil {
		return fmt.Errorf("decode input: %w", err)
	}
	root, err := os.MkdirTemp("", "domain-product-commands-")
	if err != nil {
		return err
	}
	defer func() { check.Must(os.RemoveAll(root)) }()
	state := &probe{
		ctx:       context.Background(),
		root:      root,
		stores:    make(map[string]*modelsqlite.Store),
		fsms:      make(map[string]*raftfsm.FSM),
		exports:   make(map[int]string),
		models:    make(map[string]models.Model),
		snapshots: make(map[string]models.Model),
	}
	defer func() {
		for _, store := range state.stores {
			check.Must(store.Close())
		}
	}()
	output := response{Results: make([]stepResult, 0, len(input.Steps))}
	for position, item := range input.Steps {
		result, err := state.execute(item, position)
		if err != nil {
			return err
		}
		output.Results = append(output.Results, result)
	}
	return json.NewEncoder(os.Stdout).Encode(output)
}

func (state *probe) execute(item step, position int) (stepResult, error) {
	switch item.Action {
	case "apply":
		return state.apply(item, position)
	case "migrate", "rollback":
		return state.changeModel(item, position)
	case "read":
		return state.read(item, position)
	case "appliedIndex":
		store, err := state.store(item.Target)
		if err != nil {
			return stepResult{}, err
		}
		index, err := store.AppliedRaftIndex(state.ctx)
		if err != nil {
			return stepResult{}, fmt.Errorf("step %d: applied index: %w", position, err)
		}
		return stepResult{OK: true, AppliedIndex: &index}, nil
	case "export":
		return state.export(item, position)
	case "import":
		return state.importSnapshot(item, position)
	default:
		return stepResult{}, fmt.Errorf("step %d: unknown action %q", position, item.Action)
	}
}

func (state *probe) apply(item step, position int) (stepResult, error) {
	if item.Index == 0 || len(item.Command) == 0 || !json.Valid(item.Command) {
		return stepResult{}, fmt.Errorf("step %d: apply requires a valid index and command", position)
	}
	return state.dispatch(item, position, item.Command)
}

func (state *probe) changeModel(item step, position int) (stepResult, error) {
	if item.Index == 0 {
		return stepResult{}, fmt.Errorf("step %d: %s requires an index", position, item.Action)
	}
	current := state.currentModel(item.Target)
	command := models.RaftCommand{Kind: item.Action, Epoch: item.Epoch}
	var commit func()
	switch item.Action {
	case "migrate":
		next := migratedModel(current)
		command.Previous = current
		command.Next = next
		commit = func() {
			state.models[item.Target] = next
			state.snapshots[item.Target] = current
			state.migrations++
		}
	case "rollback":
		snapshot, ok := state.snapshots[item.Target]
		if !ok {
			return stepResult{}, fmt.Errorf("step %d: no migration snapshot to roll back", position)
		}
		command.Next = snapshot
		commit = func() {
			state.models[item.Target] = snapshot
			delete(state.snapshots, item.Target)
		}
	default:
		return stepResult{}, fmt.Errorf("step %d: unknown model change %q", position, item.Action)
	}
	data, err := json.Marshal(command)
	if err != nil {
		return stepResult{}, fmt.Errorf("step %d: encode %s: %w", position, item.Action, err)
	}
	result, err := state.dispatch(item, position, data)
	if err != nil {
		return stepResult{}, err
	}
	if result.OK && (result.Duplicate == nil || !*result.Duplicate) {
		commit()
	}
	return result, nil
}

func (state *probe) currentModel(target string) models.Model {
	if model, ok := state.models[target]; ok {
		return model
	}
	return productModel()
}

func migratedModel(current models.Model) models.Model {
	next := models.Model{SchemaVersion: current.SchemaVersion}
	next.Entities = append(next.Entities, current.Entities...)
	number := len(current.Entities) - len(productModel().Entities) + 1
	next.Entities = append(next.Entities, models.Entity{
		Name:       fmt.Sprintf("mig%d", number),
		OwnerGroup: "forms",
		Fields: []models.Field{
			{Name: "id", Type: "text", PrimaryKey: true, Required: true},
			{Name: "label", Type: "text"},
		},
	})
	return next
}

func (state *probe) dispatch(item step, position int, data json.RawMessage) (stepResult, error) {
	store, err := state.store(item.Target)
	if err != nil {
		return stepResult{}, err
	}
	machine, err := state.fsm(item.Target)
	if err != nil {
		return stepResult{}, err
	}
	response := machine.Apply(&raft.Log{Index: item.Index, Type: raft.LogCommand, Data: data})
	applied, err := store.AppliedRaftIndex(state.ctx)
	if err != nil {
		return stepResult{}, fmt.Errorf("step %d: applied index: %w", position, err)
	}
	var result stepResult
	switch value := response.(type) {
	case nil:
		return stepResult{}, fmt.Errorf("step %d: FSM returned no response", position)
	case error:
		if value == nil {
			return stepResult{}, fmt.Errorf("step %d: FSM returned no response", position)
		}
		result = stepResult{OK: false, Code: errorCode(value), AppliedIndex: &applied}
	case *models.ApplyResult:
		if value == nil {
			return stepResult{}, fmt.Errorf("step %d: FSM returned no result", position)
		}
		duplicate := value.Duplicate
		result = stepResult{OK: true, Duplicate: &duplicate, AppliedIndex: &applied}
		var command models.RaftCommand
		if err := json.Unmarshal(data, &command); err != nil {
			return stepResult{}, fmt.Errorf("step %d: decode command: %w", position, err)
		}
		if command.Kind == "batch" {
			count := 0
			if !duplicate {
				count = len(command.Ops)
			}
			result.Applied = &count
		}
	default:
		return stepResult{}, fmt.Errorf("step %d: unexpected FSM response %T", position, response)
	}
	if epoch, epochErr := store.Epoch(state.ctx); epochErr == nil {
		result.Epoch = &epoch
	}
	return result, nil
}

func (state *probe) read(item step, position int) (stepResult, error) {
	store, err := state.store(item.Target)
	if err != nil {
		return stepResult{}, err
	}
	tenant, site := item.Tenant, item.Site
	if tenant == "" {
		tenant = "t1"
	}
	if site == "" {
		site = "s1"
	}
	if item.Entity == "" || item.ID == "" {
		return stepResult{}, fmt.Errorf("step %d: read requires entity and id", position)
	}
	document, err := store.ReadRow(state.ctx, tenant, site, item.Entity, item.ID)
	if errors.Is(err, sql.ErrNoRows) {
		found := false
		return stepResult{OK: true, Found: &found}, nil
	}
	if err != nil {
		return stepResult{OK: false, Code: errorCode(err)}, nil
	}
	found := true
	return stepResult{OK: true, Found: &found, Row: document}, nil
}

func (state *probe) export(item step, position int) (stepResult, error) {
	store, err := state.store(item.Target)
	if err != nil {
		return stepResult{}, err
	}
	var buffer bytes.Buffer
	if err := store.ExportTo(state.ctx, &buffer, 64<<20); err != nil {
		return stepResult{}, fmt.Errorf("step %d: export: %w", position, err)
	}
	snapshot := buffer.String()
	state.exports[position] = snapshot
	return stepResult{OK: true, Snapshot: snapshot}, nil
}

func (state *probe) importSnapshot(item step, position int) (stepResult, error) {
	store, err := state.store(item.Target)
	if err != nil {
		return stepResult{}, err
	}
	var snapshot string
	switch {
	case item.SnapshotRef != nil:
		source, ok := state.exports[*item.SnapshotRef]
		if !ok {
			return stepResult{}, fmt.Errorf("step %d: snapshot reference %d has no export", position, *item.SnapshotRef)
		}
		snapshot = source
	case item.Snapshot != "":
		snapshot = item.Snapshot
	default:
		return stepResult{}, fmt.Errorf("step %d: import requires snapshot or snapshotRef", position)
	}
	var errImport error
	switch item.Mode {
	case "full":
		errImport = store.Import(state.ctx, []byte(snapshot))
	case "stream":
		errImport = store.ImportFrom(state.ctx, strings.NewReader(snapshot))
	default:
		return stepResult{}, fmt.Errorf("step %d: unknown import mode %q", position, item.Mode)
	}
	if errImport != nil {
		return stepResult{OK: false, Code: errorCode(errImport)}, nil
	}
	return stepResult{OK: true}, nil
}

func (state *probe) store(target string) (*modelsqlite.Store, error) {
	if store, ok := state.stores[target]; ok {
		return store, nil
	}
	switch target {
	case "primary", "secondary", "tertiary":
	default:
		return nil, fmt.Errorf("unknown target %q", target)
	}
	store, err := modelsqlite.Open(state.ctx, filepath.Join(state.root, target+".sqlite"))
	if err != nil {
		return nil, err
	}
	if target == "primary" {
		if err := store.InitModel(state.ctx, productModel()); err != nil {
			check.Must(store.Close())
			return nil, fmt.Errorf("initialize model: %w", err)
		}
	}
	state.stores[target] = store
	state.fsms[target] = &raftfsm.FSM{Store: store}
	return store, nil
}

func (state *probe) fsm(target string) (*raftfsm.FSM, error) {
	if _, err := state.store(target); err != nil {
		return nil, err
	}
	return state.fsms[target], nil
}

func productModel() models.Model {
	return models.Model{SchemaVersion: "1", Entities: []models.Entity{
		{Name: "accounts", OwnerGroup: "identity", Fields: []models.Field{
			{Name: "id", Type: "text", PrimaryKey: true, Required: true},
			{Name: "code", Type: "text", Required: true, Unique: true},
		}},
		{Name: "entries", OwnerGroup: "forms", Fields: []models.Field{
			{Name: "id", Type: "text", PrimaryKey: true, Required: true},
			{Name: "email", Type: "text", Required: true, Unique: true},
			{Name: "count", Type: "int64"},
			{Name: "accountCode", Type: "text", References: &models.Reference{Entity: "accounts", Field: "code"}},
		}},
	}}
}

func errorCode(err error) string {
	switch {
	case errors.Is(err, interfaces.ErrEpochMismatch):
		return "epoch_mismatch"
	case errors.Is(err, modelsqlite.ErrRowNotFound):
		return "not_found"
	case errors.Is(err, modelsqlite.ErrForbiddenGroup):
		return "forbidden"
	case errors.Is(err, modelsqlite.ErrRowExists),
		errors.Is(err, modelsqlite.ErrUniqueViolation),
		errors.Is(err, modelsqlite.ErrForeignKeyViolation):
		return "conflict"
	case errors.Is(err, modelsqlite.ErrInvalidRow),
		errors.Is(err, modelsqlite.ErrInvalidRaftIndex),
		errors.Is(err, raftfsm.ErrInvalidCommand),
		errors.Is(err, modelsqlite.ErrUnsafeMigration):
		return "invalid_request"
	default:
		return "internal"
	}
}

package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/Liapoldus/domain/tests/fixtures/check"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/Liapoldus/domain/internal/application/migration"
	"github.com/Liapoldus/domain/internal/application/row"
	"github.com/Liapoldus/domain/internal/domain/models"
	"github.com/Liapoldus/domain/internal/infrastructure/raftfsm"
	modelsqlite "github.com/Liapoldus/domain/internal/infrastructure/sqlite"
	"github.com/Liapoldus/domain/internal/presentation/product"
	"github.com/hashicorp/raft"
)

const (
	nodeCount      = 3
	probeTenant    = "tenant-a"
	probeSite      = "site-1"
	crashExitCode  = 42
	exportLimit    = int64(64 << 20)
	probeRowEntity = "records"
)

type node struct {
	raft      *raft.Raft
	transport *raft.InmemTransport
	store     *modelsqlite.Store
	address   raft.ServerAddress
	cluster   *raftfsm.Cluster
	service   *row.Service
	migration *migration.Service
	handler   *product.Handler
}

type step struct {
	Action   string          `json:"action"`
	Target   string          `json:"target"`
	Method   string          `json:"method"`
	Identity string          `json:"identity"`
	Payload  json.RawMessage `json:"payload"`
	DelayMs  int             `json:"delayMs"`
	Data     string          `json:"data"`
	Mode     string          `json:"mode"`
}

type stepResult struct {
	OK            bool            `json:"ok"`
	Envelope      json.RawMessage `json:"envelope,omitempty"`
	ProtocolError string          `json:"protocolError,omitempty"`
}

type request struct {
	Model json.RawMessage `json:"model"`
	Steps []step          `json:"steps"`
}

type response struct {
	Results []stepResult `json:"results"`
}

type stateRow struct {
	Entity   string          `json:"entity"`
	ID       string          `json:"id"`
	Found    bool            `json:"found"`
	Document json.RawMessage `json:"document,omitempty"`
}

type stateResult struct {
	Epoch               int64                   `json:"epoch"`
	AppliedIndex        uint64                  `json:"appliedIndex"`
	Model               json.RawMessage         `json:"model"`
	Fingerprint         string                  `json:"fingerprint"`
	Rows                []stateRow              `json:"rows"`
	SnapshotAvailable   bool                    `json:"snapshotAvailable"`
	SnapshotFingerprint string                  `json:"snapshotFingerprint,omitempty"`
	Audit               []models.MigrationAudit `json:"audit"`
	Epochs              []int64                 `json:"epochs"`
	AppliedIndexes      []uint64                `json:"appliedIndexes"`
}

type importResult struct {
	Model               json.RawMessage         `json:"model"`
	Epoch               int64                   `json:"epoch"`
	Fingerprint         string                  `json:"fingerprint"`
	SnapshotAvailable   bool                    `json:"snapshotAvailable"`
	SnapshotFingerprint string                  `json:"snapshotFingerprint,omitempty"`
	Audit               []models.MigrationAudit `json:"audit"`
	Rows                []stateRow              `json:"rows"`
}

type exportDocument struct {
	Model         json.RawMessage `json:"model"`
	Epoch         int64           `json:"epoch"`
	SnapshotModel json.RawMessage `json:"snapshotModel"`
}

type runner struct {
	nodes       []node
	leaderIndex int
	root        string
	importStore *modelsqlite.Store
}

func main() {
	if err := run(); err != nil {
		check.Value(fmt.Fprintln(os.Stderr, "cluster-migration probe failed:", err))
		os.Exit(2)
	}
}

func run() error {
	var input request
	if err := json.NewDecoder(os.Stdin).Decode(&input); err != nil {
		return fmt.Errorf("decode input: %w", err)
	}
	root := os.Getenv("PROBE_ROOT")
	if root == "" {
		created, err := os.MkdirTemp("", "domain-cluster-migration-")
		if err != nil {
			return err
		}
		root = created
		defer func() { check.Must(os.RemoveAll(root)) }()
	}
	needsCluster := false
	for _, item := range input.Steps {
		switch item.Action {
		case "call", "reload", "state", "crash", "crashDuringReload":
			needsCluster = true
		}
	}
	ctx := context.Background()
	runner := &runner{leaderIndex: -1, root: root}
	if needsCluster {
		if len(input.Model) == 0 {
			return errors.New("cluster steps require an input model")
		}
		var model models.Model
		if err := json.Unmarshal(input.Model, &model); err != nil {
			return fmt.Errorf("decode input model: %w", err)
		}
		built, err := boot(ctx, root, model)
		if err != nil {
			return fmt.Errorf("boot cluster: %w", err)
		}
		runner.nodes = built[:]
		defer func() { check.Must(closeCluster(runner.nodes)) }()
		leaderIndex, err := waitLeader(runner.nodes)
		if err != nil {
			return err
		}
		if err := waitLeadersKnown(runner.nodes); err != nil {
			return err
		}
		runner.leaderIndex = leaderIndex
	}
	defer func() {
		if runner.importStore != nil {
			check.Must(runner.importStore.Close())
		}
	}()
	output := response{Results: make([]stepResult, 0, len(input.Steps))}
	for position, item := range input.Steps {
		result, err := runner.execute(ctx, item)
		if err != nil {
			return fmt.Errorf("step %d: %w", position, err)
		}
		output.Results = append(output.Results, result)
	}
	return json.NewEncoder(os.Stdout).Encode(output)
}

func (runner *runner) execute(ctx context.Context, item step) (stepResult, error) {
	switch item.Action {
	case "call":
		return runner.call(ctx, item)
	case "reload":
		return runner.reload(ctx, item)
	case "state":
		return runner.state(ctx)
	case "export":
		return runner.exportSnapshot(ctx)
	case "import":
		return runner.importSnapshot(ctx, item)
	case "crash":
		os.Exit(crashExitCode)
		return stepResult{}, errors.New("unreachable")
	case "crashDuringReload":
		return runner.crashDuring(ctx, item)
	default:
		return stepResult{}, fmt.Errorf("unknown action %q", item.Action)
	}
}

func (runner *runner) call(ctx context.Context, item step) (stepResult, error) {
	handler, ok := runner.handlerFor(item.Target)
	if !ok {
		return stepResult{}, fmt.Errorf("unknown target %q", item.Target)
	}
	envelope, err := handler.Handle(ctx, item.Method, item.Identity, item.Payload)
	if err != nil {
		return stepResult{OK: true, ProtocolError: err.Error()}, nil //nolint:nilerr // Expected protocol failures are probe data; only harness failures abort the fixture.
	}
	return stepResult{OK: true, Envelope: envelope}, nil
}

// reload is test-fixture plumbing for the internal configuration lifecycle. It
// deliberately does not expose the migration writer as a product peer method.
func (runner *runner) reload(ctx context.Context, item step) (stepResult, error) {
	index, ok := runner.nodeIndex(item.Target)
	if !ok {
		return stepResult{}, fmt.Errorf("unknown target %q", item.Target)
	}
	scope, ok := probeScopes()[item.Identity]
	if !ok {
		return stepResult{}, errors.New("unknown fixture identity")
	}
	var data json.RawMessage
	var productErr *models.ProductError
	if item.Mode == "coreRollback" {
		var payload struct {
			WriteID string `json:"writeId"`
			Epoch   int64  `json:"epoch,omitempty"`
		}
		if err := json.Unmarshal(item.Payload, &payload); err != nil {
			return stepResult{}, errors.New("invalid fixture rollback payload")
		}
		data, productErr = runner.nodes[index].migration.Rollback(ctx, scope, payload.WriteID, payload.Epoch)
	} else {
		var payload struct {
			Next    models.Model `json:"next"`
			WriteID string       `json:"writeId"`
			Epoch   int64        `json:"epoch,omitempty"`
		}
		if err := json.Unmarshal(item.Payload, &payload); err != nil {
			return stepResult{}, errors.New("invalid fixture reload payload")
		}
		data, productErr = runner.nodes[index].migration.Apply(ctx, scope, payload.Next, payload.WriteID, payload.Epoch)
	}
	var envelope json.RawMessage
	if productErr != nil {
		type fixtureError struct {
			Code           string `json:"code"`
			Retryable      bool   `json:"retryable"`
			UnknownOutcome bool   `json:"unknownOutcome"`
			Message        string `json:"message"`
		}
		envelope = check.Value(json.Marshal(struct {
			OK    bool          `json:"ok"`
			Error *fixtureError `json:"error,omitempty"`
		}{
			OK: false,
			Error: &fixtureError{
				Code: productErr.Code, Retryable: productErr.Retryable,
				UnknownOutcome: productErr.UnknownOutcome, Message: productErr.Message,
			},
		}))
	} else {
		envelope = check.Value(json.Marshal(struct {
			OK   bool            `json:"ok"`
			Data json.RawMessage `json:"data"`
		}{OK: true, Data: data}))
		var result struct {
			EpochAfter int64 `json:"epochAfter"`
		}
		if err := json.Unmarshal(data, &result); err != nil {
			return stepResult{}, fmt.Errorf("decode reload result: %w", err)
		}
		if err := runner.refresh(ctx, result.EpochAfter); err != nil {
			return stepResult{}, fmt.Errorf("refresh handlers after fixture reload: %w", err)
		}
	}
	return stepResult{OK: true, Envelope: envelope}, nil
}

func (runner *runner) nodeIndex(target string) (int, bool) {
	switch target {
	case "leader":
		return runner.leaderIndex, runner.leaderIndex >= 0 && runner.leaderIndex < len(runner.nodes)
	case "follower":
		return (runner.leaderIndex + 1) % nodeCount, runner.leaderIndex >= 0
	default:
		return 0, false
	}
}

func (runner *runner) refresh(ctx context.Context, epoch int64) error {
	if err := waitEpochs(ctx, runner.nodes, epoch); err != nil {
		return err
	}
	document, err := readExport(ctx, runner.nodes[0].store)
	if err != nil {
		return err
	}
	var model models.Model
	if err := json.Unmarshal(document.Model, &model); err != nil {
		return fmt.Errorf("decode durable model: %w", err)
	}
	for index := range runner.nodes {
		runner.nodes[index].cluster = raftfsm.NewCluster(runner.nodes[index].raft, runner.nodes[index].store, model, document.Epoch, 0)
		runner.nodes[index].service = row.New(runner.nodes[index].cluster, runner.nodes[index].cluster, runner.nodes[index].cluster, runner.nodes[index].cluster, runner.nodes[index].cluster)
		runner.nodes[index].migration = migration.New(runner.nodes[index].cluster, runner.nodes[index].cluster, runner.nodes[index].store)
		runner.nodes[index].handler = product.NewHandler(runner.nodes[index].service, probeScopes()).WithMigration(runner.nodes[index].migration)
	}
	return nil
}

func (runner *runner) state(ctx context.Context) (stepResult, error) {
	if len(runner.nodes) == 0 || runner.nodes[0].raft == nil {
		return stepResult{}, errors.New("state requires an open cluster")
	}
	leader := leaderOf(runner.nodes)
	if leader < 0 {
		return stepResult{}, errors.New("state requires a raft leader")
	}
	if _, err := runner.nodes[leader].cluster.Barrier(ctx); err != nil {
		return stepResult{}, fmt.Errorf("leader barrier: %w", err)
	}
	if err := waitConverged(ctx, runner.nodes); err != nil {
		return stepResult{}, err
	}
	document, err := readExport(ctx, runner.nodes[0].store)
	if err != nil {
		return stepResult{}, err
	}
	appliedIndex, err := runner.nodes[0].store.AppliedRaftIndex(ctx)
	if err != nil {
		return stepResult{}, fmt.Errorf("read applied index: %w", err)
	}
	audit, err := runner.nodes[0].store.RecentMigrations(ctx, probeTenant, probeSite, 16)
	if err != nil {
		return stepResult{}, fmt.Errorf("read migration audit: %w", err)
	}
	if audit == nil {
		audit = []models.MigrationAudit{}
	}
	rows, err := readRows(ctx, runner.nodes[0].store)
	if err != nil {
		return stepResult{}, err
	}
	epochs := make([]int64, len(runner.nodes))
	appliedIndexes := make([]uint64, len(runner.nodes))
	for index := range runner.nodes {
		epoch, err := runner.nodes[index].store.Epoch(ctx)
		if err != nil {
			return stepResult{}, fmt.Errorf("read epoch: %w", err)
		}
		nodeIndex, err := runner.nodes[index].store.AppliedRaftIndex(ctx)
		if err != nil {
			return stepResult{}, fmt.Errorf("read applied index: %w", err)
		}
		epochs[index] = epoch
		appliedIndexes[index] = nodeIndex
	}
	result := stateResult{
		Epoch:             document.Epoch,
		AppliedIndex:      appliedIndex,
		Model:             document.Model,
		Fingerprint:       fingerprint(document.Model),
		Rows:              rows,
		SnapshotAvailable: len(document.SnapshotModel) > 0,
		Audit:             audit,
		Epochs:            epochs,
		AppliedIndexes:    appliedIndexes,
	}
	if len(document.SnapshotModel) > 0 {
		result.SnapshotFingerprint = fingerprint(document.SnapshotModel)
	}
	return dataResult(result)
}

func (runner *runner) exportSnapshot(ctx context.Context) (stepResult, error) {
	if len(runner.nodes) == 0 || runner.nodes[0].store == nil {
		return stepResult{}, errors.New("export requires an open cluster store")
	}
	var buffer bytes.Buffer
	if err := runner.nodes[0].store.ExportTo(ctx, &buffer, exportLimit); err != nil {
		return stepResult{}, fmt.Errorf("export snapshot: %w", err)
	}
	return dataResult(struct {
		Data string `json:"data"`
	}{Data: base64.StdEncoding.EncodeToString(buffer.Bytes())})
}

func (runner *runner) importSnapshot(ctx context.Context, item step) (stepResult, error) {
	if runner.importStore == nil {
		store, err := modelsqlite.Open(ctx, filepath.Join(runner.root, "import-store.sqlite"))
		if err != nil {
			return stepResult{}, fmt.Errorf("open import store: %w", err)
		}
		runner.importStore = store
	}
	data, err := base64.StdEncoding.DecodeString(item.Data)
	if err != nil {
		return stepResult{}, fmt.Errorf("decode import payload: %w", err)
	}
	switch item.Mode {
	case "full":
		err = runner.importStore.Import(ctx, data)
	case "stream":
		err = runner.importStore.ImportFrom(ctx, bytes.NewReader(data))
	default:
		return stepResult{}, fmt.Errorf("unknown import mode %q", item.Mode)
	}
	if err != nil {
		return stepResult{}, fmt.Errorf("import snapshot: %w", err)
	}
	document, err := readExport(ctx, runner.importStore)
	if err != nil {
		return stepResult{}, err
	}
	audit, err := runner.importStore.RecentMigrations(ctx, probeTenant, probeSite, 16)
	if err != nil {
		return stepResult{}, fmt.Errorf("read migration audit: %w", err)
	}
	if audit == nil {
		audit = []models.MigrationAudit{}
	}
	rows, err := readRows(ctx, runner.importStore)
	if err != nil {
		return stepResult{}, err
	}
	result := importResult{
		Model:             document.Model,
		Epoch:             document.Epoch,
		Fingerprint:       fingerprint(document.Model),
		SnapshotAvailable: len(document.SnapshotModel) > 0,
		Audit:             audit,
		Rows:              rows,
	}
	if len(document.SnapshotModel) > 0 {
		result.SnapshotFingerprint = fingerprint(document.SnapshotModel)
	}
	return dataResult(result)
}

func (runner *runner) crashDuring(ctx context.Context, item step) (stepResult, error) {
	if _, ok := runner.nodeIndex(item.Target); !ok {
		return stepResult{}, fmt.Errorf("unknown target %q", item.Target)
	}
	go func() {
		check.Value(runner.reload(ctx, item))
	}()
	delay := item.DelayMs
	if delay <= 0 {
		delay = 5
	}
	time.Sleep(time.Duration(delay) * time.Millisecond)
	os.Exit(crashExitCode)
	return stepResult{}, errors.New("unreachable")
}

func (runner *runner) handlerFor(target string) (*product.Handler, bool) {
	switch target {
	case "leader":
		return runner.nodes[runner.leaderIndex].handler, true
	case "follower":
		return runner.nodes[(runner.leaderIndex+1)%nodeCount].handler, true
	default:
		return nil, false
	}
}

func boot(ctx context.Context, root string, inputModel models.Model) ([nodeCount]node, error) {
	var nodes [nodeCount]node
	var snapshotStores [nodeCount]raft.SnapshotStore
	for index := range nodes {
		address, transport := raft.NewInmemTransport(raft.ServerAddress(fmt.Sprintf("node-%d", index)))
		path := filepath.Join(root, fmt.Sprintf("node-%d.sqlite", index))
		_, statErr := os.Stat(path)
		fresh := errors.Is(statErr, os.ErrNotExist)
		if statErr != nil && !fresh {
			return nodes, statErr
		}
		store, err := modelsqlite.Open(ctx, path)
		if err != nil {
			check.Must(closeCluster(nodes[:]))
			return nodes, err
		}
		if fresh {
			if err := store.InitModel(ctx, inputModel); err != nil {
				check.Must(store.Close())
				check.Must(closeCluster(nodes[:]))
				return nodes, fmt.Errorf("initialize model: %w", err)
			}
		}
		nodes[index] = node{transport: transport, store: store, address: address}
		snapshotStore, err := raft.NewFileSnapshotStore(filepath.Join(root, fmt.Sprintf("snapshots-%d", index)), 2, io.Discard)
		if err != nil {
			check.Must(closeCluster(nodes[:]))
			return nodes, err
		}
		snapshotStores[index] = snapshotStore
	}
	for left := range nodes {
		for right := range nodes {
			if left != right {
				nodes[left].transport.Connect(nodes[right].address, nodes[right].transport)
			}
		}
	}
	configuration := make([]raft.Server, 0, len(nodes))
	for index := range nodes {
		configuration = append(configuration, raft.Server{ID: raft.ServerID(nodes[index].address), Address: nodes[index].address, Suffrage: raft.Voter})
	}
	hasState, err := raft.HasExistingState(nodes[0].store, nodes[0].store, snapshotStores[0])
	if err != nil {
		check.Must(closeCluster(nodes[:]))
		return nodes, err
	}
	if !hasState {
		if err := raft.BootstrapCluster(raftConfig(nodes[0].address), nodes[0].store, nodes[0].store, snapshotStores[0], nodes[0].transport, raft.Configuration{Servers: configuration}); err != nil {
			check.Must(closeCluster(nodes[:]))
			return nodes, err
		}
	}
	document, err := readExport(ctx, nodes[0].store)
	if err != nil {
		check.Must(closeCluster(nodes[:]))
		return nodes, err
	}
	var model models.Model
	if err := json.Unmarshal(document.Model, &model); err != nil {
		check.Must(closeCluster(nodes[:]))
		return nodes, fmt.Errorf("decode durable model: %w", err)
	}
	for index := range nodes {
		config := raftConfig(nodes[index].address)
		instance, err := raft.NewRaft(config, &raftfsm.FSM{Store: nodes[index].store}, nodes[index].store, nodes[index].store, snapshotStores[index], nodes[index].transport)
		if err != nil {
			check.Must(closeCluster(nodes[:]))
			return nodes, err
		}
		nodes[index].raft = instance
		nodes[index].cluster = raftfsm.NewCluster(instance, nodes[index].store, model, document.Epoch, 0)
		nodes[index].service = row.New(nodes[index].cluster, nodes[index].cluster, nodes[index].cluster, nodes[index].cluster, nodes[index].cluster)
		nodes[index].migration = migration.New(nodes[index].cluster, nodes[index].cluster, nodes[index].store)
		nodes[index].handler = product.NewHandler(nodes[index].service, probeScopes()).WithMigration(nodes[index].migration)
	}
	return nodes, nil
}

func raftConfig(address raft.ServerAddress) *raft.Config {
	config := raft.DefaultConfig()
	config.LocalID = raft.ServerID(address)
	config.LogOutput = io.Discard
	config.HeartbeatTimeout = 250 * time.Millisecond
	config.ElectionTimeout = 250 * time.Millisecond
	config.LeaderLeaseTimeout = 250 * time.Millisecond
	config.CommitTimeout = 20 * time.Millisecond
	return config
}

func leaderOf(nodes []node) int {
	for index := range nodes {
		if nodes[index].raft != nil && nodes[index].raft.State() == raft.Leader {
			return index
		}
	}
	return -1
}

func waitLeader(nodes []node) (int, error) {
	deadline := time.Now().Add(8 * time.Second)
	for time.Now().Before(deadline) {
		if leader := leaderOf(nodes); leader >= 0 {
			return leader, nil
		}
		time.Sleep(20 * time.Millisecond)
	}
	return -1, errors.New("raft leader election timed out")
}

func waitLeadersKnown(nodes []node) error {
	deadline := time.Now().Add(8 * time.Second)
	for time.Now().Before(deadline) {
		known := true
		for index := range nodes {
			_, leader := nodes[index].raft.LeaderWithID()
			if leader == "" {
				known = false
				break
			}
		}
		if known {
			return nil
		}
		time.Sleep(20 * time.Millisecond)
	}
	return errors.New("leader not known on every node")
}

func waitEpochs(ctx context.Context, nodes []node, target int64) error {
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		caughtUp := true
		for index := range nodes {
			epoch, err := nodes[index].store.Epoch(ctx)
			if err != nil || epoch != target {
				caughtUp = false
				break
			}
		}
		if caughtUp {
			return nil
		}
		time.Sleep(20 * time.Millisecond)
	}
	return fmt.Errorf("stores did not reach epoch %d", target)
}

func waitConverged(ctx context.Context, nodes []node) error {
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if converged(ctx, nodes) {
			return nil
		}
		time.Sleep(20 * time.Millisecond)
	}
	return errors.New("cluster did not converge on one epoch and applied index")
}

func converged(ctx context.Context, nodes []node) bool {
	firstEpoch, err := nodes[0].store.Epoch(ctx)
	if err != nil {
		return false
	}
	firstApplied, err := nodes[0].store.AppliedRaftIndex(ctx)
	if err != nil {
		return false
	}
	for index := 1; index < len(nodes); index++ {
		epoch, err := nodes[index].store.Epoch(ctx)
		if err != nil || epoch != firstEpoch {
			return false
		}
		applied, err := nodes[index].store.AppliedRaftIndex(ctx)
		if err != nil || applied != firstApplied {
			return false
		}
	}
	return true
}

func readExport(ctx context.Context, store *modelsqlite.Store) (exportDocument, error) {
	var buffer bytes.Buffer
	if err := store.ExportTo(ctx, &buffer, exportLimit); err != nil {
		return exportDocument{}, fmt.Errorf("export durable state: %w", err)
	}
	var document exportDocument
	if err := json.Unmarshal(buffer.Bytes(), &document); err != nil {
		return exportDocument{}, fmt.Errorf("decode export: %w", err)
	}
	if len(document.Model) == 0 || document.Epoch <= 0 {
		return exportDocument{}, errors.New("export did not contain a durable model")
	}
	return document, nil
}

func readRows(ctx context.Context, store *modelsqlite.Store) ([]stateRow, error) {
	targets := []struct{ id string }{{id: "r1"}, {id: "r2"}}
	rows := make([]stateRow, 0, len(targets))
	for _, target := range targets {
		document, err := store.ReadRow(ctx, probeTenant, probeSite, probeRowEntity, target.id)
		if err != nil && !errors.Is(err, sql.ErrNoRows) {
			return nil, fmt.Errorf("read row %s: %w", target.id, err)
		}
		row := stateRow{Entity: probeRowEntity, ID: target.id, Found: err == nil}
		if err == nil {
			row.Document = document
		}
		rows = append(rows, row)
	}
	return rows, nil
}

func fingerprint(document []byte) string {
	sum := sha256.Sum256(document)
	return hex.EncodeToString(sum[:])
}

func dataResult(data any) (stepResult, error) {
	encoded, err := json.Marshal(data)
	if err != nil {
		return stepResult{}, fmt.Errorf("encode step data: %w", err)
	}
	envelope, err := json.Marshal(struct {
		OK   bool            `json:"ok"`
		Data json.RawMessage `json:"data"`
	}{OK: true, Data: encoded})
	if err != nil {
		return stepResult{}, fmt.Errorf("encode step envelope: %w", err)
	}
	return stepResult{OK: true, Envelope: envelope}, nil
}

func probeScopes() map[string]models.Scope {
	return map[string]models.Scope{
		"spiffe://liapoldus/domain/product/owner-forms": {Tenant: probeTenant, Site: probeSite, Group: "forms"},
		"spiffe://liapoldus/domain/product/other-scope": {Tenant: "tenant-b", Site: probeSite, Group: "forms"},
	}
}

func closeCluster(nodes []node) error {
	var first error
	for index := range nodes {
		if nodes[index].raft != nil {
			if err := nodes[index].raft.Shutdown().Error(); err != nil && first == nil {
				first = err
			}
		}
	}
	for index := range nodes {
		if nodes[index].transport != nil {
			check.Must(nodes[index].transport.Close())
		}
		if nodes[index].store != nil {
			if err := nodes[index].store.Close(); err != nil && first == nil {
				first = err
			}
		}
	}
	return first
}

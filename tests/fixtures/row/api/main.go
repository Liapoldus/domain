package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/Liapoldus/domain/tests/fixtures/check"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/Liapoldus/domain/internal/application/row"
	"github.com/Liapoldus/domain/internal/domain/interfaces"
	"github.com/Liapoldus/domain/internal/domain/models"
	"github.com/Liapoldus/domain/internal/infrastructure/raftfsm"
	modelsqlite "github.com/Liapoldus/domain/internal/infrastructure/sqlite"
	"github.com/Liapoldus/domain/internal/presentation/product"
	"github.com/hashicorp/raft"
)

const nodeCount = 3

// probePeerIdentity stands in for a real cluster peer URI SAN on every
// fixture handler; the fixture classifies identities only, it never mTLS.
const probePeerIdentity = "node-1"

type node struct {
	raft    *raft.Raft
	store   *modelsqlite.Store
	cluster *raftfsm.Cluster
	service *row.Service
	handler *product.Handler
	root    string
	address string
}

type step struct {
	Action   string          `json:"action"`
	Target   string          `json:"target"`
	Method   string          `json:"method"`
	Identity string          `json:"identity"`
	Payload  json.RawMessage `json:"payload"`
}

type stepResult struct {
	OK            bool            `json:"ok"`
	Envelope      json.RawMessage `json:"envelope,omitempty"`
	ProtocolError string          `json:"protocolError,omitempty"`
	Epoch         *int64          `json:"epoch,omitempty"`
}

type request struct {
	Steps []step `json:"steps"`
}

type response struct {
	Results []stepResult `json:"results"`
}

func main() {
	if err := run(); err != nil {
		check.Value(fmt.Fprintln(os.Stderr, "product-api probe failed:", err))
		os.Exit(1)
	}
}

func run() error {
	var input request
	if err := json.NewDecoder(os.Stdin).Decode(&input); err != nil {
		return fmt.Errorf("decode input: %w", err)
	}
	nodes, err := boot(context.Background())
	if err != nil {
		return fmt.Errorf("boot cluster: %w", err)
	}
	defer cleanup(nodes)
	leaderIndex, err := waitLeader(nodes)
	if err != nil {
		return err
	}
	if err := waitLeadersKnown(nodes); err != nil {
		return err
	}
	fakes := newFakeHandlers()
	attachForwarders(&nodes)
	output := response{Results: make([]stepResult, 0, len(input.Steps))}
	for position, item := range input.Steps {
		result, err := execute(context.Background(), &nodes, leaderIndex, fakes, item)
		if err != nil {
			return fmt.Errorf("step %d: %w", position, err)
		}
		output.Results = append(output.Results, result)
	}
	return json.NewEncoder(os.Stdout).Encode(output)
}

func execute(ctx context.Context, nodes *[nodeCount]node, leaderIndex int, fakes map[string]*product.Handler, item step) (stepResult, error) {
	switch item.Action {
	case "call":
		handler, ok := handlerFor(*nodes, leaderIndex, fakes, item.Target)
		if !ok {
			return stepResult{}, fmt.Errorf("unknown target %q", item.Target)
		}
		envelopeBytes, err := handler.Handle(ctx, item.Method, item.Identity, item.Payload)
		if err != nil {
			return stepResult{OK: true, ProtocolError: err.Error()}, nil //nolint:nilerr // Expected protocol failures are probe data; only harness failures abort the fixture.
		}
		return stepResult{OK: true, Envelope: envelopeBytes}, nil
	case "migrate":
		return migrateNodes(ctx, nodes, item)
	case "rollback":
		return rollbackNodes(ctx, nodes, item)
	default:
		return stepResult{}, fmt.Errorf("unknown action %q", item.Action)
	}
}

// migrate proposes a safe entity-adding migration on the current leader and
// waits until every node's store has applied it. refresh defaults to true and
// re-wraps every node with the migrated model and the live epoch; the
// production-faithful path passes refresh:false and leaves every node's
// construction-time revision in place while the store moves on.
func migrateNodes(ctx context.Context, nodes *[nodeCount]node, item step) (stepResult, error) {
	leader := leaderOf(*nodes)
	if leader < 0 {
		return stepResult{}, fmt.Errorf("migrate requires a raft leader")
	}
	current := nodes[leader].cluster.Model(ctx)
	next := migratedModel(current)
	if _, err := nodes[leader].cluster.Apply(ctx, models.RaftCommand{Kind: "migrate", Previous: current, Next: next}); err != nil {
		return stepResult{}, fmt.Errorf("propose migrate: %w", err)
	}
	epoch, err := nodes[leader].store.Epoch(ctx)
	if err != nil {
		return stepResult{}, fmt.Errorf("read leader epoch: %w", err)
	}
	if err := waitEpochs(ctx, nodes, epoch); err != nil {
		return stepResult{}, err
	}
	refresh, err := refreshRequested(item)
	if err != nil {
		return stepResult{}, err
	}
	if refresh {
		refreshNodes(nodes, next, epoch)
	}
	return stepResult{OK: true, Epoch: &epoch}, nil
}

// rollback proposes a rollback to the durable pre-migration snapshot on the
// current leader and waits until every node's store has applied it, which
// removes the entity the migration introduced. The refresh option behaves
// exactly as it does for migrate: only an explicit refresh:false keeps the
// construction-time revision in the adapter.
func rollbackNodes(ctx context.Context, nodes *[nodeCount]node, item step) (stepResult, error) {
	leader := leaderOf(*nodes)
	if leader < 0 {
		return stepResult{}, fmt.Errorf("rollback requires a raft leader")
	}
	snapshot, found, err := nodes[leader].store.SnapshotRevision(ctx)
	if err != nil {
		return stepResult{}, fmt.Errorf("read snapshot revision: %w", err)
	}
	if !found {
		return stepResult{}, fmt.Errorf("rollback requires a migration snapshot")
	}
	if _, err := nodes[leader].cluster.Apply(ctx, models.RaftCommand{Kind: "rollback", Next: snapshot.Model}); err != nil {
		return stepResult{}, fmt.Errorf("propose rollback: %w", err)
	}
	epoch, err := nodes[leader].store.Epoch(ctx)
	if err != nil {
		return stepResult{}, fmt.Errorf("read leader epoch: %w", err)
	}
	if err := waitEpochs(ctx, nodes, epoch); err != nil {
		return stepResult{}, err
	}
	refresh, err := refreshRequested(item)
	if err != nil {
		return stepResult{}, err
	}
	if refresh {
		refreshNodes(nodes, snapshot.Model, epoch)
	}
	return stepResult{OK: true, Epoch: &epoch}, nil
}

// refreshRequested reads the shared {refresh} step option; refresh defaults to
// true so existing callers keep re-wrapping.
func refreshRequested(item step) (bool, error) {
	if len(item.Payload) == 0 {
		return true, nil
	}
	var options struct {
		Refresh *bool `json:"refresh"`
	}
	if err := json.Unmarshal(item.Payload, &options); err != nil {
		return false, fmt.Errorf("decode refresh options: %w", err)
	}
	if options.Refresh == nil {
		return true, nil
	}
	return *options.Refresh, nil
}

// refreshNodes re-wraps every node with the given revision, the refresh a
// deployment performs when it observes an applied revision change.
func refreshNodes(nodes *[nodeCount]node, model models.Model, epoch int64) {
	for index := range nodes {
		nodes[index].cluster = raftfsm.NewCluster(nodes[index].raft, nodes[index].store, model, epoch, 0)
		nodes[index].service = row.New(nodes[index].cluster, nodes[index].cluster, nodes[index].cluster, nodes[index].cluster, nodes[index].cluster)
		nodes[index].handler = product.NewHandler(nodes[index].service, productScopes()).WithPeerIdentities(peerIdentities())
	}
	attachForwarders(nodes)
}

// peerIdentities are the peer identities allowed to present the reserved
// forwardedScope claim. The fixture reuses the raft addresses as plain
// strings: classification only, no transport security here.
func peerIdentities() []string {
	identities := make([]string, 0, nodeCount)
	for index := 0; index < nodeCount; index++ {
		identities = append(identities, fmt.Sprintf("node-%d", index))
	}
	return identities
}

// attachForwarders wires every live node handler to forward reads into the
// cluster array. It must run after boot because boot returns the array by
// value while the forwarder needs the address of the array run() executes
// against, so re-wrapping after migration keeps the forwarder current.
func attachForwarders(nodes *[nodeCount]node) {
	for index := range nodes {
		nodes[index].handler = nodes[index].handler.WithReadForwarder(&probeForwarder{nodes: nodes})
	}
}

// probeForwarder stands in for the raft peer transport: it resolves the
// leader address reported by the service to the live node and re-enters that
// node's handler exactly as ForwardRead would on the wire.
type probeForwarder struct {
	nodes *[nodeCount]node
}

var _ product.ReadForwarder = (*probeForwarder)(nil)

func (forwarder *probeForwarder) ForwardRead(ctx context.Context, leaderAddress, method string, payload []byte) ([]byte, error) {
	if forwarder == nil || forwarder.nodes == nil || leaderAddress == "" {
		return nil, errors.New("probe forwarder cannot resolve the leader")
	}
	for index := range forwarder.nodes {
		node := &forwarder.nodes[index]
		if node.address != leaderAddress || node.handler == nil {
			continue
		}
		return node.handler.Handle(ctx, method, probePeerIdentity, payload)
	}
	return nil, errors.New("probe forwarder: unknown leader address")
}

func waitEpochs(ctx context.Context, nodes *[nodeCount]node, target int64) error {
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

func handlerFor(nodes [nodeCount]node, leaderIndex int, fakes map[string]*product.Handler, target string) (*product.Handler, bool) {
	switch target {
	case "leader":
		return nodes[leaderIndex].handler, true
	case "follower":
		return nodes[(leaderIndex+1)%nodeCount].handler, true
	default:
		handler, ok := fakes[target]
		return handler, ok
	}
}

func boot(ctx context.Context) ([nodeCount]node, error) {
	var nodes [nodeCount]node
	root, err := os.MkdirTemp("", "product-api-")
	if err != nil {
		return nodes, err
	}
	model := productModel()
	var logStores [nodeCount]*raft.InmemStore
	var snapshots [nodeCount]*raft.InmemSnapshotStore
	transports := make([]*raft.InmemTransport, nodeCount)
	addresses := make([]raft.ServerAddress, nodeCount)
	for index := 0; index < nodeCount; index++ {
		address, transport := raft.NewInmemTransport(raft.ServerAddress(fmt.Sprintf("node-%d", index)))
		transports[index] = transport
		addresses[index] = address
		store, err := modelsqlite.Open(ctx, filepath.Join(root, fmt.Sprintf("node-%d.sqlite", index)))
		if err != nil {
			return nodes, err
		}
		if err := store.InitModel(ctx, model); err != nil {
			check.Must(store.Close())
			return nodes, err
		}
		nodes[index].store = store
		nodes[index].root = root
		logStores[index] = raft.NewInmemStore()
		snapshots[index] = raft.NewInmemSnapshotStore()
	}
	for left := 0; left < nodeCount; left++ {
		for right := 0; right < nodeCount; right++ {
			if left != right {
				transports[left].Connect(addresses[right], transports[right])
			}
		}
	}
	configurations := make([]raft.Server, 0, nodeCount)
	for index := 0; index < nodeCount; index++ {
		configurations = append(configurations, raft.Server{ID: raft.ServerID(addresses[index]), Address: addresses[index], Suffrage: raft.Voter})
	}
	config := raftConfig(nodeCount)
	if err := raft.BootstrapCluster(config, logStores[0], logStores[0], snapshots[0], transports[0], raft.Configuration{Servers: configurations}); err != nil {
		return nodes, err
	}
	for index := 0; index < nodeCount; index++ {
		localConfig := raftConfig(nodeCount)
		localConfig.LocalID = raft.ServerID(addresses[index])
		instance, err := raft.NewRaft(localConfig, &raftfsm.FSM{Store: nodes[index].store}, logStores[index], logStores[index], snapshots[index], transports[index])
		if err != nil {
			return nodes, err
		}
		nodes[index].raft = instance
		nodes[index].cluster = raftfsm.NewCluster(instance, nodes[index].store, model, 1, 0)
		nodes[index].service = row.New(nodes[index].cluster, nodes[index].cluster, nodes[index].cluster, nodes[index].cluster, nodes[index].cluster)
		nodes[index].handler = product.NewHandler(nodes[index].service, productScopes()).WithPeerIdentities(peerIdentities())
		nodes[index].address = string(addresses[index])
	}
	return nodes, nil
}

// cleanup shuts down every raft node, closes its store, and removes the
// temporary data directory once all steps have finished. It must run only
// after the last call: the SQLite files must stay in place while raft is live.
func cleanup(nodes [nodeCount]node) {
	for index := range nodes {
		if nodes[index].raft != nil {
			check.Must(nodes[index].raft.Shutdown().Error())
		}
		if nodes[index].store != nil {
			check.Must(nodes[index].store.Close())
		}
	}
	if nodes[0].root != "" {
		check.Must(os.RemoveAll(nodes[0].root))
	}
}

func raftConfig(nodeCount int) *raft.Config {
	config := raft.DefaultConfig()
	config.LocalID = raft.ServerID("bootstrap")
	config.LogOutput = io.Discard
	config.HeartbeatTimeout = 200 * time.Millisecond
	config.ElectionTimeout = 200 * time.Millisecond
	config.LeaderLeaseTimeout = 200 * time.Millisecond
	config.CommitTimeout = 20 * time.Millisecond
	return config
}

func leaderOf(nodes [nodeCount]node) int {
	for index := range nodes {
		if nodes[index].raft.State() == raft.Leader {
			return index
		}
	}
	return -1
}

func waitLeader(nodes [nodeCount]node) (int, error) {
	deadline := time.Now().Add(8 * time.Second)
	for time.Now().Before(deadline) {
		if leader := leaderOf(nodes); leader >= 0 {
			return leader, nil
		}
		time.Sleep(20 * time.Millisecond)
	}
	return -1, fmt.Errorf("leader election timeout")
}

func newFakeHandlers() map[string]*product.Handler {
	revision := stubRevision{model: productModel(), epoch: 1}
	outsourceStatus := stubStatus{status: interfaces.Status{Ready: true, Leader: "node-0", LeaderAddress: "node-0", Term: 1, CommitIndex: 1, AppliedIndex: 1, Voters: []string{"node-0"}, Quorum: 1, Epoch: 1}}
	scopes := productScopes()
	handlers := make(map[string]*product.Handler)
	fake := func(target string, proposer stubProposer, barrier stubBarrier) *product.Handler {
		service := row.New(proposer, barrier, stubReader{}, outsourceStatus, revision)
		handler := product.NewHandler(service, scopes).WithPeerIdentities(peerIdentities())
		handlers[target] = handler
		return handler
	}
	fake("fake-unavailable", stubProposer{leader: true, applyErr: interfaces.ErrUnavailable}, stubBarrier{index: 1})
	fake("fake-unknownOutcome", stubProposer{leader: true, applyErr: interfaces.ErrUnknownOutcome}, stubBarrier{index: 1})
	fake("fake-internal", stubProposer{leader: true, applyErr: interfaces.ErrInternal}, stubBarrier{index: 1})
	fake("fake-barrier", stubProposer{leader: true, applyOK: true}, stubBarrier{err: interfaces.ErrUnavailable})
	fake("fake-notLeader", stubProposer{leader: false, leaderAddr: "node-0"}, stubBarrier{index: 1})
	fake("fake-forwardFail", stubProposer{leader: false, leaderAddr: "node-forwardFail"}, stubBarrier{index: 1}).
		WithReadForwarder(failingForwarder{})
	fake("fake-forwardHang", stubProposer{leader: false, leaderAddr: "node-forwardHang"}, stubBarrier{index: 1}).
		WithReadForwarder(hangingForwarder{})
	fake("fake-forwardRefused", stubProposer{leader: false, leaderAddr: "node-forwardRefused"}, stubBarrier{index: 1}).
		WithReadForwarder(refusingForwarder{})
	return handlers
}

// failingForwarder reports a transport-level failure: the leader address was
// known but the forward could not be delivered.
type failingForwarder struct{}

var _ product.ReadForwarder = failingForwarder{}

func (failingForwarder) ForwardRead(context.Context, string, string, []byte) ([]byte, error) {
	return nil, errors.New("probe forward transport failure")
}

// hangingForwarder blocks until the caller's deadline expires, modelling a
// leader that accepted the connection but never answered.
type hangingForwarder struct{}

var _ product.ReadForwarder = hangingForwarder{}

func (hangingForwarder) ForwardRead(ctx context.Context, _, _ string, _ []byte) ([]byte, error) {
	<-ctx.Done()
	return nil, ctx.Err()
}

// refusingForwarder returns a well-formed not_leader envelope from the
// forward target, modelling the single-hop case where the leader itself has
// moved on and refuses the proxied read.
type refusingForwarder struct{}

var _ product.ReadForwarder = refusingForwarder{}

func (refusingForwarder) ForwardRead(context.Context, string, string, []byte) ([]byte, error) {
	return []byte(`{"ok":false,"error":{"code":"not_leader","retryable":true,"unknownOutcome":false,"message":"leader declined the proxied read"}}`), nil
}

func productModel() models.Model {
	return models.Model{SchemaVersion: "1", Entities: []models.Entity{
		{Name: "records", OwnerGroup: "forms", Fields: []models.Field{
			{Name: "id", Type: "text", PrimaryKey: true, Required: true},
			{Name: "title", Type: "text", Required: true},
			{Name: "slug", Type: "text", Required: true, Unique: true},
			{Name: "count", Type: "int64"},
		}},
		{Name: "audit", OwnerGroup: "forms", Fields: []models.Field{
			{Name: "id", Type: "text", PrimaryKey: true, Required: true},
			{Name: "note", Type: "text", Required: true},
		}},
		{Name: "invoices", OwnerGroup: "records", Fields: []models.Field{
			{Name: "id", Type: "text", PrimaryKey: true, Required: true},
			{Name: "amount", Type: "decimal"},
			{Name: "paid", Type: "bool"},
		}},
	}}
}

func productScopes() map[string]models.Scope {
	return map[string]models.Scope{
		"spiffe://liapoldus/domain/product/owner-forms":   {Tenant: "tenant-a", Site: "site-1", Group: "forms"},
		"spiffe://liapoldus/domain/product/owner-records": {Tenant: "tenant-a", Site: "site-1", Group: "records"},
	}
}

func waitLeadersKnown(nodes [nodeCount]node) error {
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
	return fmt.Errorf("leader not known on every node")
}

// stubs for deterministic error paths.

type stubProposer struct {
	leader     bool
	leaderAddr string
	applyErr   error
	applyOK    bool
}

var _ interfaces.Proposer = (*stubProposer)(nil)

func (stub stubProposer) Apply(context.Context, models.RaftCommand) (*models.ApplyResult, error) {
	if stub.applyErr != nil {
		return nil, stub.applyErr
	}
	if stub.applyOK {
		return &models.ApplyResult{Duplicate: false}, nil
	}
	return nil, interfaces.ErrInternal
}

func (stub stubProposer) IsLeader() bool { return stub.leader }

func (stub stubProposer) LeaderAddress() string { return stub.leaderAddr }

type stubBarrier struct {
	index uint64
	err   error
}

var _ interfaces.ReadBarrier = (*stubBarrier)(nil)

func (stub stubBarrier) Barrier(context.Context) (uint64, error) { return stub.index, stub.err }

type stubReader struct{}

var _ interfaces.RowReader = (*stubReader)(nil)

func (stub stubReader) ReadRow(context.Context, string, string, string, string) (json.RawMessage, bool, error) {
	return nil, false, nil
}

func (stub stubReader) ScanEntity(context.Context, string, string, string) ([]interfaces.Row, error) {
	return nil, nil
}

type stubStatus struct {
	status interfaces.Status
	err    error
}

var _ interfaces.ClusterStatus = (*stubStatus)(nil)

func (stub stubStatus) Status(_ context.Context) (interfaces.Status, error) {
	return stub.status, stub.err
}

type stubRevision struct {
	model models.Model
	epoch int64
}

var _ interfaces.Revision = (*stubRevision)(nil)

func (stub stubRevision) Model(_ context.Context) models.Model { return stub.model }

func (stub stubRevision) Epoch(_ context.Context) int64 { return stub.epoch }

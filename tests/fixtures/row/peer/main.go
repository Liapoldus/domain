package main

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"github.com/Liapoldus/domain/tests/fixtures/check"
	"io"
	"math/big"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/Liapoldus/domain/internal/application/row"
	"github.com/Liapoldus/domain/internal/domain/models"
	"github.com/Liapoldus/domain/internal/infrastructure/raftfsm"
	"github.com/Liapoldus/domain/internal/infrastructure/raftpeer"
	modelsqlite "github.com/Liapoldus/domain/internal/infrastructure/sqlite"
	"github.com/Liapoldus/domain/internal/presentation/product"
	"github.com/Liapoldus/pluginprotocol/v2/presentation/peer"
	"github.com/hashicorp/raft"
)

const (
	nodeCount = 3

	identityAlpha   = "spiffe://liapoldus/domain/product/alpha"
	identityBravo   = "spiffe://liapoldus/domain/product/bravo"
	identityCharlie = "spiffe://liapoldus/domain/product/charlie"
	identityDelta   = "spiffe://liapoldus/domain/product/delta"

	// dnsName matches the DNS SAN on every fixture-issued leaf certificate.
	dnsName = "domain.test"

	// callTimeout bounds every client call so a stuck handler can never stall
	// the fixture; it always outlives the slowest method timeout (30s batch).
	callTimeout = 45 * time.Second
)

type node struct {
	index     int
	address   raft.ServerAddress
	storage   *modelsqlite.Store
	transport *raftpeer.Transport
	raft      *raft.Raft
	cluster   *raftfsm.Cluster
	service   *row.Service
	handler   *product.Handler
	cancel    context.CancelFunc
}

type envelopeError struct {
	Code           string `json:"code"`
	Retryable      bool   `json:"retryable"`
	UnknownOutcome bool   `json:"unknownOutcome"`
}

type responseEnvelope struct {
	OK    bool            `json:"ok"`
	Data  json.RawMessage `json:"data,omitempty"`
	Error *envelopeError  `json:"error,omitempty"`
}

// callOutcome is the fixture's view of one product call. Only classification
// fields are kept: no row content, no SQL, no error messages.
type callOutcome struct {
	protocol  bool // no response envelope reached the caller
	ok        bool
	code      string
	found     bool
	duplicate bool
	applied   bool
	ready     bool
	voters    int
	quorum    int
	rowCount  int
}

func (outcome callOutcome) is(code string) bool {
	return !outcome.protocol && !outcome.ok && outcome.code == code
}

func (outcome callOutcome) success() bool {
	return !outcome.protocol && outcome.ok
}

type partitionState struct {
	mu       sync.RWMutex
	isolated int
}

type partitionedTransport struct {
	raft.Transport
	source        int
	targetIndexes map[raft.ServerAddress]int
	partition     *partitionState
}

// harness drives the cluster through real mTLS product dials instead of
// in-process handler calls.
type harness struct {
	root           string
	addresses      []raft.ServerAddress
	raftIdentities []string
	cluster        []*node
	handler        peer.Handler
	callerIndex    map[string]int
}

var errFixturePartition = errors.New("fixture Raft network partition")

func main() {
	result, err := run()
	if err != nil {
		check.Value(fmt.Fprintf(os.Stderr, "Domain product peer cluster failed: %v\n", err))
		os.Exit(2)
	}
	if err := json.NewEncoder(os.Stdout).Encode(result); err != nil {
		os.Exit(2)
	}
}

func run() (map[string]interface{}, error) {
	root, err := os.MkdirTemp("", "domain-product-peer-cluster-")
	if err != nil {
		return nil, err
	}
	defer func() { check.Must(os.RemoveAll(root)) }()

	raftIdentities := []string{
		"spiffe://liapoldus/domain/raft/node-a",
		"spiffe://liapoldus/domain/raft/node-b",
		"spiffe://liapoldus/domain/raft/node-c",
	}
	callerIdentities := []string{identityAlpha, identityBravo, identityCharlie, identityDelta}
	if err := writeCertificates(root, raftIdentities, callerIdentities); err != nil {
		return nil, err
	}

	addresses := make([]raft.ServerAddress, nodeCount)
	for index := range addresses {
		address, reserveErr := reserveAddress()
		if reserveErr != nil {
			return nil, reserveErr
		}
		addresses[index] = raft.ServerAddress(address)
	}

	partition := &partitionState{isolated: -1}
	cluster := make([]*node, nodeCount)
	for index := range cluster {
		cluster[index], err = startNode(root, index, addresses, raftIdentities, index == 0, partition, callerIdentities)
		if err != nil {
			closeNodes(cluster)
			return nil, err
		}
	}
	defer closeNodes(cluster)

	clientHandler, err := peer.NewRegistry().Build()
	if err != nil {
		return nil, err
	}
	callerIndex := make(map[string]int, len(callerIdentities))
	for index, identity := range callerIdentities {
		callerIndex[identity] = index
	}
	harness := &harness{
		root:           root,
		addresses:      addresses,
		raftIdentities: raftIdentities,
		cluster:        cluster,
		handler:        clientHandler,
		callerIndex:    callerIndex,
	}

	leader, err := waitLeader(harness.cluster)
	if err != nil {
		return nil, err
	}
	follower := (leader + 1) % nodeCount
	result := make(map[string]interface{})

	// Leader round trip over the real peer transport.
	created := harness.call(leader, identityAlpha, "domain.create", map[string]interface{}{
		"entity": "records", "id": "r1", "writeId": "w-r1",
		"row": map[string]interface{}{"id": "r1", "title": "alpha", "slug": "alpha-1", "count": 1},
	})
	result["leaderCreateApplied"] = created.success() && !created.duplicate

	fetched := harness.call(leader, identityAlpha, "domain.get", map[string]interface{}{"entity": "records", "id": "r1"})
	result["leaderReadFound"] = fetched.success() && fetched.found

	// Routing: a follower refuses both writes and reads with not_leader.
	followerWrite := harness.call(follower, identityAlpha, "domain.create", map[string]interface{}{
		"entity": "records", "id": "r2", "writeId": "w-r2",
		"row": map[string]interface{}{"id": "r2", "title": "follower", "slug": "follower-2", "count": 1},
	})
	result["followerWriteNotLeader"] = followerWrite.is("not_leader")

	afterFollowerWrite := harness.call(leader, identityAlpha, "domain.get", map[string]interface{}{"entity": "records", "id": "r2"})
	result["followerWriteLeftStateUnchanged"] = afterFollowerWrite.success() && !afterFollowerWrite.found

	followerRead := harness.call(follower, identityAlpha, "domain.get", map[string]interface{}{"entity": "records", "id": "r1"})
	result["followerReadProxiedFound"] = followerRead.success() && followerRead.found

	followerQuery := harness.call(follower, identityAlpha, "domain.query", map[string]interface{}{"sql": "select id, title from records order by id"})
	result["followerQueryProxiedRows"] = followerQuery.success() && followerQuery.rowCount == 1

	// A caller-supplied forwardedScope claim is reserved and rejected even
	// when the call arrives on a follower, where it would otherwise forward.
	spoofed := harness.call(follower, identityAlpha, "domain.get", map[string]interface{}{
		"entity": "records", "id": "r1",
		"forwardedScope": map[string]interface{}{"tenant": "tenant-a", "site": "siteA", "group": "ops"},
	})
	result["spoofedForwardedScopeRejected"] = spoofed.is("invalid_request")

	fresh := harness.call(leader, identityAlpha, "domain.get", map[string]interface{}{"entity": "records", "id": "r1"})
	result["leaderReadFreshAfterProxiedFollowerRead"] = fresh.success() && fresh.found

	// Owner-group enforcement on writes.
	forbidden := harness.call(leader, identityBravo, "domain.create", map[string]interface{}{
		"entity": "records", "id": "b9", "writeId": "w-bravo",
		"row": map[string]interface{}{"id": "b9", "title": "bravo", "slug": "bravo-9", "count": 1},
	})
	result["aclOwnerGroupForbiddenWrite"] = forbidden.is("forbidden")

	// Reads are tenant/site scoped but not owner-group scoped.
	bravoRead := harness.call(leader, identityBravo, "domain.get", map[string]interface{}{"entity": "records", "id": "r1"})
	result["aclOwnerGroupRestrictsReads"] = bravoRead.is("forbidden")

	bravoOwn := harness.call(leader, identityBravo, "domain.create", map[string]interface{}{
		"entity": "audit", "id": "a1", "writeId": "w-audit",
		"row": map[string]interface{}{"id": "a1", "note": "n"},
	})
	result["aclOwnEntityWriteAllowed"] = bravoOwn.success() && !bravoOwn.duplicate

	// Tenant isolation: a foreign scope can neither delete nor observe the row.
	charlieDelete := harness.call(leader, identityCharlie, "domain.delete", map[string]interface{}{
		"entity": "records", "id": "r1", "writeId": "w-iso",
	})
	result["tenantScopedDeleteReportsNotFound"] = charlieDelete.is("not_found")

	charlieRead := harness.call(leader, identityCharlie, "domain.get", map[string]interface{}{"entity": "records", "id": "r1"})
	result["tenantScopedReadHidesForeignRow"] = charlieRead.success() && !charlieRead.found

	// The same foreign scope proxied through a follower stays hidden: the
	// forwarded claim carries the caller's own scope, never a wider one.
	followerForeign := harness.call(follower, identityCharlie, "domain.get", map[string]interface{}{"entity": "records", "id": "r1"})
	result["followerReadForeignTenantHidden"] = followerForeign.success() && !followerForeign.found

	ownerRead := harness.call(leader, identityAlpha, "domain.get", map[string]interface{}{"entity": "records", "id": "r1"})
	result["tenantScopedDeleteLeftOwnerRow"] = ownerRead.success() && ownerRead.found

	// Row documents may not carry scope fields.
	scopedRow := harness.call(leader, identityAlpha, "domain.create", map[string]interface{}{
		"entity": "records", "id": "r3", "writeId": "w-scope",
		"row": map[string]interface{}{
			"id": "r3", "title": "scoped", "slug": "alpha-3", "count": 1,
			"tenant": "tenant-z", "site": "siteZ", "ownerGroup": "root",
		},
	})
	result["rowRejectsScopeFields"] = scopedRow.is("invalid_request")

	// writeId replay is deduplicated instead of conflicting.
	replay := map[string]interface{}{
		"entity": "records", "id": "r4", "writeId": "w-dup",
		"row": map[string]interface{}{"id": "r4", "title": "dup", "slug": "alpha-4", "count": 4},
	}
	firstWrite := harness.call(leader, identityAlpha, "domain.create", replay)
	replayedWrite := harness.call(leader, identityAlpha, "domain.create", replay)
	result["writeIdReplayReportedDuplicate"] = firstWrite.success() && !firstWrite.duplicate &&
		replayedWrite.success() && replayedWrite.duplicate

	replayRead := harness.call(leader, identityAlpha, "domain.get", map[string]interface{}{"entity": "records", "id": "r4"})
	result["writeIdReplayKeptSingleRow"] = replayRead.success() && replayRead.found

	// Batch commits, deduplicates and validates every operation up front.
	ops := []interface{}{
		map[string]interface{}{"op": "create", "entity": "records", "id": "b1", "row": map[string]interface{}{"id": "b1", "title": "one", "slug": "batch-1", "count": 1}},
		map[string]interface{}{"op": "create", "entity": "records", "id": "b2", "row": map[string]interface{}{"id": "b2", "title": "two", "slug": "batch-2", "count": 2}},
	}
	batchApplied := harness.call(leader, identityAlpha, "domain.batch", map[string]interface{}{"writeId": "batch-1", "ops": ops})
	result["batchApplied"] = batchApplied.success() && batchApplied.applied && !batchApplied.duplicate

	batchReplay := harness.call(leader, identityAlpha, "domain.batch", map[string]interface{}{
		"writeId": "batch-1",
		"ops":     []interface{}{ops[0]},
	})
	result["batchReplayDeduplicated"] = batchReplay.success() && batchReplay.duplicate && !batchReplay.applied

	batchUnknown := harness.call(leader, identityAlpha, "domain.batch", map[string]interface{}{
		"writeId": "batch-2",
		"ops":     []interface{}{map[string]interface{}{"op": "create", "entity": "ghost", "id": "g1", "row": map[string]interface{}{"id": "g1"}}},
	})
	result["batchUnknownEntityRejected"] = batchUnknown.is("invalid_request")

	batchForbidden := harness.call(leader, identityAlpha, "domain.batch", map[string]interface{}{
		"writeId": "batch-3",
		"ops":     []interface{}{map[string]interface{}{"op": "create", "entity": "audit", "id": "a9", "row": map[string]interface{}{"id": "a9", "note": "n"}}},
	})
	result["batchOwnerGroupForbidden"] = batchForbidden.is("forbidden")

	batchMixed := harness.call(leader, identityAlpha, "domain.batch", map[string]interface{}{
		"writeId": "batch-4",
		"ops": []interface{}{
			map[string]interface{}{"op": "create", "entity": "records", "id": "b3", "row": map[string]interface{}{"id": "b3", "title": "three", "slug": "batch-3", "count": 3}},
			map[string]interface{}{"op": "create", "entity": "ghost", "id": "g2", "row": map[string]interface{}{"id": "g2"}},
		},
	})
	mixedRead := harness.call(leader, identityAlpha, "domain.get", map[string]interface{}{"entity": "records", "id": "b3"})
	result["batchRejectsInvalidBeforeApply"] = batchMixed.is("invalid_request") && mixedRead.success() && !mixedRead.found

	// A product caller outside the configured set is refused at the transport.
	unauthorized := harness.call(leader, identityDelta, "domain.get", map[string]interface{}{"entity": "records", "id": "r1"})
	result["unauthorizedCallerRejectedAtTransport"] = unauthorized.protocol

	// Cluster status while the cluster has a quorum.
	status := harness.status(leader)
	result["statusReadyWithQuorum"] = status.success() && status.ready
	result["statusVotersCounted"] = status.success() && status.voters == nodeCount
	result["statusQuorumCounted"] = status.success() && status.quorum == nodeCount/2+1

	// Quorum loss: stop both followers, then race one fresh read and one write
	// against the leader lease so neither can outlive it.
	lostLeader, err := waitLeader(harness.cluster)
	if err != nil {
		return nil, err
	}
	others := make([]int, 0, nodeCount-1)
	for index := range harness.cluster {
		if index != lostLeader {
			others = append(others, index)
		}
	}
	for _, index := range others {
		if err := stopNode(harness.cluster[index]); err != nil {
			return nil, err
		}
	}

	var raced sync.WaitGroup
	var quorumRead, quorumWrite callOutcome
	raced.Add(2)
	go func() {
		defer raced.Done()
		quorumRead = harness.call(lostLeader, identityAlpha, "domain.get", map[string]interface{}{"entity": "records", "id": "r1"})
	}()
	go func() {
		defer raced.Done()
		quorumWrite = harness.call(lostLeader, identityAlpha, "domain.create", map[string]interface{}{
			"entity": "records", "id": "r5", "writeId": "w-nq",
			"row": map[string]interface{}{"id": "r5", "title": "quorum", "slug": "alpha-5", "count": 5},
		})
	}()
	raced.Wait()

	result["quorumLossFreshReadUnavailable"] = quorumRead.is("unavailable")
	result["quorumLossWriteCode"] = quorumWrite.code

	// The ex-leader drops out of service once its lease expires.
	unready := harness.waitForUnready(lostLeader, 20*time.Second)
	result["quorumLossStatusUnready"] = unready.success() && !unready.ready
	result["quorumLossStatusVotersCounted"] = unready.success() && unready.voters == nodeCount
	result["quorumLossStatusQuorumCounted"] = unready.success() && unready.quorum == nodeCount/2+1

	// Restore the two stopped replicas and let a leader settle again.
	for _, index := range others {
		restarted, startErr := startNode(root, index, addresses, raftIdentities, false, partition, callerIdentities)
		if startErr != nil {
			return nil, startErr
		}
		harness.cluster[index] = restarted
	}
	restoredLeader, err := waitLeader(harness.cluster)
	if err != nil {
		return nil, err
	}
	restored := harness.status(restoredLeader)
	result["statusReadyAfterQuorumRestored"] = restored.success() && restored.ready

	// The interrupted writeId is replayable without producing a second row.
	payload := map[string]interface{}{
		"entity": "records", "id": "r5", "writeId": "w-nq",
		"row": map[string]interface{}{"id": "r5", "title": "quorum", "slug": "alpha-5", "count": 5},
	}
	firstRetry := harness.call(restoredLeader, identityAlpha, "domain.create", payload)
	secondRetry := harness.call(restoredLeader, identityAlpha, "domain.create", payload)
	result["quorumLossRetryAccepted"] = firstRetry.success()
	result["quorumLossRetryDeduplicated"] = secondRetry.success() && secondRetry.duplicate

	restoredRead := harness.call(restoredLeader, identityAlpha, "domain.get", map[string]interface{}{"entity": "records", "id": "r5"})
	result["quorumLossRetryKeptSingleRow"] = restoredRead.success() && restoredRead.found

	conflicting := harness.call(restoredLeader, identityAlpha, "domain.create", map[string]interface{}{
		"entity": "records", "id": "r5", "writeId": "w-nq-other",
		"row": map[string]interface{}{"id": "r5", "title": "quorum", "slug": "alpha-5", "count": 5},
	})
	result["quorumLossRetryConflictsOnNewWriteId"] = conflicting.is("conflict")

	// Leader down: the survivor still knows the dead leader's address, so a
	// follower read forwards into a refused connection and fails fast as
	// unavailable instead of hanging to the method deadline.
	downLeader, err := waitLeader(harness.cluster)
	if err != nil {
		return nil, err
	}
	if err := stopNode(harness.cluster[downLeader]); err != nil {
		return nil, err
	}
	survivor := (downLeader + 1) % nodeCount
	startedAt := time.Now()
	leaderDownRead := harness.call(survivor, identityAlpha, "domain.get", map[string]interface{}{"entity": "records", "id": "r1"})
	elapsed := time.Since(startedAt)
	result["leaderDownReadCode"] = leaderDownRead.code
	result["leaderDownReadBounded"] = !leaderDownRead.protocol && elapsed < 5*time.Second

	return result, nil
}

// call performs one product call over a fresh mTLS client session.
func (h *harness) call(target int, caller, method string, payload interface{}) callOutcome {
	outcome := callOutcome{protocol: true}
	encoded, err := json.Marshal(payload)
	if err != nil {
		return outcome
	}
	callerIndex, ok := h.callerIndex[caller]
	if !ok {
		return outcome
	}
	certificate, roots, err := loadIdentity(h.root, fmt.Sprintf("caller-%d", callerIndex))
	if err != nil {
		return outcome
	}
	ctx, cancel := context.WithTimeout(context.Background(), callTimeout)
	defer cancel()
	session, err := peer.Dial(ctx, peer.ClientConfig{
		Network: peer.NetworkConfig{
			Carrier:    peer.CarrierTCP,
			Endpoint:   string(h.addresses[target]),
			ServerName: dnsName,
		},
		Security: peer.SecurityConfig{
			Identity:     callerIdentity(callerIndex),
			Certificate:  certificate,
			Roots:        roots,
			PeerIdentity: h.raftIdentities[target],
		},
		Handler: h.handler,
	})
	if err != nil {
		return outcome
	}
	defer func() { check.Must(session.Close()) }()
	result, err := session.Call(ctx, peer.Method(method), encoded)
	if err != nil {
		return outcome
	}
	var envelope responseEnvelope
	if err := json.Unmarshal(result.Payload, &envelope); err != nil {
		return outcome
	}
	outcome.protocol = false
	outcome.ok = envelope.OK
	if envelope.Error != nil {
		outcome.code = envelope.Error.Code
	}
	if len(envelope.Data) == 0 {
		return outcome
	}
	var data struct {
		Found     bool     `json:"found"`
		Duplicate bool     `json:"duplicate"`
		Applied   bool     `json:"applied"`
		Ready     bool     `json:"ready"`
		Voters    []string `json:"voters"`
		Quorum    int      `json:"quorum"`
		RowCount  int      `json:"rowCount"`
	}
	if err := json.Unmarshal(envelope.Data, &data); err != nil {
		return outcome
	}
	outcome.found = data.Found
	outcome.duplicate = data.Duplicate
	outcome.applied = data.Applied
	outcome.ready = data.Ready
	outcome.voters = len(data.Voters)
	outcome.quorum = data.Quorum
	outcome.rowCount = data.RowCount
	return outcome
}

func callerIdentity(index int) string {
	switch index {
	case 0:
		return identityAlpha
	case 1:
		return identityBravo
	case 2:
		return identityCharlie
	default:
		return identityDelta
	}
}

func (h *harness) status(target int) callOutcome {
	return h.call(target, identityAlpha, "domain.cluster.status", map[string]interface{}{})
}

func (h *harness) waitForUnready(target int, timeout time.Duration) callOutcome {
	deadline := time.Now().Add(timeout)
	outcome := callOutcome{protocol: true}
	for time.Now().Before(deadline) {
		outcome = h.status(target)
		if outcome.success() && !outcome.ready {
			return outcome
		}
		time.Sleep(150 * time.Millisecond)
	}
	return outcome
}

// waitLeader returns the index of a leader that has held leadership for several
// consecutive polls, so follow-up calls do not race an in-progress election.
func waitLeader(nodes []*node) (int, error) {
	deadline := time.Now().Add(20 * time.Second)
	previous := -1
	stable := 0
	for time.Now().Before(deadline) {
		current := -1
		for index, candidate := range nodes {
			if candidate != nil && candidate.raft != nil && candidate.raft.State() == raft.Leader {
				current = index
				break
			}
		}
		switch {
		case current < 0:
			previous, stable = -1, 0
		case current == previous:
			stable++
			if stable >= 4 {
				return current, nil
			}
		default:
			previous, stable = current, 1
		}
		time.Sleep(100 * time.Millisecond)
	}
	return -1, errors.New("raft leader election timed out")
}

func reserveAddress() (string, error) {
	listener, err := (&net.ListenConfig{}).Listen(context.Background(), "tcp", "127.0.0.1:0")
	if err != nil {
		return "", err
	}
	address := listener.Addr().String()
	if err := listener.Close(); err != nil {
		return "", err
	}
	return address, nil
}

func closeNodes(nodes []*node) {
	for _, candidate := range nodes {
		check.Must(stopNode(candidate))
	}
}

func stopNode(candidate *node) error {
	if candidate == nil || candidate.raft == nil {
		return nil
	}
	if err := candidate.raft.Shutdown().Error(); err != nil {
		return err
	}
	candidate.cancel()
	check.Must(candidate.transport.Close())
	check.Must(candidate.storage.Close())
	candidate.raft = nil
	candidate.transport = nil
	candidate.storage = nil
	return nil
}

func writePEM(path, blockType string, data []byte) error {
	return check.WriteFile(path, pem.EncodeToMemory(&pem.Block{Type: blockType, Bytes: data}), 0o600)
}

func writeCertificates(root string, raftIdentities, callerIdentities []string) error {
	caKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return err
	}
	start := time.Now().Add(-time.Minute)
	caTemplate := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "Domain product peer fixture CA"},
		NotBefore:             start,
		NotAfter:              start.Add(time.Hour),
		IsCA:                  true,
		BasicConstraintsValid: true,
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature,
	}
	caDER, err := x509.CreateCertificate(rand.Reader, caTemplate, caTemplate, &caKey.PublicKey, caKey)
	if err != nil {
		return err
	}
	caCert, err := x509.ParseCertificate(caDER)
	if err != nil {
		return err
	}
	if err := writePEM(filepath.Join(root, "ca.crt"), "CERTIFICATE", caDER); err != nil {
		return err
	}
	issuances := []struct {
		prefix     string
		identities []string
		serialBase int64
		label      string
	}{
		{prefix: "node", identities: raftIdentities, serialBase: 2, label: "Domain Raft node"},
		{prefix: "caller", identities: callerIdentities, serialBase: 100, label: "Domain product caller"},
	}
	for _, issuance := range issuances {
		for index, identity := range issuance.identities {
			leafKey, keyErr := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
			if keyErr != nil {
				return keyErr
			}
			parsed, parseErr := url.Parse(identity)
			if parseErr != nil {
				return parseErr
			}
			leafTemplate := &x509.Certificate{
				SerialNumber: big.NewInt(issuance.serialBase + int64(index)),
				Subject:      pkix.Name{CommonName: fmt.Sprintf("%s %d", issuance.label, index)},
				NotBefore:    start,
				NotAfter:     start.Add(time.Hour),
				DNSNames:     []string{dnsName},
				URIs:         []*url.URL{parsed},
				KeyUsage:     x509.KeyUsageDigitalSignature,
				ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
			}
			leafDER, certErr := x509.CreateCertificate(rand.Reader, leafTemplate, caCert, &leafKey.PublicKey, caKey)
			if certErr != nil {
				return certErr
			}
			base := filepath.Join(root, fmt.Sprintf("%s-%d", issuance.prefix, index))
			if err := writePEM(base+".crt", "CERTIFICATE", leafDER); err != nil {
				return err
			}
			encodedKey, marshalErr := x509.MarshalPKCS8PrivateKey(leafKey)
			if marshalErr != nil {
				return marshalErr
			}
			if err := writePEM(base+".key", "PRIVATE KEY", encodedKey); err != nil {
				return err
			}
		}
	}
	return nil
}

func loadIdentity(root, name string) (tls.Certificate, *x509.CertPool, error) {
	certBytes, err := check.ReadFile(filepath.Join(root, name+".crt"))
	if err != nil {
		return tls.Certificate{}, nil, err
	}
	keyBytes, err := check.ReadFile(filepath.Join(root, name+".key"))
	if err != nil {
		return tls.Certificate{}, nil, err
	}
	cert, err := tls.X509KeyPair(certBytes, keyBytes)
	if err != nil {
		return tls.Certificate{}, nil, err
	}
	caBytes, err := check.ReadFile(filepath.Join(root, "ca.crt"))
	if err != nil {
		return tls.Certificate{}, nil, err
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(caBytes) {
		return tls.Certificate{}, nil, errors.New("fixture trust root could not be loaded")
	}
	return cert, roots, nil
}

func (t *partitionedTransport) blocked(target raft.ServerAddress) bool {
	if t == nil || t.partition == nil {
		return false
	}
	t.partition.mu.RLock()
	defer t.partition.mu.RUnlock()
	if t.partition.isolated < 0 {
		return false
	}
	return t.source == t.partition.isolated || t.targetIndexes[target] == t.partition.isolated
}

func (t *partitionedTransport) AppendEntriesPipeline(id raft.ServerID, target raft.ServerAddress) (raft.AppendPipeline, error) {
	if t.blocked(target) {
		return nil, errFixturePartition
	}
	return t.Transport.AppendEntriesPipeline(id, target)
}
func (t *partitionedTransport) AppendEntries(id raft.ServerID, target raft.ServerAddress, req *raft.AppendEntriesRequest, resp *raft.AppendEntriesResponse) error {
	if t.blocked(target) {
		return errFixturePartition
	}
	return t.Transport.AppendEntries(id, target, req, resp)
}
func (t *partitionedTransport) RequestVote(id raft.ServerID, target raft.ServerAddress, req *raft.RequestVoteRequest, resp *raft.RequestVoteResponse) error {
	if t.blocked(target) {
		return errFixturePartition
	}
	return t.Transport.RequestVote(id, target, req, resp)
}
func (t *partitionedTransport) RequestPreVote(id raft.ServerID, target raft.ServerAddress, req *raft.RequestPreVoteRequest, resp *raft.RequestPreVoteResponse) error {
	if t.blocked(target) {
		return errFixturePartition
	}
	if pv, ok := t.Transport.(raft.WithPreVote); ok {
		return pv.RequestPreVote(id, target, req, resp)
	}
	return errFixturePartition
}
func (t *partitionedTransport) TimeoutNow(id raft.ServerID, target raft.ServerAddress, req *raft.TimeoutNowRequest, resp *raft.TimeoutNowResponse) error {
	if t.blocked(target) {
		return errFixturePartition
	}
	return t.Transport.TimeoutNow(id, target, req, resp)
}
func (t *partitionedTransport) InstallSnapshot(id raft.ServerID, target raft.ServerAddress, req *raft.InstallSnapshotRequest, resp *raft.InstallSnapshotResponse, snap io.Reader) error {
	if t.blocked(target) {
		return errFixturePartition
	}
	return t.Transport.InstallSnapshot(id, target, req, resp, snap)
}

func startNode(root string, index int, addresses []raft.ServerAddress, identities []string, bootstrap bool, partition *partitionState, callerIdentities []string) (*node, error) {
	endpoint := string(addresses[index])
	peerIdentities := make(map[raft.ServerAddress]string, nodeCount-1)
	peerIDs := make(map[raft.ServerAddress]raft.ServerID, nodeCount-1)
	for peerIndex := range addresses {
		if peerIndex == index {
			continue
		}
		peerIdentities[addresses[peerIndex]] = identities[peerIndex]
		peerIDs[addresses[peerIndex]] = raft.ServerID(fmt.Sprintf("domain-node-%d", peerIndex))
	}
	cert, roots, err := loadIdentity(root, fmt.Sprintf("node-%d", index))
	if err != nil {
		return nil, err
	}

	// Pre-create node shell so the registered closures always resolve a handler.
	shell := &node{index: index}

	transport, err := raftpeer.Listen(raftpeer.Config{
		Network:        peer.NetworkConfig{Carrier: peer.CarrierTCP, Endpoint: endpoint, ServerName: dnsName},
		Security:       peer.SecurityConfig{Identity: identities[index], Certificate: cert, Roots: roots},
		PeerIdentities: peerIdentities,
		PeerServerIDs:  peerIDs,
		ProductCallers: []raftpeer.ProductCaller{{Identity: identityAlpha, Tenant: "tenant-a", Site: "siteA", Group: "ops"}, {Identity: identityBravo, Tenant: "tenant-a", Site: "siteA", Group: "auditors"}, {Identity: identityCharlie, Tenant: "tenant-b", Site: "siteB", Group: "ops"}},
		ProductCalls: []raftpeer.RegisteredCall{
			{Method: "domain.create", Handler: serveProduct(shell, "domain.create")},
			{Method: "domain.update", Handler: serveProduct(shell, "domain.update")},
			{Method: "domain.delete", Handler: serveProduct(shell, "domain.delete")},
			{Method: "domain.batch", Handler: serveProduct(shell, "domain.batch")},
			{Method: "domain.get", Handler: serveProduct(shell, "domain.get")},
			{Method: "domain.query", Handler: serveProduct(shell, "domain.query")},
			{Method: "domain.cluster.status", Handler: serveProduct(shell, "domain.cluster.status")},
		},
		PeerProxiedReads:     []string{"domain.get", "domain.query"},
		Limits:               peer.Limits{MaxMessageBytes: 4 << 20, MaxStreamMessageBytes: 4 << 20},
		SnapshotTimeout:      30 * time.Second,
		MaximumSnapshotBytes: 2 << 20,
		InboundQueueCapacity: 1024,
	})
	if err != nil {
		return nil, err
	}
	serveCtx, cancel := context.WithCancel(context.Background())
	go func() { check.Served(transport.Serve(serveCtx)) }()

	storagePath := filepath.Join(root, fmt.Sprintf("node-%d.sqlite", index))
	newStorage := false
	if _, statErr := os.Stat(storagePath); errors.Is(statErr, os.ErrNotExist) {
		newStorage = true
	} else if statErr != nil {
		cancel()
		check.Must(transport.Close())
		return nil, statErr
	}
	storage, err := modelsqlite.Open(context.Background(), storagePath)
	if err != nil {
		cancel()
		check.Must(transport.Close())
		return nil, err
	}
	model := productModel()
	if newStorage {
		if err := storage.InitModel(context.Background(), model); err != nil {
			cancel()
			check.Must(transport.Close())
			check.Must(storage.Close())
			return nil, err
		}
	}

	snapshotStore, err := raft.NewFileSnapshotStore(filepath.Join(root, fmt.Sprintf("snapshots-%d", index)), 2, io.Discard)
	if err != nil {
		cancel()
		check.Must(transport.Close())
		check.Must(storage.Close())
		return nil, err
	}

	servers := make([]raft.Server, 0, nodeCount)
	for peerIndex, address := range addresses {
		servers = append(servers, raft.Server{ID: raft.ServerID(fmt.Sprintf("domain-node-%d", peerIndex)), Address: address, Suffrage: raft.Voter})
	}
	cfg := raftConfig(index)
	if bootstrap {
		if err := raft.BootstrapCluster(cfg, storage, storage, snapshotStore, transport, raft.Configuration{Servers: servers}); err != nil {
			cancel()
			check.Must(transport.Close())
			check.Must(storage.Close())
			return nil, err
		}
	}

	targetIndexes := make(map[raft.ServerAddress]int, len(addresses))
	for peerIndex, address := range addresses {
		targetIndexes[address] = peerIndex
	}
	pTransport := &partitionedTransport{Transport: transport, source: index, targetIndexes: targetIndexes, partition: partition}

	instance, err := raft.NewRaft(cfg, &raftfsm.FSM{Store: storage}, storage, storage, snapshotStore, pTransport)
	if err != nil {
		cancel()
		check.Must(transport.Close())
		check.Must(storage.Close())
		return nil, err
	}

	cluster := raftfsm.NewCluster(instance, storage, model, 1, 0)
	service := row.New(cluster, cluster, cluster, cluster, cluster)
	handler := product.NewHandler(service, productScopes(callerIdentities)).
		WithPeerIdentities(identities).
		WithReadForwarder(transport)
	shell.handler = handler
	shell.address = transport.LocalAddr()
	shell.storage = storage
	shell.transport = transport
	shell.raft = instance
	shell.cluster = cluster
	shell.service = service
	shell.cancel = cancel
	return shell, nil
}

// serveProduct adapts the node's product handler to the peer call signature.
func serveProduct(shell *node, method string) func(context.Context, peer.Call) (peer.Result, error) {
	return func(ctx context.Context, call peer.Call) (peer.Result, error) {
		response, err := shell.handler.Handle(ctx, method, call.From.URI, call.Payload)
		if err != nil {
			return peer.Result{}, err
		}
		return peer.Result{Payload: response}, nil
	}
}

func productModel() models.Model {
	return models.Model{SchemaVersion: "1", Entities: []models.Entity{
		{Name: "records", OwnerGroup: "ops", Fields: []models.Field{
			{Name: "id", Type: "text", PrimaryKey: true, Required: true},
			{Name: "title", Type: "text", Required: true},
			{Name: "slug", Type: "text", Required: true, Unique: true},
			{Name: "count", Type: "int64"},
		}},
		{Name: "audit", OwnerGroup: "auditors", Fields: []models.Field{
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

func productScopes(callerIdentities []string) map[string]models.Scope {
	return map[string]models.Scope{
		identityAlpha:   {Tenant: "tenant-a", Site: "siteA", Group: "ops"},
		identityBravo:   {Tenant: "tenant-a", Site: "siteA", Group: "auditors"},
		identityCharlie: {Tenant: "tenant-b", Site: "siteB", Group: "ops"},
	}
}

// raftConfig keeps the leader lease long enough that the quorum-loss scenario
// can issue its read and its write before the leader gives up leadership, while
// still stepping down well inside the 15s write timeout.
func raftConfig(index int) *raft.Config {
	config := raft.DefaultConfig()
	config.LocalID = raft.ServerID(fmt.Sprintf("domain-node-%d", index))
	config.LogOutput = io.Discard
	config.HeartbeatTimeout = 2 * time.Second
	config.ElectionTimeout = 2 * time.Second
	config.LeaderLeaseTimeout = 2 * time.Second
	config.CommitTimeout = 20 * time.Millisecond
	return config
}

package main

import (
	"bufio"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"database/sql"
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
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/Liapoldus/domain/internal/domain/models"
	"github.com/Liapoldus/domain/internal/infrastructure/raftfsm"
	"github.com/Liapoldus/domain/internal/infrastructure/raftpeer"
	modelsqlite "github.com/Liapoldus/domain/internal/infrastructure/sqlite"
	"github.com/Liapoldus/pluginprotocol/v2/presentation/peer"
	"github.com/hashicorp/raft"
)

const nodeCount = 3

type node struct {
	index     int
	address   raft.ServerAddress
	storage   *modelsqlite.Store
	transport *raftpeer.Transport
	raft      *raft.Raft
	cancel    context.CancelFunc
}

// partitionState injects a test-only transport partition without changing the
// production peer adapter. It drops Raft RPCs both to and from one member while
// leaving its process, listener, SQLite store and authenticated sessions alive.
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

var errFixturePartition = errors.New("fixture Raft network partition")

func main() {
	if len(os.Args) > 1 && os.Args[1] == "network-node" {
		if err := runOSClusterNode(os.Args[2:]); err != nil {
			check.Value(fmt.Fprintf(os.Stderr, "Domain Raft network node failed: %v\n", err))
			os.Exit(2)
		}
		return
	}
	if len(os.Args) > 1 && os.Args[1] == "network-partition" {
		result, err := runOSNetworkPartitionCluster()
		if err != nil {
			check.Value(fmt.Fprintf(os.Stderr, "Domain Raft OS partition cluster failed: %v\n", err))
			os.Exit(2)
		}
		if err := json.NewEncoder(os.Stdout).Encode(result); err != nil {
			os.Exit(2)
		}
		return
	}
	result, err := run()
	if err != nil {
		check.Value(fmt.Fprintf(os.Stderr, "Domain Raft peer cluster failed: %v\n", err))
		os.Exit(2)
	}
	if err := json.NewEncoder(os.Stdout).Encode(result); err != nil {
		os.Exit(2)
	}
}

type osClusterCommand struct {
	Action        string `json:"action"`
	RowID         string `json:"rowId,omitempty"`
	RequiredIndex uint64 `json:"requiredIndex,omitempty"`
}

type osClusterResponse struct {
	Ready            bool   `json:"ready,omitempty"`
	Action           string `json:"action,omitempty"`
	OK               bool   `json:"ok"`
	State            string `json:"state,omitempty"`
	LeaderID         string `json:"leaderId,omitempty"`
	AppliedIndex     uint64 `json:"appliedIndex,omitempty"`
	RowPresent       bool   `json:"rowPresent,omitempty"`
	WriteAccepted    bool   `json:"writeAccepted,omitempty"`
	EgressBlocked    bool   `json:"egressBlocked,omitempty"`
	BarrierIndex     uint64 `json:"barrierIndex,omitempty"`
	FreshReadAllowed bool   `json:"freshReadAllowed,omitempty"`
	Incarnation      string `json:"incarnation,omitempty"`
	PID              int    `json:"pid,omitempty"`
}

type osClusterChild struct {
	index       int
	uid         uint32
	process     *exec.Cmd
	input       *bufio.Writer
	output      *bufio.Reader
	incarnation string
	pid         int
}

// runOSNetworkPartitionCluster exercises a real three-process Raft cluster.
// Each process has a distinct uid, so Linux's owner match can isolate all
// egress traffic from one replica while INPUT drops traffic to its listener.
// The caller must already have created a private loopback-only network namespace.
func runOSNetworkPartitionCluster() (map[string]bool, error) {
	if err := requirePrivateLinuxNetworkNamespace(); err != nil {
		return nil, err
	}
	if output, err := exec.CommandContext(context.Background(), "ip", "link", "set", "lo", "up").CombinedOutput(); err != nil {
		return nil, fmt.Errorf("private loopback setup failed: %s (%w)", strings.TrimSpace(string(output)), err)
	}
	root, err := os.MkdirTemp("", "domain-raft-os-cluster-")
	if err != nil {
		return nil, err
	}
	defer func() { check.Must(os.RemoveAll(root)) }()
	if err := os.Chmod(root, 0o755); err != nil { // #nosec G302 -- Private fixture directory must be traversable by the three isolated Linux UIDs.
		return nil, err
	}
	identities := []string{
		"spiffe://liapoldus/domain/raft/node-a",
		"spiffe://liapoldus/domain/raft/node-b",
		"spiffe://liapoldus/domain/raft/node-c",
	}
	if err := writeCertificates(root, identities); err != nil {
		return nil, err
	}
	if err := prepareOSClusterFiles(root); err != nil {
		return nil, err
	}
	addresses := make([]raft.ServerAddress, nodeCount)
	for index := range addresses {
		address, err := reserveAddress()
		if err != nil {
			return nil, err
		}
		addresses[index] = raft.ServerAddress(address)
	}
	children := make([]*osClusterChild, nodeCount)
	defer stopOSClusterChildren(children)
	for index := range children {
		child, err := startOSClusterChild(root, index, addresses, index == 0)
		if err != nil {
			return nil, err
		}
		children[index] = child
		ready, err := child.readResponse(15 * time.Second)
		if err != nil || !ready.Ready || ready.Incarnation == "" || ready.PID != child.process.Process.Pid {
			return nil, fmt.Errorf("raft child %d did not become ready", index)
		}
		child.incarnation = ready.Incarnation
		child.pid = ready.PID
	}
	leader, err := waitOSClusterLeader(children, 20*time.Second)
	if err != nil {
		return nil, err
	}
	if response, err := children[leader].request(osClusterCommand{Action: "put", RowID: "before-os-partition"}, 8*time.Second); err != nil || !response.OK || !response.WriteAccepted {
		return nil, errors.New("initial majority write did not succeed")
	}
	if err := waitOSClusterRow(children, "before-os-partition", -1, 12*time.Second); err != nil {
		return nil, err
	}
	partitioned := (leader + 1) % nodeCount
	partitionPort := addressPort(addresses[partitioned])
	if err := installOSNodePartition(children[partitioned].uid, partitionPort); err != nil {
		return nil, err
	}
	partitionInstalled := true
	defer func() {
		if partitionInstalled {
			check.Must(removeOSNodePartition(children[partitioned].uid, partitionPort))
		}
	}()
	if response, err := children[leader].request(osClusterCommand{Action: "put", RowID: "during-os-partition"}, 8*time.Second); err != nil || !response.OK || !response.WriteAccepted {
		return nil, errors.New("connected majority could not commit while one Raft process was network-partitioned")
	}
	if err := waitOSClusterRow(children, "during-os-partition", partitioned, 12*time.Second); err != nil {
		return nil, fmt.Errorf("majority did not apply the partition-time write: %w", err)
	}
	leaderStatus, err := children[leader].request(osClusterCommand{Action: "status"}, time.Second)
	if err != nil || leaderStatus.AppliedIndex == 0 {
		return nil, errors.New("leader did not expose a committed FSM index")
	}
	partitionedStatus, err := children[partitioned].request(osClusterCommand{Action: "status", RowID: "during-os-partition"}, time.Second)
	if err != nil || partitionedStatus.RowPresent || partitionedStatus.AppliedIndex >= leaderStatus.AppliedIndex {
		return nil, errors.New("partitioned node was not observably stale after the majority write")
	}
	leaderBarrier, err := children[leader].request(osClusterCommand{Action: "barrier"}, 3*time.Second)
	if err != nil || !leaderBarrier.OK || leaderBarrier.BarrierIndex == 0 {
		return nil, errors.New("connected majority failed to commit a fresh-read barrier")
	}
	fencedRead, err := children[partitioned].request(osClusterCommand{Action: "fence-read", RowID: "during-os-partition", RequiredIndex: leaderBarrier.BarrierIndex}, time.Second)
	if err != nil || !fencedRead.OK || fencedRead.FreshReadAllowed || fencedRead.RowPresent {
		return nil, errors.New("fresh-read gate allowed a stale read on the partitioned node")
	}
	fencedWrite, err := children[partitioned].request(osClusterCommand{Action: "put", RowID: "isolated-write"}, 3*time.Second)
	if err != nil || !fencedWrite.OK || fencedWrite.WriteAccepted || fencedWrite.RowPresent {
		return nil, errors.New("partitioned Raft node accepted a write")
	}
	egressProbe, err := children[partitioned].request(osClusterCommand{Action: "probe-egress"}, 12*time.Second)
	if err != nil || !egressProbe.OK || !egressProbe.EgressBlocked {
		return nil, errors.New("isolated Raft process could still send peer RPC traffic")
	}
	if err := processIsAlive(children[partitioned]); err != nil {
		return nil, err
	}
	if err := osNodeDropCounters(children[partitioned].uid, partitionPort); err != nil {
		return nil, err
	}
	if err := removeOSNodePartition(children[partitioned].uid, partitionPort); err != nil {
		return nil, err
	}
	partitionInstalled = false
	if err := waitOSClusterRow(children, "during-os-partition", -1, 20*time.Second); err != nil {
		return nil, fmt.Errorf("same Raft process did not catch up after network heal: %w", err)
	}
	recovered, err := children[partitioned].request(osClusterCommand{Action: "status", RowID: "during-os-partition"}, time.Second)
	if err != nil || !recovered.RowPresent || recovered.Incarnation != children[partitioned].incarnation || recovered.PID != children[partitioned].pid {
		return nil, errors.New("partitioned Raft process did not recover in the same incarnation")
	}
	postHealLeader, err := waitOSClusterLeader(children, 12*time.Second)
	if err != nil {
		return nil, fmt.Errorf("cluster did not elect a leader after network recovery: %w", err)
	}
	if response, err := children[postHealLeader].request(osClusterCommand{Action: "put", RowID: "after-os-partition"}, 8*time.Second); err != nil || !response.OK || !response.WriteAccepted {
		return nil, errors.New("cluster did not commit after network recovery")
	}
	if err := waitOSClusterRow(children, "after-os-partition", -1, 12*time.Second); err != nil {
		return nil, fmt.Errorf("post-heal majority write did not reach all nodes: %w", err)
	}
	return map[string]bool{
		"threeNodeElection":                   true,
		"majorityCommitDuringKernelPartition": true,
		"isolatedProcessStayedAlive":          true,
		"isolatedWriteRejected":               true,
		"isolatedReadFenceRejected":           true,
		"isolatedRowStayedStale":              true,
		"kernelDroppedPeerPackets":            true,
		"sameIncarnationCaughtUpAfterHeal":    true,
		"postHealMajorityCommit":              true,
	}, nil
}

func runOSClusterNode(args []string) error {
	if len(args) != nodeCount+3 {
		return errors.New("invalid network fixture node arguments")
	}
	root := args[0]
	index, err := strconv.Atoi(args[1])
	if err != nil || index < 0 || index >= nodeCount {
		return errors.New("invalid network fixture node index")
	}
	bootstrap := args[2] == "true"
	addresses := make([]raft.ServerAddress, nodeCount)
	for peerIndex := range addresses {
		addresses[peerIndex] = raft.ServerAddress(args[3+peerIndex])
	}
	identities := []string{"spiffe://liapoldus/domain/raft/node-a", "spiffe://liapoldus/domain/raft/node-b", "spiffe://liapoldus/domain/raft/node-c"}
	certificate, roots, err := loadIdentity(root, index)
	if err != nil {
		return err
	}
	peerIdentities := make(map[raft.ServerAddress]string, nodeCount-1)
	peerIDs := make(map[raft.ServerAddress]raft.ServerID, nodeCount-1)
	for peerIndex, address := range addresses {
		if peerIndex == index {
			continue
		}
		peerIdentities[address] = identities[peerIndex]
		peerIDs[address] = raft.ServerID(fmt.Sprintf("domain-node-%d", peerIndex))
	}
	transport, err := raftpeer.Listen(raftpeer.Config{
		Network:        peer.NetworkConfig{Carrier: peer.CarrierTCP, Endpoint: string(addresses[index]), ServerName: "domain.test"},
		Security:       peer.SecurityConfig{Identity: identities[index], Certificate: certificate, Roots: roots},
		PeerIdentities: peerIdentities, PeerServerIDs: peerIDs,
	})
	if err != nil {
		return err
	}
	serveContext, cancelServe := context.WithCancel(context.Background())
	defer cancelServe()
	go func() { check.Must(transport.Serve(serveContext)) }()
	dataDirectory := filepath.Join(root, fmt.Sprintf("node-%d-data", index))
	storage, err := modelsqlite.Open(context.Background(), filepath.Join(dataDirectory, "state.sqlite"))
	if err != nil {
		check.Must(transport.Close())
		return err
	}
	model := models.Model{SchemaVersion: "1", Entities: []models.Entity{{Name: "records", OwnerGroup: "forms", Fields: []models.Field{{Name: "id", Type: "text", PrimaryKey: true, Required: true}}}}}
	if err := storage.InitModel(context.Background(), model); err != nil {
		check.Must(storage.Close())
		check.Must(transport.Close())
		return err
	}
	snapshotStore, err := raft.NewFileSnapshotStore(filepath.Join(dataDirectory, "snapshots"), 2, io.Discard)
	if err != nil {
		check.Must(storage.Close())
		check.Must(transport.Close())
		return err
	}
	servers := make([]raft.Server, nodeCount)
	for peerIndex, address := range addresses {
		servers[peerIndex] = raft.Server{ID: raft.ServerID(fmt.Sprintf("domain-node-%d", peerIndex)), Address: address, Suffrage: raft.Voter}
	}
	configuration := raft.Configuration{Servers: servers}
	if bootstrap {
		if err := raft.BootstrapCluster(raftConfig(index), storage, storage, snapshotStore, transport, configuration); err != nil {
			check.Must(storage.Close())
			check.Must(transport.Close())
			return err
		}
	}
	instance, err := raft.NewRaft(raftConfig(index), &raftfsm.FSM{Store: storage}, storage, storage, snapshotStore, transport)
	if err != nil {
		check.Must(storage.Close())
		check.Must(transport.Close())
		return err
	}
	var incarnationBytes [16]byte
	if _, err := rand.Read(incarnationBytes[:]); err != nil {
		check.Must(instance.Shutdown().Error())
		check.Must(storage.Close())
		check.Must(transport.Close())
		return err
	}
	writer := bufio.NewWriter(os.Stdout)
	incarnation := fmt.Sprintf("%x", incarnationBytes)
	if err := encodeOSClusterResponse(writer, osClusterResponse{Ready: true, Incarnation: incarnation, PID: os.Getpid()}); err != nil {
		return err
	}
	scanner := bufio.NewScanner(os.Stdin)
	for scanner.Scan() {
		var command osClusterCommand
		if err := json.Unmarshal(scanner.Bytes(), &command); err != nil {
			return errors.New("invalid network fixture control message")
		}
		response, stop := handleOSClusterCommand(index, addresses, transport, instance, storage, command)
		response.Action = command.Action
		response.Incarnation = incarnation
		if err := encodeOSClusterResponse(writer, response); err != nil {
			return err
		}
		if stop {
			break
		}
	}
	shutdownError := instance.Shutdown().Error()
	cancelServe()
	transportError := transport.Close()
	storageError := storage.Close()
	if err := scanner.Err(); err != nil {
		return err
	}
	if shutdownError != nil {
		return shutdownError
	}
	if transportError != nil {
		return transportError
	}
	return storageError
}

func handleOSClusterCommand(index int, addresses []raft.ServerAddress, transport *raftpeer.Transport, instance *raft.Raft, storage *modelsqlite.Store, command osClusterCommand) (osClusterResponse, bool) {
	response := osClusterResponse{OK: true, PID: os.Getpid()}
	response.State = instance.State().String()
	_, initialLeaderID := instance.LeaderWithID()
	response.LeaderID = string(initialLeaderID)
	response.AppliedIndex = check.Value(storage.AppliedRaftIndex(context.Background()))
	switch command.Action {
	case "status":
		if command.RowID != "" {
			_, err := storage.ReadRow(context.Background(), "tenant-a", "site-1", "records", command.RowID)
			response.RowPresent = err == nil
		}
	case "put":
		commandBytes, err := json.Marshal(models.RaftCommand{Kind: "put", Tenant: "tenant-a", Site: "site-1", Entity: "records", ID: command.RowID, Row: json.RawMessage(fmt.Sprintf(`{"id":%q}`, command.RowID))})
		if err != nil {
			response.OK = false
			break
		}
		future := instance.Apply(commandBytes, 3500*time.Millisecond)
		response.WriteAccepted = future.Error() == nil
		if response.WriteAccepted {
			if commandError, ok := future.Response().(error); ok && commandError != nil {
				response.WriteAccepted = false
			}
		}
		_, err = storage.ReadRow(context.Background(), "tenant-a", "site-1", "records", command.RowID)
		response.RowPresent = err == nil
	case "barrier":
		index, err := raftfsm.CommitReadBarrier(context.Background(), instance, storage, 1500*time.Millisecond)
		response.BarrierIndex = index
		response.OK = err == nil
	case "fence-read":
		ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
		err := raftfsm.WaitForAppliedIndex(ctx, storage, command.RequiredIndex, 5*time.Millisecond)
		cancel()
		response.FreshReadAllowed = err == nil
		_, rowErr := storage.ReadRow(context.Background(), "tenant-a", "site-1", "records", command.RowID)
		response.RowPresent = rowErr == nil
	case "probe-egress":
		target := (index + 1) % nodeCount
		vote := new(raft.RequestPreVoteResponse)
		err := transport.RequestPreVote(raft.ServerID(fmt.Sprintf("domain-node-%d", target)), addresses[target], &raft.RequestPreVoteRequest{RPCHeader: raft.RPCHeader{ID: []byte(fmt.Sprintf("domain-node-%d", index)), Addr: []byte(transport.LocalAddr())}, Term: 1}, vote)
		response.EgressBlocked = err != nil
	case "shutdown":
		return response, true
	default:
		response.OK = false
	}
	response.AppliedIndex = check.Value(storage.AppliedRaftIndex(context.Background()))
	response.State = instance.State().String()
	_, leaderID := instance.LeaderWithID()
	response.LeaderID = string(leaderID)
	return response, false
}

func encodeOSClusterResponse(writer *bufio.Writer, response osClusterResponse) error {
	if err := json.NewEncoder(writer).Encode(response); err != nil {
		return err
	}
	return writer.Flush()
}

func prepareOSClusterFiles(root string) error {
	if err := os.Chmod(filepath.Join(root, "ca.crt"), 0o644); err != nil { // #nosec G302 -- Public CA certificate must be readable by isolated fixture UIDs.
		return err
	}
	for index := 0; index < nodeCount; index++ {
		if index < 0 || index >= 3 {
			return errors.New("invalid fixture node index")
		}
		uid := uint32(31000 + index)
		if err := os.Chown(filepath.Join(root, fmt.Sprintf("node-%d.crt", index)), int(uid), int(uid)); err != nil {
			return err
		}
		if err := os.Chown(filepath.Join(root, fmt.Sprintf("node-%d.key", index)), int(uid), int(uid)); err != nil {
			return err
		}
		dataDirectory := filepath.Join(root, fmt.Sprintf("node-%d-data", index))
		if err := os.Mkdir(dataDirectory, 0o700); err != nil {
			return err
		}
		if err := os.Chown(dataDirectory, int(uid), int(uid)); err != nil {
			return err
		}
	}
	return nil
}

func startOSClusterChild(root string, index int, addresses []raft.ServerAddress, bootstrap bool) (*osClusterChild, error) {
	args := []string{"network-node", root, strconv.Itoa(index), strconv.FormatBool(bootstrap)}
	for _, address := range addresses {
		args = append(args, string(address))
	}
	uid := uint32(31000 + index)
	command := exec.CommandContext(context.Background(), check.Executable(), args...)
	command.SysProcAttr = &syscall.SysProcAttr{Credential: &syscall.Credential{Uid: uid, Gid: uid, NoSetGroups: true}}
	command.Stderr = io.Discard
	input, err := command.StdinPipe()
	if err != nil {
		return nil, err
	}
	output, err := command.StdoutPipe()
	if err != nil {
		return nil, err
	}
	if err := command.Start(); err != nil {
		return nil, err
	}
	return &osClusterChild{index: index, uid: uid, process: command, input: bufio.NewWriter(input), output: bufio.NewReader(output)}, nil
}

func (child *osClusterChild) request(command osClusterCommand, timeout time.Duration) (osClusterResponse, error) {
	if child == nil || child.input == nil || child.output == nil {
		return osClusterResponse{}, errors.New("network fixture child is unavailable")
	}
	if err := json.NewEncoder(child.input).Encode(command); err != nil {
		return osClusterResponse{}, err
	}
	if err := child.input.Flush(); err != nil {
		return osClusterResponse{}, err
	}
	return child.readResponse(timeout)
}

func (child *osClusterChild) readResponse(timeout time.Duration) (osClusterResponse, error) {
	type result struct {
		response osClusterResponse
		err      error
	}
	completed := make(chan result, 1)
	go func() {
		line, err := child.output.ReadBytes('\n')
		var response osClusterResponse
		if err == nil {
			err = json.Unmarshal(line, &response)
		}
		completed <- result{response: response, err: err}
	}()
	select {
	case output := <-completed:
		return output.response, output.err
	case <-time.After(timeout):
		return osClusterResponse{}, errors.New("network fixture child response timed out")
	}
}

func waitOSClusterLeader(children []*osClusterChild, timeout time.Duration) (int, error) {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		for _, child := range children {
			response, err := child.request(osClusterCommand{Action: "status"}, time.Second)
			if err == nil && response.State == raft.Leader.String() {
				return child.index, nil
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
	return -1, errors.New("separate-process Raft nodes did not elect a leader")
}

func waitOSClusterRow(children []*osClusterChild, rowID string, exclude int, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		complete := true
		for _, child := range children {
			if child.index == exclude {
				continue
			}
			response, err := child.request(osClusterCommand{Action: "status", RowID: rowID}, time.Second)
			if err != nil || !response.RowPresent {
				complete = false
				break
			}
		}
		if complete {
			return nil
		}
		time.Sleep(50 * time.Millisecond)
	}
	return errors.New("expected Raft child processes did not apply the committed row")
}

func processIsAlive(child *osClusterChild) error {
	if child == nil || child.process == nil || child.process.Process == nil {
		return errors.New("partitioned Raft process was not started")
	}
	return child.process.Process.Signal(syscall.Signal(0))
}

func stopOSClusterChildren(children []*osClusterChild) {
	for _, child := range children {
		if child == nil || child.process == nil || child.process.Process == nil {
			continue
		}
		check.Value(child.request(osClusterCommand{Action: "shutdown"}, 2*time.Second))
		finished := make(chan error, 1)
		go func(process *exec.Cmd) { finished <- process.Wait() }(child.process)
		select {
		case <-finished:
		case <-time.After(4 * time.Second):
			check.Must(child.process.Process.Kill())
			<-finished
		}
	}
}

func requirePrivateLinuxNetworkNamespace() error {
	if runtime.GOOS != "linux" || os.Geteuid() != 0 {
		return errors.New("OS Raft partition fixture requires Linux root inside a private network namespace")
	}
	currentNamespace, err := os.Readlink("/proc/self/ns/net")
	if err != nil {
		return err
	}
	initNamespace, err := os.Readlink("/proc/1/ns/net")
	if err != nil {
		return err
	}
	if currentNamespace == initNamespace {
		return errors.New("refusing firewall changes in the fixture runner network namespace")
	}
	output, err := exec.CommandContext(context.Background(), "ip", "-o", "link", "show").CombinedOutput()
	if err != nil {
		return fmt.Errorf("could not inspect isolated network interfaces: %w", err)
	}
	interfaces := strings.Split(strings.TrimSpace(string(output)), "\n")
	if len(interfaces) != 1 || !strings.HasPrefix(interfaces[0], "1: lo:") {
		return errors.New("refusing firewall changes outside a private loopback-only network namespace")
	}
	return nil
}

func installOSNodePartition(uid uint32, port string) error {
	if err := runOSIPTables("-I", "OUTPUT", "1", "-m", "owner", "--uid-owner", strconv.FormatUint(uint64(uid), 10), "-j", "DROP"); err != nil {
		return err
	}
	if err := runOSIPTables("-I", "INPUT", "1", "-p", "tcp", "--dport", port, "-j", "DROP"); err != nil {
		check.Must(runOSIPTables("-D", "OUTPUT", "-m", "owner", "--uid-owner", strconv.FormatUint(uint64(uid), 10), "-j", "DROP"))
		return err
	}
	return nil
}

func removeOSNodePartition(uid uint32, port string) error {
	outputErr := runOSIPTables("-D", "OUTPUT", "-m", "owner", "--uid-owner", strconv.FormatUint(uint64(uid), 10), "-j", "DROP")
	inputErr := runOSIPTables("-D", "INPUT", "-p", "tcp", "--dport", port, "-j", "DROP")
	if outputErr != nil {
		return outputErr
	}
	return inputErr
}

func osNodeDropCounters(uid uint32, port string) error {
	output, err := exec.CommandContext(context.Background(), "iptables", "-L", "OUTPUT", "-v", "-n", "-x", "--line-numbers").CombinedOutput()
	if err != nil {
		return fmt.Errorf("could not inspect isolated OUTPUT counters: %w", err)
	}
	ownerRuleCounted := false
	uidText := strconv.FormatUint(uint64(uid), 10)
	for _, line := range strings.Split(string(output), "\n") {
		fields := strings.Fields(line)
		if len(fields) >= 8 && fields[3] == "DROP" && strings.Contains(line, "owner UID match "+uidText) {
			packets, parseErr := strconv.ParseUint(fields[1], 10, 64)
			ownerRuleCounted = parseErr == nil && packets > 0
		}
	}
	inputOutput, err := exec.CommandContext(context.Background(), "iptables", "-L", "INPUT", "-v", "-n", "-x", "--line-numbers").CombinedOutput()
	if err != nil {
		return fmt.Errorf("could not inspect isolated INPUT counters: %w", err)
	}
	inputRuleCounted := false
	for _, line := range strings.Split(string(inputOutput), "\n") {
		fields := strings.Fields(line)
		if len(fields) >= 8 && fields[3] == "DROP" && strings.Contains(line, "dpt:"+port) {
			packets, parseErr := strconv.ParseUint(fields[1], 10, 64)
			inputRuleCounted = parseErr == nil && packets > 0
		}
	}
	if !ownerRuleCounted || !inputRuleCounted {
		return fmt.Errorf("kernel partition rules did not count both isolated egress and peer ingress packets (egress=%v ingress=%v; output=%q)", ownerRuleCounted, inputRuleCounted, strings.TrimSpace(string(output)))
	}
	return nil
}

func runOSIPTables(args ...string) error {
	output, err := exec.CommandContext(context.Background(), "iptables", args...).CombinedOutput()
	if err != nil {
		return fmt.Errorf("isolated iptables operation failed: %s (%w)", strings.TrimSpace(string(output)), err)
	}
	return nil
}

func addressPort(address raft.ServerAddress) string {
	_, port, _ := net.SplitHostPort(string(address))
	return port
}

func run() (map[string]bool, error) {
	root, err := os.MkdirTemp("", "domain-raft-peer-cluster-")
	if err != nil {
		return nil, err
	}
	defer func() { check.Must(os.RemoveAll(root)) }()
	identities := []string{
		"spiffe://liapoldus/domain/raft/node-a",
		"spiffe://liapoldus/domain/raft/node-b",
		"spiffe://liapoldus/domain/raft/node-c",
	}
	if err := writeCertificates(root, identities); err != nil {
		return nil, err
	}
	addresses := make([]raft.ServerAddress, nodeCount)
	for index := range addresses {
		address, err := reserveAddress()
		if err != nil {
			return nil, err
		}
		addresses[index] = raft.ServerAddress(address)
	}
	partition := &partitionState{isolated: -1}
	cluster := make([]*node, nodeCount)
	for index := range cluster {
		cluster[index], err = startNode(root, index, addresses, identities, index == 0, partition)
		if err != nil {
			closeNodes(cluster)
			return nil, err
		}
	}
	defer closeNodes(cluster)

	leader, err := waitLeader(cluster, -1)
	if err != nil {
		return nil, err
	}
	emptyReadIndex, err := raftfsm.RequireFreshRead(context.Background(), cluster[leader].raft, cluster[leader].storage, cluster[(leader+1)%nodeCount].storage, time.Second, 5*time.Millisecond)
	if err != nil || emptyReadIndex != 0 {
		return nil, errors.New("quorum-backed read of an empty Domain state was refused")
	}
	if err := commit(cluster[leader].raft, "peer-cluster-before-failure"); err != nil {
		return nil, fmt.Errorf("majority commit failed: %w", err)
	}
	if err := waitRows(cluster, "peer-cluster-before-failure"); err != nil {
		return nil, err
	}

	// Keep a follower process alive but cut both directions of its Raft traffic.
	// The majority commits while the isolated SQLite materialization stays stale;
	// healing the partition must catch that follower up before another commit.
	partitioned := (leader + 1) % nodeCount
	partition.isolate(partitioned)
	if err := commit(cluster[leader].raft, "peer-cluster-during-partition"); err != nil {
		return nil, fmt.Errorf("majority commit during network partition failed: %w", err)
	}
	majorityIndexes := []int{leader, (leader + 2) % nodeCount}
	if err := waitRowsAt(cluster, majorityIndexes, "peer-cluster-during-partition"); err != nil {
		return nil, err
	}
	if _, err := cluster[partitioned].storage.ReadRow(context.Background(), "tenant-a", "site-1", "records", "peer-cluster-during-partition"); !errors.Is(err, sql.ErrNoRows) {
		return nil, errors.New("isolated follower applied a write without quorum communication")
	}
	barrierIndex, err := raftfsm.CommitReadBarrier(context.Background(), cluster[leader].raft, cluster[leader].storage, time.Second)
	if err != nil {
		return nil, errors.New("leader could not commit a read barrier with quorum")
	}
	readContext, cancelRead := context.WithTimeout(context.Background(), 100*time.Millisecond)
	fencedReadErr := raftfsm.WaitForAppliedIndex(readContext, cluster[partitioned].storage, barrierIndex, 5*time.Millisecond)
	cancelRead()
	if !errors.Is(fencedReadErr, raftfsm.ErrFreshReadUnavailable) {
		return nil, errors.New("stale replica passed a fresh-read barrier during partition")
	}
	time.Sleep(2 * time.Second)
	if cluster[partitioned].raft.State() == raft.Leader {
		return nil, errors.New("isolated follower became leader without quorum communication")
	}
	partition.heal()
	if err := waitRows(cluster, "peer-cluster-during-partition"); err != nil {
		return nil, fmt.Errorf("partitioned follower did not catch up after heal: %w", err)
	}
	if err := commit(cluster[leader].raft, "peer-cluster-after-partition-heal"); err != nil {
		return nil, fmt.Errorf("post-partition majority commit failed: %w", err)
	}
	if err := waitRows(cluster, "peer-cluster-after-partition-heal"); err != nil {
		return nil, err
	}
	freshReadContext, cancelFreshRead := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancelFreshRead()
	index, err := raftfsm.RequireFreshRead(freshReadContext, cluster[leader].raft, cluster[leader].storage, cluster[partitioned].storage, 2*time.Second, 5*time.Millisecond)
	if err != nil || index == 0 {
		return nil, fmt.Errorf("caught-up follower did not pass fresh-read barrier: %w", err)
	}

	// Leave the former leader as a one-node minority and prove it cannot commit.
	for index := range cluster {
		if index != leader {
			if err := stopNode(cluster[index]); err != nil {
				return nil, err
			}
		}
	}
	minorityRejected := commit(cluster[leader].raft, "peer-cluster-minority") != nil
	if !minorityRejected {
		return nil, errors.New("single-node minority committed a Raft write")
	}
	if _, err := raftfsm.CommitReadBarrier(context.Background(), cluster[leader].raft, cluster[leader].storage, 150*time.Millisecond); !errors.Is(err, raftfsm.ErrFreshReadUnavailable) {
		return nil, errors.New("fresh read barrier succeeded without quorum")
	}
	if _, err := cluster[leader].storage.ReadRow(context.Background(), "tenant-a", "site-1", "records", "peer-cluster-minority"); !errors.Is(err, sql.ErrNoRows) {
		return nil, errors.New("minority command changed the local materialized state")
	}
	if err := stopNode(cluster[leader]); err != nil {
		return nil, err
	}

	// Restart only the two former followers from their own SQLite Raft stores.
	for index := range cluster {
		if index != leader {
			cluster[index], err = startNode(root, index, addresses, identities, false, partition)
			if err != nil {
				return nil, err
			}
		}
	}
	newLeader, err := waitLeader(cluster, leader)
	if err != nil {
		return nil, err
	}
	if err := commit(cluster[newLeader].raft, "peer-cluster-after-failover"); err != nil {
		return nil, fmt.Errorf("post-failover majority commit failed: %w", err)
	}
	if err := waitRowsAt(cluster, []int{(leader + 1) % nodeCount, (leader + 2) % nodeCount}, "peer-cluster-after-failover"); err != nil {
		return nil, err
	}
	cluster[leader], err = startNode(root, leader, addresses, identities, false, partition)
	if err != nil {
		return nil, err
	}
	if err := waitRows(cluster, "peer-cluster-after-failover"); err != nil {
		return nil, err
	}
	restartedFollower := (newLeader + 1) % nodeCount
	if err := stopNode(cluster[restartedFollower]); err != nil {
		return nil, err
	}
	if err := commit(cluster[newLeader].raft, "peer-cluster-during-peer-outage"); err != nil {
		return nil, fmt.Errorf("majority commit with one peer stopped failed: %w", err)
	}
	activeIndexes := make([]int, 0, nodeCount-1)
	for index, candidate := range cluster {
		if candidate != nil && candidate.storage != nil {
			activeIndexes = append(activeIndexes, index)
		}
	}
	if err := waitRowsAt(cluster, activeIndexes, "peer-cluster-during-peer-outage"); err != nil {
		return nil, err
	}
	cluster[restartedFollower], err = startNode(root, restartedFollower, addresses, identities, false, partition)
	if err != nil {
		return nil, err
	}
	if err := commit(cluster[newLeader].raft, "peer-cluster-after-peer-reconnect"); err != nil {
		return nil, fmt.Errorf("majority commit after peer restart failed: %w", err)
	}
	if err := waitRows(cluster, "peer-cluster-after-peer-reconnect"); err != nil {
		return nil, err
	}
	return map[string]bool{
		"threeNodeElection":                        true,
		"majorityCommitApplied":                    true,
		"minorityWriteRejected":                    minorityRejected,
		"newLeaderElectedAfterFailure":             newLeader != leader,
		"postFailoverMajorityCommitApplied":        true,
		"peerMTLSUsed":                             true,
		"quorumLossRejected":                       minorityRejected,
		"noStaleWriteSuccessAfterQuorumLoss":       minorityRejected,
		"transportRecoveredAfterPeerRestart":       true,
		"networkPartitionFenced":                   true,
		"networkPartitionNoStaleApply":             true,
		"networkPartitionRecovered":                true,
		"freshReadBarrierFenced":                   true,
		"freshReadBarrierReleased":                 true,
		"freshReadBarrierUnavailableWithoutQuorum": true,
		"freshReadBarrierAllowsEmptyState":         true,
	}, nil
}

func startNode(root string, index int, addresses []raft.ServerAddress, identities []string, bootstrap bool, partition *partitionState) (*node, error) {
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
	certificate, roots, err := loadIdentity(root, index)
	if err != nil {
		return nil, err
	}
	transport, err := raftpeer.Listen(raftpeer.Config{
		Network:        peer.NetworkConfig{Carrier: peer.CarrierTCP, Endpoint: endpoint, ServerName: "domain.test"},
		Security:       peer.SecurityConfig{Identity: identities[index], Certificate: certificate, Roots: roots},
		PeerIdentities: peerIdentities, PeerServerIDs: peerIDs,
	})
	if err != nil {
		return nil, err
	}
	serveCtx, cancel := context.WithCancel(context.Background())
	go func() { check.Served(transport.Serve(serveCtx)) }()
	storagePath := filepath.Join(root, fmt.Sprintf("node-%d.sqlite", index))
	_, statErr := os.Stat(storagePath)
	newStorage := errors.Is(statErr, os.ErrNotExist)
	if statErr != nil && !newStorage {
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
	if newStorage {
		model := models.Model{SchemaVersion: "1", Entities: []models.Entity{{Name: "records", OwnerGroup: "forms", Fields: []models.Field{{Name: "id", Type: "text", PrimaryKey: true, Required: true}}}}}
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
	configuration := raft.Configuration{Servers: servers}
	if bootstrap {
		if err := raft.BootstrapCluster(raftConfig(index), storage, storage, snapshotStore, transport, configuration); err != nil {
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
	partitionedTransport := &partitionedTransport{
		Transport: transport, source: index, targetIndexes: targetIndexes, partition: partition,
	}
	instance, err := raft.NewRaft(raftConfig(index), &raftfsm.FSM{Store: storage}, storage, storage, snapshotStore, partitionedTransport)
	if err != nil {
		cancel()
		check.Must(transport.Close())
		check.Must(storage.Close())
		return nil, err
	}
	return &node{index: index, address: transport.LocalAddr(), storage: storage, transport: transport, raft: instance, cancel: cancel}, nil
}

func (transport *partitionedTransport) blocked(target raft.ServerAddress) bool {
	if transport == nil || transport.partition == nil {
		return false
	}
	transport.partition.mu.RLock()
	defer transport.partition.mu.RUnlock()
	if transport.partition.isolated < 0 {
		return false
	}
	return transport.source == transport.partition.isolated || transport.targetIndexes[target] == transport.partition.isolated
}

func (partition *partitionState) isolate(index int) {
	partition.mu.Lock()
	partition.isolated = index
	partition.mu.Unlock()
}

func (partition *partitionState) heal() {
	partition.mu.Lock()
	partition.isolated = -1
	partition.mu.Unlock()
}

func (transport *partitionedTransport) AppendEntriesPipeline(id raft.ServerID, target raft.ServerAddress) (raft.AppendPipeline, error) {
	if transport.blocked(target) {
		return nil, errFixturePartition
	}
	return transport.Transport.AppendEntriesPipeline(id, target)
}

func (transport *partitionedTransport) AppendEntries(id raft.ServerID, target raft.ServerAddress, request *raft.AppendEntriesRequest, response *raft.AppendEntriesResponse) error {
	if transport.blocked(target) {
		return errFixturePartition
	}
	return transport.Transport.AppendEntries(id, target, request, response)
}

func (transport *partitionedTransport) RequestVote(id raft.ServerID, target raft.ServerAddress, request *raft.RequestVoteRequest, response *raft.RequestVoteResponse) error {
	if transport.blocked(target) {
		return errFixturePartition
	}
	return transport.Transport.RequestVote(id, target, request, response)
}

func (transport *partitionedTransport) RequestPreVote(id raft.ServerID, target raft.ServerAddress, request *raft.RequestPreVoteRequest, response *raft.RequestPreVoteResponse) error {
	if transport.blocked(target) {
		return errFixturePartition
	}
	preVote, ok := transport.Transport.(raft.WithPreVote)
	if !ok {
		return errFixturePartition
	}
	return preVote.RequestPreVote(id, target, request, response)
}

func (transport *partitionedTransport) TimeoutNow(id raft.ServerID, target raft.ServerAddress, request *raft.TimeoutNowRequest, response *raft.TimeoutNowResponse) error {
	if transport.blocked(target) {
		return errFixturePartition
	}
	return transport.Transport.TimeoutNow(id, target, request, response)
}

func (transport *partitionedTransport) InstallSnapshot(id raft.ServerID, target raft.ServerAddress, request *raft.InstallSnapshotRequest, response *raft.InstallSnapshotResponse, snapshot io.Reader) error {
	if transport.blocked(target) {
		return errFixturePartition
	}
	return transport.Transport.InstallSnapshot(id, target, request, response, snapshot)
}

func raftConfig(index int) *raft.Config {
	config := raft.DefaultConfig()
	config.LocalID = raft.ServerID(fmt.Sprintf("domain-node-%d", index))
	config.LogOutput = io.Discard
	config.HeartbeatTimeout = 500 * time.Millisecond
	config.ElectionTimeout = 500 * time.Millisecond
	config.LeaderLeaseTimeout = 500 * time.Millisecond
	config.CommitTimeout = 20 * time.Millisecond
	return config
}

func commit(instance *raft.Raft, id string) error {
	command, err := json.Marshal(models.RaftCommand{Kind: "put", Tenant: "tenant-a", Site: "site-1", Entity: "records", ID: id, Row: json.RawMessage(fmt.Sprintf(`{"id":%q}`, id))})
	if err != nil {
		return err
	}
	future := instance.Apply(command, 1100*time.Millisecond)
	if err := future.Error(); err != nil {
		return err
	}
	if err, ok := future.Response().(error); ok {
		return err
	}
	return nil
}

func waitLeader(nodes []*node, excluded int) (int, error) {
	deadline := time.Now().Add(12 * time.Second)
	for time.Now().Before(deadline) {
		for index, candidate := range nodes {
			if candidate != nil && index != excluded && candidate.raft != nil && candidate.raft.State() == raft.Leader {
				return index, nil
			}
		}
		time.Sleep(25 * time.Millisecond)
	}
	return -1, errors.New("raft leader election timed out")
}

func waitRows(nodes []*node, id string) error {
	indexes := []int{0, 1, 2}
	return waitRowsAt(nodes, indexes, id)
}

func waitRowsAt(nodes []*node, indexes []int, id string) error {
	deadline := time.Now().Add(8 * time.Second)
	for time.Now().Before(deadline) {
		applied := true
		for _, index := range indexes {
			if nodes[index] == nil || nodes[index].storage == nil {
				applied = false
				break
			}
			if _, err := nodes[index].storage.ReadRow(context.Background(), "tenant-a", "site-1", "records", id); err != nil {
				applied = false
				break
			}
		}
		if applied {
			return nil
		}
		time.Sleep(25 * time.Millisecond)
	}
	return errors.New("committed Raft row was not applied by expected nodes")
}

func stopNode(candidate *node) error {
	if candidate == nil || candidate.raft == nil {
		return nil
	}
	if err := candidate.raft.Shutdown().Error(); err != nil {
		return err
	}
	candidate.cancel()
	if err := candidate.transport.Close(); err != nil {
		return err
	}
	if err := candidate.storage.Close(); err != nil {
		return err
	}
	candidate.raft = nil
	candidate.transport = nil
	candidate.storage = nil
	return nil
}

func closeNodes(nodes []*node) {
	for _, candidate := range nodes {
		check.Must(stopNode(candidate))
	}
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

func loadIdentity(root string, index int) (tls.Certificate, *x509.CertPool, error) {
	name := fmt.Sprintf("node-%d", index)
	certificateBytes, err := check.ReadFile(filepath.Join(root, name+".crt"))
	if err != nil {
		return tls.Certificate{}, nil, err
	}
	keyBytes, err := check.ReadFile(filepath.Join(root, name+".key"))
	if err != nil {
		return tls.Certificate{}, nil, err
	}
	certificate, err := tls.X509KeyPair(certificateBytes, keyBytes)
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
	return certificate, roots, nil
}

func writeCertificates(root string, identities []string) error {
	caKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return err
	}
	start := time.Now().Add(-time.Minute)
	caTemplate := &x509.Certificate{SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "Domain Raft fixture CA"}, NotBefore: start, NotAfter: start.Add(time.Hour), IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature}
	caDER, err := x509.CreateCertificate(rand.Reader, caTemplate, caTemplate, &caKey.PublicKey, caKey)
	if err != nil {
		return err
	}
	caCertificate, err := x509.ParseCertificate(caDER)
	if err != nil {
		return err
	}
	if err := writePEM(filepath.Join(root, "ca.crt"), "CERTIFICATE", caDER); err != nil {
		return err
	}
	for index, identity := range identities {
		leafKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
		if err != nil {
			return err
		}
		parsedIdentity, err := url.Parse(identity)
		if err != nil {
			return err
		}
		leafTemplate := &x509.Certificate{SerialNumber: big.NewInt(int64(index + 2)), Subject: pkix.Name{CommonName: fmt.Sprintf("Domain Raft node %d", index)}, NotBefore: start, NotAfter: start.Add(time.Hour), DNSNames: []string{"domain.test"}, URIs: []*url.URL{parsedIdentity}, KeyUsage: x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth}}
		leafDER, err := x509.CreateCertificate(rand.Reader, leafTemplate, caCertificate, &leafKey.PublicKey, caKey)
		if err != nil {
			return err
		}
		if err := writePEM(filepath.Join(root, fmt.Sprintf("node-%d.crt", index)), "CERTIFICATE", leafDER); err != nil {
			return err
		}
		encodedKey, err := x509.MarshalPKCS8PrivateKey(leafKey)
		if err != nil {
			return err
		}
		if err := writePEM(filepath.Join(root, fmt.Sprintf("node-%d.key", index)), "PRIVATE KEY", encodedKey); err != nil {
			return err
		}
	}
	return nil
}

func writePEM(path, blockType string, data []byte) error {
	return check.WriteFile(path, pem.EncodeToMemory(&pem.Block{Type: blockType, Bytes: data}), 0o600)
}

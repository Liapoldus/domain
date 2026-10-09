package main

import (
	"bufio"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/sha256"
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
	"os/exec"
	"os/signal"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync/atomic"
	"syscall"
	"time"

	domainraft "github.com/Liapoldus/domain/internal/infrastructure/raftpeer"
	"github.com/Liapoldus/pluginprotocol/v2/presentation/peer"
	"github.com/hashicorp/raft"
)

const dnsName = "domain.test"

type fixtureResult struct {
	MTLSIdentityPinned          bool `json:"mTLSIdentityPinned"`
	WrongPeerIdentityRejected   bool `json:"wrongPeerIdentityRejected"`
	UnauthorizedPeerRejected    bool `json:"unauthorizedPeerRejected"`
	SpoofedRaftServerIDRejected bool `json:"spoofedRaftServerIDRejected"`
	HeartbeatFastPath           bool `json:"heartbeatFastPath"`
	AppendEntriesRoundTrip      bool `json:"appendEntriesRoundTrip"`
	SnapshotStreamRoundTrip     bool `json:"snapshotStreamRoundTrip"`
	SnapshotBytesPreserved      bool `json:"snapshotBytesPreserved"`
}

func main() {
	var err error
	mode := ""
	if len(os.Args) > 1 {
		mode = os.Args[1]
	}
	switch mode {
	case "server":
		err = runServer(os.Args[2], os.Args[3], os.Args[4])
	case "client":
		err = runClient(os.Args[2], os.Args[3], os.Args[4])
	case "partition-client":
		err = runPartitionClient(os.Args[2], os.Args[3], os.Args[4])
	case "network-partition":
		err = runNetworkPartitionFixture()
	default:
		err = runFixture()
	}
	if err != nil {
		check.Value(fmt.Fprintf(os.Stderr, "Domain Raft peer fixture failed: %v\n", err))
		os.Exit(2)
	}
}

// runNetworkPartitionFixture is an opt-in Linux-only test. It refuses to change
// firewall state unless it is already inside a private network namespace with
// no interface other than loopback; the namespace is created by the TS runner
// and disappears when this child process exits.
func runNetworkPartitionFixture() error {
	if err := requirePrivateLinuxNetworkNamespace(); err != nil {
		return err
	}
	if output, err := exec.CommandContext(context.Background(), "ip", "link", "set", "lo", "up").CombinedOutput(); err != nil {
		return fmt.Errorf("private loopback setup failed: %s (%w)", strings.TrimSpace(string(output)), err)
	}
	root, err := os.MkdirTemp("", "domain-raft-os-partition-")
	if err != nil {
		return err
	}
	defer func() { check.Must(os.RemoveAll(root)) }()
	if err := writeCertificates(root,
		"spiffe://liapoldus/domain/raft/node-a",
		"spiffe://liapoldus/domain/raft/node-b",
		"spiffe://liapoldus/domain/raft/node-c",
	); err != nil {
		return err
	}
	address, err := reserveTCPAddress()
	if err != nil {
		return err
	}
	clientAddress, err := reserveTCPAddress()
	if err != nil {
		return err
	}
	server := exec.CommandContext(context.Background(), check.Executable(), "server", root, address, clientAddress)
	server.Stdout = io.Discard
	server.Stderr = os.Stderr
	if err := server.Start(); err != nil {
		return err
	}
	serverAlive := true
	defer func() { check.Must(stopFixtureProcess(server, &serverAlive)) }()
	if err := waitFile(filepath.Join(root, "address"), 10*time.Second); err != nil {
		return err
	}
	client := exec.CommandContext(context.Background(), check.Executable(), "partition-client", root, address, clientAddress)
	client.Stderr = os.Stderr
	clientInput, err := client.StdinPipe()
	if err != nil {
		return err
	}
	clientOutput, err := client.StdoutPipe()
	if err != nil {
		return err
	}
	if err := client.Start(); err != nil {
		return err
	}
	clientAlive := true
	defer func() { check.Must(stopFixtureProcess(client, &clientAlive)) }()
	reader := bufio.NewReader(clientOutput)
	if err := writeProbeCommand(clientInput); err != nil {
		return err
	}
	initial, err := readProbeResult(reader, 12*time.Second)
	if err != nil || !initial.OK {
		return errors.New("initial peer TCP request did not succeed")
	}
	port := address[strings.LastIndex(address, ":")+1:]
	if _, err := strconv.Atoi(port); err != nil {
		return errors.New("server endpoint did not contain a TCP port")
	}
	if err := installTCPDrop(port); err != nil {
		return err
	}
	dropInstalled := true
	defer func() {
		if dropInstalled {
			check.Must(removeTCPDrop(port))
		}
	}()
	if err := writeProbeCommand(clientInput); err != nil {
		return err
	}
	partitioned, err := readProbeResult(reader, 12*time.Second)
	if err != nil || partitioned.OK {
		return errors.New("peer TCP request succeeded while the isolated kernel dropped its packets")
	}
	if err := processAlive(server); err != nil {
		return fmt.Errorf("server process did not survive the network partition: %w", err)
	}
	if err := processAlive(client); err != nil {
		return fmt.Errorf("client process did not survive the network partition: %w", err)
	}
	if err := inputDropCounter(port); err != nil {
		return err
	}
	if err := removeTCPDrop(port); err != nil {
		return err
	}
	dropInstalled = false
	if err := writeProbeCommand(clientInput); err != nil {
		return err
	}
	recovered, err := readProbeResult(reader, 12*time.Second)
	if err != nil || !recovered.OK {
		return errors.New("live peer process did not reconnect after the TCP packet drop was removed")
	}
	if err := processAlive(server); err != nil {
		return fmt.Errorf("server process did not survive TCP recovery: %w", err)
	}
	if err := processAlive(client); err != nil {
		return fmt.Errorf("client process did not survive TCP recovery: %w", err)
	}
	if err := stopFixtureProcess(client, &clientAlive); err != nil {
		return err
	}
	if err := stopFixtureProcess(server, &serverAlive); err != nil {
		return err
	}
	return json.NewEncoder(os.Stdout).Encode(map[string]bool{
		"clientProcessSurvivedPartition": true,
		"serverProcessSurvivedPartition": true,
		"initialTcpCallSucceeded":        true,
		"kernelDroppedPeerPackets":       true,
		"partitionedTcpCallFailed":       true,
		"tcpCallSucceededAfterHeal":      true,
		"sameClientProcessReconnected":   true,
	})
}

func runPartitionClient(root, address, clientAddress string) error {
	identity := "spiffe://liapoldus/domain/raft/node-b"
	config, err := transportConfig(root, "node-b", identity, clientAddress, map[raft.ServerAddress]string{raft.ServerAddress(address): "spiffe://liapoldus/domain/raft/node-a"}, map[raft.ServerAddress]raft.ServerID{raft.ServerAddress(address): "node-a"})
	if err != nil {
		return err
	}
	transport, err := domainraft.Listen(config)
	if err != nil {
		return err
	}
	defer func() { check.Must(transport.Close()) }()
	reader := bufio.NewScanner(os.Stdin)
	writer := bufio.NewWriter(os.Stdout)
	for reader.Scan() {
		response := struct {
			OK bool `json:"ok"`
		}{}
		err := transport.RequestVote("node-a", raft.ServerAddress(address), &raft.RequestVoteRequest{RPCHeader: peerHeader("node-b", transport.LocalAddr()), Term: 11}, new(raft.RequestVoteResponse))
		response.OK = err == nil
		if err := json.NewEncoder(writer).Encode(response); err != nil {
			return err
		}
		if err := writer.Flush(); err != nil {
			return err
		}
	}
	return reader.Err()
}

func requirePrivateLinuxNetworkNamespace() error {
	if runtime.GOOS != "linux" || os.Geteuid() != 0 {
		return errors.New("OS network partition fixture requires Linux root inside an isolated network namespace")
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

func installTCPDrop(port string) error {
	if err := runIPTables("-I", "INPUT", "1", "-p", "tcp", "--dport", port, "-j", "DROP"); err != nil {
		return err
	}
	if err := runIPTables("-I", "OUTPUT", "1", "-p", "tcp", "--sport", port, "-j", "DROP"); err != nil {
		check.Must(runIPTables("-D", "INPUT", "-p", "tcp", "--dport", port, "-j", "DROP"))
		return err
	}
	return nil
}

func removeTCPDrop(port string) error {
	outputRule := runIPTables("-D", "OUTPUT", "-p", "tcp", "--sport", port, "-j", "DROP")
	inputRule := runIPTables("-D", "INPUT", "-p", "tcp", "--dport", port, "-j", "DROP")
	if outputRule != nil {
		return outputRule
	}
	return inputRule
}

func inputDropCounter(port string) error {
	output, err := exec.CommandContext(context.Background(), "iptables", "-L", "INPUT", "-v", "-n", "-x", "--line-numbers").CombinedOutput()
	if err != nil {
		return fmt.Errorf("could not inspect isolated INPUT counters: %w", err)
	}
	for _, line := range strings.Split(string(output), "\n") {
		fields := strings.Fields(line)
		if len(fields) >= 8 && fields[3] == "DROP" && strings.Contains(line, "dpt:"+port) {
			packets, parseErr := strconv.ParseUint(fields[1], 10, 64)
			if parseErr == nil && packets > 0 {
				return nil
			}
		}
	}
	return errors.New("kernel INPUT drop rule did not count any peer TCP packets")
}

func runIPTables(args ...string) error {
	output, err := exec.CommandContext(context.Background(), "iptables", args...).CombinedOutput()
	if err != nil {
		return fmt.Errorf("isolated iptables operation failed: %s (%w)", strings.TrimSpace(string(output)), err)
	}
	return nil
}

func writeProbeCommand(writer io.Writer) error {
	_, err := io.WriteString(writer, "call\n")
	return err
}

func readProbeResult(reader *bufio.Reader, timeout time.Duration) (struct {
	OK bool `json:"ok"`
}, error) {
	type probeResult struct {
		OK bool `json:"ok"`
	}
	result := make(chan struct {
		value probeResult
		err   error
	}, 1)
	go func() {
		line, err := reader.ReadBytes('\n')
		if err != nil {
			result <- struct {
				value probeResult
				err   error
			}{err: err}
			return
		}
		var value probeResult
		err = json.Unmarshal(line, &value)
		result <- struct {
			value probeResult
			err   error
		}{value: value, err: err}
	}()
	select {
	case read := <-result:
		return struct {
			OK bool `json:"ok"`
		}{OK: read.value.OK}, read.err
	case <-time.After(timeout):
		return struct {
			OK bool `json:"ok"`
		}{}, errors.New("peer TCP probe timed out")
	}
}

func processAlive(command *exec.Cmd) error {
	if command == nil || command.Process == nil {
		return errors.New("fixture process was not started")
	}
	return command.Process.Signal(syscall.Signal(0))
}

func stopFixtureProcess(command *exec.Cmd, alive *bool) error {
	if !*alive {
		return nil
	}
	*alive = false
	if command == nil || command.Process == nil {
		return nil
	}
	check.Must(command.Process.Signal(syscall.SIGTERM))
	done := make(chan error, 1)
	go func() { done <- command.Wait() }()
	select {
	case err := <-done:
		if err != nil {
			var exitError *exec.ExitError
			if !errors.As(err, &exitError) {
				return err
			}
		}
	case <-time.After(3 * time.Second):
		check.Must(command.Process.Kill())
		<-done
		return errors.New("fixture process did not stop after SIGTERM")
	}
	return nil
}

func runFixture() error {
	root, err := os.MkdirTemp("", "domain-raft-peer-")
	if err != nil {
		return err
	}
	defer func() { check.Must(os.RemoveAll(root)) }()
	identityA := "spiffe://liapoldus/domain/raft/node-a"
	identityB := "spiffe://liapoldus/domain/raft/node-b"
	identityC := "spiffe://liapoldus/domain/raft/node-c"
	if err := writeCertificates(root, identityA, identityB, identityC); err != nil {
		return err
	}
	addressA, err := reserveTCPAddress()
	if err != nil {
		return err
	}
	addressB, err := reserveTCPAddress()
	if err != nil {
		return err
	}
	if addressA == addressB {
		return errors.New("fixture endpoints must be distinct")
	}
	command := exec.CommandContext(context.Background(), check.Executable(), "server", root, addressA, addressB)
	command.Stdout = io.Discard
	command.Stderr = os.Stderr
	if err := command.Start(); err != nil {
		return err
	}
	serverStopped := false
	defer func() {
		if !serverStopped {
			check.Must(command.Process.Signal(syscall.SIGTERM))
			check.Value(command.Process.Wait())
		}
	}()
	addressPath := filepath.Join(root, "address")
	if err := waitFile(addressPath, 10*time.Second); err != nil {
		return err
	}
	addressBytes, err := check.ReadFile(addressPath)
	if err != nil {
		return err
	}
	if string(addressBytes) != addressA {
		return errors.New("server bound an unexpected address")
	}
	client := exec.CommandContext(context.Background(), check.Executable(), "client", root, addressA, addressB)
	clientOutput, err := client.CombinedOutput()
	if err != nil {
		return fmt.Errorf("client child: %s (%w)", string(clientOutput), err)
	}
	if err := command.Process.Signal(syscall.SIGTERM); err != nil {
		return err
	}
	if _, err := command.Process.Wait(); err != nil {
		return err
	}
	serverStopped = true
	var result fixtureResult
	if err := json.Unmarshal(clientOutput, &result); err != nil {
		return errors.New("client returned malformed result")
	}
	if err := waitFile(filepath.Join(root, "heartbeat"), 5*time.Second); err != nil {
		return err
	}
	if err := waitFile(filepath.Join(root, "snapshot.sha256"), 5*time.Second); err != nil {
		return err
	}
	expected, err := check.ReadFile(filepath.Join(root, "expected.sha256"))
	if err != nil {
		return err
	}
	actual, err := check.ReadFile(filepath.Join(root, "snapshot.sha256"))
	if err != nil {
		return err
	}
	result.HeartbeatFastPath = stringMustUint(filepath.Join(root, "heartbeat")) == "1"
	result.SnapshotBytesPreserved = string(expected) == string(actual)
	result.WrongPeerIdentityRejected = true
	result.UnauthorizedPeerRejected = true
	result.SpoofedRaftServerIDRejected = true
	return json.NewEncoder(os.Stdout).Encode(result)
}

func runServer(root, addressA, addressB string) error {
	identityA := "spiffe://liapoldus/domain/raft/node-a"
	identityB := "spiffe://liapoldus/domain/raft/node-b"
	config, err := transportConfig(root, "node-a", identityA, addressA, map[raft.ServerAddress]string{raft.ServerAddress(addressB): identityB}, map[raft.ServerAddress]raft.ServerID{raft.ServerAddress(addressB): "node-b"})
	if err != nil {
		return err
	}
	transport, err := domainraft.Listen(config)
	if err != nil {
		return err
	}
	defer func() { check.Must(transport.Close()) }()
	var heartbeats atomic.Int32
	transport.SetHeartbeatHandler(func(rpc raft.RPC) {
		request, ok := rpc.Command.(*raft.AppendEntriesRequest)
		if !ok || len(request.Entries) != 0 {
			rpc.Respond(nil, errors.New("unexpected heartbeat RPC"))
			return
		}
		heartbeats.Add(1)
		rpc.Respond(&raft.AppendEntriesResponse{Term: request.Term, Success: true}, nil)
	})
	go func() {
		for rpc := range transport.Consumer() {
			switch request := rpc.Command.(type) {
			case *raft.RequestVoteRequest:
				rpc.Respond(&raft.RequestVoteResponse{Term: request.Term, Granted: true}, nil)
			case *raft.AppendEntriesRequest:
				ok := len(request.Entries) == 1 && request.Entries[0] != nil && string(request.Entries[0].Data) == "replicated-entry"
				rpc.Respond(&raft.AppendEntriesResponse{Term: request.Term, LastLog: 41, Success: ok}, nil)
			case *raft.InstallSnapshotRequest:
				hash := sha256.New()
				_, copyErr := io.Copy(hash, rpc.Reader)
				if copyErr != nil {
					rpc.Respond(nil, copyErr)
					continue
				}
				if writeErr := check.WriteFile(filepath.Join(root, "snapshot.sha256"), []byte(fmt.Sprintf("%x", hash.Sum(nil))), 0o600); writeErr != nil {
					rpc.Respond(nil, writeErr)
					continue
				}
				rpc.Respond(&raft.InstallSnapshotResponse{Term: request.Term, Success: true}, nil)
			default:
				rpc.Respond(nil, errors.New("unsupported fixture RPC"))
			}
		}
	}()
	if err := check.WriteFile(filepath.Join(root, "address"), []byte(transport.LocalAddr()), 0o600); err != nil {
		return err
	}
	ctx, cancel := signalContext()
	defer cancel()
	serveErr := transport.Serve(ctx)
	check.Must(check.WriteFile(filepath.Join(root, "heartbeat"), []byte(fmt.Sprint(heartbeats.Load())), 0o600))
	return serveErr
}

func runClient(root, addressA, addressB string) error {
	identityA := "spiffe://liapoldus/domain/raft/node-a"
	identityB := "spiffe://liapoldus/domain/raft/node-b"
	config, err := transportConfig(root, "node-b", identityB, addressB, map[raft.ServerAddress]string{raft.ServerAddress(addressA): identityA}, map[raft.ServerAddress]raft.ServerID{raft.ServerAddress(addressA): "node-a"})
	if err != nil {
		return err
	}
	transport, err := domainraft.Listen(config)
	if err != nil {
		return err
	}
	defer func() { check.Must(transport.Close()) }()
	serverAddress := raft.ServerAddress(addressA)
	wrongSecurity := config.Security
	wrongSecurity.PeerIdentity = "spiffe://liapoldus/domain/raft/unexpected"
	wrongNetwork := config.Network
	wrongNetwork.Endpoint = addressA
	emptyHandler, err := peer.NewRegistry().Build()
	if err != nil {
		return err
	}
	if wrongClient, err := peer.Dial(context.Background(), peer.ClientConfig{Network: wrongNetwork, Security: wrongSecurity, Handler: emptyHandler}); err == nil {
		check.Must(wrongClient.Close())
		return errors.New("TLS peer identity pin accepted an unexpected server identity")
	}
	spoofedResponse := new(raft.RequestVoteResponse)
	if err := transport.RequestVote("node-a", serverAddress, &raft.RequestVoteRequest{RPCHeader: peerHeader("node-a", transport.LocalAddr()), Term: 2}, spoofedResponse); err == nil {
		return errors.New("authenticated peer was allowed to claim another Raft server ID")
	}
	vote := new(raft.RequestVoteResponse)
	if err := transport.RequestVote("node-a", serverAddress, &raft.RequestVoteRequest{RPCHeader: peerHeader("node-b", transport.LocalAddr()), Term: 3, LastLogIndex: 9}, vote); err != nil || !vote.Granted || vote.Term != 3 {
		return errors.New("request-vote call did not round-trip")
	}
	appendResponse := new(raft.AppendEntriesResponse)
	if err := transport.AppendEntries("node-a", serverAddress, &raft.AppendEntriesRequest{RPCHeader: peerHeader("node-b", transport.LocalAddr()), Term: 4, Entries: []*raft.Log{{Index: 41, Data: []byte("replicated-entry")}}}, appendResponse); err != nil || !appendResponse.Success || appendResponse.LastLog != 41 {
		return errors.New("append-entries call did not round-trip")
	}
	heartbeat := new(raft.AppendEntriesResponse)
	if err := transport.AppendEntries("node-a", serverAddress, &raft.AppendEntriesRequest{RPCHeader: peerHeader("node-b", transport.LocalAddr()), Term: 5}, heartbeat); err != nil || !heartbeat.Success {
		return errors.New("append-entries heartbeat did not round-trip")
	}
	snapshotBytes := make([]byte, (1<<20)+123)
	for index := range snapshotBytes {
		snapshotBytes[index] = byte(index % 251)
	}
	digest := sha256.Sum256(snapshotBytes)
	if err := check.WriteFile(filepath.Join(root, "expected.sha256"), []byte(fmt.Sprintf("%x", digest[:])), 0o600); err != nil {
		return err
	}
	snapshotResponse := new(raft.InstallSnapshotResponse)
	if err := transport.InstallSnapshot("node-a", serverAddress, &raft.InstallSnapshotRequest{RPCHeader: peerHeader("node-b", transport.LocalAddr()), Term: 6, Size: int64(len(snapshotBytes))}, snapshotResponse, bytesReader(snapshotBytes)); err != nil {
		return fmt.Errorf("snapshot stream failed: %w", err)
	}
	if !snapshotResponse.Success || snapshotResponse.Term != 6 {
		return fmt.Errorf("snapshot response mismatch: success=%v term=%d", snapshotResponse.Success, snapshotResponse.Term)
	}
	identityC := "spiffe://liapoldus/domain/raft/node-c"
	unauthorizedConfig, err := transportConfig(root, "node-c", identityC, "127.0.0.1:0", map[raft.ServerAddress]string{serverAddress: identityA}, map[raft.ServerAddress]raft.ServerID{serverAddress: "node-a"})
	if err != nil {
		return err
	}
	unauthorizedTransport, err := domainraft.Listen(unauthorizedConfig)
	if err != nil {
		return err
	}
	defer func() { check.Must(unauthorizedTransport.Close()) }()
	unauthorizedResponse := new(raft.RequestVoteResponse)
	if err := unauthorizedTransport.RequestVote("node-a", serverAddress, &raft.RequestVoteRequest{RPCHeader: peerHeader("node-c", unauthorizedTransport.LocalAddr()), Term: 7}, unauthorizedResponse); err == nil {
		return errors.New("validly certified but unlisted peer identity was accepted")
	}
	if err := transport.Close(); err != nil {
		return err
	}
	if err := transport.RequestVote("node-a", serverAddress, &raft.RequestVoteRequest{RPCHeader: peerHeader("node-b", transport.LocalAddr()), Term: 8}, new(raft.RequestVoteResponse)); !errors.Is(err, domainraft.ErrTransportUnavailable) {
		return errors.New("closed transport accepted a new peer session")
	}
	return json.NewEncoder(os.Stdout).Encode(fixtureResult{
		MTLSIdentityPinned:          true,
		WrongPeerIdentityRejected:   true,
		UnauthorizedPeerRejected:    true,
		SpoofedRaftServerIDRejected: true,
		AppendEntriesRoundTrip:      true,
		SnapshotStreamRoundTrip:     true,
	})
}

func transportConfig(root, nodeID, identity, endpoint string, target map[raft.ServerAddress]string, serverIDs map[raft.ServerAddress]raft.ServerID) (domainraft.Config, error) {
	certificateBytes, err := check.ReadFile(filepath.Join(root, nodeID+".crt"))
	if err != nil {
		return domainraft.Config{}, err
	}
	keyBytes, err := check.ReadFile(filepath.Join(root, nodeID+".key"))
	if err != nil {
		return domainraft.Config{}, err
	}
	certificate, err := tls.X509KeyPair(certificateBytes, keyBytes)
	if err != nil {
		return domainraft.Config{}, err
	}
	caBytes, err := check.ReadFile(filepath.Join(root, "ca.crt"))
	if err != nil {
		return domainraft.Config{}, err
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(caBytes) {
		return domainraft.Config{}, errors.New("fixture CA certificate could not be loaded")
	}
	return domainraft.Config{
		Network:        peer.NetworkConfig{Carrier: peer.CarrierTCP, Endpoint: endpoint, ServerName: dnsName},
		Security:       peer.SecurityConfig{Identity: identity, Certificate: certificate, Roots: roots},
		PeerIdentities: target, PeerServerIDs: serverIDs,
	}, nil
}

func writeCertificates(root, identityA, identityB, identityC string) error {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return err
	}
	now := time.Now().Add(-time.Minute)
	caTemplate := &x509.Certificate{SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "Domain fixture CA"}, NotBefore: now, NotAfter: now.Add(time.Hour), IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature}
	caDER, err := x509.CreateCertificate(rand.Reader, caTemplate, caTemplate, &key.PublicKey, key)
	if err != nil {
		return err
	}
	ca, err := x509.ParseCertificate(caDER)
	if err != nil {
		return err
	}
	if err := writePEM(filepath.Join(root, "ca.crt"), "CERTIFICATE", caDER); err != nil {
		return err
	}
	for index, entry := range []struct{ node, identity string }{{"node-a", identityA}, {"node-b", identityB}, {"node-c", identityC}} {
		leafKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
		if err != nil {
			return err
		}
		parsedURI, err := url.Parse(entry.identity)
		if err != nil {
			return err
		}
		leafTemplate := &x509.Certificate{
			SerialNumber: big.NewInt(int64(index + 2)),
			Subject:      pkix.Name{CommonName: entry.node}, NotBefore: now, NotAfter: now.Add(time.Hour),
			DNSNames: []string{dnsName}, URIs: []*url.URL{parsedURI},
			KeyUsage: x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
		}
		leafDER, err := x509.CreateCertificate(rand.Reader, leafTemplate, ca, &leafKey.PublicKey, key)
		if err != nil {
			return err
		}
		if err := writePEM(filepath.Join(root, entry.node+".crt"), "CERTIFICATE", leafDER); err != nil {
			return err
		}
		encodedKey, err := x509.MarshalPKCS8PrivateKey(leafKey)
		if err != nil {
			return err
		}
		if err := writePEM(filepath.Join(root, entry.node+".key"), "PRIVATE KEY", encodedKey); err != nil {
			return err
		}
	}
	return nil
}

func writePEM(path, blockType string, data []byte) error {
	return check.WriteFile(path, pem.EncodeToMemory(&pem.Block{Type: blockType, Bytes: data}), 0o600)
}

func waitFile(path string, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if _, err := os.Stat(path); err == nil {
			return nil
		}
		time.Sleep(10 * time.Millisecond)
	}
	return fmt.Errorf("fixture readiness file was not created")
}

func reserveTCPAddress() (string, error) {
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

func peerHeader(id string, address raft.ServerAddress) raft.RPCHeader {
	return raft.RPCHeader{ID: []byte(id), Addr: []byte(address)}
}

func stringMustUint(path string) string {
	data, err := check.ReadFile(path)
	if err != nil {
		return ""
	}
	return string(data)
}

func signalContext() (context.Context, context.CancelFunc) {
	return signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
}

type byteSliceReader struct{ data []byte }

func bytesReader(data []byte) io.Reader { return &byteSliceReader{data: data} }

func (reader *byteSliceReader) Read(buffer []byte) (int, error) {
	if len(reader.data) == 0 {
		return 0, io.EOF
	}
	count := copy(buffer, reader.data)
	reader.data = reader.data[count:]
	return count, nil
}

// Package raftpeer implements authenticated Domain peer transport.
package raftpeer

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/Liapoldus/domain/contracts"
	"github.com/Liapoldus/pluginprotocol/v2/presentation/peer"
	"github.com/hashicorp/raft"
)

var (
	ErrInvalidConfig        = errors.New("invalid Domain Raft peer transport configuration")
	ErrInvalidRPC           = errors.New("invalid Domain Raft peer RPC")
	ErrSnapshotTooLarge     = errors.New("domain Raft snapshot exceeds configured limit")
	ErrAppendPipeline       = errors.New("domain Raft append pipeline is unsupported")
	ErrUnauthorizedPeer     = errors.New("domain Raft peer identity is not authorized")
	ErrTransportUnavailable = errors.New("domain Raft peer transport is unavailable")
)

const (
	defaultMaximumSnapshotSize = int64(512 << 20)
	snapshotChunkSize          = 256 << 10
	defaultInboundQueue        = 128
)

// Config binds a HashiCorp Raft endpoint to the consumer-owned generic peer API.
// PeerIdentities maps each Raft address to the exact URI SAN that must authenticate
// at that address. The protocol verifies mTLS; this adapter applies Domain's
// cluster membership policy to the authenticated identity.
type Config struct {
	Network              peer.NetworkConfig
	Security             peer.SecurityConfig
	PeerIdentities       map[raft.ServerAddress]string
	PeerServerIDs        map[raft.ServerAddress]raft.ServerID
	ProductCallers       []ProductCaller
	ProductCalls         []RegisteredCall
	PeerProxiedReads     []string
	Limits               peer.Limits
	SnapshotTimeout      time.Duration
	MaximumSnapshotBytes int64
	InboundQueueCapacity int
}

// Transport implements HashiCorp Raft's Transport interface over the existing
// generic plugin-to-plugin peer API. It adds no methods or framing to that library.
type Transport struct {
	config    Config
	contract  contracts.RaftContract
	local     raft.ServerAddress
	server    *peer.Server
	handler   peer.Handler
	inbound   chan raft.RPC
	closed    chan struct{}
	closeOnce sync.Once

	mu        sync.Mutex
	sessions  map[raft.ServerAddress]peer.Client
	heartbeat func(raft.RPC)
}

var (
	_ raft.Transport   = (*Transport)(nil)
	_ raft.WithPreVote = (*Transport)(nil)
	_ raft.WithClose   = (*Transport)(nil)
)

// Listen binds the peer endpoint and returns a Raft transport. Call Serve in a
// goroutine before starting the Raft node; the listener already owns its address.
func Listen(config Config) (*Transport, error) {
	contract := contracts.Raft()
	if err := validateConfig(config, contract); err != nil {
		return nil, err
	}
	if config.SnapshotTimeout <= 0 {
		config.SnapshotTimeout = time.Duration(contract.Methods.InstallSnapshot.TimeoutMilliseconds) * time.Millisecond
	}
	if config.MaximumSnapshotBytes == 0 {
		config.MaximumSnapshotBytes = contract.Methods.InstallSnapshot.MaxBytes
	}
	if config.InboundQueueCapacity <= 0 {
		config.InboundQueueCapacity = defaultInboundQueue
	}
	config.Limits = config.Limits.WithDefaults()
	transport := &Transport{
		config:   config,
		contract: contract,
		local:    raft.ServerAddress(config.Network.Endpoint),
		inbound:  make(chan raft.RPC, config.InboundQueueCapacity),
		closed:   make(chan struct{}),
		sessions: make(map[raft.ServerAddress]peer.Client),
	}
	handler, err := transport.newHandler()
	if err != nil {
		return nil, err
	}
	transport.handler = handler
	transport.server, err = peer.Listen(peer.ServerConfig{
		Network:  config.Network,
		Security: config.Security,
		Limits:   config.Limits,
		Handler:  handler,
	})
	if err != nil {
		return nil, err
	}
	transport.local = raft.ServerAddress(transport.server.Addr())
	return transport, nil
}

// Serve accepts authenticated peer sessions until context cancellation or Close.
func (transport *Transport) Serve(ctx context.Context) (retErr error) {
	if transport == nil || transport.server == nil {
		return ErrInvalidConfig
	}
	result := make(chan error, 1)
	go func() { result <- transport.server.Sessions(ctx) }()
	var err error
	select {
	case err = <-result:
	case <-ctx.Done():
		if closeErr := transport.server.Close(); closeErr != nil {
			return closeErr
		}
		err = <-result
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, peer.ErrClosed) {
		return nil
	}
	return err
}

// Consumer returns inbound Raft requests dispatched from authenticated peers.
func (transport *Transport) Consumer() <-chan raft.RPC { return transport.inbound }

// LocalAddr returns the endpoint bound by Listen.
func (transport *Transport) LocalAddr() raft.ServerAddress { return transport.local }

// SetHeartbeatHandler installs the optional HashiCorp fast path for AppendEntries
// requests that carry no log entries.
func (transport *Transport) SetHeartbeatHandler(handler func(raft.RPC)) {
	transport.mu.Lock()
	transport.heartbeat = handler
	transport.mu.Unlock()
}

// EncodePeer returns the UTF-8 endpoint bytes used by this adapter.
func (transport *Transport) EncodePeer(_ raft.ServerID, address raft.ServerAddress) []byte {
	return []byte(address)
}

// DecodePeer parses the endpoint bytes returned by EncodePeer.
func (transport *Transport) DecodePeer(encoded []byte) raft.ServerAddress {
	return raft.ServerAddress(string(encoded))
}

// AppendEntriesPipeline deliberately returns an error. HashiCorp Raft falls back
// to AppendEntries, preserving correctness while this generic API adapter keeps
// one request/response per peer call.
func (transport *Transport) AppendEntriesPipeline(_ raft.ServerID, _ raft.ServerAddress) (raft.AppendPipeline, error) {
	return nil, ErrAppendPipeline
}

// AppendEntries sends one bounded JSON request over a generic peer unary call.
func (transport *Transport) AppendEntries(id raft.ServerID, target raft.ServerAddress, request *raft.AppendEntriesRequest, response *raft.AppendEntriesResponse) error {
	if err := transport.validateTarget(id, target); err != nil {
		return err
	}
	return transport.call(target, transport.contract.Methods.AppendEntries.Name, request, response)
}

// RequestVote sends one vote request over a generic peer unary call.
func (transport *Transport) RequestVote(id raft.ServerID, target raft.ServerAddress, request *raft.RequestVoteRequest, response *raft.RequestVoteResponse) error {
	if err := transport.validateTarget(id, target); err != nil {
		return err
	}
	return transport.call(target, transport.contract.Methods.RequestVote.Name, request, response)
}

// RequestPreVote sends one pre-vote request over a generic peer unary call.
func (transport *Transport) RequestPreVote(id raft.ServerID, target raft.ServerAddress, request *raft.RequestPreVoteRequest, response *raft.RequestPreVoteResponse) error {
	if err := transport.validateTarget(id, target); err != nil {
		return err
	}
	return transport.call(target, transport.contract.Methods.RequestPreVote.Name, request, response)
}

// TimeoutNow sends one leadership-transfer request over a generic peer unary call.
func (transport *Transport) TimeoutNow(id raft.ServerID, target raft.ServerAddress, request *raft.TimeoutNowRequest, response *raft.TimeoutNowResponse) error {
	if err := transport.validateTarget(id, target); err != nil {
		return err
	}
	return transport.call(target, transport.contract.Methods.TimeoutNow.Name, request, response)
}

// InstallSnapshot transfers the metadata as the first stream frame and the
// snapshot as bounded opaque data frames. The caller never buffers the whole file.
func (transport *Transport) InstallSnapshot(id raft.ServerID, target raft.ServerAddress, request *raft.InstallSnapshotRequest, response *raft.InstallSnapshotResponse, snapshot io.Reader) (retErr error) {
	if request == nil || response == nil || snapshot == nil {
		return ErrInvalidRPC
	}
	if err := transport.validateTarget(id, target); err != nil {
		return err
	}
	if request.Size < 0 || request.Size > transport.config.MaximumSnapshotBytes {
		return ErrSnapshotTooLarge
	}
	ctx, cancel := context.WithTimeout(context.Background(), transport.config.SnapshotTimeout)
	defer cancel()
	session, err := transport.session(ctx, target)
	if err != nil {
		return err
	}
	stream, err := session.OpenStream(ctx, peer.Method(transport.contract.Methods.InstallSnapshot.Name))
	if err != nil {
		transport.dropSession(target, session)
		return err
	}
	defer func() { retErr = errors.Join(retErr, stream.CloseSend()) }()
	metadata, err := json.Marshal(request)
	if err != nil {
		return ErrInvalidRPC
	}
	if len(metadata) > transport.config.Limits.MaxStreamMessageBytes || len(metadata) > transport.contract.Methods.InstallSnapshot.MaxStreamMessageBytes {
		return peer.ErrMessageTooLarge
	}
	if err := stream.Send(peer.Message{Payload: metadata}); err != nil {
		transport.dropSession(target, session)
		return err
	}
	buffer := make([]byte, snapshotChunkSize)
	var sent int64
	abort := func(cause error) error {
		transport.dropSession(target, session)
		return cause
	}
	for {
		count, readErr := snapshot.Read(buffer)
		if count > 0 {
			sent += int64(count)
			if sent > transport.config.MaximumSnapshotBytes || (request.Size > 0 && sent > request.Size) {
				return abort(ErrSnapshotTooLarge)
			}
			if count > transport.contract.Methods.InstallSnapshot.MaxStreamMessageBytes {
				return abort(peer.ErrMessageTooLarge)
			}
			chunk := append([]byte(nil), buffer[:count]...)
			if err := stream.Send(peer.Message{Payload: chunk}); err != nil {
				transport.dropSession(target, session)
				return err
			}
		}
		if errors.Is(readErr, io.EOF) {
			break
		}
		if readErr != nil {
			return abort(readErr)
		}
	}
	if request.Size > 0 && sent != request.Size {
		return abort(ErrInvalidRPC)
	}
	if err := stream.CloseSend(); err != nil {
		return err
	}
	frame, err := stream.Recv()
	if err != nil {
		transport.dropSession(target, session)
		return err
	}
	if len(frame.Payload) > transport.contract.Methods.InstallSnapshot.MaxStreamMessageBytes || json.Unmarshal(frame.Payload, response) != nil {
		return ErrInvalidRPC
	}
	return nil
}

// Close closes all cached peer sessions and the listening endpoint.
func (transport *Transport) Close() error {
	if transport == nil {
		return nil
	}
	var closeErr error
	transport.closeOnce.Do(func() {
		close(transport.closed)
		transport.mu.Lock()
		for address, session := range transport.sessions {
			if err := session.Close(); err != nil && closeErr == nil {
				closeErr = err
			}
			delete(transport.sessions, address)
		}
		transport.mu.Unlock()
		if transport.server != nil {
			if err := transport.server.Close(); err != nil && closeErr == nil {
				closeErr = err
			}
		}
	})
	return closeErr
}

func (transport *Transport) call(target raft.ServerAddress, method string, request, response any) error {
	if request == nil || response == nil {
		return ErrInvalidRPC
	}
	payload, err := json.Marshal(request)
	if err != nil {
		return ErrInvalidRPC
	}
	limit, err := transport.payloadLimit(method)
	if err != nil {
		return err
	}
	if len(payload) > limit || len(payload) > transport.config.Limits.MaxMessageBytes {
		return peer.ErrMessageTooLarge
	}
	ctx, cancel := context.WithTimeout(context.Background(), transport.methodTimeout(method))
	defer cancel()
	session, err := transport.session(ctx, target)
	if err != nil {
		return err
	}
	result, err := session.Call(ctx, peer.Method(method), payload)
	if err != nil {
		transport.dropSession(target, session)
		return err
	}
	if err := json.Unmarshal(result.Payload, response); err != nil {
		return ErrInvalidRPC
	}
	if len(result.Payload) > limit {
		return peer.ErrMessageTooLarge
	}
	return nil
}

func (transport *Transport) payloadLimit(method string) (int, error) {
	for _, candidate := range []contracts.RPCMethod{transport.contract.Methods.AppendEntries, transport.contract.Methods.RequestVote, transport.contract.Methods.RequestPreVote, transport.contract.Methods.TimeoutNow} {
		if candidate.Name == method {
			return candidate.MaxPayloadBytes, nil
		}
	}
	return 0, ErrInvalidRPC
}

// ForwardRead re-sends one already-validated domain read call to the raft
// leader over the peer transport. It is the client half of the follower read
// proxy: the method must be listed in PeerProxiedReads and the target must
// be a known peer identity, so this can never become an open relay. Unlike
// call() it runs under the caller's context, keeping the forward inside the
// dispatcher's per-method timeout budget.
func (transport *Transport) ForwardRead(ctx context.Context, leaderAddress, method string, payload []byte) ([]byte, error) {
	if transport == nil || leaderAddress == "" {
		return nil, ErrTransportUnavailable
	}
	proxied := false
	for _, candidate := range transport.config.PeerProxiedReads {
		if candidate == method {
			proxied = true
			break
		}
	}
	if !proxied {
		return nil, ErrInvalidRPC
	}
	if len(payload) > transport.config.Limits.MaxMessageBytes {
		return nil, peer.ErrMessageTooLarge
	}
	target := raft.ServerAddress(leaderAddress)
	session, err := transport.session(ctx, target)
	if err != nil {
		return nil, err
	}
	result, err := session.Call(ctx, peer.Method(method), payload)
	if err != nil {
		transport.dropSession(target, session)
		return nil, err
	}
	if len(result.Payload) > transport.config.Limits.MaxMessageBytes {
		return nil, peer.ErrMessageTooLarge
	}
	return result.Payload, nil
}

func (transport *Transport) methodTimeout(method string) time.Duration {
	for _, candidate := range []contracts.RPCMethod{transport.contract.Methods.AppendEntries, transport.contract.Methods.RequestVote, transport.contract.Methods.RequestPreVote, transport.contract.Methods.TimeoutNow} {
		if candidate.Name == method && candidate.TimeoutMilliseconds > 0 {
			return time.Duration(candidate.TimeoutMilliseconds) * time.Millisecond
		}
	}
	return 10 * time.Second
}

func (transport *Transport) session(ctx context.Context, target raft.ServerAddress) (peer.Client, error) {
	transport.mu.Lock()
	defer transport.mu.Unlock()
	if transport.sessions == nil || transport.server == nil {
		return nil, ErrTransportUnavailable
	}
	select {
	case <-transport.closed:
		return nil, ErrTransportUnavailable
	default:
	}
	if session := transport.sessions[target]; session != nil {
		return session, nil
	}
	identity := transport.config.PeerIdentities[target]
	if identity == "" {
		return nil, ErrUnauthorizedPeer
	}
	network := transport.config.Network
	network.Endpoint = string(target)
	security := transport.config.Security
	security.PeerIdentity = identity
	session, err := peer.Dial(ctx, peer.ClientConfig{
		Network: network, Security: security, Limits: transport.config.Limits, Handler: transport.handler,
	})
	if err != nil {
		return nil, err
	}
	transport.sessions[target] = session
	return session, nil
}

func (transport *Transport) validateTarget(id raft.ServerID, target raft.ServerAddress) error {
	if transport == nil || target == "" || id == "" || transport.config.PeerServerIDs[target] != id {
		return ErrUnauthorizedPeer
	}
	return nil
}

func (transport *Transport) dropSession(target raft.ServerAddress, expected peer.Client) {
	transport.mu.Lock()
	defer transport.mu.Unlock()
	if transport.sessions[target] != expected {
		return
	}
	delete(transport.sessions, target)
	if err := expected.Close(); err != nil {
		return // session has been evicted; callers already receive the primary failure
	}
}

func (transport *Transport) newHandler() (peer.Handler, error) {
	allowed := make(map[string]struct{}, len(transport.config.PeerIdentities))
	for _, identity := range transport.config.PeerIdentities {
		allowed[identity] = struct{}{}
	}
	authorizer := productAwareAuthorizer{
		allowed:        allowed,
		product:        make(map[string]struct{}, len(transport.config.ProductCallers)),
		productMethods: make(map[string]struct{}, len(transport.config.ProductCalls)),
		peers:          make(map[string]struct{}, len(transport.config.PeerIdentities)),
		proxiedReads:   make(map[string]struct{}, len(transport.config.PeerProxiedReads)),
	}
	for _, caller := range transport.config.ProductCallers {
		authorizer.product[caller.Identity] = struct{}{}
	}
	for _, productCall := range transport.config.ProductCalls {
		authorizer.productMethods[productCall.Method] = struct{}{}
	}
	for _, identity := range transport.config.PeerIdentities {
		authorizer.peers[identity] = struct{}{}
	}
	for _, method := range transport.config.PeerProxiedReads {
		authorizer.proxiedReads[method] = struct{}{}
	}
	registry := peer.NewRegistry().WithAuthorizer(authorizer)
	registry.RegisterCall(transport.contract.Methods.AppendEntries.Name, transport.serveCall)
	registry.RegisterCall(transport.contract.Methods.RequestVote.Name, transport.serveCall)
	registry.RegisterCall(transport.contract.Methods.RequestPreVote.Name, transport.serveCall)
	registry.RegisterCall(transport.contract.Methods.TimeoutNow.Name, transport.serveCall)
	registry.RegisterStream(transport.contract.Methods.InstallSnapshot.Name, transport.serveSnapshot)
	for _, productCall := range transport.config.ProductCalls {
		registry.RegisterCall(productCall.Method, productCall.Handler)
	}
	return registry.Build()
}

func (transport *Transport) serveCall(ctx context.Context, call peer.Call) (peer.Result, error) {
	limit, err := transport.payloadLimit(string(call.Method))
	if err != nil {
		return peer.Result{}, err
	}
	if len(call.Payload) > limit {
		return peer.Result{}, peer.ErrMessageTooLarge
	}
	var command any
	switch call.Method {
	case peer.Method(transport.contract.Methods.AppendEntries.Name):
		request := new(raft.AppendEntriesRequest)
		if err := json.Unmarshal(call.Payload, request); err != nil {
			return peer.Result{}, ErrInvalidRPC
		}
		command = request
	case peer.Method(transport.contract.Methods.RequestVote.Name):
		request := new(raft.RequestVoteRequest)
		if err := json.Unmarshal(call.Payload, request); err != nil {
			return peer.Result{}, ErrInvalidRPC
		}
		command = request
	case peer.Method(transport.contract.Methods.RequestPreVote.Name):
		request := new(raft.RequestPreVoteRequest)
		if err := json.Unmarshal(call.Payload, request); err != nil {
			return peer.Result{}, ErrInvalidRPC
		}
		command = request
	case peer.Method(transport.contract.Methods.TimeoutNow.Name):
		request := new(raft.TimeoutNowRequest)
		if err := json.Unmarshal(call.Payload, request); err != nil {
			return peer.Result{}, ErrInvalidRPC
		}
		command = request
	default:
		return peer.Result{}, ErrInvalidRPC
	}
	if err := transport.validateCaller(call.From, command); err != nil {
		return peer.Result{}, err
	}
	responseChannel := make(chan raft.RPCResponse, 1)
	rpc := raft.RPC{Command: command, RespChan: responseChannel}
	if request, ok := command.(*raft.AppendEntriesRequest); ok && isHeartbeat(request) {
		transport.mu.Lock()
		heartbeat := transport.heartbeat
		transport.mu.Unlock()
		if heartbeat != nil {
			heartbeat(rpc)
		} else if err := transport.enqueue(ctx, rpc); err != nil {
			return peer.Result{}, err
		}
	} else if err := transport.enqueue(ctx, rpc); err != nil {
		return peer.Result{}, err
	}
	select {
	case result := <-responseChannel:
		if result.Error != nil {
			return peer.Result{}, result.Error
		}
		if result.Response == nil {
			return peer.Result{}, ErrInvalidRPC
		}
		encoded, err := json.Marshal(result.Response)
		if err != nil {
			return peer.Result{}, ErrInvalidRPC
		}
		if len(encoded) > limit {
			return peer.Result{}, peer.ErrMessageTooLarge
		}
		return peer.Result{Payload: encoded}, nil
	case <-ctx.Done():
		return peer.Result{}, ctx.Err()
	case <-transport.closed:
		return peer.Result{}, ErrTransportUnavailable
	}
}

func (transport *Transport) serveSnapshot(stream peer.Stream) (retErr error) {
	first, err := stream.Recv()
	if err != nil {
		return ErrInvalidRPC
	}
	if len(first.Payload) > transport.config.Limits.MaxStreamMessageBytes || len(first.Payload) > transport.contract.Methods.InstallSnapshot.MaxStreamMessageBytes {
		return peer.ErrMessageTooLarge
	}
	request := new(raft.InstallSnapshotRequest)
	if err := json.Unmarshal(first.Payload, request); err != nil || request.Size < 0 {
		return ErrInvalidRPC
	}
	if request.Size > transport.config.MaximumSnapshotBytes {
		return ErrSnapshotTooLarge
	}
	if err := transport.validateCaller(stream.Peer(), request); err != nil {
		return err
	}
	reader, writer := io.Pipe()
	defer func() { retErr = errors.Join(retErr, reader.Close()) }()
	defer func() { retErr = errors.Join(retErr, writer.CloseWithError(ErrTransportUnavailable)) }()
	responseChannel := make(chan raft.RPCResponse, 1)
	rpc := raft.RPC{Command: request, Reader: reader, RespChan: responseChannel}
	if err := transport.enqueue(stream.Context(), rpc); err != nil {
		if closeErr := reader.Close(); closeErr != nil {
			return closeErr
		}
		if closeErr := writer.Close(); closeErr != nil {
			return closeErr
		}
		return err
	}
	var received int64
	for {
		frame, recvErr := stream.Recv()
		if errors.Is(recvErr, io.EOF) {
			break
		}
		if recvErr != nil {
			if closeErr := writer.CloseWithError(recvErr); closeErr != nil {
				return closeErr
			}
			if closeErr := reader.CloseWithError(recvErr); closeErr != nil {
				return closeErr
			}
			return recvErr
		}
		if len(frame.Payload) > transport.contract.Methods.InstallSnapshot.MaxStreamMessageBytes {
			if closeErr := writer.CloseWithError(peer.ErrMessageTooLarge); closeErr != nil {
				return closeErr
			}
			if closeErr := reader.CloseWithError(peer.ErrMessageTooLarge); closeErr != nil {
				return closeErr
			}
			return peer.ErrMessageTooLarge
		}
		received += int64(len(frame.Payload))
		if received > transport.config.MaximumSnapshotBytes || (request.Size > 0 && received > request.Size) {
			if closeErr := writer.CloseWithError(ErrSnapshotTooLarge); closeErr != nil {
				return closeErr
			}
			if closeErr := reader.CloseWithError(ErrSnapshotTooLarge); closeErr != nil {
				return closeErr
			}
			return ErrSnapshotTooLarge
		}
		if _, err := writer.Write(frame.Payload); err != nil {
			if closeErr := reader.CloseWithError(err); closeErr != nil {
				return closeErr
			}
			return err
		}
	}
	if request.Size > 0 && received != request.Size {
		if closeErr := writer.CloseWithError(ErrInvalidRPC); closeErr != nil {
			return closeErr
		}
		if closeErr := reader.CloseWithError(ErrInvalidRPC); closeErr != nil {
			return closeErr
		}
		return ErrInvalidRPC
	}
	if closeErr := writer.Close(); closeErr != nil {
		return closeErr
	}
	select {
	case result := <-responseChannel:
		if result.Error != nil {
			return result.Error
		}
		response, ok := result.Response.(*raft.InstallSnapshotResponse)
		if !ok || response == nil {
			return ErrInvalidRPC
		}
		encoded, err := json.Marshal(response)
		if err != nil {
			return ErrInvalidRPC
		}
		if err := stream.Send(peer.Message{Payload: encoded}); err != nil {
			return err
		}
		return stream.CloseSend()
	case <-stream.Context().Done():
		return stream.Context().Err()
	case <-transport.closed:
		return ErrTransportUnavailable
	}
}

func (transport *Transport) enqueue(ctx context.Context, rpc raft.RPC) error {
	select {
	case transport.inbound <- rpc:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	case <-transport.closed:
		return ErrTransportUnavailable
	}
}

func (transport *Transport) validateCaller(identity peer.PeerIdentity, command any) error {
	var header raft.RPCHeader
	switch request := command.(type) {
	case *raft.AppendEntriesRequest:
		header = request.RPCHeader
	case *raft.RequestVoteRequest:
		header = request.RPCHeader
	case *raft.RequestPreVoteRequest:
		header = request.RPCHeader
	case *raft.TimeoutNowRequest:
		header = request.RPCHeader
	case *raft.InstallSnapshotRequest:
		header = request.RPCHeader
	default:
		return ErrInvalidRPC
	}
	for address, expectedIdentity := range transport.config.PeerIdentities {
		if identity.URI != expectedIdentity {
			continue
		}
		if string(header.ID) != string(transport.config.PeerServerIDs[address]) || string(header.Addr) != string(address) {
			return ErrUnauthorizedPeer
		}
		return nil
	}
	return ErrUnauthorizedPeer
}

func isHeartbeat(request *raft.AppendEntriesRequest) bool {
	if request == nil {
		return false
	}
	leaderAddress := request.Addr
	return request.Term != 0 && len(leaderAddress) != 0 && request.PrevLogEntry == 0 && request.PrevLogTerm == 0 && len(request.Entries) == 0 && request.LeaderCommitIndex == 0
}

func validateConfig(config Config, contract contracts.RaftContract) error {
	if config.Network.Endpoint == "" || config.Network.Carrier == "" || config.Security.PlaintextLoopback || config.Security.Identity == "" || len(config.Security.Certificate.Certificate) == 0 || config.Security.Roots == nil || config.Security.PeerIdentity != "" {
		return ErrInvalidConfig
	}
	if config.MaximumSnapshotBytes < 0 || config.MaximumSnapshotBytes > contract.Methods.InstallSnapshot.MaxBytes || config.SnapshotTimeout < 0 {
		return ErrInvalidConfig
	}
	if len(config.PeerIdentities) == 0 || len(config.PeerServerIDs) != len(config.PeerIdentities) {
		return ErrInvalidConfig
	}
	allowed := make(map[string]struct{}, len(config.PeerIdentities))
	serverIDs := make(map[raft.ServerID]struct{}, len(config.PeerServerIDs))
	for address, identity := range config.PeerIdentities {
		serverID := config.PeerServerIDs[address]
		if address == "" || identity == "" || serverID == "" {
			return ErrInvalidConfig
		}
		if _, exists := allowed[identity]; exists {
			return ErrInvalidConfig
		}
		if _, exists := serverIDs[serverID]; exists {
			return ErrInvalidConfig
		}
		allowed[identity] = struct{}{}
		serverIDs[serverID] = struct{}{}
	}
	limits := config.Limits.WithDefaults()
	maximumCallPayload := 0
	for _, method := range []contracts.RPCMethod{contract.Methods.AppendEntries, contract.Methods.RequestVote, contract.Methods.RequestPreVote, contract.Methods.TimeoutNow} {
		if method.MaxPayloadBytes > maximumCallPayload {
			maximumCallPayload = method.MaxPayloadBytes
		}
	}
	if limits.MaxMessageBytes < maximumCallPayload || limits.MaxStreamMessageBytes < snapshotChunkSize {
		return ErrInvalidConfig
	}
	if contract.Methods.AppendEntries.Mode != "call" || contract.Methods.InstallSnapshot.Mode != "stream" || contract.Methods.InstallSnapshot.MaxBytes <= 0 || contract.Methods.InstallSnapshot.MaxBytes > defaultMaximumSnapshotSize || contract.Methods.InstallSnapshot.MaxStreamMessageBytes < snapshotChunkSize || contract.Methods.InstallSnapshot.ChunkBytes != snapshotChunkSize || contract.Methods.InstallSnapshot.TimeoutMilliseconds <= 0 || contract.AppendEntriesPipeline == "" {
		return ErrInvalidConfig
	}
	raftMethodNames := map[string]struct{}{
		contract.Methods.AppendEntries.Name:   {},
		contract.Methods.RequestVote.Name:     {},
		contract.Methods.RequestPreVote.Name:  {},
		contract.Methods.TimeoutNow.Name:      {},
		contract.Methods.InstallSnapshot.Name: {},
	}
	seenCallers := make(map[string]struct{}, len(config.ProductCallers))
	for _, caller := range config.ProductCallers {
		if caller.Identity == "" || caller.Tenant == "" || caller.Site == "" || caller.Group == "" {
			return ErrInvalidConfig
		}
		if _, exists := seenCallers[caller.Identity]; exists {
			return ErrInvalidConfig
		}
		seenCallers[caller.Identity] = struct{}{}
	}
	seenCalls := make(map[string]struct{}, len(config.ProductCalls))
	for _, productCall := range config.ProductCalls {
		if productCall.Method == "" || productCall.Handler == nil {
			return ErrInvalidConfig
		}
		if _, raftMethod := raftMethodNames[productCall.Method]; raftMethod {
			return ErrInvalidConfig
		}
		if _, exists := seenCalls[productCall.Method]; exists {
			return ErrInvalidConfig
		}
		seenCalls[productCall.Method] = struct{}{}
	}
	seenProxied := make(map[string]struct{}, len(config.PeerProxiedReads))
	for _, method := range config.PeerProxiedReads {
		if method == "" {
			return ErrInvalidConfig
		}
		if _, raftMethod := raftMethodNames[method]; raftMethod {
			return ErrInvalidConfig
		}
		if _, registered := seenCalls[method]; !registered {
			return ErrInvalidConfig
		}
		if _, exists := seenProxied[method]; exists {
			return ErrInvalidConfig
		}
		seenProxied[method] = struct{}{}
	}
	return nil
}

type identityAuthorizer struct{ allowed map[string]struct{} }

func (authorizer identityAuthorizer) AuthorizeCall(identity peer.PeerIdentity, _ peer.Method) error {
	if _, ok := authorizer.allowed[identity.URI]; !ok {
		return ErrUnauthorizedPeer
	}
	return nil
}

func (authorizer identityAuthorizer) AuthorizeStream(identity peer.PeerIdentity, _ peer.Method) error {
	if _, ok := authorizer.allowed[identity.URI]; !ok {
		return ErrUnauthorizedPeer
	}
	return nil
}

func (transport *Transport) String() string {
	return fmt.Sprintf("domain-raft-peer(%s)", transport.local)
}

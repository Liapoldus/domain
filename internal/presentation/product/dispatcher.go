// Package product implements the domain v1 product peer presentation layer:
// identity-to-scope resolution, contract schema validation of request
// payloads, per-method payload/timeout limits and the response envelope.
// It never touches raft, sqlite or the peer transport.
package product

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/Liapoldus/domain/internal/application/call"
	"github.com/Liapoldus/domain/internal/application/migration"
	"github.com/Liapoldus/domain/internal/application/row"
	"github.com/Liapoldus/domain/internal/domain/models"
)

// Payload bounds per method, mirroring contracts/v1/plugin.json.
const (
	defaultMaxPayloadBytes = 1 << 20
	batchMaxPayloadBytes   = 2 << 20

	// forwardedScopeClaimMaxBytes bounds the reserved claim a cluster peer
	// presents on a forwarded read before its contents are decoded anywhere.
	// A schema-legal claim is three ≤128-byte fields; fully \u-escaped that
	// encodes to under 2.4 KiB, so 4 KiB caps every claim decode while never
	// rejecting a contract-legal one.
	forwardedScopeClaimMaxBytes = 4 << 10
)

const (
	defaultWriteTimeout = 15000 * time.Millisecond
	defaultReadTimeout  = 10000 * time.Millisecond
	defaultBatchTimeout = 30000 * time.Millisecond
	statusTimeout       = 3000 * time.Millisecond
)

// Envelope is the domain v1 response envelope shape.
type envelope struct {
	OK    bool            `json:"ok"`
	Data  json.RawMessage `json:"data,omitempty"`
	Error *envelopeError  `json:"error,omitempty"`
}

type envelopeError struct {
	Code           string `json:"code"`
	Retryable      bool   `json:"retryable"`
	UnknownOutcome bool   `json:"unknownOutcome"`
	Message        string `json:"message"`
}

type callOutcome struct {
	data       json.RawMessage
	productErr *models.ProductError
}

type methodSpec struct {
	maxPayloadBytes int
	timeout         time.Duration
	call            func(context.Context, *row.Service, models.Scope, json.RawMessage) callOutcome
}

// Handler resolves product peer calls to the application service.
type Handler struct {
	service   *row.Service
	migration *migration.Service
	scopes    map[string]models.Scope
	peers     map[string]struct{}
	forwarder ReadForwarder
	schemas   *schemaRegistry
	methods   map[string]methodSpec
}

// NewHandler wires the product methods to one service for the given
// identity-to-scope map. An identity absent from the map is rejected before
// any method runs (the peer authorizer is the enforcement point at transport
// level; this handler is defensive for in-process callers). It also compiles
// the embedded domain v1 contract schemas once; a malformed contract asset
// panics here rather than failing per request.
func NewHandler(service *row.Service, scopes map[string]models.Scope) *Handler {
	handler := &Handler{
		service: service,
		scopes:  scopes,
		schemas: newSchemaRegistry(),
		methods: make(map[string]methodSpec, 9),
	}
	handler.methods["domain.create"] = methodSpec{defaultMaxPayloadBytes, defaultWriteTimeout, callCreate}
	handler.methods["domain.update"] = methodSpec{defaultMaxPayloadBytes, defaultWriteTimeout, callUpdate}
	handler.methods["domain.delete"] = methodSpec{defaultMaxPayloadBytes, defaultWriteTimeout, callDelete}
	handler.methods["domain.batch"] = methodSpec{batchMaxPayloadBytes, defaultBatchTimeout, callBatch}
	handler.methods["domain.get"] = methodSpec{defaultMaxPayloadBytes, defaultReadTimeout, callGet}
	handler.methods["domain.query"] = methodSpec{defaultMaxPayloadBytes, defaultReadTimeout, callQuery}
	handler.methods["domain.cluster.status"] = methodSpec{defaultMaxPayloadBytes, statusTimeout, callStatus}
	handler.registerMigrationMethods()
	return handler
}

// Handle serves one unary product call. The identity is the authenticated URI
// SAN of the calling peer; payload is the strict-decode request document. It
// returns the response envelope bytes on success and a protocol-level error
// for an unknown method or unknown identity. Business failures are always
// inside the envelope. A not_leader read from a proxied peer method is
// re-sent to the current leader when a forwarder is attached.
func (handler *Handler) Handle(ctx context.Context, method, identity string, payload []byte) ([]byte, error) {
	if ctx == nil {
		return nil, errors.New("nil product call context")
	}
	spec, ok := handler.methods[method]
	if !ok {
		return nil, errors.New("unknown domain product method")
	}
	if productErr := handler.payloadSizeError(method, identity, payload); productErr != nil {
		return handler.marshal(handler.envelopeFor(callOutcome{productErr: productErr}))
	}
	if productErr := handler.schemas.validateRequest(method, payload); productErr != nil {
		return handler.marshal(handler.envelopeFor(callOutcome{productErr: productErr}))
	}
	scope, productErr, err := handler.resolveScope(method, identity, payload)
	if err != nil {
		return nil, err
	}
	if productErr != nil {
		return handler.marshal(handler.envelopeFor(callOutcome{productErr: productErr}))
	}
	bounded, cancel := context.WithTimeout(ctx, spec.timeout)
	defer cancel()
	_, hasClaim := forwardedScopeClaim(payload)
	resulted := make(chan callOutcome, 1)
	go func() {
		outcome := spec.call(bounded, handler.service, scope, payload)
		_, proxied := proxiedReads[method]
		if proxied && handler.forwarder != nil && !hasClaim &&
			outcome.productErr != nil && outcome.productErr.Code == call.CodeNotLeader {
			outcome = handler.forwardRead(bounded, method, scope, payload, outcome.productErr)
		}
		resulted <- outcome
	}()
	select {
	case <-bounded.Done():
		return handler.marshal(envelope{OK: false, Error: &envelopeError{
			Code:           call.CodeUnknownOutcome,
			Retryable:      true,
			UnknownOutcome: true,
			Message:        "the method deadline expired before a quorum outcome was confirmed; retry with the same writeId",
		}})
	case outcome := <-resulted:
		return handler.marshal(handler.envelopeFor(outcome))
	}
}

// payloadSizeMessage is the only size-limit rejection; it names the limit
// and never echoes the submitted bytes. forwardedScopeClaimSizeMessage
// rejects an oversized reserved claim before its contents are decoded.
const (
	payloadSizeMessage             = "payload exceeds the method size limit"
	forwardedScopeClaimSizeMessage = "the forwardedScope claim exceeds the claim size limit"
)

// payloadSizeError enforces the per-method payload limit against the bytes
// the caller actually submitted. Every call keeps the plain byte-length
// check unchanged, with one exception: a cluster peer's forwarded proxied
// read, whose document is the client payload plus the claim the entry node
// injects. For that call the limit is measured on the claim-stripped
// document — the client-submitted bytes — so the same request gets the same
// verdict on both paths, while the claim itself is bounded separately and a
// document more than one maximum claim over the limit is refused before it
// is parsed at all. Run before schema validation and resolveScope, it is
// also what keeps the claim decode in resolveScope bounded.
func (handler *Handler) payloadSizeError(method, identity string, payload []byte) *models.ProductError {
	spec := handler.methods[method]
	_, isPeer := handler.peers[identity]
	_, proxied := proxiedReads[method]
	if !isPeer || !proxied {
		if len(payload) > spec.maxPayloadBytes {
			return invalidRequest(payloadSizeMessage)
		}
		return nil
	}
	if len(payload) > spec.maxPayloadBytes+forwardedScopeClaimMaxBytes {
		return invalidRequest(payloadSizeMessage)
	}
	claim, hasClaim := forwardedScopeClaim(payload)
	if !hasClaim {
		if len(payload) > spec.maxPayloadBytes {
			return invalidRequest(payloadSizeMessage)
		}
		return nil
	}
	if len(claim) > forwardedScopeClaimMaxBytes {
		return invalidRequest(forwardedScopeClaimSizeMessage)
	}
	if len(payload) <= spec.maxPayloadBytes {
		return nil
	}
	stripped, ok := withoutForwardedScope(payload)
	if !ok || len(stripped) > spec.maxPayloadBytes {
		return invalidRequest(payloadSizeMessage)
	}
	return nil
}

// forwardedScopeReservedMessage is the only claim-rejection message. It names
// the reserved field and never echoes submitted scope values.
const forwardedScopeReservedMessage = "forwardedScope is reserved for cluster peer read proxying"

// resolveScope maps the authenticated identity onto the trusted scope of the
// call. Product callers resolve through handler.scopes. A cluster peer may
// only resolve through the reserved forwardedScope claim, and only on a
// proxied read; without it, or on any other method, it maps to no scope. A
// claim presented by anyone else, or an incomplete claim, is a business
// invalid_request; an identity that maps to no scope stays a protocol error.
func (handler *Handler) resolveScope(method, identity string, payload []byte) (models.Scope, *models.ProductError, error) {
	claim, hasClaim := forwardedScopeClaim(payload)
	if _, isPeer := handler.peers[identity]; isPeer {
		_, proxied := proxiedReads[method]
		if !proxied || !hasClaim {
			return models.Scope{}, nil, errors.New("caller identity is not mapped to a domain scope")
		}
		var scope models.Scope
		if err := json.Unmarshal(claim, &scope); err != nil ||
			scope.Tenant == "" || scope.Site == "" || scope.Group == "" {
			return models.Scope{}, invalidRequest(forwardedScopeReservedMessage), nil
		}
		return scope, nil, nil
	}
	scoped, ok := handler.scopes[identity]
	if !ok {
		return models.Scope{}, nil, errors.New("caller identity is not mapped to a domain scope")
	}
	if hasClaim {
		return models.Scope{}, invalidRequest(forwardedScopeReservedMessage), nil
	}
	return scoped, nil, nil
}

func (handler *Handler) envelopeFor(outcome callOutcome) envelope {
	if outcome.productErr != nil {
		return envelope{OK: false, Error: &envelopeError{
			Code:           outcome.productErr.Code,
			Retryable:      outcome.productErr.Retryable,
			UnknownOutcome: outcome.productErr.UnknownOutcome,
			Message:        outcome.productErr.Message,
		}}
	}
	if len(outcome.data) == 0 {
		return envelope{OK: false, Error: &envelopeError{
			Code:    call.CodeInternal,
			Message: "the method returned no response data",
		}}
	}
	return envelope{OK: true, Data: outcome.data}
}

func (handler *Handler) marshal(value envelope) ([]byte, error) {
	return json.Marshal(value)
}

func callCreate(ctx context.Context, service *row.Service, scope models.Scope, payload json.RawMessage) callOutcome {
	request := new(writeRequest)
	if err := decodeStrict(payload, request); err != nil {
		return callOutcome{productErr: err}
	}
	data, productErr := service.Create(ctx, scope, request.Entity, request.ID, request.WriteID, request.Row, request.Epoch)
	return callOutcome{data: data, productErr: productErr}
}

func callUpdate(ctx context.Context, service *row.Service, scope models.Scope, payload json.RawMessage) callOutcome {
	request := new(writeRequest)
	if err := decodeStrict(payload, request); err != nil {
		return callOutcome{productErr: err}
	}
	data, productErr := service.Update(ctx, scope, request.Entity, request.ID, request.WriteID, request.Row, request.Epoch)
	return callOutcome{data: data, productErr: productErr}
}

func callDelete(ctx context.Context, service *row.Service, scope models.Scope, payload json.RawMessage) callOutcome {
	request := new(deleteRequest)
	if err := decodeStrict(payload, request); err != nil {
		return callOutcome{productErr: err}
	}
	data, productErr := service.Delete(ctx, scope, request.Entity, request.ID, request.WriteID, request.Epoch)
	return callOutcome{data: data, productErr: productErr}
}

func callBatch(ctx context.Context, service *row.Service, scope models.Scope, payload json.RawMessage) callOutcome {
	request := new(batchRequest)
	if err := decodeStrict(payload, request); err != nil {
		return callOutcome{productErr: err}
	}
	ops := make([]row.BatchOp, 0, len(request.Ops))
	for _, operation := range request.Ops {
		ops = append(ops, row.BatchOp{
			Op:     operation.Op,
			Entity: operation.Entity,
			ID:     operation.ID,
			Row:    operation.Row,
		})
	}
	data, productErr := service.Batch(ctx, scope, request.WriteID, ops, request.Epoch)
	return callOutcome{data: data, productErr: productErr}
}

func callGet(ctx context.Context, service *row.Service, scope models.Scope, payload json.RawMessage) callOutcome {
	request := new(idRequest)
	if err := decodeStrict(payload, request); err != nil {
		return callOutcome{productErr: err}
	}
	data, productErr := service.Get(ctx, scope, request.Entity, request.ID, request.Epoch)
	return callOutcome{data: data, productErr: productErr}
}

func callQuery(ctx context.Context, service *row.Service, scope models.Scope, payload json.RawMessage) callOutcome {
	request := new(queryRequest)
	if err := decodeStrict(payload, request); err != nil {
		return callOutcome{productErr: err}
	}
	data, productErr := service.Query(ctx, scope, request.SQL, request.Params, int(request.MaxRows), request.Epoch)
	return callOutcome{data: data, productErr: productErr}
}

func callStatus(ctx context.Context, service *row.Service, scope models.Scope, payload json.RawMessage) callOutcome {
	request := new(statusRequest)
	if err := decodeStrict(payload, request); err != nil {
		return callOutcome{productErr: err}
	}
	data, productErr := service.Status(ctx)
	return callOutcome{data: data, productErr: productErr}
}

func invalidRequest(message string) *models.ProductError {
	return &models.ProductError{Code: call.CodeInvalidRequest, Message: message}
}

package product

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/Liapoldus/domain/internal/application/call"
	"github.com/Liapoldus/domain/internal/domain/models"
)

// ReadForwarder re-sends one already-validated domain read call to the
// current raft leader over the plugin peer transport. The interface lives in
// the presentation layer and is satisfied structurally by the raft peer
// transport; the presentation layer never imports the transport.
type ReadForwarder interface {
	ForwardRead(ctx context.Context, leaderAddress, method string, payload []byte) ([]byte, error)
}

// proxiedReads lists the product methods a cluster peer may forward to the
// leader. Writes, batch and migration methods stay local and keep answering
// not_leader with the leader address.
var proxiedReads = map[string]struct{}{
	"domain.get":   {},
	"domain.query": {},
}

// WithReadForwarder attaches the leader forwarder used for peer read
// proxying. Handlers built without it answer not_leader exactly as before.
func (handler *Handler) WithReadForwarder(forwarder ReadForwarder) *Handler {
	handler.forwarder = forwarder
	return handler
}

// WithPeerIdentities registers the cluster peer identities allowed to
// present the reserved forwardedScope claim on a proxied read. Product
// callers stay in handler.scopes; the two sets are disjoint in production.
func (handler *Handler) WithPeerIdentities(identities []string) *Handler {
	handler.peers = make(map[string]struct{}, len(identities))
	for _, identity := range identities {
		handler.peers[identity] = struct{}{}
	}
	return handler
}

// forwardedScopeClaim returns the reserved claim document carried by a read
// payload and whether the payload carries it at all. Payloads reaching it
// are schema-validated JSON objects.
func forwardedScopeClaim(payload []byte) (json.RawMessage, bool) {
	var document map[string]json.RawMessage
	if err := json.Unmarshal(payload, &document); err != nil {
		return nil, false
	}
	claim, ok := document["forwardedScope"]
	return claim, ok
}

// withForwardedScope injects the caller's trusted scope into a read document
// as the reserved forwardedScope claim, preserving every other field verbatim.
func withForwardedScope(payload []byte, scope models.Scope) ([]byte, error) {
	var document map[string]json.RawMessage
	if err := json.Unmarshal(payload, &document); err != nil {
		return nil, err
	}
	claim, err := json.Marshal(map[string]string{
		"tenant": scope.Tenant,
		"site":   scope.Site,
		"group":  scope.Group,
	})
	if err != nil {
		return nil, err
	}
	document["forwardedScope"] = claim
	return json.Marshal(document)
}

// withoutForwardedScope strips the reserved claim from a forwarded read
// document and returns the remaining client-submitted bytes, which are what
// the leader's method size limit must be measured against. ok is false when
// the payload carries no claim or cannot be rebuilt.
func withoutForwardedScope(payload []byte) ([]byte, bool) {
	var document map[string]json.RawMessage
	if err := json.Unmarshal(payload, &document); err != nil {
		return nil, false
	}
	if _, ok := document["forwardedScope"]; !ok {
		return nil, false
	}
	delete(document, "forwardedScope")
	stripped, err := json.Marshal(document)
	if err != nil {
		return nil, false
	}
	return stripped, true
}

// forwardRead re-sends one not_leader read to the current leader with the
// caller's trusted scope in the reserved claim, and classifies the outcome.
// It forwards exactly once: the leader's own not_leader answer comes back as
// a single rebuilt refusal, never as a second forward. An empty leader
// address passes the local refusal through untouched; a transport failure is
// unavailable; a deadline that expires during the forward is unknown_outcome.
func (handler *Handler) forwardRead(ctx context.Context, method string, scope models.Scope, payload []byte, refusal *models.ProductError) callOutcome {
	address := handler.service.LeaderAddress()
	if address == "" {
		return callOutcome{productErr: refusal}
	}
	forwarded, err := withForwardedScope(payload, scope)
	if err != nil {
		return callOutcome{productErr: &models.ProductError{
			Code:    call.CodeInternal,
			Message: "the proxied read payload could not be rebuilt",
		}}
	}
	response, err := handler.forwarder.ForwardRead(ctx, address, method, forwarded)
	if err != nil {
		if ctx.Err() != nil {
			return callOutcome{productErr: &models.ProductError{
				Code:           call.CodeUnknownOutcome,
				Retryable:      true,
				UnknownOutcome: true,
				Message:        "the proxied read deadline expired before the leader outcome was confirmed",
			}}
		}
		return callOutcome{productErr: &models.ProductError{
			Code:      call.CodeUnavailable,
			Retryable: true,
			Message:   "the leader peer read could not be reached",
		}}
	}
	var leader envelope
	if err := json.Unmarshal(response, &leader); err != nil {
		return callOutcome{productErr: &models.ProductError{
			Code:    call.CodeInternal,
			Message: "the leader peer read returned a malformed response",
		}}
	}
	if leader.OK {
		return callOutcome{data: leader.Data}
	}
	if leader.Error != nil && leader.Error.Code == call.CodeNotLeader {
		return callOutcome{productErr: &models.ProductError{
			Code:      call.CodeNotLeader,
			Retryable: true,
			Message:   fmt.Sprintf("the proxied leader refused the read; leader: %s", address),
		}}
	}
	if leader.Error != nil {
		return callOutcome{productErr: &models.ProductError{
			Code:           leader.Error.Code,
			Retryable:      leader.Error.Retryable,
			UnknownOutcome: leader.Error.UnknownOutcome,
			Message:        leader.Error.Message,
		}}
	}
	return callOutcome{productErr: &models.ProductError{
		Code:    call.CodeInternal,
		Message: "the leader peer read returned no outcome",
	}}
}

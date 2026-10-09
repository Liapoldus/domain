package raftpeer

import (
	"context"

	"github.com/Liapoldus/pluginprotocol/v2/presentation/peer"
)

// ProductCaller maps one authenticated product-caller URI SAN to the Domain
// scope its calls may access. On the wire the identity still authenticates via
// the peer security profile; this map is the Domain product-authorization
// policy evaluated by the handler authorizer.
type ProductCaller struct {
	Identity string
	Tenant   string
	Site     string
	Group    string
}

// RegisteredCall binds one domain v1 product method name to its unary handler.
// The handler must return business failures inside the response envelope; a
// non-nil error is reserved for protocol-level failures.
type RegisteredCall struct {
	Method  string
	Handler func(context.Context, peer.Call) (peer.Result, error)
}

// productAwareAuthorizer authorizes raft RPC methods with the existing raft
// identity policy and domain.* product methods against the product caller map.
// Proxied read methods additionally accept the cluster peer identities, which
// is what lets a follower's read forward reach the leader's transport; the
// forwardedScope claim itself is still required and checked by the handler.
// With no product calls registered the behaviour is byte-identical to the
// raft-only identityAuthorizer.
type productAwareAuthorizer struct {
	allowed        map[string]struct{}
	product        map[string]struct{}
	productMethods map[string]struct{}
	peers          map[string]struct{}
	proxiedReads   map[string]struct{}
}

func (authorizer productAwareAuthorizer) authorizeProduct(identity peer.PeerIdentity, method peer.Method) error {
	if _, ok := authorizer.product[identity.URI]; ok {
		return nil
	}
	if _, proxied := authorizer.proxiedReads[string(method)]; proxied {
		if _, peerIdentity := authorizer.peers[identity.URI]; peerIdentity {
			return nil
		}
	}
	return ErrUnauthorizedPeer
}

func (authorizer productAwareAuthorizer) AuthorizeCall(identity peer.PeerIdentity, method peer.Method) error {
	if _, ok := authorizer.productMethods[string(method)]; ok {
		return authorizer.authorizeProduct(identity, method)
	}
	return identityAuthorizer{allowed: authorizer.allowed}.AuthorizeCall(identity, method)
}

func (authorizer productAwareAuthorizer) AuthorizeStream(identity peer.PeerIdentity, method peer.Method) error {
	if _, ok := authorizer.productMethods[string(method)]; ok {
		return authorizer.authorizeProduct(identity, method)
	}
	return identityAuthorizer{allowed: authorizer.allowed}.AuthorizeStream(identity, method)
}

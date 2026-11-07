package main

import (
	"context"

	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
	"github.com/stokaro/ptah/provider-sdk/provider"
)

// readFeatures are the features a read of a session uses, where the host
// negotiated them.
var readFeatures = []string{provider.ObservedColumnsFeature, provider.ObservedConstraintsFeature, provider.ObservedIndexesFeature, provider.IndexNullsOrderFeature,
	provider.IndexPredicatesFeature}

// Open opens a backend session that states the schema changes as
// nontransactional, as the rebuild runs them, and the read features the host
// negotiated.
func (o *owner) Open(ctx context.Context, request *protocol.OpenRequest) (*protocol.Session, error) {
	session, err := o.Provider.Open(ctx, request)
	if err != nil {
		return nil, err
	}
	session.Capabilities = map[string]bool{"transactional_ddl": false}
	session.Features = o.negotiatedReads(ctx)
	return session, nil
}

// negotiatedReads are the read features of the description negotiated with
// the host of ctx.
func (o *owner) negotiatedReads(ctx context.Context) []string {
	negotiated := provider.NegotiatedDescription(o.description, provider.HostFeatures(ctx))
	var result []string
	for _, feature := range readFeatures {
		if provider.EngineOffers(negotiated, "fixture", feature) {
			result = append(result, feature)
		}
	}
	return result
}

package main

import (
	"context"
	"slices"

	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
	"github.com/stokaro/ptah/provider-sdk/provider"
)

// Open declares the same DDL profile used by native planning.
// A live session must supply this fact instead of relying on a caller preset.
func (p *owner) Open(ctx context.Context, request *protocol.OpenRequest) (*protocol.Session, error) {
	session, err := p.Provider.Open(ctx, request)
	if err != nil {
		return nil, err
	}
	session.Capabilities = map[string]bool{"transactional_ddl": p.transactionalDDL}
	if p.refusesDDLTransactions {
		session.Capabilities["ddl_inside_transaction"] = false
	}
	// A host pairs renamed indexes under index_rename, and only a host that
	// lists planning.index-renames sends the pair as a step to claim.
	if p.reportsRenames || (p.renames && slices.Contains(provider.HostFeatures(ctx), provider.IndexRenamesFeature)) {
		session.Capabilities["index_rename"] = true
	}
	return session, nil
}

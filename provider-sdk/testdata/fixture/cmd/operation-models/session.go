package main

import (
	"context"

	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
)

// Open declares the same nontransactional DDL profile used by native planning.
// A live session must supply this fact instead of relying on a caller preset.
func (p *owner) Open(ctx context.Context, request *protocol.OpenRequest) (*protocol.Session, error) {
	session, err := p.Provider.Open(ctx, request)
	if err != nil {
		return nil, err
	}
	session.Capabilities = map[string]bool{"transactional_ddl": false}
	return session, nil
}

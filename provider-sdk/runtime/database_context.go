package runtime

import (
	"context"

	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
	"github.com/stokaro/ptah/provider-sdk/provider"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

// ResolveDatabaseContext obtains the provider's non-secret interpretation of an
// explicit address without opening a session. Errors exclude provider text.
func (c *Client) ResolveDatabaseContext(ctx context.Context, request *protocol.DatabaseContextRequest) (*protocol.DatabaseContext, error) {
	return callOffline(ctx, c.description, proto.CloneOf(request), "resolve database context", provider.ValidateDatabaseContextRequest,
		provider.ValidateResolvedDatabaseContext, c.rpc.ResolveDatabaseContext)
}

func (s *serverAdapter) ResolveDatabaseContext(ctx context.Context, request *protocol.DatabaseContextRequest) (*protocol.DatabaseContext, error) {
	if nilService(s.server.DatabaseContexts) {
		return nil, status.Error(codes.Unimplemented, "database context resolution is unavailable")
	}
	return serveOffline(ctx, s, request, provider.ValidateDatabaseContextRequest, provider.ValidateResolvedDatabaseContext,
		s.server.DatabaseContexts.ResolveDatabaseContext)
}

func (c *Client) addressContext(ctx context.Context, request *protocol.OpenRequest) (*protocol.DatabaseContext, error) {
	if !provider.EngineOffers(c.description, request.GetEngine(), provider.DatabaseContextFeature) {
		return nil, nil
	}
	return c.ResolveDatabaseContext(ctx, &protocol.DatabaseContextRequest{Engine: request.GetEngine(), Address: request.GetAddress()})
}

func (s *serverAdapter) addressContext(ctx context.Context, request *protocol.OpenRequest) (*protocol.DatabaseContext, error) {
	if !provider.EngineOffers(s.negotiated(), request.GetEngine(), provider.DatabaseContextFeature) {
		return nil, nil
	}
	return s.ResolveDatabaseContext(ctx, &protocol.DatabaseContextRequest{Engine: request.GetEngine(), Address: request.GetAddress()})
}

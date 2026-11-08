package runtime

import (
	"context"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	protocol "ptah.run/provider-sdk/protocol/experimental"
	"ptah.run/provider-sdk/provider"
)

// ProjectTableCreations predicts effects for a complete captured batch offline.
// Invalid replies and cancellation expose no prefix or inspection evidence.
func (c *Client) ProjectTableCreations(ctx context.Context, request *protocol.ProjectTableCreationsRequest) (*protocol.ProjectedTableCreations, error) {
	return callOffline(ctx, c.description, provider.CloneSharingModels(request), "project table creations", provider.ValidateCreationRequest, provider.ValidateProjectedCreationsReply, c.rpc.ProjectTableCreations)
}

func (s *serverAdapter) ProjectTableCreations(ctx context.Context, request *protocol.ProjectTableCreationsRequest) (*protocol.ProjectedTableCreations, error) {
	if s.server.TableCreations == nil {
		return nil, status.Error(codes.Unimplemented, "project table creations is unavailable")
	}
	return serveOffline(ctx, s, request, provider.ValidateCreationRequest, provider.ValidateProjectedCreationsReply, s.server.TableCreations.ProjectTableCreations)
}

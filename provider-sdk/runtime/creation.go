package runtime

import (
	"context"

	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
	"github.com/stokaro/ptah/provider-sdk/provider"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

// ProjectTableCreations predicts effects for a complete captured batch offline.
// Invalid replies and cancellation expose no prefix or inspection evidence.
func (c *Client) ProjectTableCreations(ctx context.Context, request *protocol.ProjectTableCreationsRequest) (*protocol.ProjectedTableCreations, error) {
	return callOffline(ctx, c.description, proto.CloneOf(request), "project table creations", provider.ValidateCreationRequest, provider.ValidateProjectedCreations, c.rpc.ProjectTableCreations)
}

func (s *serverAdapter) ProjectTableCreations(ctx context.Context, request *protocol.ProjectTableCreationsRequest) (*protocol.ProjectedTableCreations, error) {
	if s.server.TableCreations == nil {
		return nil, status.Error(codes.Unimplemented, "project table creations is unavailable")
	}
	return serveOffline(ctx, s.server.Description, request, provider.ValidateCreationRequest, provider.ValidateProjectedCreations, s.server.TableCreations.ProjectTableCreations)
}

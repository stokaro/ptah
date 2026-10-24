package runtime

import (
	"context"

	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
	"github.com/stokaro/ptah/provider-sdk/provider"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// CompareModels validates one complete contextual comparison. Refusal,
// cancellation, and malformed replies expose no partial result.
func (c *Client) CompareModels(ctx context.Context, request *protocol.CompareModelsRequest) (*protocol.ComparedModels, error) {
	return callOffline(ctx, c.description, request, "compare models", provider.ValidateComparisonRequest, provider.ValidateComparedModels, c.rpc.CompareModels)
}

func (s *serverAdapter) CompareModels(ctx context.Context, request *protocol.CompareModelsRequest) (*protocol.ComparedModels, error) {
	if s.server.ModelComparer == nil {
		return nil, status.Error(codes.Unimplemented, "compare models is unavailable")
	}
	return serveOffline(ctx, s, request, provider.ValidateComparisonRequest, provider.ValidateComparedModels, s.server.ModelComparer.CompareModels)
}

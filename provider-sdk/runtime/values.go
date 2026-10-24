package runtime

import (
	"context"

	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
	"github.com/stokaro/ptah/provider-sdk/provider"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// NormalizeValues validates a complete offline batch. Cancellation, refusal,
// transport errors, and malformed responses expose no partial result.
func (c *Client) NormalizeValues(ctx context.Context, request *protocol.NormalizeValuesRequest) (*protocol.NormalizedValues, error) {
	return callOffline(ctx, c.description, request, "normalize values", provider.ValidateValueInputs, provider.ValidateNormalizedValues, c.rpc.NormalizeValues)
}

func (s *serverAdapter) NormalizeValues(ctx context.Context, request *protocol.NormalizeValuesRequest) (*protocol.NormalizedValues, error) {
	if s.server.ValueNormalizer == nil {
		return nil, status.Error(codes.Unimplemented, "normalize values is unavailable")
	}
	return serveOffline(ctx, s, request, provider.ValidateValueInputs, provider.ValidateNormalizedValues, s.server.ValueNormalizer.NormalizeValues)
}

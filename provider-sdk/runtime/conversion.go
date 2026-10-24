package runtime

import (
	"context"

	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
	"github.com/stokaro/ptah/provider-sdk/provider"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// ConvertValues interprets an offline batch in the other schema representation.
// Refusal, cancellation, malformed output, or transport failure exposes no
// partial result and never implies that an uninspected object is absent.
func (c *Client) ConvertValues(ctx context.Context, request *protocol.ConvertValuesRequest) (*protocol.NormalizedValues, error) {
	return callOffline(ctx, c.description, request, "convert values", provider.ValidateConversionRequest, provider.ValidateConvertedValues, c.rpc.ConvertValues)
}

func (s *serverAdapter) ConvertValues(ctx context.Context, request *protocol.ConvertValuesRequest) (*protocol.NormalizedValues, error) {
	if s.server.ValueConverter == nil {
		return nil, status.Error(codes.Unimplemented, "convert values is unavailable")
	}
	return serveOffline(ctx, s, request, provider.ValidateConversionRequest, provider.ValidateConvertedValues, s.server.ValueConverter.ConvertValues)
}

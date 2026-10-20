package runtime

import (
	"context"

	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
	"github.com/stokaro/ptah/provider-sdk/provider"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

// NormalizeValues validates a whole offline batch and exposes no prefix on
// cancellation, refusal, transport failure, or a malformed owner response.
func (c *Client) NormalizeValues(ctx context.Context, request *protocol.NormalizeValuesRequest) (*protocol.NormalizedValues, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := provider.ValidateValueInputs(c.description, request); err != nil {
		return nil, err
	}
	call, cancel := context.WithTimeout(ctx, CallTimeout)
	defer cancel()
	result, err := c.rpc.NormalizeValues(call, request)
	if err != nil {
		return nil, remoteError(call, "normalize values", err)
	}
	if err := call.Err(); err != nil {
		return nil, err
	}
	if err := provider.ValidateNormalizedValues(c.description, request, result); err != nil {
		return nil, err
	}
	return result, nil
}

func (s *serverAdapter) NormalizeValues(ctx context.Context, request *protocol.NormalizeValuesRequest) (*protocol.NormalizedValues, error) {
	if s.server.ValueNormalizer == nil {
		return nil, status.Error(codes.Unimplemented, "value normalization is unavailable")
	}
	if err := provider.ValidateValueInputs(s.server.Description, request); err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	if err := ctx.Err(); err != nil {
		return nil, status.FromContextError(err).Err()
	}
	result, err := s.server.ValueNormalizer.NormalizeValues(ctx, proto.CloneOf(request))
	if err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, status.FromContextError(err).Err()
	}
	if err := provider.ValidateNormalizedValues(s.server.Description, request, result); err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	return result, nil
}

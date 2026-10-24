package runtime

import (
	"context"

	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
	"github.com/stokaro/ptah/provider-sdk/provider"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// DecodeSource validates a complete offline batch. Cancellation, refusal,
// transport errors, and malformed responses expose no partial result.
func (c *Client) DecodeSource(ctx context.Context, request *protocol.DecodeSourceRequest) (*protocol.DecodedSource, error) {
	return callOffline(ctx, c.description, request, "decode source", provider.ValidateSourceRequest, provider.ValidateDecodedSource, c.rpc.DecodeSource)
}

func (s *serverAdapter) DecodeSource(ctx context.Context, request *protocol.DecodeSourceRequest) (*protocol.DecodedSource, error) {
	if s.server.SourceDecoder == nil {
		return nil, status.Error(codes.Unimplemented, "decode source is unavailable")
	}
	return serveOffline(ctx, s, request, provider.ValidateSourceRequest, provider.ValidateDecodedSource, s.server.SourceDecoder.DecodeSource)
}

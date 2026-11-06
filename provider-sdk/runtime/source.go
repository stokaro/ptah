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
	return callOffline(ctx, c.description, request, "decode source", provider.ValidateSourceRequest, provider.ValidateDecodedSourceReply, c.rpc.DecodeSource)
}

// EncodeSource writes values as the fragments the provider's claims read,
// offline. Every value of a valid reply is written or lost once; an invalid
// reply and cancellation expose nothing.
func (c *Client) EncodeSource(ctx context.Context, request *protocol.EncodeSourceRequest) (*protocol.EncodedSource, error) {
	return callOffline(ctx, c.description, provider.CloneSharingModels(request), "encode source", provider.ValidateEncodeSourceRequest, provider.ValidateEncodedSourceReply, c.rpc.EncodeSource)
}

func (s *serverAdapter) EncodeSource(ctx context.Context, request *protocol.EncodeSourceRequest) (*protocol.EncodedSource, error) {
	if nilService(s.server.SourceEncoder) {
		return nil, unavailable("source encoding")
	}
	return serveOffline(ctx, s, request, provider.ValidateEncodeSourceRequest, provider.ValidateEncodedSourceReply, s.server.SourceEncoder.EncodeSource)
}

func (s *serverAdapter) DecodeSource(ctx context.Context, request *protocol.DecodeSourceRequest) (*protocol.DecodedSource, error) {
	if s.server.SourceDecoder == nil {
		return nil, status.Error(codes.Unimplemented, "decode source is unavailable")
	}
	return serveOffline(ctx, s, request, provider.ValidateSourceRequest, provider.ValidateDecodedSourceReply, s.server.SourceDecoder.DecodeSource)
}

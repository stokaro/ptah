package runtime

import (
	"context"

	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
	"github.com/stokaro/ptah/provider-sdk/provider"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// ValidateSchema checks the captured schema offline and returns complete typed
// diagnostics. Invalid replies and cancellation return no diagnostics.
func (c *Client) ValidateSchema(ctx context.Context, request *protocol.ValidateSchemaRequest) (*protocol.SchemaValidation, error) {
	return callOffline(ctx, c.description, provider.CloneSharingModels(request), "validate schema", provider.ValidateSchemaRequest, provider.ValidateSchemaResultReply, c.rpc.ValidateSchema)
}
func (s *serverAdapter) ValidateSchema(ctx context.Context, request *protocol.ValidateSchemaRequest) (*protocol.SchemaValidation, error) {
	if s.server.SchemaValidator == nil {
		return nil, status.Error(codes.Unimplemented, "schema validation is unavailable")
	}
	return serveOffline(ctx, s, request, provider.ValidateSchemaRequest, provider.ValidateSchemaResultReply, s.server.SchemaValidator.ValidateSchema)
}

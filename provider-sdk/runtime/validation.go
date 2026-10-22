package runtime

import (
	"context"

	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
	"github.com/stokaro/ptah/provider-sdk/provider"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

// ValidateSchema checks the captured schema offline and returns complete typed
// diagnostics. Invalid replies and cancellation return no diagnostics.
func (c *Client) ValidateSchema(ctx context.Context, request *protocol.ValidateSchemaRequest) (*protocol.SchemaValidation, error) {
	return callOffline(ctx, c.description, proto.CloneOf(request), "validate schema", provider.ValidateSchemaRequest, provider.ValidateSchemaResult, c.rpc.ValidateSchema)
}
func (s *serverAdapter) ValidateSchema(ctx context.Context, request *protocol.ValidateSchemaRequest) (*protocol.SchemaValidation, error) {
	if s.server.SchemaValidator == nil {
		return nil, status.Error(codes.Unimplemented, "schema validation is unavailable")
	}
	return serveOffline(ctx, s.server.Description, request, provider.ValidateSchemaRequest, provider.ValidateSchemaResult, s.server.SchemaValidator.ValidateSchema)
}

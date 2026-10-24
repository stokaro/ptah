package runtime

import (
	"context"

	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
	"github.com/stokaro/ptah/provider-sdk/provider"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

// ReadSQLSchema reads the SQL documents of a DOCUMENT engine offline. The
// reply is validated against the request. Cancellation, refusal, transport
// errors and malformed replies return no schema. A reply with complete false is
// returned with its error diagnostics for the caller to refuse the load.
func (c *Client) ReadSQLSchema(ctx context.Context, request *protocol.ReadSQLSchemaRequest) (*protocol.SQLSchema, error) {
	return callOffline(ctx, c.description, proto.CloneOf(request), "read SQL schema", provider.ValidateReadSQLSchemaRequest, provider.ValidateSQLSchema, c.rpc.ReadSQLSchema)
}

func (s *serverAdapter) ReadSQLSchema(ctx context.Context, request *protocol.ReadSQLSchemaRequest) (*protocol.SQLSchema, error) {
	if s.server.SQLSchemaReader == nil {
		return nil, status.Error(codes.Unimplemented, "SQL schema reading is unavailable")
	}
	return serveOffline(ctx, s, request, provider.ValidateReadSQLSchemaRequest, provider.ValidateSQLSchema, s.server.SQLSchemaReader.ReadSQLSchema)
}

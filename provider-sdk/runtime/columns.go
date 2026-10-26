package runtime

import (
	"context"

	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
	"github.com/stokaro/ptah/provider-sdk/provider"
	"google.golang.org/protobuf/proto"
)

// CompareColumns obtains complete offline owner receipts for captured pairs.
// Cancellation, transport failures and invalid replies expose no result prefix.
func (c *Client) CompareColumns(ctx context.Context, request *protocol.CompareColumnsRequest) (*protocol.ComparedColumns, error) {
	return callOffline(ctx, c.description, proto.CloneOf(request), "compare columns", provider.ValidateColumnsRequest, provider.ValidateComparedColumns, c.rpc.CompareColumns)
}

// ReadColumnDefaults obtains typed defaults for the exact observed inventory.
// Missing or malformed receipts refuse conversion instead of selecting a fallback.
func (c *Client) ReadColumnDefaults(ctx context.Context, request *protocol.ReadColumnDefaultsRequest) (*protocol.ReadColumnDefaultsResult, error) {
	return callOffline(ctx, c.description, proto.CloneOf(request), "read column defaults", provider.ValidateColumnDefaultsRequest, provider.ValidateColumnDefaults, c.rpc.ReadColumnDefaults)
}

func (s *serverAdapter) CompareColumns(ctx context.Context, request *protocol.CompareColumnsRequest) (*protocol.ComparedColumns, error) {
	if nilService(s.server.Columns) {
		return nil, unavailable("column semantics")
	}
	return serveOffline(ctx, s, request, provider.ValidateColumnsRequest, provider.ValidateComparedColumns, s.server.Columns.CompareColumns)
}

func (s *serverAdapter) ReadColumnDefaults(ctx context.Context, request *protocol.ReadColumnDefaultsRequest) (*protocol.ReadColumnDefaultsResult, error) {
	if nilService(s.server.Columns) {
		return nil, unavailable("column semantics")
	}
	return serveOffline(ctx, s, request, provider.ValidateColumnDefaultsRequest, provider.ValidateColumnDefaults, s.server.Columns.ReadColumnDefaults)
}

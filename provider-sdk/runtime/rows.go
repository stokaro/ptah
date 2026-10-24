package runtime

import (
	"context"

	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
	"github.com/stokaro/ptah/provider-sdk/provider"
	"google.golang.org/protobuf/proto"
)

// TypeRows types the declared and live rows of one table offline. An invalid
// reply and cancellation expose no rows.
func (c *Client) TypeRows(ctx context.Context, request *protocol.TypeRowsRequest) (*protocol.TypedRows, error) {
	return callOffline(ctx, c.description, proto.CloneOf(request), "type rows", provider.ValidateTypeRowsRequest, provider.ValidateTypedRows, c.rpc.TypeRows)
}

// RenderRowWrites renders typed row writes offline. A complete reply has one
// unit per write in input order; a refusal names a write and carries no unit.
// An invalid reply exposes neither.
func (c *Client) RenderRowWrites(ctx context.Context, request *protocol.RenderRowWritesRequest) (*protocol.RenderResult, error) {
	return callOffline(ctx, c.description, proto.CloneOf(request), "render row writes", provider.ValidateRenderRowWritesRequest, provider.ValidateRenderedRowWrites, c.rpc.RenderRowWrites)
}

func rowRead(request *protocol.ReadRowsRequest) sessionCall[*protocol.ReadRowsRequest, *protocol.RowPage] {
	return sessionCall[*protocol.ReadRowsRequest, *protocol.RowPage]{
		feature: provider.ReadRowsFeature, operation: "read rows", timeout: CallTimeout, transaction: request.GetTransactionId(),
		validateInput: provider.ValidateReadRowsRequest, validateOutput: provider.ValidateRowPage,
	}
}

// ReadRows reads one page of a table on a live session. A read that cannot
// finish returns an error and no rows; the caller resumes with the page's
// next page token, never by repeating a request whose reply it did not get.
func (c *Client) ReadRows(ctx context.Context, request *protocol.ReadRowsRequest) (*protocol.RowPage, error) {
	return rowRead(request).call(ctx, c, request, c.rpc.ReadRows)
}

func (s *serverAdapter) TypeRows(ctx context.Context, request *protocol.TypeRowsRequest) (*protocol.TypedRows, error) {
	if nilService(s.server.RowTyper) {
		return nil, unavailable("row typing")
	}
	return serveOffline(ctx, s, request, provider.ValidateTypeRowsRequest, provider.ValidateTypedRows, s.server.RowTyper.TypeRows)
}

func (s *serverAdapter) RenderRowWrites(ctx context.Context, request *protocol.RenderRowWritesRequest) (*protocol.RenderResult, error) {
	if nilService(s.server.RowWriter) {
		return nil, unavailable("row write rendering")
	}
	return serveOffline(ctx, s, request, provider.ValidateRenderRowWritesRequest, provider.ValidateRenderedRowWrites, s.server.RowWriter.RenderRowWrites)
}

func (s *serverAdapter) ReadRows(ctx context.Context, request *protocol.ReadRowsRequest) (*protocol.RowPage, error) {
	if nilService(s.server.RowReader) {
		return nil, unavailable("row reading")
	}
	return rowRead(request).serve(ctx, s, request, s.server.RowReader.ReadRows)
}

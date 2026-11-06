package runtime

import (
	"context"

	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
	"github.com/stokaro/ptah/provider-sdk/provider"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// PrepareTables resolves a complete captured table batch offline. Inputs are
// copied before dispatch; cancellation and invalid replies expose no prefix.
func (c *Client) PrepareTables(ctx context.Context, request *protocol.PrepareTablesRequest) (*protocol.PreparedTables, error) {
	return callOffline(ctx, c.description, provider.CloneSharingModels(request), "prepare tables", provider.ValidatePreparationRequest, provider.ValidatePreparedTablesReply, c.rpc.PrepareTables)
}
func (s *serverAdapter) PrepareTables(ctx context.Context, request *protocol.PrepareTablesRequest) (*protocol.PreparedTables, error) {
	if s.server.TablePreparer == nil {
		return nil, status.Error(codes.Unimplemented, "prepare tables is unavailable")
	}
	return serveOffline(ctx, s, request, provider.ValidatePreparationRequest, provider.ValidatePreparedTablesReply, s.server.TablePreparer.PrepareTables)
}

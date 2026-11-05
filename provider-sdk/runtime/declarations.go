package runtime

import (
	"context"

	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
	"github.com/stokaro/ptah/provider-sdk/provider"
)

// PlanDeclarations plans authored objects offline. Invalid replies, transport
// failures and cancellation expose no contributions or receipts.
func (c *Client) PlanDeclarations(ctx context.Context, request *protocol.PlanDeclarationsRequest) (*protocol.PlannedDeclarations, error) {
	return callOffline(ctx, c.description, provider.CloneSharingModels(request), "plan declarations", provider.ValidateDeclarationsRequest, provider.ValidatePlannedDeclarations, c.rpc.PlanDeclarations)
}

func (s *serverAdapter) PlanDeclarations(ctx context.Context, request *protocol.PlanDeclarationsRequest) (*protocol.PlannedDeclarations, error) {
	if nilService(s.server.Declarations) {
		return nil, unavailable("declaration planning")
	}
	return serveOffline(ctx, s, request, provider.ValidateDeclarationsRequest, provider.ValidatePlannedDeclarations, s.server.Declarations.PlanDeclarations)
}

package runtime

import (
	"context"

	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
	"github.com/stokaro/ptah/provider-sdk/provider"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

// PlanFeatures validates one complete contextual planning exchange. Errors and
// cancellation discard the entire result, including diagnostics and receipts.
func (c *Client) PlanFeatures(ctx context.Context, request *protocol.PlanFeaturesRequest) (*protocol.PlannedFeatures, error) {
	return callOffline(ctx, c.description, proto.CloneOf(request), "plan features", provider.ValidatePlanningRequest, provider.ValidatePlannedFeatures, c.rpc.PlanFeatures)
}

func (s *serverAdapter) PlanFeatures(ctx context.Context, request *protocol.PlanFeaturesRequest) (*protocol.PlannedFeatures, error) {
	if nilService(s.server.FeaturePlanner) {
		return nil, status.Error(codes.Unimplemented, "feature planning is unavailable")
	}
	return serveOffline(ctx, s.server.Description, request, provider.ValidatePlanningRequest, provider.ValidatePlannedFeatures, s.server.FeaturePlanner.PlanFeatures)
}

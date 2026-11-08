package runtime

import (
	"context"

	protocol "ptah.run/provider-sdk/protocol/experimental"
	"ptah.run/provider-sdk/provider"
)

// PrepareCommonObjects returns a declaration of common objects in the
// engine's spelling, offline. The input is copied before dispatch; an invalid
// reply and cancellation expose no prefix.
func (c *Client) PrepareCommonObjects(ctx context.Context, request *protocol.PrepareCommonObjectsRequest) (*protocol.PreparedCommonObjects, error) {
	return callOffline(ctx, c.description, provider.CloneSharingModels(request), "prepare common objects",
		provider.ValidatePrepareCommonObjectsRequest, provider.ValidatePreparedCommonObjectsReply, c.rpc.PrepareCommonObjects)
}

func (s *serverAdapter) PrepareCommonObjects(ctx context.Context, request *protocol.PrepareCommonObjectsRequest) (*protocol.PreparedCommonObjects, error) {
	if nilService(s.server.CommonObjects) {
		return nil, unavailable("common object preparation")
	}
	return serveOffline(ctx, s, request, provider.ValidatePrepareCommonObjectsRequest, provider.ValidatePreparedCommonObjectsReply, s.server.CommonObjects.PrepareCommonObjects)
}

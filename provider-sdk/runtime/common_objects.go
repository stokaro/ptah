package runtime

import (
	"context"

	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
	"github.com/stokaro/ptah/provider-sdk/provider"
	"google.golang.org/protobuf/proto"
)

// PrepareCommonObjects returns a declaration of common objects in the
// engine's spelling, offline. The input is copied before dispatch; an invalid
// reply and cancellation expose no prefix.
func (c *Client) PrepareCommonObjects(ctx context.Context, request *protocol.PrepareCommonObjectsRequest) (*protocol.PreparedCommonObjects, error) {
	return callOffline(ctx, c.description, proto.CloneOf(request), "prepare common objects",
		provider.ValidatePrepareCommonObjectsRequest, provider.ValidatePreparedCommonObjects, c.rpc.PrepareCommonObjects)
}

func (s *serverAdapter) PrepareCommonObjects(ctx context.Context, request *protocol.PrepareCommonObjectsRequest) (*protocol.PreparedCommonObjects, error) {
	if nilService(s.server.CommonObjects) {
		return nil, unavailable("common object preparation")
	}
	return serveOffline(ctx, s, request, provider.ValidatePrepareCommonObjectsRequest, provider.ValidatePreparedCommonObjects, s.server.CommonObjects.PrepareCommonObjects)
}

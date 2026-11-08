package runtime

import (
	"context"

	"google.golang.org/protobuf/proto"

	protocol "ptah.run/provider-sdk/protocol/experimental"
	"ptah.run/provider-sdk/provider"
)

// ReverseChanges undoes accepted changes offline. Both sides must list
// provider.ReverseChangesFeature, and the engine must declare the service.
// The input is copied before dispatch; an invalid reply and cancellation
// expose no result.
func (c *Client) ReverseChanges(ctx context.Context, request *protocol.ReverseChangesRequest) (*protocol.ReversedChanges, error) {
	return callOffline(ctx, c.description, proto.CloneOf(request), "reverse changes",
		provider.ValidateReverseChangesRequest, provider.ValidateReversedChangesReply, c.rpc.ReverseChanges)
}

func (s *serverAdapter) ReverseChanges(ctx context.Context, request *protocol.ReverseChangesRequest) (*protocol.ReversedChanges, error) {
	if nilService(s.server.ChangeReverser) {
		return nil, unavailable("change reversal")
	}
	return serveOffline(ctx, s, request, provider.ValidateReverseChangesRequest, provider.ValidateReversedChangesReply, s.server.ChangeReverser.ReverseChanges)
}

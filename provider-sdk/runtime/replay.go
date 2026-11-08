package runtime

import (
	"context"

	"google.golang.org/protobuf/proto"

	protocol "ptah.run/provider-sdk/protocol/experimental"
	"ptah.run/provider-sdk/provider"
)

// GuardReplay asks the owner which replay statements it cannot confine to the
// scope. An invalid reply and cancellation expose no verdict, so a caller
// replays nothing it has no verdict for.
func (c *Client) GuardReplay(ctx context.Context, request *protocol.GuardReplayRequest) (*protocol.GuardedReplay, error) {
	return callOffline(ctx, c.description, proto.CloneOf(request), "guard replay", provider.ValidateGuardReplayRequest, provider.ValidateGuardedReplayReply, c.rpc.GuardReplay)
}

func (s *serverAdapter) GuardReplay(ctx context.Context, request *protocol.GuardReplayRequest) (*protocol.GuardedReplay, error) {
	if nilService(s.server.ReplayGuard) {
		return nil, unavailable("the replay guard")
	}
	return serveOffline(ctx, s, request, provider.ValidateGuardReplayRequest, provider.ValidateGuardedReplayReply, s.server.ReplayGuard.GuardReplay)
}

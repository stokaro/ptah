package runtime

import (
	"context"

	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
	"github.com/stokaro/ptah/provider-sdk/provider"
	"google.golang.org/protobuf/proto"
)

// GuardReplay asks the owner which replay statements it cannot confine to the
// scope. An invalid reply and cancellation expose no verdict, so a caller
// replays nothing it has no verdict for.
func (c *Client) GuardReplay(ctx context.Context, request *protocol.GuardReplayRequest) (*protocol.GuardedReplay, error) {
	return callOffline(ctx, c.description, proto.CloneOf(request), "guard replay", provider.ValidateGuardReplayRequest, provider.ValidateGuardedReplay, c.rpc.GuardReplay)
}

func (s *serverAdapter) GuardReplay(ctx context.Context, request *protocol.GuardReplayRequest) (*protocol.GuardedReplay, error) {
	if nilService(s.server.ReplayGuard) {
		return nil, unavailable("the replay guard")
	}
	return serveOffline(ctx, s, request, provider.ValidateGuardReplayRequest, provider.ValidateGuardedReplay, s.server.ReplayGuard.GuardReplay)
}

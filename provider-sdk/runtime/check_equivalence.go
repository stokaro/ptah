package runtime

import (
	"context"

	"google.golang.org/protobuf/proto"

	protocol "ptah.run/provider-sdk/protocol/experimental"
	"ptah.run/provider-sdk/provider"
)

// CompareChecks asks whether each declared CHECK expression and the held
// CHECK it is paired with are one constraint on the engine, offline. The
// provider and the engine must list provider.CheckEquivalenceFeature. The
// input is copied before dispatch; an invalid reply and cancellation expose no
// answer.
func (c *Client) CompareChecks(ctx context.Context, request *protocol.CompareChecksRequest) (*protocol.ComparedChecks, error) {
	return callOffline(ctx, c.description, proto.CloneOf(request), "compare checks",
		provider.ValidateCompareChecksRequest, provider.ValidateComparedChecksReply, c.rpc.CompareChecks)
}

func (s *serverAdapter) CompareChecks(ctx context.Context, request *protocol.CompareChecksRequest) (*protocol.ComparedChecks, error) {
	if nilService(s.server.CheckComparer) {
		return nil, unavailable("CHECK comparison")
	}
	return serveOffline(ctx, s, request, provider.ValidateCompareChecksRequest, provider.ValidateComparedChecksReply, s.server.CheckComparer.CompareChecks)
}

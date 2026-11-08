package runtime

import (
	"context"

	"google.golang.org/protobuf/proto"

	protocol "ptah.run/provider-sdk/protocol/experimental"
	"ptah.run/provider-sdk/provider"
)

// CompareViewQueries asks whether the query each declared view states and the
// query of the held view it is paired with are one query on the engine,
// offline. The provider and the engine must list
// provider.ViewQueryEquivalenceFeature. The input is copied before dispatch;
// an invalid reply and cancellation expose no answer.
func (c *Client) CompareViewQueries(ctx context.Context, request *protocol.CompareViewQueriesRequest) (*protocol.ComparedViewQueries, error) {
	return callOffline(ctx, c.description, proto.CloneOf(request), "compare view queries",
		provider.ValidateCompareViewQueriesRequest, provider.ValidateComparedViewQueriesReply, c.rpc.CompareViewQueries)
}

func (s *serverAdapter) CompareViewQueries(ctx context.Context, request *protocol.CompareViewQueriesRequest) (*protocol.ComparedViewQueries, error) {
	if nilService(s.server.ViewComparer) {
		return nil, unavailable("view query comparison")
	}
	return serveOffline(ctx, s, request, provider.ValidateCompareViewQueriesRequest, provider.ValidateComparedViewQueriesReply, s.server.ViewComparer.CompareViewQueries)
}

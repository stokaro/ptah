package runtime

import (
	"context"

	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
	"github.com/stokaro/ptah/provider-sdk/provider"
	"google.golang.org/protobuf/proto"
)

var (
	devObjects = sessionCall[*protocol.ListDevObjectsRequest, *protocol.DevObjects]{
		feature: provider.DevCleanupFeature, operation: "list dev objects", timeout: CallTimeout,
		validateInput: provider.ValidateListDevObjectsRequest, validateOutput: provider.ValidateDevObjects,
	}
	devClean = sessionCall[*protocol.CleanDevRequest, *protocol.DevCleanup]{
		feature: provider.DevCleanupFeature, operation: "clean dev database", timeout: ProvisionTimeout,
		validateInput: provider.ValidateCleanDevRequest, validateOutput: provider.ValidateDevCleanup, uncertain: unknownCleanup,
	}
)

func unknownCleanup() error { return &provider.DevCleanupError{} }

// ListDevObjects returns every object a cleanup of the scope would remove or
// stop at. An incomplete or invalid reply is an error and no list, never an
// empty dev database.
func (c *Client) ListDevObjects(ctx context.Context, request *protocol.ListDevObjectsRequest) (*protocol.DevObjects, error) {
	return devObjects.call(ctx, c, request, c.rpc.ListDevObjects)
}

// CleanDevDatabase empties the scope once. A refusal is returned with a
// provider.DevCleanupError and names the objects that stopped it. A lost or
// invalid reply exposes no result and leaves the effect unknown; this method
// never retries it.
func (c *Client) CleanDevDatabase(ctx context.Context, request *protocol.CleanDevRequest) (*protocol.DevCleanup, error) {
	result, err := devClean.call(ctx, c, request, c.rpc.CleanDevDatabase)
	if err != nil {
		return nil, err
	}
	return result, provider.CheckDevCleanup(proto.CloneOf(request), result)
}

func (s *serverAdapter) ListDevObjects(ctx context.Context, request *protocol.ListDevObjectsRequest) (*protocol.DevObjects, error) {
	if nilService(s.server.DevCleaner) {
		return nil, unavailable("dev database cleanup")
	}
	return devObjects.serve(ctx, s, request, s.server.DevCleaner.ListDevObjects)
}

func (s *serverAdapter) CleanDevDatabase(ctx context.Context, request *protocol.CleanDevRequest) (*protocol.DevCleanup, error) {
	if nilService(s.server.DevCleaner) {
		return nil, unavailable("dev database cleanup")
	}
	return devClean.serve(ctx, s, request, s.server.DevCleaner.CleanDevDatabase)
}

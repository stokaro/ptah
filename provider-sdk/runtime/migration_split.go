package runtime

import (
	"context"

	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
	"github.com/stokaro/ptah/provider-sdk/provider"
	"google.golang.org/protobuf/proto"
)

// SplitMigration splits one migration body into the units its engine runs,
// offline. Both sides must list provider.MigrationSplitFeature, and the
// engine must declare it. The input is copied before dispatch; an invalid
// reply and cancellation expose no unit.
func (c *Client) SplitMigration(ctx context.Context, request *protocol.SplitMigrationRequest) (*protocol.MigrationSplit, error) {
	return callOffline(ctx, c.description, proto.CloneOf(request), "split migration",
		provider.ValidateSplitMigrationRequest, provider.ValidateMigrationSplitReply, c.rpc.SplitMigration)
}

func (s *serverAdapter) SplitMigration(ctx context.Context, request *protocol.SplitMigrationRequest) (*protocol.MigrationSplit, error) {
	if nilService(s.server.MigrationSplits) {
		return nil, unavailable("migration splitting")
	}
	return serveOffline(ctx, s, request, provider.ValidateSplitMigrationRequest, provider.ValidateMigrationSplitReply, s.server.MigrationSplits.SplitMigration)
}

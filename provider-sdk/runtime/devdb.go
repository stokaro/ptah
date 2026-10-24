package runtime

import (
	"context"

	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
	"github.com/stokaro/ptah/provider-sdk/provider"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

// ProvisionDevDatabase asks the owner for a throwaway database. An invalid reply
// and cancellation expose no lease. A reply that is not a lease is a typed
// refusal in the result, not an error.
func (c *Client) ProvisionDevDatabase(ctx context.Context, request *protocol.ProvisionDevRequest) (*protocol.DevDatabase, error) {
	return callWithin(ctx, ProvisionTimeout, c.description, proto.CloneOf(request), "provision dev database", provider.ValidateProvisionDevRequest, provider.ValidateDevDatabase, c.rpc.ProvisionDevDatabase)
}

// ReleaseDevDatabase removes what a lease names.
func (c *Client) ReleaseDevDatabase(ctx context.Context, request *protocol.ReleaseDevRequest) (*protocol.Empty, error) {
	return callWithin(ctx, ProvisionTimeout, c.description, proto.CloneOf(request), "release dev database", provider.ValidateReleaseDevRequest, provider.ValidateReleasedDev, c.rpc.ReleaseDevDatabase)
}

func (s *serverAdapter) ProvisionDevDatabase(ctx context.Context, request *protocol.ProvisionDevRequest) (*protocol.DevDatabase, error) {
	if s.server.DevProvisioner == nil {
		return nil, status.Error(codes.Unimplemented, "dev database provisioning is unavailable")
	}
	var created *protocol.DevDatabase
	result, err := serveOffline(ctx, s, request, provider.ValidateProvisionDevRequest, provider.ValidateDevDatabase,
		func(ctx context.Context, request *protocol.ProvisionDevRequest) (*protocol.DevDatabase, error) {
			reply, err := s.server.DevProvisioner.ProvisionDevDatabase(s.owner(ctx), request)
			created = reply
			return reply, err
		})
	if err != nil && created.GetLease() != "" {
		// The host never receives this lease, so nobody else can release it. A
		// failed release is left to the owner, which removes its leases at exit.
		release, cancel := context.WithTimeout(context.WithoutCancel(ctx), CallTimeout)
		defer cancel()
		_, _ = s.server.DevProvisioner.ReleaseDevDatabase(release, &protocol.ReleaseDevRequest{Engine: request.Engine, Lease: created.Lease})
	}
	return result, err
}

func (s *serverAdapter) ReleaseDevDatabase(ctx context.Context, request *protocol.ReleaseDevRequest) (*protocol.Empty, error) {
	if s.server.DevProvisioner == nil {
		return nil, status.Error(codes.Unimplemented, "dev database provisioning is unavailable")
	}
	return serveOffline(ctx, s, request, provider.ValidateReleaseDevRequest, provider.ValidateReleasedDev, s.server.DevProvisioner.ReleaseDevDatabase)
}

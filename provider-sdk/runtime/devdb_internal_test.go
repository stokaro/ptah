package runtime

// White-box testing required: malformed peer replies must bypass the SDK server,
// and a lease the host never receives must be observed on the owner.

import (
	"context"
	"testing"

	qt "github.com/frankban/quicktest"
	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
	"github.com/stokaro/ptah/provider-sdk/provider"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

func runtimeDevExchange() (*protocol.Description, *protocol.ProvisionDevRequest, *protocol.DevDatabase) {
	description, _, _ := normalizationExchange()
	description.Features = append(description.Features, provider.DevDatabaseFeature)
	description.Engines[0].Features = []string{provider.DevDatabaseFeature}
	request := &protocol.ProvisionDevRequest{Engine: "test", Spec: "docker://test/1"}
	return description, request, &protocol.DevDatabase{Lease: "lease-1", Url: "test://localhost/dev"}
}

func TestDevClientDiscardsUntrustedReply(t *testing.T) {
	c := qt.New(t)
	description, request, valid := runtimeDevExchange()
	peer := &devPeer{result: proto.CloneOf(valid)}
	client := &Client{description: description, rpc: peer}
	result, err := client.ProvisionDevDatabase(c.Context(), request)
	c.Assert(err, qt.IsNil)
	c.Assert(proto.Equal(result, valid), qt.IsTrue)
	peer.result = &protocol.DevDatabase{Lease: "lease-1", Url: "test://user:secret@localhost/dev"} // #nosec G101 -- fixture address, not a credential
	result, err = client.ProvisionDevDatabase(c.Context(), request)
	c.Assert(err, qt.ErrorIs, provider.ErrProtocol)
	c.Assert(result, qt.IsNil)
	peer.result = &protocol.DevDatabase{Refusal: &protocol.DevDatabaseRefusal{Kind: protocol.DevDatabaseRefusal_UNAVAILABLE, Reason: "no container engine"}}
	result, err = client.ProvisionDevDatabase(c.Context(), request)
	c.Assert(err, qt.IsNil)
	c.Assert(result.Refusal.Kind, qt.Equals, protocol.DevDatabaseRefusal_UNAVAILABLE)
	peer.err = status.Error(codes.Unavailable, "password=secret")
	result, err = client.ProvisionDevDatabase(c.Context(), request)
	c.Assert(err, qt.ErrorMatches, `provider provision dev database failed \(Unavailable\)`)
	c.Assert(result, qt.IsNil)
	c.Assert(peer.calls, qt.Equals, 4)
}

func TestDevClientReleasesOnlyAValidLease(t *testing.T) {
	c := qt.New(t)
	description, _, _ := runtimeDevExchange()
	peer := &devPeer{}
	client := &Client{description: description, rpc: peer}
	_, err := client.ReleaseDevDatabase(c.Context(), &protocol.ReleaseDevRequest{Engine: "test"})
	c.Assert(err, qt.ErrorIs, provider.ErrProtocol)
	c.Assert(peer.releases, qt.Equals, 0)
	_, err = client.ReleaseDevDatabase(c.Context(), &protocol.ReleaseDevRequest{Engine: "test", Lease: "lease-1"})
	c.Assert(err, qt.IsNil)
	c.Assert(peer.releases, qt.Equals, 1)
}

func TestDevServerReleasesALeaseTheHostNeverReceives(t *testing.T) {
	c := qt.New(t)
	description, request, valid := runtimeDevExchange()
	owner := &devOwner{result: proto.CloneOf(valid)}
	server := &serverAdapter{server: Server{Description: description, DevProvisioner: owner}}
	ctx, cancel := context.WithCancel(c.Context())
	owner.afterProvision = cancel
	result, err := server.ProvisionDevDatabase(ctx, request)
	c.Assert(status.Code(err), qt.Equals, codes.Canceled)
	c.Assert(result, qt.IsNil)
	c.Assert(owner.released, qt.DeepEquals, []string{"lease-1"})

	owner.afterProvision = func() {}
	owner.result = &protocol.DevDatabase{Lease: "lease-2", Url: "test://user:secret@localhost/dev"} // #nosec G101 -- fixture address, not a credential
	result, err = server.ProvisionDevDatabase(c.Context(), request)
	c.Assert(status.Code(err), qt.Equals, codes.Internal)
	c.Assert(result, qt.IsNil)
	c.Assert(owner.released, qt.DeepEquals, []string{"lease-1", "lease-2"})

	owner.result = proto.CloneOf(valid)
	result, err = server.ProvisionDevDatabase(c.Context(), request)
	c.Assert(err, qt.IsNil)
	c.Assert(proto.Equal(result, valid), qt.IsTrue)
	c.Assert(owner.released, qt.HasLen, 2)
}

func TestDevServerIsUnimplementedWithoutAnOwner(t *testing.T) {
	c := qt.New(t)
	description, request, _ := runtimeDevExchange()
	server := &serverAdapter{server: Server{Description: description}}
	_, err := server.ProvisionDevDatabase(c.Context(), request)
	c.Assert(status.Code(err), qt.Equals, codes.Unimplemented)
	_, err = server.ReleaseDevDatabase(c.Context(), &protocol.ReleaseDevRequest{})
	c.Assert(status.Code(err), qt.Equals, codes.Unimplemented)
}

type devPeer struct {
	protocol.ProviderClient
	result   *protocol.DevDatabase
	err      error
	calls    int
	releases int
}

func (p *devPeer) ProvisionDevDatabase(context.Context, *protocol.ProvisionDevRequest, ...grpc.CallOption) (*protocol.DevDatabase, error) {
	p.calls++
	return p.result, p.err
}

func (p *devPeer) ReleaseDevDatabase(context.Context, *protocol.ReleaseDevRequest, ...grpc.CallOption) (*protocol.Empty, error) {
	p.releases++
	return &protocol.Empty{}, nil
}

type devOwner struct {
	result         *protocol.DevDatabase
	afterProvision func()
	released       []string
}

func (o *devOwner) ProvisionDevDatabase(context.Context, *protocol.ProvisionDevRequest) (*protocol.DevDatabase, error) {
	o.afterProvision()
	return o.result, nil
}

func (o *devOwner) ReleaseDevDatabase(_ context.Context, request *protocol.ReleaseDevRequest) (*protocol.Empty, error) {
	o.released = append(o.released, request.Lease)
	return &protocol.Empty{}, nil
}

package runtime

// White-box testing required: malformed peer receipts and request-mutating
// owners must reach both boundaries before transport validation rejects them.

import (
	"context"
	"net/url"
	"testing"

	qt "github.com/frankban/quicktest"
	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
	"github.com/stokaro/ptah/provider-sdk/provider"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

func runtimeContextDescription() *protocol.Description {
	return &protocol.Description{ProviderId: "example.org/context", Features: []string{provider.DatabaseContextFeature},
		Engines: []*protocol.Engine{{Id: "fixture", Features: []string{provider.DatabaseContextFeature}}}}
}

func runtimeContext() *protocol.DatabaseContext {
	return &protocol.DatabaseContext{Database: "/root/realm", SourceDatabase: "/root"}
}

type contextPeer struct {
	*sessionPeer
	context *protocol.DatabaseContext
	err     error
	calls   int
}

func (p *contextPeer) ResolveDatabaseContext(context.Context, *protocol.DatabaseContextRequest, ...grpc.CallOption) (*protocol.DatabaseContext, error) {
	p.calls++
	return proto.CloneOf(p.context), p.err
}

type contextOwner struct {
	context *protocol.DatabaseContext
	cancel  context.CancelFunc
	calls   int
}

func (p *contextOwner) ResolveDatabaseContext(_ context.Context, request *protocol.DatabaseContextRequest) (*protocol.DatabaseContext, error) {
	p.calls++
	request.Engine, request.Address = "mutated", "mutated"
	if p.cancel != nil {
		p.cancel()
	}
	return proto.CloneOf(p.context), nil
}

func TestContextClientRefusesMalformedAndCanceledResolution(t *testing.T) {
	c := qt.New(t)
	peer := &contextPeer{context: runtimeContext()}
	client := &Client{description: runtimeContextDescription(), rpc: peer}
	request := &protocol.DatabaseContextRequest{Engine: "fixture", Address: "fixture://" + url.UserPassword("user", t.Name()).String() + "@host/root"}
	result, err := client.ResolveDatabaseContext(c.Context(), request)
	c.Assert(err, qt.IsNil)
	c.Assert(proto.Equal(result, runtimeContext()), qt.IsTrue)
	peer.context.SourceDatabase = ""
	result, err = client.ResolveDatabaseContext(c.Context(), request)
	c.Assert(err, qt.ErrorIs, provider.ErrProtocol)
	c.Assert(result, qt.IsNil)
	peer.err = status.Error(codes.InvalidArgument, request.Address)
	_, err = client.ResolveDatabaseContext(c.Context(), request)
	c.Assert(err, qt.ErrorMatches, "provider resolve database context failed \\(InvalidArgument\\)")
	calls := peer.calls
	ctx, cancel := context.WithCancel(c.Context())
	cancel()
	_, err = client.ResolveDatabaseContext(ctx, request)
	c.Assert(err, qt.ErrorIs, context.Canceled)
	c.Assert(peer.calls, qt.Equals, calls)
	request.Engine = "foreign"
	_, err = client.ResolveDatabaseContext(c.Context(), request)
	c.Assert(err, qt.ErrorIs, provider.ErrProtocol)
	c.Assert(peer.calls, qt.Equals, calls)
}

func TestContextServerSnapshotsInputsAndDropsCanceledReceipts(t *testing.T) {
	c := qt.New(t)
	owner := &contextOwner{context: runtimeContext()}
	server := negotiatedServer(Server{Description: runtimeContextDescription(), DatabaseContexts: owner}, provider.DatabaseContextFeature)
	request := &protocol.DatabaseContextRequest{Engine: "fixture", Address: "opaque-address"}
	result, err := server.ResolveDatabaseContext(c.Context(), request)
	c.Assert(err, qt.IsNil)
	c.Assert(proto.Equal(result, runtimeContext()), qt.IsTrue)
	c.Assert(request.Engine, qt.Equals, "fixture")
	c.Assert(request.Address, qt.Equals, "opaque-address")
	ctx, cancel := context.WithCancel(c.Context())
	t.Cleanup(cancel)
	owner.cancel = cancel
	result, err = server.ResolveDatabaseContext(ctx, request)
	c.Assert(status.Code(err), qt.Equals, codes.Canceled)
	c.Assert(result, qt.IsNil)
	owner.cancel = nil
	owner.context = nil
	result, err = server.ResolveDatabaseContext(c.Context(), request)
	c.Assert(status.Code(err), qt.Equals, codes.Internal)
	c.Assert(result, qt.IsNil)
	unnegotiated := negotiatedServer(Server{Description: runtimeContextDescription(), DatabaseContexts: owner})
	calls := owner.calls
	_, err = unnegotiated.ResolveDatabaseContext(c.Context(), request)
	c.Assert(status.Code(err), qt.Equals, codes.InvalidArgument)
	c.Assert(owner.calls, qt.Equals, calls)
}

func TestOpenClosesAContextMismatchAndKeepsIndependentReceipts(t *testing.T) {
	c := qt.New(t)
	session := sessionReply()
	session.Database, session.Context = runtimeContext().Database, runtimeContext()
	peer := &contextPeer{sessionPeer: &sessionPeer{reply: session}, context: runtimeContext()}
	client := &Client{description: runtimeContextDescription(), rpc: peer}
	request := &protocol.OpenRequest{Engine: "fixture", Address: "opaque-address"}
	opened, err := client.Open(c.Context(), request)
	c.Assert(err, qt.IsNil)
	peer.reply.Context.SourceDatabase = "/foreign"
	c.Assert(opened.Context.SourceDatabase, qt.Equals, "/root")
	opened, err = client.Open(c.Context(), request)
	c.Assert(err, qt.ErrorIs, provider.ErrProtocol)
	c.Assert(opened, qt.IsNil)
	c.Assert(peer.closed, qt.DeepEquals, []string{"session"})
	owner := &sessionOwner{reply: session}
	resolver := &contextOwner{context: runtimeContext()}
	server := negotiatedServer(Server{Description: runtimeContextDescription(), Sessions: owner, DatabaseContexts: resolver}, provider.DatabaseContextFeature)
	opened, err = server.Open(c.Context(), request)
	c.Assert(status.Code(err), qt.Equals, codes.Internal)
	c.Assert(opened, qt.IsNil)
	c.Assert(owner.closed, qt.DeepEquals, []string{"session"})
}

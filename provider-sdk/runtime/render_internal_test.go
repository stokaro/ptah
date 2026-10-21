package runtime

// White-box testing required: a malicious renderer can mutate its input or
// return after cancellation; the public process boundary cannot inject these peers.

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

func renderExchange() (*protocol.Description, *protocol.RenderRequest, *protocol.RenderResult) {
	d, _, _ := normalizationExchange()
	d.Features = append(d.Features, provider.RenderFeature)
	request := &protocol.RenderRequest{Engine: "test", Changes: []*protocol.Change{{Id: "first", Object: &protocol.Reference{Engine: "test", Kind: "table", Name: "items"}, Operation: &protocol.Change_DropTable{DropTable: &protocol.Empty{}}}}}
	return d, request, &protocol.RenderResult{Complete: true, Units: []*protocol.Unit{{ChangeId: "first", Body: &protocol.Unit_Sql{Sql: "DROP TABLE items;"}}}}
}

type renderPeer struct {
	protocol.ProviderClient
	result *protocol.RenderResult
	calls  int
	cancel context.CancelFunc
}

func (p *renderPeer) Render(context.Context, *protocol.RenderRequest, ...grpc.CallOption) (*protocol.RenderResult, error) {
	p.calls++
	if p.cancel != nil {
		p.cancel()
	}
	return proto.CloneOf(p.result), nil
}

type renderFunc func(context.Context, *protocol.RenderRequest) (*protocol.RenderResult, error)

func (f renderFunc) Render(ctx context.Context, r *protocol.RenderRequest) (*protocol.RenderResult, error) {
	return f(ctx, r)
}

func TestRenderClientDiscardsPartialAndCanceledReplies(t *testing.T) {
	c := qt.New(t)
	d, r, valid := renderExchange()
	peer := &renderPeer{result: valid}
	client := &Client{description: d, rpc: peer}
	result, err := client.Render(c.Context(), r)
	c.Assert(err, qt.IsNil)
	c.Assert(proto.Equal(result, valid), qt.IsTrue)
	peer.result.Complete = false
	result, err = client.Render(c.Context(), r)
	c.Assert(err, qt.ErrorIs, provider.ErrProtocol)
	c.Assert(result, qt.IsNil)
	peer.result.Complete = true
	ctx, cancel := context.WithCancel(c.Context())
	defer cancel()
	peer.cancel = cancel
	result, err = client.Render(ctx, r)
	c.Assert(err, qt.ErrorIs, context.Canceled)
	c.Assert(result, qt.IsNil)
	result, err = client.Render(ctx, r)
	c.Assert(err, qt.ErrorIs, context.Canceled)
	c.Assert(result, qt.IsNil)
	c.Assert(peer.calls, qt.Equals, 3)
	r.Changes[0].Operation = nil
	result, err = client.Render(c.Context(), r)
	c.Assert(err, qt.ErrorIs, provider.ErrProtocol)
	c.Assert(result, qt.IsNil)
	c.Assert(peer.calls, qt.Equals, 3)
}

func TestRenderServerPreservesOriginalProvenance(t *testing.T) {
	c := qt.New(t)
	d, r, valid := renderExchange()
	adapter := &serverAdapter{server: Server{Description: d, Renderer: renderFunc(func(_ context.Context, request *protocol.RenderRequest) (*protocol.RenderResult, error) {
		request.Changes[0].Id = "changed"
		reply := proto.CloneOf(valid)
		reply.Units[0].ChangeId = "changed"
		return reply, nil
	})}}
	result, err := adapter.Render(c.Context(), r)
	c.Assert(status.Code(err), qt.Equals, codes.Internal)
	c.Assert(result, qt.IsNil)
	c.Assert(r.Changes[0].Id, qt.Equals, "first")
}

func TestRenderServerDiscardsReplyAfterCancellation(t *testing.T) {
	c := qt.New(t)
	d, r, valid := renderExchange()
	ctx, cancel := context.WithCancel(c.Context())
	defer cancel()
	adapter := &serverAdapter{server: Server{Description: d, Renderer: renderFunc(func(context.Context, *protocol.RenderRequest) (*protocol.RenderResult, error) {
		cancel()
		return valid, nil
	})}}
	result, err := adapter.Render(ctx, r)
	c.Assert(status.Code(err), qt.Equals, codes.Canceled)
	c.Assert(result, qt.IsNil)
}

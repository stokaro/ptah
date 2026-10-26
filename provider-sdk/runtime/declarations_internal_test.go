package runtime

// White-box testing required: inject malformed peers and mutating owners at both transport
// boundaries. The public client cannot install those behaviors by itself.

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

func runtimeDeclarationsExchange() (*protocol.Description, *protocol.PlanDeclarationsRequest, *protocol.PlannedDeclarations) {
	d, _, normalized := normalizationExchange()
	d.Features = append(d.Features, provider.SchemaDeclarationsFeature)
	engine := d.Engines[0]
	engine.Features = append(engine.Features, provider.SchemaDeclarationsFeature)
	kind := engine.ValueModels[0].Kind
	engine.Declarations = []*protocol.ModelDeclarations{{Id: "authored", Kinds: []string{kind}}}
	value := normalized.Values[0]
	value.Value.Owner.Kind, value.NormalizedOwner.Kind = kind, kind
	subject := &protocol.ComparisonIdentity{Kind: kind, Name: &protocol.ComparisonPart{Source: "original", Normalized: "original"}}
	request := &protocol.PlanDeclarationsRequest{Engine: "test", DeclarationId: "authored",
		Identifiers: &protocol.ComparisonIdentifiers{Rules: &protocol.IdentifierRules{TableNames: protocol.IdentifierRules_EXACT, ColumnNames: protocol.IdentifierRules_EXACT,
			IndexNames: protocol.IdentifierRules_EXACT, IndexNamespace: protocol.IdentifierRules_TABLE}},
		Objects: []*protocol.ComparisonRecord{{Kind: kind, Subject: subject, Value: value}}}
	result := &protocol.PlannedDeclarations{Complete: true, Declarations: []*protocol.DeclarationReceipt{{Subject: proto.CloneOf(subject), Strategy: "Common creation covers the declaration"}}}
	return d, request, result
}

type declarationsPeer struct {
	protocol.ProviderClient
	result *protocol.PlannedDeclarations
	err    error
	cancel context.CancelFunc
	calls  int
}

func (p *declarationsPeer) PlanDeclarations(context.Context, *protocol.PlanDeclarationsRequest, ...grpc.CallOption) (*protocol.PlannedDeclarations, error) {
	p.calls++
	if p.cancel != nil {
		p.cancel()
	}
	return proto.CloneOf(p.result), p.err
}

type declarationsFunc func(context.Context, *protocol.PlanDeclarationsRequest) (*protocol.PlannedDeclarations, error)

func (f declarationsFunc) PlanDeclarations(ctx context.Context, request *protocol.PlanDeclarationsRequest) (*protocol.PlannedDeclarations, error) {
	return f(ctx, request)
}

func TestDeclarationClientDiscardsPartialReplies(t *testing.T) {
	c := qt.New(t)
	d, request, valid := runtimeDeclarationsExchange()
	peer := &declarationsPeer{result: valid}
	client := &Client{description: d, rpc: peer}
	result, err := client.PlanDeclarations(c.Context(), request)
	c.Assert(err, qt.IsNil)
	c.Assert(proto.Equal(result, valid), qt.IsTrue)
	peer.result.Declarations = nil
	result, err = client.PlanDeclarations(c.Context(), request)
	c.Assert(err, qt.ErrorIs, provider.ErrProtocol)
	c.Assert(result, qt.IsNil)
	peer.err = status.Error(codes.Unavailable, "private value")
	result, err = client.PlanDeclarations(c.Context(), request)
	c.Assert(err.Error(), qt.Not(qt.Contains), "private value")
	c.Assert(result, qt.IsNil)
	request.Engine = ""
	result, err = client.PlanDeclarations(c.Context(), request)
	c.Assert(err, qt.ErrorIs, provider.ErrProtocol)
	c.Assert(result, qt.IsNil)
	c.Assert(peer.calls, qt.Equals, 3)
}

func TestDeclarationServerPreservesInputAndCancellation(t *testing.T) {
	c := qt.New(t)
	d, request, valid := runtimeDeclarationsExchange()
	before := proto.CloneOf(request)
	owner := declarationsFunc(func(_ context.Context, request *protocol.PlanDeclarationsRequest) (*protocol.PlannedDeclarations, error) {
		request.Objects[0].Subject.Name.Source = "changed"
		changed := proto.CloneOf(valid)
		changed.Declarations[0].Subject = request.Objects[0].Subject
		return changed, nil
	})
	server := negotiatedServer(Server{Description: d, Declarations: owner}, provider.SchemaDeclarationsFeature)
	result, err := server.PlanDeclarations(c.Context(), request)
	c.Assert(status.Code(err), qt.Equals, codes.Internal)
	c.Assert(result, qt.IsNil)
	c.Assert(proto.Equal(before, request), qt.IsTrue)
	ctx, cancel := context.WithCancel(c.Context())
	t.Cleanup(cancel)
	server.server.Declarations = declarationsFunc(func(context.Context, *protocol.PlanDeclarationsRequest) (*protocol.PlannedDeclarations, error) {
		cancel()
		return valid, nil
	})
	result, err = server.PlanDeclarations(ctx, request)
	c.Assert(status.Code(err), qt.Equals, codes.Canceled)
	c.Assert(result, qt.IsNil)
	peer := &declarationsPeer{result: valid, cancel: cancel}
	client := &Client{description: d, rpc: peer}
	result, err = client.PlanDeclarations(ctx, request)
	c.Assert(err, qt.ErrorIs, context.Canceled)
	c.Assert(result, qt.IsNil)
	c.Assert(peer.calls, qt.Equals, 0)
}

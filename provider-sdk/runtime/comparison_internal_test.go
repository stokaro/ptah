package runtime

// White-box tests bypass the SDK server to exercise malformed peer replies.
// A mutating owner also must not rewrite the server's original exclusions.

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

func runtimeComparisonExchange() (*protocol.Description, *protocol.CompareModelsRequest, *protocol.ComparedModels) {
	d, _, _ := normalizationExchange()
	d.Features = append(d.Features, provider.CompareModelsFeature)
	engine := d.Engines[0]
	observed := proto.CloneOf(engine.ValueModels[0])
	observed.Representation = protocol.ValueInput_OBSERVED
	engine.ValueModels = append(engine.ValueModels, observed)
	engine.Comparisons = []*protocol.ModelComparison{{Id: "settings", Placement: protocol.ModelComparison_FACETS, Kinds: []string{observed.Kind}, OwnerKinds: []string{"table"}}}
	rules := &protocol.IdentifierRules{TableNames: protocol.IdentifierRules_EXACT, ColumnNames: protocol.IdentifierRules_EXACT, IndexNames: protocol.IdentifierRules_EXACT, IndexNamespace: protocol.IdentifierRules_TABLE}
	desired := &protocol.ComparisonState{Representation: protocol.ValueInput_DESIRED, Coverage: &protocol.ComparisonCoverage{}, Records: []*protocol.ComparisonRecord{{Kind: observed.Kind, Targets: []string{"other"}, Subject: &protocol.ComparisonIdentity{Kind: "table", Name: &protocol.ComparisonPart{Source: "excluded", Normalized: "excluded"}}}}}
	request := &protocol.CompareModelsRequest{Engine: "test", ComparisonId: "settings", Identifiers: &protocol.ComparisonIdentifiers{Rules: rules}, Kinds: []string{observed.Kind}, Desired: desired, Current: &protocol.ComparisonState{Representation: protocol.ValueInput_OBSERVED, Coverage: &protocol.ComparisonCoverage{}}}
	return d, request, &protocol.ComparedModels{Complete: true, Desired: proto.CloneOf(desired)}
}

func TestComparisonClientDiscardsPartialAndUntrustedReplies(t *testing.T) {
	c := qt.New(t)
	description, request, valid := runtimeComparisonExchange()
	peer := &comparisonPeer{result: proto.CloneOf(valid)}
	client := &Client{description: description, rpc: peer}
	result, err := client.CompareModels(c.Context(), request)
	c.Assert(err, qt.IsNil)
	c.Assert(proto.Equal(result, valid), qt.IsTrue)
	peer.result.Complete = false
	result, err = client.CompareModels(c.Context(), request)
	c.Assert(err, qt.ErrorIs, provider.ErrProtocol)
	c.Assert(result, qt.IsNil)
	peer.result = proto.CloneOf(valid)
	peer.result.Desired.Records = nil
	result, err = client.CompareModels(c.Context(), request)
	c.Assert(err, qt.ErrorIs, provider.ErrProtocol)
	c.Assert(result, qt.IsNil)
	peer.err = status.Error(codes.Unavailable, "password=secret")
	result, err = client.CompareModels(c.Context(), request)
	c.Assert(err, qt.ErrorMatches, `provider compare models failed \(Unavailable\)`)
	c.Assert(result, qt.IsNil)
	request.ComparisonId = "foreign"
	result, err = client.CompareModels(c.Context(), request)
	c.Assert(err, qt.ErrorIs, provider.ErrProtocol)
	c.Assert(result, qt.IsNil)
	c.Assert(peer.calls, qt.Equals, 4)
}

func TestComparisonServerPreservesContextAndCancellation(t *testing.T) {
	c := qt.New(t)
	description, request, _ := runtimeComparisonExchange()
	original := proto.CloneOf(request)
	owner := &mutatingComparer{}
	server := &serverAdapter{server: Server{Description: description, ModelComparer: owner}}
	result, err := server.CompareModels(c.Context(), request)
	c.Assert(status.Code(err), qt.Equals, codes.Internal)
	c.Assert(result, qt.IsNil)
	c.Assert(proto.Equal(request, original), qt.IsTrue)
	ctx, cancel := context.WithCancel(c.Context())
	cancel()
	result, err = server.CompareModels(ctx, request)
	c.Assert(status.Code(err), qt.Equals, codes.Canceled)
	c.Assert(result, qt.IsNil)
	c.Assert(owner.calls, qt.Equals, 1)
	ctx, cancel = context.WithCancel(c.Context())
	defer cancel()
	owner.cancel = cancel
	result, err = server.CompareModels(ctx, request)
	c.Assert(status.Code(err), qt.Equals, codes.Canceled)
	c.Assert(result, qt.IsNil)
	c.Assert(owner.calls, qt.Equals, 2)
}

type comparisonPeer struct {
	protocol.ProviderClient
	result *protocol.ComparedModels
	err    error
	calls  int
}

func (p *comparisonPeer) CompareModels(context.Context, *protocol.CompareModelsRequest, ...grpc.CallOption) (*protocol.ComparedModels, error) {
	p.calls++
	return p.result, p.err
}

type mutatingComparer struct {
	calls  int
	cancel context.CancelFunc
}

func (p *mutatingComparer) CompareModels(_ context.Context, request *protocol.CompareModelsRequest) (*protocol.ComparedModels, error) {
	p.calls++
	request.Desired.Records = nil
	if p.cancel != nil {
		p.cancel()
	}
	return &protocol.ComparedModels{Complete: true, Desired: request.Desired}, nil
}

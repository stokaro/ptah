package runtime

// White-box testing required: inject peers that bypass server validation and
// owners that mutate their request or return after cancellation.

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

func runtimePlanningExchange() (*protocol.Description, *protocol.PlanFeaturesRequest, *protocol.PlannedFeatures) {
	d, comparison, _ := runtimeComparisonExchange()
	d.Features = append(d.Features, provider.PlanFeaturesFeature)
	kind := d.Engines[0].ValueModels[0].Kind
	d.Engines[0].Planning = []*protocol.ModelPlanning{{Id: "settings", ParentKinds: []string{kind}}}
	subject := proto.CloneOf(comparison.Desired.Records[0].Subject)
	empty := func(rep protocol.ValueInput_Representation) *protocol.CapturedFeatures {
		state := &protocol.ComparisonState{Representation: rep, Coverage: &protocol.ComparisonCoverage{}}
		return &protocol.CapturedFeatures{Objects: state, Facets: proto.CloneOf(state)}
	}
	request := &protocol.PlanFeaturesRequest{Engine: comparison.Engine, PlanningId: "settings", Identifiers: comparison.Identifiers, ParentKinds: []string{kind},
		Tables: []*protocol.PlanningTable{{Subject: subject, Action: protocol.PlanningTable_ALTER, Desired: &protocol.DeclaredTable{Name: "excluded", Features: empty(protocol.ValueInput_DESIRED)}, Current: &protocol.ObservedTable{Name: "excluded", Features: empty(protocol.ValueInput_OBSERVED)}}}}
	result := &protocol.PlannedFeatures{Complete: true, Parents: []*protocol.PlanningParentReceipt{{Action: protocol.PlanningTable_ALTER, Receipt: &protocol.PlanningReceipt{Subject: subject, Kind: kind, Strategy: "No settings require a separate operation"}}}}
	return d, request, result
}

type planningPeer struct {
	protocol.ProviderClient
	reply  *protocol.PlannedFeatures
	err    error
	cancel context.CancelFunc
	calls  int
}

func (p *planningPeer) PlanFeatures(context.Context, *protocol.PlanFeaturesRequest, ...grpc.CallOption) (*protocol.PlannedFeatures, error) {
	p.calls++
	if p.cancel != nil {
		p.cancel()
	}
	return proto.CloneOf(p.reply), p.err
}

type planningFunc func(context.Context, *protocol.PlanFeaturesRequest) (*protocol.PlannedFeatures, error)

func (f planningFunc) PlanFeatures(ctx context.Context, r *protocol.PlanFeaturesRequest) (*protocol.PlannedFeatures, error) {
	return f(ctx, r)
}

func TestPlanningClientDiscardsPartialRepliesAndProviderErrors(t *testing.T) {
	c := qt.New(t)
	d, r, valid := runtimePlanningExchange()
	peer := &planningPeer{reply: proto.CloneOf(valid)}
	client := &Client{description: d, rpc: peer}
	result, err := client.PlanFeatures(c.Context(), r)
	c.Assert(err, qt.IsNil)
	c.Assert(proto.Equal(result, valid), qt.IsTrue)
	peer.reply.Parents = nil
	result, err = client.PlanFeatures(c.Context(), r)
	c.Assert(err, qt.ErrorIs, provider.ErrProtocol)
	c.Assert(result, qt.IsNil)
	peer.reply = proto.CloneOf(valid)
	peer.err = status.Error(codes.Unavailable, "password=secret")
	result, err = client.PlanFeatures(c.Context(), r)
	c.Assert(err, qt.ErrorMatches, `provider plan features failed \(Unavailable\)`)
	c.Assert(result, qt.IsNil)
	r.PlanningId = "foreign"
	result, err = client.PlanFeatures(c.Context(), r)
	c.Assert(err, qt.ErrorIs, provider.ErrProtocol)
	c.Assert(result, qt.IsNil)
	c.Assert(peer.calls, qt.Equals, 3)
}

func TestPlanningCancellationDiscardsOtherwiseValidReplies(t *testing.T) {
	c := qt.New(t)
	d, r, valid := runtimePlanningExchange()
	ctx, cancel := context.WithCancel(c.Context())
	defer cancel()
	peer := &planningPeer{reply: valid, cancel: cancel}
	client := &Client{description: d, rpc: peer}
	result, err := client.PlanFeatures(ctx, r)
	c.Assert(err, qt.ErrorIs, context.Canceled)
	c.Assert(result, qt.IsNil)
	result, err = client.PlanFeatures(ctx, r)
	c.Assert(err, qt.ErrorIs, context.Canceled)
	c.Assert(result, qt.IsNil)
	c.Assert(peer.calls, qt.Equals, 1)
	ctx, cancelServer := context.WithCancel(c.Context())
	defer cancelServer()
	server := &serverAdapter{server: Server{Description: d, FeaturePlanner: planningFunc(func(context.Context, *protocol.PlanFeaturesRequest) (*protocol.PlannedFeatures, error) {
		cancelServer()
		return valid, nil
	})}}
	result, err = server.PlanFeatures(ctx, r)
	c.Assert(status.Code(err), qt.Equals, codes.Canceled)
	c.Assert(result, qt.IsNil)
}

func TestPlanningServerRetainsOriginalParentAssignment(t *testing.T) {
	c := qt.New(t)
	d, r, _ := runtimePlanningExchange()
	before := proto.CloneOf(r)
	server := &serverAdapter{server: Server{Description: d, FeaturePlanner: planningFunc(func(_ context.Context, request *protocol.PlanFeaturesRequest) (*protocol.PlannedFeatures, error) {
		request.ParentKinds = nil
		return &protocol.PlannedFeatures{Complete: true}, nil
	})}}
	result, err := server.PlanFeatures(c.Context(), r)
	c.Assert(status.Code(err), qt.Equals, codes.Internal)
	c.Assert(result, qt.IsNil)
	c.Assert(proto.Equal(before, r), qt.IsTrue)
}

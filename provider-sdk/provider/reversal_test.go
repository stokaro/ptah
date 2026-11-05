package provider_test

import (
	"slices"
	"testing"

	qt "github.com/frankban/quicktest"
	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
	"github.com/stokaro/ptah/provider-sdk/provider"
	"google.golang.org/protobuf/proto"
)

// reversalExchange is a reversal of the one retention change the comparison
// exchange returns, on a provider and an engine that list models.reverse,
// and its valid reply: the reverse change, the facet the forward change
// leaves, and how the reverse restores it.
func reversalExchange() (*protocol.Description, *protocol.ReverseChangesRequest, *protocol.ReversedChanges) {
	d, comparison, compared := comparisonExchange()
	d.Features = append(d.Features, provider.PlanFeaturesFeature, provider.ReverseChangesFeature)
	engine := d.Engines[0]
	engine.Features = append(engine.Features, provider.ReverseChangesFeature)
	change := compared.Changes[0]
	engine.Reversals = []*protocol.ModelReversal{{Id: "retention", ChangeKinds: []string{change.Value.Value.Kind}}}
	request := &protocol.ReverseChangesRequest{Engine: comparison.Engine, ReversalId: "retention",
		TargetFeatures: append(slices.Clone(comparison.TargetFeatures), provider.ReverseChangesFeature), Identifiers: proto.CloneOf(comparison.Identifiers),
		Capabilities: map[string]bool{"transactional_ddl": false},
		Changes:      []*protocol.PlanningChange{{Subject: proto.CloneOf(change.Subject), Value: proto.CloneOf(change.Value)}}}
	reversed := proto.CloneOf(change.Value)
	reversed.Id = "reverse"
	result := &protocol.ReversedChanges{Complete: true, Changes: []*protocol.ReversedChange{{
		Subject: proto.CloneOf(change.Subject), Value: reversed, Strategy: "Restore the retention in place",
		ForwardState: []*protocol.ForwardState{{Placement: protocol.ForwardState_FACET, Kind: change.Kind, Value: proto.CloneOf(comparison.Current.Records[0].Value)}},
	}}}
	return d, request, result
}

func TestReverseChanges_HappyPath(t *testing.T) {
	t.Parallel()
	c := qt.New(t)
	d, request, result := reversalExchange()
	before := proto.CloneOf(request)

	c.Assert(provider.ValidateDescription(d, nil), qt.IsNil)
	c.Assert(provider.ValidateReversedChanges(d, request, result), qt.IsNil)
	c.Assert(proto.Equal(before, request), qt.IsTrue)
	c.Assert(provider.TwoWayFeatures(), qt.Contains, provider.ReverseChangesFeature)
	negotiated := provider.NegotiatedDescription(d, []string{provider.ReverseChangesFeature})
	c.Assert(negotiated.Engines[0].Reversals, qt.HasLen, 1)
	c.Assert(provider.ValidateReverseChangesRequest(negotiated, request), qt.IsNil)
}

// A reverse with nothing to run is a result without a value that says what
// the rollback leaves, and a forward state may establish an absence.
func TestReverseChanges_AReverseWithNothingToRun(t *testing.T) {
	t.Parallel()
	c := qt.New(t)
	d, request, result := reversalExchange()
	result.Changes[0].Value = nil
	result.Changes[0].Limitations = []string{"The server never returns the value, so the rollback keeps the new one"}
	result.Changes[0].ForwardState[0].Value = nil

	c.Assert(provider.ValidateReversedChanges(d, request, result), qt.IsNil)
}

// A host that does not list the feature receives no reversal service and
// cannot ask for one.
func TestReverseChanges_NeedsTheHostsFeature(t *testing.T) {
	t.Parallel()
	c := qt.New(t)
	d, request, _ := reversalExchange()

	negotiated := provider.NegotiatedDescription(d, nil)

	c.Assert(negotiated.Engines[0].Reversals, qt.HasLen, 0)
	c.Assert(negotiated.Engines[0].Features, qt.Not(qt.Contains), provider.ReverseChangesFeature)
	c.Assert(provider.ValidateDescription(negotiated, nil), qt.IsNil)
	c.Assert(provider.ValidateReverseChangesRequest(negotiated, request), qt.ErrorMatches, `.*change reversal: the feature was not negotiated`)
}

func TestReverseChangesDescription_FailurePath(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		mutate  func(*protocol.Description)
		wantErr string
	}{
		{"services without the feature", func(d *protocol.Description) {
			d.Engines[0].Features = slices.DeleteFunc(d.Engines[0].Features, func(f string) bool { return f == provider.ReverseChangesFeature })
		}, `.*the engine declares reversal services without the feature`},
		{"the engine's feature without the provider's", func(d *protocol.Description) {
			d.Features = slices.DeleteFunc(d.Features, func(f string) bool { return f == provider.ReverseChangesFeature })
		}, `.*the engine requires the provider's feature, planning and comparison`},
		{"without planning", func(d *protocol.Description) {
			d.Features = slices.DeleteFunc(d.Features, func(f string) bool { return f == provider.PlanFeaturesFeature })
		}, `.*the engine requires the provider's feature, planning and comparison`},
		{"the feature without a service", func(d *protocol.Description) { d.Engines[0].Reversals = nil },
			`.*the engine lists the feature and declares no reversal service`},
		{"an unnamed service", func(d *protocol.Description) { d.Engines[0].Reversals[0].Id = "" },
			`.*a reversal service is unnamed, repeated or owns no change kind`},
		{"a service without kinds", func(d *protocol.Description) { d.Engines[0].Reversals[0].ChangeKinds = nil },
			`.*a reversal service is unnamed, repeated or owns no change kind`},
		{"a repeated service", func(d *protocol.Description) {
			d.Engines[0].Reversals = append(d.Engines[0].Reversals, proto.CloneOf(d.Engines[0].Reversals[0]))
		}, `.*a reversal service is unnamed, repeated or owns no change kind`},
		{"a kind two services own", func(d *protocol.Description) {
			second := proto.CloneOf(d.Engines[0].Reversals[0])
			second.Id = "second"
			d.Engines[0].Reversals = append(d.Engines[0].Reversals, second)
		}, `.*a change kind has no CHANGE model or two reversal services`},
		{"a kind with no CHANGE model", func(d *protocol.Description) {
			d.Engines[0].Reversals[0].ChangeKinds = []string{d.Engines[0].ValueModels[0].Kind}
		}, `.*a change kind has no CHANGE model or two reversal services`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			c := qt.New(t)
			d, _, _ := reversalExchange()
			test.mutate(d)
			c.Assert(provider.ValidateDescription(d, nil), qt.ErrorMatches, test.wantErr)
		})
	}
}

func TestReverseChangesRequest_FailurePath(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		mutate  func(*protocol.ReverseChangesRequest)
		wantErr string
	}{
		{"a foreign service", func(r *protocol.ReverseChangesRequest) { r.ReversalId = "foreign" }, `.*the reversal service is unavailable`},
		{"without the target feature", func(r *protocol.ReverseChangesRequest) {
			r.TargetFeatures = slices.DeleteFunc(r.TargetFeatures, func(f string) bool { return f == provider.ReverseChangesFeature })
		}, `.*the engine does not reverse changes`},
		{"without identifier rules", func(r *protocol.ReverseChangesRequest) { r.Identifiers = nil }, `.*the request carries no identifier rules`},
		{"without a change", func(r *protocol.ReverseChangesRequest) { r.Changes = nil }, `.*the request carries no change`},
		{"a change another service owns", func(r *protocol.ReverseChangesRequest) { r.Changes[0].Value.Value.Kind = "example.org/fixture/other" },
			`.*a change kind belongs to another service`},
		{"a change twice", func(r *protocol.ReverseChangesRequest) { r.Changes = append(r.Changes, proto.CloneOf(r.Changes[0])) },
			`.*a subject and change kind appear twice`},
		{"a change without a subject", func(r *protocol.ReverseChangesRequest) { r.Changes[0].Subject = nil }, `.*`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			c := qt.New(t)
			d, request, _ := reversalExchange()
			test.mutate(request)
			c.Assert(provider.ValidateReverseChangesRequest(d, request), qt.ErrorIs, provider.ErrProtocol)
			c.Assert(provider.ValidateReverseChangesRequest(d, request), qt.ErrorMatches, test.wantErr)
		})
	}
}

func TestReversedChanges_FailurePath(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		mutate  func(*protocol.ReversedChanges)
		wantErr string
	}{
		{"incomplete", func(r *protocol.ReversedChanges) { r.Complete = false }, `.*the reply is incomplete`},
		{"a result short", func(r *protocol.ReversedChanges) { r.Changes = nil }, `.*the reply is incomplete`},
		{"another subject", func(r *protocol.ReversedChanges) { r.Changes[0].Subject.Name.Normalized = "D" }, `.*a result changed its subject`},
		{"no strategy", func(r *protocol.ReversedChanges) { r.Changes[0].Strategy = "" }, `.*a strategy or a limitation is not one line`},
		{"a limitation on two lines", func(r *protocol.ReversedChanges) { r.Changes[0].Limitations = []string{"rows\nare lost"} },
			`.*a strategy or a limitation is not one line`},
		{"nothing to run and no limitation", func(r *protocol.ReversedChanges) { r.Changes[0].Value = nil },
			`.*a reverse without a change states no limitation`},
		{"another change kind", func(r *protocol.ReversedChanges) { r.Changes[0].Value.Value.Kind = "example.org/fixture/other" },
			`.*a result changed the change kind`},
		{"a forward state without placement", func(r *protocol.ReversedChanges) {
			r.Changes[0].ForwardState[0].Placement = protocol.ForwardState_PLACEMENT_UNSPECIFIED
		},
			`.*a forward state has no placement`},
		{"an object state of another kind", func(r *protocol.ReversedChanges) {
			r.Changes[0].ForwardState[0].Placement = protocol.ForwardState_OBJECT
		},
			`.*an object state names another kind than its subject`},
		{"a state of a kind with no OBSERVED model", func(r *protocol.ReversedChanges) {
			r.Changes[0].ForwardState[0].Kind = "example.org/fixture/retention-change"
			r.Changes[0].ForwardState[0].Value = nil
		}, `.*a forward state names a kind with no OBSERVED model`},
		{"one state twice", func(r *protocol.ReversedChanges) {
			r.Changes[0].ForwardState = append(r.Changes[0].ForwardState, proto.CloneOf(r.Changes[0].ForwardState[0]))
		}, `.*a subject's forward state names one placement and kind twice`},
		{"a state value of another kind", func(r *protocol.ReversedChanges) {
			r.Changes[0].ForwardState[0].Value.Value.Kind = "example.org/fixture/other"
		},
			`.*a forward state value differs from its kind`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			c := qt.New(t)
			d, request, result := reversalExchange()
			test.mutate(result)
			c.Assert(provider.ValidateReversedChanges(d, request, result), qt.ErrorMatches, test.wantErr)
		})
	}
}

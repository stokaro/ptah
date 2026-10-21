package provider_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
	"github.com/stokaro/ptah/provider-sdk/provider"
	"google.golang.org/protobuf/proto"
)

func comparisonExchange() (*protocol.Description, *protocol.CompareModelsRequest, *protocol.ComparedModels) {
	description, inputs := valueInputs()
	description.Features = append(description.Features, provider.CompareModelsFeature)
	engine := description.Engines[0]
	desired := engine.ValueModels[0]
	observed, change := proto.CloneOf(desired), proto.CloneOf(desired)
	observed.Representation = protocol.ValueInput_OBSERVED
	change.Representation, change.Kind = protocol.ValueInput_CHANGE, "example.org/fixture/retention-change"
	engine.ValueModels = append(engine.ValueModels, observed, change)
	engine.Comparisons = []*protocol.ModelComparison{{Id: "retention", Placement: protocol.ModelComparison_FACETS, Kinds: []string{desired.Kind}, ChangeKinds: []string{change.Kind}, OwnerKinds: []string{"table"}}}
	subject := &protocol.ComparisonIdentity{Kind: "table", Name: &protocol.ComparisonPart{Source: "C", Normalized: "C"}, Schema: &protocol.ComparisonPart{Source: "a.b", Normalized: "a.b", Quoted: true}}
	value := normalizedValues(inputs).Values[0]
	complete := &protocol.ModelKnowledge{State: protocol.ModelKnowledge_COMPLETE}
	state := &protocol.ComparisonState{Representation: protocol.ValueInput_DESIRED, Records: []*protocol.ComparisonRecord{{Kind: desired.Kind, Subject: subject, Value: value}}, Coverage: &protocol.ComparisonCoverage{Kinds: []*protocol.ComparisonKindCoverage{{Model: proto.CloneOf(desired), Sources: []*protocol.ComparisonKnowledgeSource{{Outside: complete}}}}}}
	current := proto.CloneOf(state)
	current.Representation = protocol.ValueInput_OBSERVED
	current.Coverage.Kinds[0].Model = observed
	request := &protocol.CompareModelsRequest{Engine: "fixture", ComparisonId: "retention", TargetFeatures: []string{"retention"}, Identifiers: &protocol.ComparisonIdentifiers{Rules: proto.CloneOf(engine.Identifiers)}, Kinds: []string{desired.Kind}, Desired: state, Current: current, Parents: []*protocol.ComparisonParent{{Subject: proto.CloneOf(subject), Desired: true, Current: true}}}
	changed := proto.CloneOf(value)
	changed.Value.Kind = change.Kind
	changed.ChangeSemantics = &protocol.ChangeSemantics{ReplacesOwner: new(false), RemovesState: new(false), Impact: protocol.ChangeSemantics_BEHAVIORAL, Reason: "Retention changes", Access: protocol.ChangeSemantics_UNCHANGED, AccessReason: "No access change"}
	result := &protocol.ComparedModels{Complete: true, Desired: proto.CloneOf(state), Changes: []*protocol.ComparedChange{{Kind: desired.Kind, Subject: proto.CloneOf(subject), Value: changed}}}
	return description, request, result
}

func TestComparisonRefusesMalformedContext(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*protocol.Description, *protocol.CompareModelsRequest)
	}{
		{"unadvertised", func(d *protocol.Description, _ *protocol.CompareModelsRequest) { d.Features = nil }},
		{"unowned service", func(_ *protocol.Description, r *protocol.CompareModelsRequest) { r.ComparisonId = "foreign" }},
		{"unowned model", func(_ *protocol.Description, r *protocol.CompareModelsRequest) { r.Kinds = []string{"foreign"} }},
		{"missing rules", func(_ *protocol.Description, r *protocol.CompareModelsRequest) { r.Identifiers = nil }},
		{"missing state", func(_ *protocol.Description, r *protocol.CompareModelsRequest) { r.Current = nil }},
		{"missing coverage", func(_ *protocol.Description, r *protocol.CompareModelsRequest) { r.Current.Coverage = nil }},
		{"wrong representation", func(_ *protocol.Description, r *protocol.CompareModelsRequest) {
			r.Current.Representation = protocol.ValueInput_DESIRED
		}},
		{"changed coverage definition", func(_ *protocol.Description, r *protocol.CompareModelsRequest) {
			r.Current.Coverage.Kinds[0].Model.CanonicalFormat = "foreign"
		}},
		{"unknown coverage", func(_ *protocol.Description, r *protocol.CompareModelsRequest) {
			r.Current.Coverage.Kinds[0].Sources[0].Outside.State = 99
		}},
		{"observed default", func(_ *protocol.Description, r *protocol.CompareModelsRequest) {
			r.Current.Coverage.Subjects = []*protocol.ComparisonSubjectCoverage{{Kind: r.Kinds[0], Subject: r.Parents[0].Subject, Knowledge: &protocol.ModelKnowledge{State: protocol.ModelKnowledge_DEFAULTED}}}
		}},
		{"unbounded region", func(_ *protocol.Description, r *protocol.CompareModelsRequest) {
			r.Current.Coverage.Kinds[0].Sources[0].Regions = []*protocol.ComparisonRegion{{Scope: &protocol.ComparisonNamespace{}, Knowledge: &protocol.ModelKnowledge{State: protocol.ModelKnowledge_COMPLETE}}}
		}},
		{"duplicate record", func(_ *protocol.Description, r *protocol.CompareModelsRequest) {
			r.Desired.Records = append(r.Desired.Records, proto.CloneOf(r.Desired.Records[0]))
		}},
		{"unowned facet parent", func(_ *protocol.Description, r *protocol.CompareModelsRequest) {
			r.Desired.Records[0].Subject.Kind = "column"
		}},
		{"missing normalized name", func(_ *protocol.Description, r *protocol.CompareModelsRequest) {
			r.Desired.Records[0].Subject.Name.Normalized = ""
		}},
		{"unbound exclusion", func(_ *protocol.Description, r *protocol.CompareModelsRequest) { r.Desired.Records[0].Value = nil }},
		{"foreign payload owner", func(_ *protocol.Description, r *protocol.CompareModelsRequest) {
			r.Desired.Records[0].Value.Value.ProviderId = "foreign"
		}},
		{"object context in facet call", func(_ *protocol.Description, r *protocol.CompareModelsRequest) { r.DatabasePath = "/database" }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			description, request, _ := comparisonExchange()
			c.Assert(provider.ValidateDescription(description, nil), qt.IsNil)
			c.Assert(provider.ValidateComparisonRequest(description, request), qt.IsNil)
			test.mutate(description, request)
			c.Assert(provider.ValidateComparisonRequest(description, request), qt.ErrorIs, provider.ErrProtocol)
		})
	}
}

func TestComparisonRefusesMalformedResults(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*protocol.ComparedModels)
	}{
		{"incomplete", func(r *protocol.ComparedModels) { r.Complete = false }},
		{"missing desired", func(r *protocol.ComparedModels) { r.Desired = nil }},
		{"foreign change", func(r *protocol.ComparedModels) { r.Changes[0].Value.Value.Kind = "foreign" }},
		{"missing change facts", func(r *protocol.ComparedModels) { r.Changes[0].Value.ChangeSemantics = nil }},
		{"implicit replacement", func(r *protocol.ComparedModels) { r.Changes[0].Value.ChangeSemantics.ReplacesOwner = nil }},
		{"implicit removal", func(r *protocol.ComparedModels) { r.Changes[0].Value.ChangeSemantics.RemovesState = nil }},
		{"unknown impact", func(r *protocol.ComparedModels) { r.Changes[0].Value.ChangeSemantics.Impact = 99 }},
		{"unspecified access", func(r *protocol.ComparedModels) {
			r.Changes[0].Value.ChangeSemantics.Access = protocol.ChangeSemantics_ACCESS_UNSPECIFIED
		}},
		{"multiline reason", func(r *protocol.ComparedModels) { r.Changes[0].Value.ChangeSemantics.Reason = "first\nsecond" }},
		{"duplicate change", func(r *protocol.ComparedModels) { r.Changes = append(r.Changes, proto.CloneOf(r.Changes[0])) }},
		{"invented exclusion", func(r *protocol.ComparedModels) {
			r.Desired.Records[0].Value = nil
			r.Desired.Records[0].Targets = []string{"other"}
		}},
		{"unbound diagnostic", func(r *protocol.ComparedModels) {
			r.Undecided = []*protocol.ComparisonUndecided{{Kind: r.Desired.Records[0].Kind, Reason: "Unknown state"}}
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			description, request, result := comparisonExchange()
			c.Assert(provider.ValidateComparedModels(description, request, result), qt.IsNil)
			test.mutate(result)
			c.Assert(provider.ValidateComparedModels(description, request, result), qt.ErrorIs, provider.ErrProtocol)
		})
	}
}

func TestComparisonRetainsExclusionsAndIndependentCoverage(t *testing.T) {
	c := qt.New(t)
	description, request, result := comparisonExchange()
	excluded := request.Desired.Records[0]
	excluded.Value, excluded.Targets = nil, []string{"other"}
	unknown := &protocol.ComparisonKnowledgeSource{Outside: &protocol.ModelKnowledge{State: protocol.ModelKnowledge_UNINSPECTED, Reason: "Other source was not inspected"}, Regions: []*protocol.ComparisonRegion{{Scope: &protocol.ComparisonNamespace{Schema: &protocol.ComparisonPart{}, PathExact: true}, Knowledge: &protocol.ModelKnowledge{State: protocol.ModelKnowledge_COMPLETE}}}}
	request.Desired.Coverage.Kinds[0].Sources = append(request.Desired.Coverage.Kinds[0].Sources, unknown)
	request.Capabilities = map[string]bool{"extensions": false}
	result.Desired, result.Changes = proto.CloneOf(request.Desired), nil
	original := proto.CloneOf(request)
	c.Assert(provider.ValidateComparedModels(description, request, result), qt.IsNil)
	c.Assert(proto.Equal(request, original), qt.IsTrue)
	result.Undecided = []*protocol.ComparisonUndecided{{Kind: excluded.Kind, Subject: proto.CloneOf(excluded.Subject), Reason: "Unknown state"}}
	c.Assert(provider.ValidateComparedModels(description, request, result), qt.ErrorIs, provider.ErrProtocol)
	result.Undecided = nil
	result.Desired.Records = nil
	c.Assert(provider.ValidateComparedModels(description, request, result), qt.ErrorIs, provider.ErrProtocol)
}

func TestComparisonIdentityPreservesComponentBoundaries(t *testing.T) {
	c := qt.New(t)
	left := &protocol.ComparisonIdentity{Kind: "table", Name: &protocol.ComparisonPart{Source: "Name", Normalized: "name"}, Path: []*protocol.ComparisonPart{{Source: "a/b", Normalized: "a/b"}, {Source: "c", Normalized: "c"}}}
	right := proto.CloneOf(left)
	right.Name.Source, right.Name.Quoted, right.Name.Defaulted = "name", true, true
	c.Assert(provider.CompareIdentities(left, right), qt.Equals, 0)
	right.Path = []*protocol.ComparisonPart{{Source: "a", Normalized: "a"}, {Source: "b/c", Normalized: "b/c"}}
	c.Assert(provider.CompareIdentities(left, right), qt.Not(qt.Equals), 0)
	right = proto.CloneOf(left)
	right.Parent = &protocol.ComparisonPart{Source: "parent", Normalized: "parent"}
	c.Assert(provider.CompareIdentities(left, right), qt.Not(qt.Equals), 0)
}

package provider_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"google.golang.org/protobuf/proto"

	protocol "ptah.run/provider-sdk/protocol/experimental"
	"ptah.run/provider-sdk/provider"
)

// renameExchange is a planning request whose altered table C holds the index
// c_by_label and declares it as c_by_name, with the common step that renames
// it, on a provider and an engine that negotiated index renames.
func renameExchange() (*protocol.Description, *protocol.PlanFeaturesRequest) {
	d, request, _ := planningExchange()
	features := []string{provider.ObservedColumnsFeature, provider.ObservedIndexesFeature, provider.IndexRenamesFeature}
	d.Features = append(d.Features, features...)
	d.Engines[0].Features = append(d.Engines[0].Features, features...)
	request.TargetFeatures = append(request.TargetFeatures, provider.IndexRenamesFeature)
	table := request.Tables[0]
	table.Desired.Indexes = []*protocol.DeclaredIndex{{Name: "c_by_name", Fields: []string{"id"}}}
	table.Current.Indexes = []*protocol.ObservedIndex{{Name: "c_by_label", TableName: "C", Schema: "a.b", Columns: []string{"id"}}}
	index := func(name string) *protocol.ComparisonIdentity {
		return &protocol.ComparisonIdentity{Kind: "index", Schema: proto.CloneOf(table.Subject.Schema), Parent: proto.CloneOf(table.Subject.Name),
			Name: &protocol.ComparisonPart{Source: name, Normalized: name}}
	}
	request.CommonSteps = append(request.CommonSteps, &protocol.CommonPlanningStep{
		Id: &protocol.StepIdentity{Owner: "host", Name: "rename"}, Parent: proto.CloneOf(table.Subject),
		RenamedIndex: &protocol.CommonIndexRename{From: "c_by_label", To: "c_by_name"},
		Metadata: &protocol.PlanningMetadata{Impact: protocol.ChangeSemantics_BEHAVIORAL, Effects: []*protocol.PlanningEffect{
			{Subject: proto.CloneOf(table.Subject), Action: protocol.PlanningEffect_READ},
			{Subject: index("c_by_label"), Action: protocol.PlanningEffect_DROP},
			{Subject: index("c_by_name"), Action: protocol.PlanningEffect_CREATE},
		}},
	})
	return d, request
}

// renameStep is the rename step of request.
func renameStep(request *protocol.PlanFeaturesRequest) *protocol.CommonPlanningStep {
	return request.CommonSteps[len(request.CommonSteps)-1]
}

func TestIndexRenamesFeature_HappyPath(t *testing.T) {
	t.Parallel()
	c := qt.New(t)
	d, request := renameExchange()
	before := proto.CloneOf(request)
	c.Assert(provider.ValidateDescription(d, nil), qt.IsNil)
	c.Assert(provider.ValidatePlanningRequest(d, request), qt.IsNil)
	c.Assert(proto.Equal(before, request), qt.IsTrue)
	c.Assert(provider.TwoWayFeatures(), qt.Contains, provider.IndexRenamesFeature)
	// Where index names are scoped to the schema, an index subject names no
	// parent table.
	for _, effect := range renameStep(request).Metadata.Effects[1:] {
		effect.Subject.Parent = nil
	}
	c.Assert(provider.ValidatePlanningRequest(d, request), qt.IsNil)
	negotiated := provider.NegotiatedDescription(d, []string{provider.ObservedColumnsFeature, provider.ObservedIndexesFeature, provider.IndexRenamesFeature})
	c.Assert(negotiated.Engines[0].Features, qt.Contains, provider.IndexRenamesFeature)
}

// An engine lists the feature beside the provider's and beside observed
// indexes, through which the host compares the indexes it pairs.
func TestIndexRenamesFeatureOfAnEngine_FailurePath(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name   string
		mutate func(*protocol.Description)
	}{
		{"without the provider's feature", func(d *protocol.Description) { d.Features = withoutFeature(d.Features, provider.IndexRenamesFeature) }},
		{"without observed indexes", func(d *protocol.Description) {
			d.Engines[0].Features = withoutFeature(d.Engines[0].Features, provider.ObservedIndexesFeature)
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			c := qt.New(t)
			d, _ := renameExchange()
			test.mutate(d)
			c.Assert(provider.ValidateDescription(d, nil), qt.ErrorIs, provider.ErrProtocol)
			c.Assert(provider.ValidateDescription(d, nil), qt.ErrorMatches, `.*index rename: the engine requires the provider's feature and observed indexes`)
		})
	}
}

// A host that does not list the feature leaves it out of the engine and the
// request's target features, and a rename step it sends anyway is refused.
func TestIndexRenamesWithoutTheHost_FailurePath(t *testing.T) {
	t.Parallel()
	c := qt.New(t)
	d, request := renameExchange()
	negotiated := provider.NegotiatedDescription(d, []string{provider.ObservedColumnsFeature, provider.ObservedIndexesFeature})
	request.TargetFeatures = withoutFeature(request.TargetFeatures, provider.IndexRenamesFeature)
	c.Assert(negotiated.Engines[0].Features, qt.Not(qt.Contains), provider.IndexRenamesFeature)
	c.Assert(provider.ValidatePlanningRequest(negotiated, request), qt.ErrorMatches, `.*index rename: the feature was not negotiated`)
}

// Each row breaks one rule of a rename step, and the request is refused for
// that rule before owner code runs.
func TestIndexRenameStep_FailurePath(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name   string
		mutate func(*protocol.PlanFeaturesRequest)
		want   string
	}{
		{"target without the feature", func(r *protocol.PlanFeaturesRequest) {
			r.TargetFeatures = withoutFeature(r.TargetFeatures, provider.IndexRenamesFeature)
		}, "the feature was not negotiated"},
		{"no parent", func(r *protocol.PlanFeaturesRequest) { renameStep(r).Parent = nil }, "the step needs its parent and no other operand"},
		{"a column beside it", func(r *protocol.PlanFeaturesRequest) {
			renameStep(r).AddedColumn = &protocol.PlanningColumn{Name: "extra", Type: "INTEGER"}
		}, "the step needs its parent and no other operand"},
		{"one name twice", func(r *protocol.PlanFeaturesRequest) { renameStep(r).RenamedIndex.To = "c_by_label" }, "the step names two different indexes"},
		{"an empty name", func(r *protocol.PlanFeaturesRequest) { renameStep(r).RenamedIndex.From = "" }, "the step names two different indexes"},
		{"a parent the request creates", func(r *protocol.PlanFeaturesRequest) {
			r.Tables[0].Action, r.Tables[0].Current = protocol.PlanningTable_CREATE, nil
		}, "the parent is not a table the request alters"},
		{"an index the table does not hold", func(r *protocol.PlanFeaturesRequest) { r.Tables[0].Current.Indexes[0].Name = "c_other" },
			"the parent does not hold the index as from and declare it as to"},
		{"an index the table does not declare", func(r *protocol.PlanFeaturesRequest) { r.Tables[0].Desired.Indexes[0].Name = "c_other" },
			"the parent does not hold the index as from and declare it as to"},
		{"no read of the parent", func(r *protocol.PlanFeaturesRequest) {
			step := renameStep(r)
			step.Metadata.Effects = step.Metadata.Effects[1:]
		}, "the step's effects are not read the parent, drop from and create to"},
		{"the names swapped in the effects", func(r *protocol.PlanFeaturesRequest) {
			effects := renameStep(r).Metadata.Effects
			effects[1].Subject, effects[2].Subject = effects[2].Subject, effects[1].Subject
		}, "the step's effects are not read the parent, drop from and create to"},
		{"an index of another table", func(r *protocol.PlanFeaturesRequest) {
			renameStep(r).Metadata.Effects[2].Subject.Parent = &protocol.ComparisonPart{Source: "D", Normalized: "D"}
		}, "the step's effects are not read the parent, drop from and create to"},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			c := qt.New(t)
			d, request := renameExchange()
			test.mutate(request)
			err := provider.ValidatePlanningRequest(d, request)
			c.Assert(err, qt.ErrorIs, provider.ErrProtocol)
			c.Assert(err, qt.ErrorMatches, `.*index rename: `+test.want)
		})
	}
}

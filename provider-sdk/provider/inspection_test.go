package provider_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
	"github.com/stokaro/ptah/provider-sdk/provider"
	"google.golang.org/protobuf/proto"
)

func inspection() (*protocol.Description, *protocol.InspectRequest, *protocol.Schema) {
	description, input := valueInputs()
	model := description.Engines[0].ValueModels[0]
	model.Representation = protocol.ValueInput_OBSERVED
	value := input.Values[0].Value
	value.Owner.Kind = value.Kind
	coverage := &protocol.ModelCoverage{Representation: protocol.ValueInput_OBSERVED,
		Kinds:    []*protocol.ModelKindCoverage{{Model: proto.CloneOf(model), Knowledge: &protocol.ModelKnowledge{State: protocol.ModelKnowledge_COMPLETE}}},
		Subjects: []*protocol.ModelSubjectCoverage{observedSubject(value)},
	}
	return description, &protocol.InspectRequest{SessionId: "session"}, &protocol.Schema{
		Complete: true, Engine: "fixture", TargetFeatures: []string{"retention"}, Coverage: coverage, NativeObjects: []*protocol.Envelope{value},
	}
}

func observedSubject(value *protocol.Envelope) *protocol.ModelSubjectCoverage {
	return &protocol.ModelSubjectCoverage{Kind: value.Kind, Subject: proto.CloneOf(value.Owner), NormalizedSubject: proto.CloneOf(value.Owner), Knowledge: &protocol.ModelKnowledge{State: protocol.ModelKnowledge_COMPLETE}}
}

func TestInspectionRefusesUnboundOrAmbiguousValues(t *testing.T) {
	for _, row := range []struct {
		name   string
		change func(*protocol.Description, *protocol.InspectRequest, *protocol.Schema)
	}{
		{"missing engine", func(_ *protocol.Description, _ *protocol.InspectRequest, s *protocol.Schema) { s.Engine = "" }},
		{"wrong engine", func(_ *protocol.Description, _ *protocol.InspectRequest, s *protocol.Schema) { s.Engine = "other" }},
		{"missing session", func(_ *protocol.Description, r *protocol.InspectRequest, _ *protocol.Schema) { r.SessionId = "" }},
		{"unknown feature", func(_ *protocol.Description, _ *protocol.InspectRequest, s *protocol.Schema) {
			s.TargetFeatures = []string{"unknown"}
		}},
		{"missing coverage", func(_ *protocol.Description, _ *protocol.InspectRequest, s *protocol.Schema) { s.Coverage = nil }},
		{"desired coverage", func(_ *protocol.Description, _ *protocol.InspectRequest, s *protocol.Schema) {
			s.Coverage.Representation = protocol.ValueInput_DESIRED
		}},
		{"missing model", func(_ *protocol.Description, _ *protocol.InspectRequest, s *protocol.Schema) { s.Coverage.Kinds = nil }},
		{"missing subject", func(_ *protocol.Description, _ *protocol.InspectRequest, s *protocol.Schema) {
			s.Coverage.Subjects = nil
		}},
		{"absent value", func(_ *protocol.Description, _ *protocol.InspectRequest, s *protocol.Schema) {
			s.Coverage.Subjects[0].Knowledge.State = protocol.ModelKnowledge_ABSENT
		}},
		{"duplicate value", func(_ *protocol.Description, _ *protocol.InspectRequest, s *protocol.Schema) {
			s.NativeObjects = append(s.NativeObjects, proto.CloneOf(s.NativeObjects[0]))
		}},
		{"foreign owner", func(_ *protocol.Description, _ *protocol.InspectRequest, s *protocol.Schema) {
			s.NativeObjects[0].ProviderId = "foreign"
		}},
		{"changed model", func(_ *protocol.Description, _ *protocol.InspectRequest, s *protocol.Schema) {
			s.NativeObjects[0].DefinitionSha256 = "changed"
		}},
		{"native identity kind", func(_ *protocol.Description, _ *protocol.InspectRequest, s *protocol.Schema) {
			s.NativeObjects[0].Owner.Kind = "table"
		}},
		{"missing normalizer", func(d *protocol.Description, _ *protocol.InspectRequest, _ *protocol.Schema) {
			d.Features = []string{provider.InspectFeature}
		}},
		{"filtered namespace", func(_ *protocol.Description, r *protocol.InspectRequest, _ *protocol.Schema) {
			r.Schemas = []string{"selected"}
		}},
	} {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)
			description, request, snapshot := inspection()
			_, err := provider.InspectionValues(description, request, snapshot)
			c.Assert(err, qt.IsNil)
			row.change(description, request, snapshot)
			result, err := provider.InspectionValues(description, request, snapshot)
			c.Assert(err, qt.ErrorIs, provider.ErrProtocol)
			c.Assert(result, qt.IsNil)
		})
	}
}

func TestInspectionRetainsUnknownNamespacesAndExplicitAbsence(t *testing.T) {
	c := qt.New(t)
	description, request, snapshot := inspection()
	snapshot.Coverage.Kinds[0].Knowledge = &protocol.ModelKnowledge{State: protocol.ModelKnowledge_UNINSPECTED, Reason: "enumeration unavailable"}
	original := proto.CloneOf(snapshot)
	values, err := provider.InspectionValues(description, request, snapshot)
	c.Assert(err, qt.IsNil)
	c.Assert(values.Values, qt.HasLen, 1)
	c.Assert(values.Values[0].Representation, qt.Equals, protocol.ValueInput_OBSERVED)
	values.Values[0].Value.Owner.Path[0] = "changed"
	values.TargetFeatures[0] = "changed"
	c.Assert(proto.Equal(snapshot, original), qt.IsTrue)
	snapshot.NativeObjects = nil
	snapshot.Coverage.Subjects[0].Knowledge.State = protocol.ModelKnowledge_ABSENT
	values, err = provider.InspectionValues(description, request, snapshot)
	c.Assert(err, qt.IsNil)
	c.Assert(values.Values, qt.HasLen, 0)
	snapshot.Coverage = &protocol.ModelCoverage{Representation: protocol.ValueInput_OBSERVED}
	description.Features = []string{provider.InspectFeature}
	values, err = provider.InspectionValues(description, request, snapshot)
	c.Assert(err, qt.IsNil)
	c.Assert(values.Values, qt.HasLen, 0)
	c.Assert(snapshot.Coverage.Kinds, qt.HasLen, 0)
}

func TestInspectionBindsTableAndColumnFacets(t *testing.T) {
	c := qt.New(t)
	description, request, snapshot := inspection()
	table := &protocol.Table{Identity: &protocol.Reference{Engine: "fixture", Kind: "table", Name: "a.b"}, Columns: []*protocol.Column{{Name: "c", Type: "INTEGER"}}}
	facet := proto.CloneOf(snapshot.NativeObjects[0])
	facet.Owner = proto.CloneOf(table.Identity)
	column := proto.CloneOf(facet)
	column.Owner = provider.ColumnReference(table.Identity, "c")
	table.Facets = []*protocol.Envelope{facet}
	table.Columns[0].Facets = []*protocol.Envelope{column}
	snapshot.Tables = []*protocol.Table{table}
	snapshot.Coverage.Subjects = append(snapshot.Coverage.Subjects, observedSubject(facet), observedSubject(column))
	values, err := provider.InspectionValues(description, request, snapshot)
	c.Assert(err, qt.IsNil)
	c.Assert(values.Values, qt.HasLen, 3)
	c.Assert(values.Values[1].Value.Owner.Kind, qt.Equals, "table")
	c.Assert(values.Values[2].Value.Owner.Path, qt.DeepEquals, []string{"a.b"})
	table.Columns[0].Name = "changed"
	values, err = provider.InspectionValues(description, request, snapshot)
	c.Assert(err, qt.ErrorIs, provider.ErrProtocol)
	c.Assert(values, qt.IsNil)
	table.Columns[0].Name = "c"
	table.Identity.Name = "another"
	values, err = provider.InspectionValues(description, request, snapshot)
	c.Assert(err, qt.ErrorIs, provider.ErrProtocol)
	c.Assert(values, qt.IsNil)
}

func TestInspectionNormalizationMustRetainCoverageIdentity(t *testing.T) {
	c := qt.New(t)
	description, request, snapshot := inspection()
	batch, err := provider.InspectionValues(description, request, snapshot)
	c.Assert(err, qt.IsNil)
	model := description.Engines[0].ValueModels[0]
	normalized := &protocol.NormalizedValues{Complete: true, Values: []*protocol.NormalizedValue{{
		Id: batch.Values[0].Id, Value: proto.CloneOf(batch.Values[0].Value), NormalizedOwner: proto.CloneOf(batch.Values[0].Value.Owner),
		CanonicalFormat: model.CanonicalFormat, CanonicalProjection: []byte(`{"seconds":60}`),
	}}}
	c.Assert(provider.ValidateNormalizedInspection(description, request, snapshot, normalized), qt.IsNil)
	normalized.Values[0].NormalizedOwner.Name = "another"
	c.Assert(provider.ValidateNormalizedInspection(description, request, snapshot, normalized), qt.ErrorIs, provider.ErrProtocol)
	snapshot.Coverage.Subjects[0].NormalizedSubject.Name = "another"
	c.Assert(provider.ValidateNormalizedInspection(description, request, snapshot, normalized), qt.IsNil)
	normalized.Complete = false
	c.Assert(provider.ValidateNormalizedInspection(description, request, snapshot, normalized), qt.ErrorIs, provider.ErrProtocol)
}

func TestInspectionRegionsCannotReplaceRequestScopeBinding(t *testing.T) {
	c := qt.New(t)
	description, request, snapshot := inspection()
	_, scoped := scopedModelCoverage()
	snapshot.Coverage.Kinds[0].Regions = scoped.Kinds[0].Regions
	batch, err := provider.InspectionValues(description, request, snapshot)
	c.Assert(err, qt.IsNil)
	c.Assert(batch.Values, qt.HasLen, 1)
	request.Schemas = []string{"Private"}
	batch, err = provider.InspectionValues(description, request, snapshot)
	c.Assert(err, qt.ErrorIs, provider.ErrProtocol)
	c.Assert(err, qt.ErrorMatches, ".*requires owner scope binding")
	c.Assert(batch, qt.IsNil)
}

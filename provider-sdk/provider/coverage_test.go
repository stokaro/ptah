package provider_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
	"github.com/stokaro/ptah/provider-sdk/provider"
	"google.golang.org/protobuf/proto"
)

func modelCoverage() (*protocol.Description, *protocol.ModelCoverage) {
	d, r := valueInputs()
	model := d.Engines[0].ValueModels[0]
	ref := r.Values[0].Value.Owner
	return d, &protocol.ModelCoverage{Representation: protocol.ValueInput_DESIRED,
		Kinds:    []*protocol.ModelKindCoverage{{Model: proto.CloneOf(model), Knowledge: &protocol.ModelKnowledge{State: protocol.ModelKnowledge_COMPLETE}}},
		Subjects: []*protocol.ModelSubjectCoverage{{Kind: model.Kind, Subject: proto.CloneOf(ref), NormalizedSubject: proto.CloneOf(ref), Knowledge: &protocol.ModelKnowledge{State: protocol.ModelKnowledge_DEFAULTED}}}}
}

func TestModelCoverageRefusesUnknownOrAmbiguousScope(t *testing.T) {
	for _, test := range []struct {
		name   string
		mutate func(*protocol.ModelCoverage)
	}{
		{"unspecified", func(v *protocol.ModelCoverage) { v.Representation = 0 }},
		{"operations", func(v *protocol.ModelCoverage) { v.Representation = protocol.ValueInput_OPERATION }},
		{"unenrolled", func(v *protocol.ModelCoverage) { v.Kinds = nil }},
		{"foreign model", func(v *protocol.ModelCoverage) { v.Kinds[0].Model.Kind = "foreign" }},
		{"changed definition", func(v *protocol.ModelCoverage) { v.Kinds[0].Model.DefinitionSha256 = "changed" }},
		{"nil model", func(v *protocol.ModelCoverage) { v.Kinds[0].Model = nil }},
		{"duplicate model", func(v *protocol.ModelCoverage) { v.Kinds = append(v.Kinds, proto.CloneOf(v.Kinds[0])) }},
		{"kind absence", func(v *protocol.ModelCoverage) { v.Kinds[0].Knowledge.State = protocol.ModelKnowledge_ABSENT }},
		{"kind default", func(v *protocol.ModelCoverage) { v.Kinds[0].Knowledge.State = protocol.ModelKnowledge_DEFAULTED }},
		{"unexplained unknown", func(v *protocol.ModelCoverage) { v.Kinds[0].Knowledge.State = protocol.ModelKnowledge_UNINSPECTED }},
		{"unexplained unrepresentable", func(v *protocol.ModelCoverage) { v.Kinds[0].Knowledge.State = protocol.ModelKnowledge_UNREPRESENTABLE }},
		{"unknown state", func(v *protocol.ModelCoverage) { v.Kinds[0].Knowledge.State = 99 }},
		{"nil knowledge", func(v *protocol.ModelCoverage) { v.Subjects[0].Knowledge = nil }},
		{"foreign subject", func(v *protocol.ModelCoverage) { v.Subjects[0].Subject.Engine = "foreign" }},
		{"foreign normalized subject", func(v *protocol.ModelCoverage) { v.Subjects[0].NormalizedSubject.Engine = "foreign" }},
		{"changed subject kind", func(v *protocol.ModelCoverage) { v.Subjects[0].NormalizedSubject.Kind = "column" }},
		{"duplicate normalized subject", func(v *protocol.ModelCoverage) {
			other := proto.CloneOf(v.Subjects[0])
			other.Subject.Name = "another spelling"
			other.NormalizedSubject.DisplayName = "another display"
			v.Subjects = append(v.Subjects, other)
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			d, v := modelCoverage()
			c.Assert(provider.ValidateModelCoverage(d, "fixture", v), qt.IsNil)
			test.mutate(v)
			c.Assert(provider.ValidateModelCoverage(d, "fixture", v), qt.ErrorIs, provider.ErrProtocol)
		})
	}
}

func TestModelCoveragePreservesExplicitUnknownsAndStructuredIdentities(t *testing.T) {
	c := qt.New(t)
	d, v := modelCoverage()
	v.Kinds[0].Knowledge = &protocol.ModelKnowledge{State: protocol.ModelKnowledge_UNINSPECTED, Reason: "not inspected"}
	other := proto.CloneOf(v.Subjects[0])
	other.NormalizedSubject.Path = []string{"a", "b/c"}
	v.Subjects = append(v.Subjects, other)
	original := proto.CloneOf(v)
	c.Assert(provider.ValidateModelCoverage(d, "fixture", v), qt.IsNil)
	c.Assert(proto.Equal(v, original), qt.IsTrue)
	c.Assert(provider.ValidateModelCoverage(d, "fixture", &protocol.ModelCoverage{Representation: protocol.ValueInput_DESIRED}), qt.IsNil)
	c.Assert(provider.ValidateModelCoverage(d, "fixture", nil), qt.ErrorIs, provider.ErrProtocol)
	c.Assert(provider.ValidateModelCoverage(nil, "fixture", v), qt.ErrorIs, provider.ErrProtocol)
	c.Assert(provider.ValidateModelCoverage(d, "foreign", v), qt.ErrorIs, provider.ErrProtocol)
	v.Representation = protocol.ValueInput_OBSERVED
	d.Engines[0].ValueModels[0].Representation = protocol.ValueInput_OBSERVED
	v.Kinds[0].Model.Representation = protocol.ValueInput_OBSERVED
	c.Assert(provider.ValidateModelCoverage(d, "fixture", v), qt.ErrorIs, provider.ErrProtocol)
	v.Subjects[0].Knowledge.State = protocol.ModelKnowledge_ABSENT
	v.Subjects[1].Knowledge.State = protocol.ModelKnowledge_COMPLETE
	c.Assert(provider.ValidateModelCoverage(d, "fixture", v), qt.IsNil)
}

package provider_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
	"github.com/stokaro/ptah/provider-sdk/provider"
	"google.golang.org/protobuf/proto"
)

func sourceExchange() (*protocol.Description, *protocol.DecodeSourceRequest, *protocol.DecodedSource) {
	d, r := valueInputs()
	d.Features = append(d.Features, provider.DecodeSourceFeature)
	model := d.Engines[0].ValueModels[0]
	model.SourceFormats = []protocol.SourceDocument_Format{protocol.SourceDocument_YAML}
	doc := sourceDocument()
	doc.Models = []*protocol.ValueModel{proto.CloneOf(model)}
	d.Engines[0].YamlSourceKeys = []*protocol.YAMLSourceKey{{Scope: protocol.YAMLSourceKey_TABLE, Key: "retention", Selector: "retention", Models: []string{model.Kind}}}
	doc.Fragments[0].YamlKey = proto.CloneOf(d.Engines[0].YamlSourceKeys[0])
	request := &protocol.DecodeSourceRequest{Engine: "fixture", TargetFeatures: r.TargetFeatures, Documents: []*protocol.SourceDocument{doc}}
	normalized := normalizedValues(r).Values[0]
	coverage := &protocol.ModelCoverage{Representation: protocol.ValueInput_DESIRED,
		Kinds:    []*protocol.ModelKindCoverage{{Model: proto.CloneOf(model), Knowledge: &protocol.ModelKnowledge{State: protocol.ModelKnowledge_COMPLETE}}},
		Subjects: []*protocol.ModelSubjectCoverage{{Kind: model.Kind, Subject: proto.CloneOf(normalized.Value.Owner), NormalizedSubject: proto.CloneOf(normalized.NormalizedOwner), Knowledge: &protocol.ModelKnowledge{State: protocol.ModelKnowledge_COMPLETE}}}}
	result := &protocol.DecodedSource{Complete: true, Documents: []*protocol.DecodedSourceDocument{{Id: doc.Id, Coverage: coverage, Fragments: []*protocol.DecodedSourceFragment{{Id: doc.Fragments[0].Id, Values: []*protocol.SourceModelValue{{Placement: protocol.SourceModelValue_FACET, Value: normalized, Source: &protocol.SourceTrace{DocumentId: doc.Id, FragmentId: doc.Fragments[0].Id, Root: protocol.SourceTrace_CONTENT}}}}}}}}
	return d, request, result
}

func TestSourceDecodingRefusesChangedScopeAndPartialProvenance(t *testing.T) {
	for _, test := range []struct {
		name   string
		mutate func(*protocol.DecodedSource)
	}{
		{"partial batch", func(v *protocol.DecodedSource) { v.Complete = false }},
		{"missing document", func(v *protocol.DecodedSource) { v.Documents = nil }},
		{"foreign document", func(v *protocol.DecodedSource) { v.Documents[0].Id = "foreign" }},
		{"missing fragment", func(v *protocol.DecodedSource) { v.Documents[0].Fragments = nil }},
		{"foreign fragment", func(v *protocol.DecodedSource) { v.Documents[0].Fragments[0].Id = "foreign" }},
		{"missing coverage", func(v *protocol.DecodedSource) { v.Documents[0].Coverage = nil }},
		{"expanded scope", func(v *protocol.DecodedSource) {
			v.Documents[0].Coverage.Kinds = append(v.Documents[0].Coverage.Kinds, proto.CloneOf(v.Documents[0].Coverage.Kinds[0]))
		}},
		{"unknown coverage", func(v *protocol.DecodedSource) {
			v.Documents[0].Coverage.Kinds[0].Knowledge = &protocol.ModelKnowledge{State: protocol.ModelKnowledge_UNINSPECTED, Reason: "unknown"}
		}},
		{"bounded document knowledge", func(v *protocol.DecodedSource) {
			_, scoped := scopedModelCoverage()
			v.Documents[0].Coverage.Kinds[0].Regions = scoped.Kinds[0].Regions
		}},
		{"absent value", func(v *protocol.DecodedSource) {
			v.Documents[0].Coverage.Subjects[0].Knowledge.State = protocol.ModelKnowledge_ABSENT
		}},
		{"missing subject coverage", func(v *protocol.DecodedSource) { v.Documents[0].Coverage.Subjects = nil }},
		{"unknown placement", func(v *protocol.DecodedSource) { v.Documents[0].Fragments[0].Values[0].Placement = 0 }},
		{"native facet identity", func(v *protocol.DecodedSource) {
			v.Documents[0].Fragments[0].Values[0].Placement = protocol.SourceModelValue_NATIVE_OBJECT
		}},
		{"foreign provenance", func(v *protocol.DecodedSource) { v.Documents[0].Fragments[0].Values[0].Source.DocumentId = "foreign" }},
		{"missing provenance", func(v *protocol.DecodedSource) { v.Documents[0].Fragments[0].Values[0].Source = nil }},
		{"invented path", func(v *protocol.DecodedSource) {
			v.Documents[0].Fragments[0].Values[0].Source.Path = []*protocol.SourcePathStep{{Step: &protocol.SourcePathStep_Key{Key: "missing"}}}
		}},
		{"duplicate subject under another ID", func(v *protocol.DecodedSource) {
			f := v.Documents[0].Fragments[0]
			other := proto.CloneOf(f.Values[0])
			other.Value.Id = "another"
			f.Values = append(f.Values, other)
		}},
		{"duplicate value", func(v *protocol.DecodedSource) {
			f := v.Documents[0].Fragments[0]
			f.Values = append(f.Values, proto.CloneOf(f.Values[0]))
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			d, r, v := sourceExchange()
			c.Assert(provider.ValidateDecodedSource(d, r, v), qt.IsNil)
			test.mutate(v)
			c.Assert(provider.ValidateDecodedSource(d, r, v), qt.ErrorIs, provider.ErrProtocol)
		})
	}
}

func TestSourceRequestRequiresFrozenNegotiatedScope(t *testing.T) {
	for _, test := range []struct {
		name   string
		mutate func(*protocol.Description, *protocol.DecodeSourceRequest)
	}{
		{"unadvertised", func(d *protocol.Description, _ *protocol.DecodeSourceRequest) { d.Features = nil }},
		{"foreign engine", func(_ *protocol.Description, r *protocol.DecodeSourceRequest) { r.Engine = "other" }},
		{"absent scope", func(_ *protocol.Description, r *protocol.DecodeSourceRequest) { r.Documents[0].Models = nil }},
		{"changed definition", func(_ *protocol.Description, r *protocol.DecodeSourceRequest) {
			r.Documents[0].Models[0].DefinitionSha256 = "changed"
		}},
		{"duplicate scope", func(_ *protocol.Description, r *protocol.DecodeSourceRequest) {
			r.Documents[0].Models = append(r.Documents[0].Models, proto.CloneOf(r.Documents[0].Models[0]))
		}},
		{"unsupported format", func(d *protocol.Description, r *protocol.DecodeSourceRequest) {
			d.Engines[0].ValueModels[0].SourceFormats = nil
			r.Documents[0].Models[0].SourceFormats = nil
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			d, r, _ := sourceExchange()
			c.Assert(provider.ValidateSourceRequest(d, r), qt.IsNil)
			test.mutate(d, r)
			c.Assert(provider.ValidateSourceRequest(d, r), qt.ErrorIs, provider.ErrProtocol)
		})
	}
	c := qt.New(t)
	d, r, v := sourceExchange()
	c.Assert(provider.ValidateDecodedSource(d, r, nil), qt.ErrorIs, provider.ErrProtocol)
	c.Assert(provider.ValidateDecodedSource(d, nil, v), qt.ErrorIs, provider.ErrProtocol)
	r.Documents = nil
	c.Assert(provider.ValidateDecodedSource(d, r, &protocol.DecodedSource{Complete: true}), qt.IsNil)
}

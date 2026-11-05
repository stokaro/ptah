package provider_test

import (
	"regexp"
	"testing"

	qt "github.com/frankban/quicktest"
	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
	"github.com/stokaro/ptah/provider-sdk/provider"
	"google.golang.org/protobuf/proto"
)

// platformKind is a model the HCL platform claim of hclPlatformExchange names
// and no other claim does. It lists no source format.
const platformKind = "example.org/fixture/platform-retention"

func platformMapping(entries ...*protocol.SourceEntry) *protocol.SourceFragment_Value {
	return &protocol.SourceFragment_Value{Value: &protocol.SourceValue{Span: sourceSpan(), Value: &protocol.SourceValue_Mapping{
		Mapping: &protocol.SourceMapping{Entries: entries}}}}
}

// hclPlatformExchange adds to hclExchange a claim of the fixture engine's
// table platform group, which names a model of its own, and a fragment of the
// group bound to the document's table, decoded as a value of that model.
func hclPlatformExchange() (*protocol.Description, *protocol.DecodeSourceRequest, *protocol.DecodedSource) {
	d, request, result := hclExchange()
	d.Features = append(d.Features, provider.HCLPlatformFeature)
	engine := d.Engines[0]
	engine.Features = append(engine.Features, provider.HCLPlatformFeature)
	model := proto.CloneOf(engine.ValueModels[0])
	model.Kind, model.SourceFormats = platformKind, nil
	engine.ValueModels = append(engine.ValueModels, model)
	claim := &protocol.HCLSourceBlock{Site: protocol.HCLSourceBlock_TABLE_PLATFORM, Type: "fixture", Selector: "platform", Models: []string{platformKind},
		Attributes: []*protocol.HCLSourceAttribute{{Name: "seconds", Kind: protocol.HCLSourceAttribute_STRING}}}
	engine.HclSource.Blocks = append(engine.HclSource.Blocks, claim)
	document := request.Documents[0]
	document.Models = append(document.Models, proto.CloneOf(model))
	document.Fragments = append(document.Fragments, &protocol.SourceFragment{Id: "platform", Selector: "platform", BindingId: "table", Span: sourceSpan(),
		HclBlock: proto.CloneOf(claim), Content: platformMapping(textEntry("seconds", "30"))})
	decoded := result.Documents[0]
	decoded.Coverage.Kinds = append(decoded.Coverage.Kinds, &protocol.ModelKindCoverage{Model: proto.CloneOf(model),
		Knowledge: &protocol.ModelKnowledge{State: protocol.ModelKnowledge_COMPLETE}})
	value := proto.CloneOf(decoded.Fragments[0].Values[0])
	value.Value.Id, value.Value.Value.Kind = "platform", platformKind
	value.Source.FragmentId = "platform"
	decoded.Fragments = append(decoded.Fragments, &protocol.DecodedSourceFragment{Id: "platform", Values: []*protocol.SourceModelValue{value}})
	decoded.Coverage.Subjects = append(decoded.Coverage.Subjects, &protocol.ModelSubjectCoverage{Kind: platformKind,
		Subject: proto.CloneOf(value.Value.Value.Owner), NormalizedSubject: proto.CloneOf(value.Value.NormalizedOwner),
		Knowledge: &protocol.ModelKnowledge{State: protocol.ModelKnowledge_COMPLETE}})
	return d, request, result
}

// A table platform claim brings its model into an HCL document that the model
// lists no HCL for, and a value of the model decodes from the group.
func TestHCLPlatformClaims_HappyPath(t *testing.T) {
	c := qt.New(t)
	d, request, result := hclPlatformExchange()
	before := proto.CloneOf(request)

	c.Assert(provider.ValidateDescription(d, nil), qt.IsNil)
	c.Assert(provider.ValidateSourceRequest(d, request), qt.IsNil)
	c.Assert(provider.ValidateDecodedSource(d, request, result), qt.IsNil)
	c.Assert(proto.Equal(before, request), qt.IsTrue)
}

func TestHCLPlatformClaimDeclarations_FailurePath(t *testing.T) {
	platform := func(d *protocol.Description) *protocol.HCLSourceBlock { return d.Engines[0].HclSource.Blocks[2] }
	for _, test := range []struct {
		name   string
		want   string
		mutate func(*protocol.Description)
	}{
		{"a claim without the engine's feature", "an HCL platform claim needs the engine's source.hcl-platform", func(d *protocol.Description) {
			d.Engines[0].Features = d.Engines[0].Features[:len(d.Engines[0].Features)-1]
		}},
		{"the engine's feature without the provider's", "HCL platform claims need the provider's source.hcl-platform and the engine's source.hcl", func(d *protocol.Description) {
			d.Features = d.Features[:len(d.Features)-1]
		}},
		{"another engine's group", "an HCL platform claim names its engine's group", func(d *protocol.Description) { platform(d).Type = "postgres" }},
		{"an override that is not a string", "an HCL platform claim reads string overrides and inherits nothing", func(d *protocol.Description) {
			platform(d).Attributes[0].Kind = protocol.HCLSourceAttribute_TEXT
		}},
		{"an inherited attribute", "an HCL platform claim reads string overrides and inherits nothing", func(d *protocol.Description) {
			platform(d).Inherited = []string{"engine"}
		}},
		{"a naming", "only a top-level HCL block takes a naming", func(d *protocol.Description) { platform(d).Naming = protocol.HCLSourceBlock_UNLABELED }},
		{"a child", "children, a declared type or a grant kind at a site that takes none", func(d *protocol.Description) {
			platform(d).Children = []*protocol.HCLSourceChild{{Type: "tier", Unlabeled: true}}
		}},
		{"a grant kind", "children, a declared type or a grant kind at a site that takes none", func(d *protocol.Description) { platform(d).GrantKind = "TABLE" }},
		{"an unknown model", "HCL block names an unknown, duplicate, or unsupported desired model", func(d *protocol.Description) {
			platform(d).Models = []string{"example.org/other"}
		}},
		{"a model that lists HCL and no other claim names", "HCL model has no block claim", func(d *protocol.Description) {
			d.Engines[0].ValueModels[1].SourceFormats = []protocol.SourceDocument_Format{protocol.SourceDocument_HCL}
		}},
		{"two claims of one group", "duplicate HCL block claim", func(d *protocol.Description) {
			d.Engines[0].HclSource.Blocks = append(d.Engines[0].HclSource.Blocks, proto.CloneOf(platform(d)))
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			d, _, _ := hclPlatformExchange()
			test.mutate(d)

			err := provider.ValidateDescription(d, nil)

			c.Assert(err, qt.ErrorIs, provider.ErrProtocol)
			c.Assert(err, qt.ErrorMatches, ".*"+regexp.QuoteMeta(test.want))
		})
	}
}

func TestHCLPlatformFragments_FailurePath(t *testing.T) {
	platform := func(r *protocol.DecodeSourceRequest) *protocol.SourceFragment { return r.Documents[0].Fragments[2] }
	for _, test := range []struct {
		name   string
		want   string
		mutate func(*protocol.DecodeSourceRequest)
	}{
		{"a group without its table", "HCL fragment binding or subject does not match its site", func(r *protocol.DecodeSourceRequest) { platform(r).BindingId = "" }},
		{"a group with a subject", "HCL fragment binding or subject does not match its site", func(r *protocol.DecodeSourceRequest) {
			platform(r).Subject = indexSubject()
		}},
		{"a group as a block", "HCL attribute claim content must be a mapping", func(r *protocol.DecodeSourceRequest) {
			platform(r).Content = r.Documents[0].Fragments[0].Content
		}},
		{"an override outside the claim", "HCL attribute is outside its claim", func(r *protocol.DecodeSourceRequest) {
			platform(r).GetValue().GetMapping().Entries[0].Key = "minutes"
		}},
		{"the claim's model outside the document", "HCL fragment model is outside document scope", func(r *protocol.DecodeSourceRequest) {
			r.Documents[0].Models = r.Documents[0].Models[:1]
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			d, request, _ := hclPlatformExchange()
			test.mutate(request)

			err := provider.ValidateSourceRequest(d, request)

			c.Assert(err, qt.ErrorIs, provider.ErrProtocol)
			c.Assert(err, qt.ErrorMatches, ".*"+regexp.QuoteMeta(test.want))
		})
	}
}

// A platform claim's model enters the scope of an HCL document only: a YAML
// document cannot enroll a model that lists no YAML.
func TestHCLPlatformModelInAnotherFormat(t *testing.T) {
	c := qt.New(t)
	d, request, _ := hclPlatformExchange()
	yaml := sourceDocument()
	yaml.Models = []*protocol.ValueModel{proto.CloneOf(d.Engines[0].ValueModels[1])}
	yaml.Fragments = nil
	request.Documents = []*protocol.SourceDocument{yaml}

	err := provider.ValidateSourceRequest(d, request)

	c.Assert(err, qt.ErrorMatches, `.*unknown, changed, duplicate, or unsupported source model`)
}

// A host that does not list the feature receives no platform claim and the
// same models, so a model it receives is the provider's own copy; a group's
// fragment is then refused as a claim the description does not hold.
func TestHCLPlatformClaimsAreTwoWay(t *testing.T) {
	c := qt.New(t)
	d, request, _ := hclPlatformExchange()
	without := []string{provider.HCLSourceFeature, provider.DecodeSourceFeature}

	unlisted := provider.NegotiatedDescription(d, without)
	listed := provider.NegotiatedDescription(d, append(without, provider.HCLPlatformFeature))

	c.Assert(provider.ValidateDescription(unlisted, nil), qt.IsNil)
	c.Assert(unlisted.Engines[0].HclSource.Blocks, qt.HasLen, 2)
	c.Assert(unlisted.Engines[0].Features, qt.Not(qt.Contains), provider.HCLPlatformFeature)
	c.Assert(unlisted.Features, qt.Not(qt.Contains), provider.HCLPlatformFeature)
	c.Assert(proto.Equal(unlisted.Engines[0].ValueModels[1], d.Engines[0].ValueModels[1]), qt.IsTrue)
	c.Assert(provider.ValidateSourceRequest(unlisted, request), qt.ErrorMatches, `.*unknown, changed, duplicate, or unsupported source model`)
	request.Documents[0].Models = request.Documents[0].Models[:1]
	c.Assert(provider.ValidateSourceRequest(unlisted, request), qt.ErrorMatches, `.*HCL fragment changed its negotiated block claim`)
	c.Assert(listed.Engines[0].HclSource.Blocks, qt.HasLen, 3)
	c.Assert(provider.ValidateDescription(listed, nil), qt.IsNil)
}

// hclPlatformEncodeExchange writes the value of the platform model under the
// table's platform claim and reports, in a loss of the same value, the part
// of it the group cannot spell.
func hclPlatformEncodeExchange() (*protocol.Description, *protocol.EncodeSourceRequest, *protocol.EncodedSource) {
	d, decodeRequest, decoded := hclPlatformExchange()
	d.Features = append(d.Features, provider.EncodeSourceFeature)
	d.Engines[0].Features = append(d.Engines[0].Features, provider.EncodeSourceFeature)
	value := decoded.Documents[0].Fragments[2].Values[0].Value
	request := &protocol.EncodeSourceRequest{Engine: "fixture", TargetFeatures: decodeRequest.TargetFeatures, Format: protocol.SourceDocument_HCL,
		Bindings: []*protocol.SourceBinding{{Id: "table", Object: proto.CloneOf(value.Value.Owner), StructName: "Record"}},
		Values:   []*protocol.EncodeSourceValue{{Placement: protocol.SourceModelValue_FACET, Value: value, BindingId: "table"}}}
	claim := d.Engines[0].HclSource.Blocks[2]
	fragment := &protocol.SourceFragment{Id: "platform", Selector: claim.GetSelector(), BindingId: "table", HclBlock: proto.CloneOf(claim),
		Content: &protocol.SourceFragment_Value{Value: &protocol.SourceValue{Value: &protocol.SourceValue_Mapping{Mapping: &protocol.SourceMapping{
			Entries: []*protocol.SourceEntry{{Key: "seconds", Value: &protocol.SourceValue{Value: &protocol.SourceValue_Text{Text: "60"}}}}}}}}}
	result := &protocol.EncodedSource{Complete: true, Fragments: []*protocol.EncodedFragment{{Fragment: fragment, ValueIds: []string{value.Id}}},
		Losses: []*protocol.EncodeLoss{{ValueId: value.Id, Message: "the minutes of the retention have no HCL spelling"}}}
	return d, request, result
}

// A loss may name a value a platform part writes, in a flat reply and in a
// group: the value is written in part.
func TestHCLPlatformPartialLoss_HappyPath(t *testing.T) {
	c := qt.New(t)
	d, request, result := hclPlatformEncodeExchange()

	c.Assert(provider.ValidateDescription(d, nil), qt.IsNil)
	c.Assert(provider.ValidateEncodedSource(d, request, result), qt.IsNil)
	d.Features = append(d.Features, provider.EncodeGroupsFeature)
	d.Engines[0].Features = append(d.Engines[0].Features, provider.EncodeGroupsFeature)
	encoded := result.Fragments[0]
	result.Fragments = nil
	result.Groups = []*protocol.EncodedSourceGroup{{ValueIds: encoded.ValueIds, Parts: []*protocol.EncodedSourcePart{{Fragment: encoded.Fragment}}}}
	c.Assert(provider.ValidateEncodedSource(d, request, result), qt.IsNil)
}

// A loss names a value once, and a written value only where a platform part
// writes it.
func TestHCLPlatformPartialLoss_FailurePath(t *testing.T) {
	for _, test := range []struct {
		name   string
		want   string
		mutate func(*protocol.Description, *protocol.EncodedSource)
	}{
		{"a value lost twice", "a loss names a value of the request once, in one line", func(_ *protocol.Description, r *protocol.EncodedSource) {
			r.Losses = append(r.Losses, proto.CloneOf(r.Losses[0]))
		}},
		{"a value another site writes", "a loss names a value of the request once, in one line", func(d *protocol.Description, r *protocol.EncodedSource) {
			table := &protocol.HCLSourceBlock{Site: protocol.HCLSourceBlock_TABLE_ATTRIBUTES, Type: "table", Selector: "table", Models: []string{platformKind},
				Attributes: []*protocol.HCLSourceAttribute{{Name: "seconds", Kind: protocol.HCLSourceAttribute_TEXT}}}
			d.Engines[0].HclSource.Blocks = append(d.Engines[0].HclSource.Blocks, table)
			d.Engines[0].ValueModels[1].SourceFormats = []protocol.SourceDocument_Format{protocol.SourceDocument_HCL}
			fragment := r.Fragments[0].Fragment
			fragment.HclBlock, fragment.Selector = proto.CloneOf(table), table.Selector
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			d, request, result := hclPlatformEncodeExchange()
			test.mutate(d, result)

			err := provider.ValidateEncodedSource(d, request, result)

			c.Assert(err, qt.ErrorIs, provider.ErrProtocol)
			c.Assert(err, qt.ErrorMatches, ".*"+regexp.QuoteMeta(test.want))
		})
	}
}

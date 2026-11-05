package provider_test

import (
	"regexp"
	"testing"

	qt "github.com/frankban/quicktest"
	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
	"github.com/stokaro/ptah/provider-sdk/provider"
	"google.golang.org/protobuf/proto"
)

func textValue(text string) *protocol.SourceValue {
	return &protocol.SourceValue{Span: sourceSpan(), Value: &protocol.SourceValue_Text{Text: text}}
}

func textSequence(texts ...string) *protocol.SourceValue {
	sequence := &protocol.SourceSequence{}
	for _, text := range texts {
		sequence.Values = append(sequence.Values, textValue(text))
	}
	return &protocol.SourceValue{Span: sourceSpan(), Value: &protocol.SourceValue_Sequence{Sequence: sequence}}
}

// indexKindAttributes is the frontend's part of the fixture's tree index: its
// type as written, its key columns and the column it covers.
func indexKindAttributes() *protocol.SourceMapping {
	return &protocol.SourceMapping{Entries: []*protocol.SourceEntry{
		{Key: "type", KeySpan: sourceSpan(), Value: textValue("Global  Using TREE")},
		{Key: "columns", KeySpan: sourceSpan(), Value: textSequence("gen", "emb")},
		{Key: "include", KeySpan: sourceSpan(), Value: textSequence("body")},
	}}
}

// indexKindExchange claims the fixture engine's "tree" index type in a YAML
// document, which sends a tree index as one fragment bound to its table with
// the index as its subject, and decodes it as a facet of the table. The
// engine also claims the index entry's clusters key, which the fragment's
// content carries.
func indexKindExchange() (*protocol.Description, *protocol.DecodeSourceRequest, *protocol.DecodedSource) {
	d, request, result := sourceExchange()
	d.Features = append(d.Features, provider.IndexKindsFeature, provider.EntryKeysFeature)
	engine := d.Engines[0]
	engine.Features = append(engine.Features, provider.IndexKindsFeature, provider.EntryKeysFeature)
	model := engine.ValueModels[0]
	engine.YamlSourceKeys = append(engine.YamlSourceKeys, &protocol.YAMLSourceKey{Scope: protocol.YAMLSourceKey_INDEX, Key: "clusters", Selector: "index.clusters",
		Models: []string{model.Kind}})
	claim := &protocol.SourceIndexKind{Types: []string{"tree", "global using tree"}, Selector: "tree", Models: []string{model.Kind}}
	engine.IndexKinds = []*protocol.SourceIndexKind{claim}
	value := result.Documents[0].Fragments[0].Values[0]
	table := proto.CloneOf(value.Value.Value.Owner)
	document := request.Documents[0]
	document.Bindings[0].Object = table
	document.Fragments[0] = &protocol.SourceFragment{Id: "tree", Selector: "tree", BindingId: "table", Span: sourceSpan(), IndexKind: proto.CloneOf(claim),
		Subject: &protocol.Reference{Engine: table.Engine, Kind: "index", Database: table.Database, Catalog: table.Catalog, Schema: table.Schema,
			Path: []string{table.Name}, Name: "by_emb"},
		InheritedAttributes: indexKindAttributes(),
		Content: &protocol.SourceFragment_Value{Value: &protocol.SourceValue{Span: sourceSpan(), Value: &protocol.SourceValue_Mapping{
			Mapping: &protocol.SourceMapping{Entries: []*protocol.SourceEntry{textEntry("clusters", "4")}}}}}}
	result.Documents[0].Fragments[0].Id = "tree"
	value.Source.FragmentId = "tree"
	return d, request, result
}

// A declaration of a claimed type reaches the provider whole, and decodes to a
// facet of its table.
func TestIndexKinds_HappyPath(t *testing.T) {
	c := qt.New(t)
	d, request, result := indexKindExchange()
	before := proto.CloneOf(request)

	c.Assert(provider.ValidateDescription(d, nil), qt.IsNil)
	c.Assert(provider.ValidateSourceRequest(d, request), qt.IsNil)
	c.Assert(provider.ValidateDecodedSource(d, request, result), qt.IsNil)
	c.Assert(proto.Equal(before, request), qt.IsTrue)
	c.Assert(provider.IndexKindClaim(d.Engines[0], " GLOBAL\tusing  Tree "), qt.Equals, d.Engines[0].IndexKinds[0])
	c.Assert(provider.IndexKindClaim(d.Engines[0], "btree"), qt.IsNil)
	c.Assert(provider.NormalizeIndexType("  Vector_KMeans_Tree "), qt.Equals, "vector_kmeans_tree")
}

// The content may be empty: a declaration that writes none of the engine's
// attributes.
func TestIndexKindsWithoutOwnAttributes(t *testing.T) {
	c := qt.New(t)
	d, request, result := indexKindExchange()
	request.Documents[0].Fragments[0].GetValue().GetMapping().Entries = nil

	c.Assert(provider.ValidateSourceRequest(d, request), qt.IsNil)
	c.Assert(provider.ValidateDecodedSource(d, request, result), qt.IsNil)
}

func TestIndexKindDeclarations_FailurePath(t *testing.T) {
	claim := func(d *protocol.Description) *protocol.SourceIndexKind { return d.Engines[0].IndexKinds[0] }
	for _, test := range []struct {
		name   string
		want   string
		mutate func(*protocol.Description)
	}{
		{"a claim without the engine's feature", "an engine claims index kinds exactly when it lists source.index-kinds", func(d *protocol.Description) {
			d.Engines[0].Features = []string{"retention", provider.EntryKeysFeature}
		}},
		{"the feature without a claim", "an engine claims index kinds exactly when it lists source.index-kinds", func(d *protocol.Description) {
			d.Engines[0].IndexKinds = nil
		}},
		{"the engine's feature without the provider's", "index-kind claims need the provider's source.index-kinds and source decoding", func(d *protocol.Description) {
			d.Features = d.Features[:len(d.Features)-2]
			d.Features = append(d.Features, provider.EntryKeysFeature)
		}},
		{"no type", "incomplete index-kind claim", func(d *protocol.Description) { claim(d).Types = nil }},
		{"no selector", "incomplete index-kind claim", func(d *protocol.Description) { claim(d).Selector = " " }},
		{"no model", "incomplete index-kind claim", func(d *protocol.Description) { claim(d).Models = nil }},
		{"a type in upper case", "an index type is empty, not normalized, or claimed twice", func(d *protocol.Description) { claim(d).Types[0] = "Tree" }},
		{"a type with two spaces", "an index type is empty, not normalized, or claimed twice", func(d *protocol.Description) {
			claim(d).Types[1] = "global  using tree"
		}},
		{"an empty type", "an index type is empty, not normalized, or claimed twice", func(d *protocol.Description) { claim(d).Types[0] = "" }},
		{"a type two claims name", "an index type is empty, not normalized, or claimed twice", func(d *protocol.Description) {
			d.Engines[0].IndexKinds = append(d.Engines[0].IndexKinds, &protocol.SourceIndexKind{Types: []string{"tree"}, Selector: "other",
				Models: claim(d).Models})
		}},
		{"an unknown model", "an index-kind claim names an unknown or duplicate desired model", func(d *protocol.Description) {
			claim(d).Models = []string{"example.org/other"}
		}},
		{"a model twice", "an index-kind claim names an unknown or duplicate desired model", func(d *protocol.Description) {
			claim(d).Models = append(claim(d).Models, claim(d).Models[0])
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			d, _, _ := indexKindExchange()
			test.mutate(d)

			err := provider.ValidateDescription(d, nil)

			c.Assert(err, qt.ErrorIs, provider.ErrProtocol)
			c.Assert(err, qt.ErrorMatches, ".*"+regexp.QuoteMeta(test.want))
		})
	}
}

func TestIndexKindFragments_FailurePath(t *testing.T) {
	fragment := func(r *protocol.DecodeSourceRequest) *protocol.SourceFragment { return r.Documents[0].Fragments[0] }
	inherited := func(r *protocol.DecodeSourceRequest) []*protocol.SourceEntry {
		return fragment(r).InheritedAttributes.Entries
	}
	for _, test := range []struct {
		name   string
		want   string
		mutate func(*protocol.DecodeSourceRequest)
	}{
		{"a changed claim", "index-kind fragment changed its negotiated claim", func(r *protocol.DecodeSourceRequest) {
			fragment(r).IndexKind.Types = []string{"tree"}
		}},
		{"a changed selector", "index-kind fragment changed its negotiated claim", func(r *protocol.DecodeSourceRequest) { fragment(r).Selector = "other" }},
		{"the claim's model outside the document", "index-kind fragment model is outside document scope", func(r *protocol.DecodeSourceRequest) {
			r.Documents[0].Models = nil
		}},
		{"no binding", "index-kind fragment needs its table's binding and an index of that table as its subject", func(r *protocol.DecodeSourceRequest) {
			fragment(r).BindingId = ""
		}},
		{"a column as the subject", "index-kind fragment needs its table's binding and an index of that table as its subject", func(r *protocol.DecodeSourceRequest) {
			fragment(r).Subject.Kind = "column"
		}},
		{"an index of another table", "index-kind fragment needs its table's binding and an index of that table as its subject", func(r *protocol.DecodeSourceRequest) {
			fragment(r).Subject.Path = []string{"other"}
		}},
		{"an index of another schema", "index-kind fragment needs its table's binding and an index of that table as its subject", func(r *protocol.DecodeSourceRequest) {
			fragment(r).Subject.Schema = "other"
		}},
		{"a type the claim does not name", "index-kind attribute type has no valid value", func(r *protocol.DecodeSourceRequest) {
			inherited(r)[0].Value = textValue("btree")
		}},
		{"no type", "index-kind fragment needs its type and its columns", func(r *protocol.DecodeSourceRequest) {
			fragment(r).InheritedAttributes.Entries = inherited(r)[1:]
		}},
		{"no columns", "index-kind fragment needs its type and its columns", func(r *protocol.DecodeSourceRequest) {
			fragment(r).InheritedAttributes.Entries = []*protocol.SourceEntry{inherited(r)[0], inherited(r)[2]}
		}},
		{"an empty column list", "index-kind attribute columns has no valid value", func(r *protocol.DecodeSourceRequest) {
			inherited(r)[1].Value = textSequence()
		}},
		{"a column without a name", "index-kind attribute include has no valid value", func(r *protocol.DecodeSourceRequest) {
			inherited(r)[2].Value = textSequence("")
		}},
		{"columns as text", "index-kind attribute columns has no valid value", func(r *protocol.DecodeSourceRequest) {
			inherited(r)[1].Value = textValue("gen,emb")
		}},
		{"a unique that is false", "index-kind attribute unique has no valid value", func(r *protocol.DecodeSourceRequest) {
			fragment(r).InheritedAttributes.Entries = append(inherited(r), &protocol.SourceEntry{Key: "unique", KeySpan: sourceSpan(),
				Value: &protocol.SourceValue{Span: sourceSpan(), Value: &protocol.SourceValue_Boolean{Boolean: false}}})
		}},
		{"an attribute out of order", "index-kind attribute is unknown, repeated or out of order", func(r *protocol.DecodeSourceRequest) {
			fragment(r).InheritedAttributes.Entries = []*protocol.SourceEntry{inherited(r)[1], inherited(r)[0]}
		}},
		{"an unknown attribute", "index-kind attribute is unknown, repeated or out of order", func(r *protocol.DecodeSourceRequest) {
			fragment(r).InheritedAttributes.Entries = append(inherited(r), &protocol.SourceEntry{Key: "where", KeySpan: sourceSpan(), Value: textValue("x > 0")})
		}},
		{"an attribute the engine does not claim", "index-kind fragment attribute is not one the engine claims on the index declaration", func(r *protocol.DecodeSourceRequest) {
			fragment(r).GetValue().GetMapping().Entries[0].Key = "levels"
		}},
		{"content that is no mapping", "index-kind fragment content must be a mapping", func(r *protocol.DecodeSourceRequest) {
			fragment(r).Content = &protocol.SourceFragment_Value{Value: textValue("4")}
		}},
		{"a YAML claim beside it", "an index-kind claim in a SQL document or beside another claim", func(r *protocol.DecodeSourceRequest) {
			fragment(r).YamlKey = &protocol.YAMLSourceKey{Scope: protocol.YAMLSourceKey_INDEX, Key: "clusters", Selector: "index.clusters"}
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			d, request, _ := indexKindExchange()
			test.mutate(request)

			err := provider.ValidateSourceRequest(d, request)

			c.Assert(err, qt.ErrorIs, provider.ErrProtocol)
			c.Assert(err, qt.ErrorMatches, ".*"+regexp.QuoteMeta(test.want))
		})
	}
}

// The content's keys are the engine's claim on the format's index
// declaration: a Go document reads them from the INDEX-scope claim on
// ptah:schema:index, which the YAML keys are not.
func TestIndexKindFragmentsOfAGoDocument(t *testing.T) {
	c := qt.New(t)
	d, request, _ := indexKindExchange()
	engine := d.Engines[0]
	model := engine.ValueModels[0]
	model.SourceFormats = []protocol.SourceDocument_Format{protocol.SourceDocument_GO_ANNOTATIONS}
	engine.YamlSourceKeys = nil
	document := request.Documents[0]
	document.Format, document.File = protocol.SourceDocument_GO_ANNOTATIONS, "models.go"
	document.Models = []*protocol.ValueModel{proto.CloneOf(model)}

	c.Assert(provider.ValidateSourceRequest(d, request), qt.ErrorMatches,
		`.*index-kind fragment attribute is not one the engine claims on the index declaration`)
	engine.GoSourceDirectives = []*protocol.GoSourceDirective{{Scope: protocol.GoSourceDirective_INDEX, Directive: "ptah:schema:index", Selector: "index",
		Attributes: []string{"clusters"}, Models: []string{model.Kind}}}
	c.Assert(provider.ValidateDescription(d, nil), qt.IsNil)
	c.Assert(provider.ValidateSourceRequest(d, request), qt.IsNil)
	document.Format = protocol.SourceDocument_SQL
	c.Assert(provider.ValidateSourceRequest(d, request), qt.ErrorMatches, `.*an index-kind claim in a SQL document or beside another claim`)
}

func TestIndexKindValues_FailurePath(t *testing.T) {
	value := func(r *protocol.DecodedSource) *protocol.SourceModelValue {
		return r.Documents[0].Fragments[0].Values[0]
	}
	for _, test := range []struct {
		name   string
		want   string
		mutate func(*protocol.DecodedSource)
	}{
		{"a native object", "a value of an index-kind fragment is a facet of the index's table", func(r *protocol.DecodedSource) {
			value(r).Placement = protocol.SourceModelValue_NATIVE_OBJECT
		}},
		{"a facet of the index", "a value of an index-kind fragment is a facet of the index's table", func(r *protocol.DecodedSource) {
			owner := value(r).Value.Value.Owner
			owner.Kind, owner.Path, owner.Name = "index", []string{owner.Name}, "by_emb"
			value(r).Value.NormalizedOwner = proto.CloneOf(owner)
			r.Documents[0].Coverage.Subjects[0].Subject, r.Documents[0].Coverage.Subjects[0].NormalizedSubject = proto.CloneOf(owner), proto.CloneOf(owner)
		}},
		{"a facet of another table", "a value of an index-kind fragment is a facet of the index's table", func(r *protocol.DecodedSource) {
			owner := value(r).Value.Value.Owner
			owner.Name = "other"
			value(r).Value.NormalizedOwner = proto.CloneOf(owner)
			r.Documents[0].Coverage.Subjects[0].Subject, r.Documents[0].Coverage.Subjects[0].NormalizedSubject = proto.CloneOf(owner), proto.CloneOf(owner)
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			d, request, result := indexKindExchange()
			test.mutate(result)

			err := provider.ValidateDecodedSource(d, request, result)

			c.Assert(err, qt.ErrorIs, provider.ErrProtocol)
			c.Assert(err, qt.ErrorMatches, ".*"+regexp.QuoteMeta(test.want))
		})
	}
}

// A host that does not list the feature receives no index-kind claim, and an
// index-kind fragment is then refused as a claim the description does not
// hold.
func TestIndexKindsAreTwoWay(t *testing.T) {
	c := qt.New(t)
	d, request, _ := indexKindExchange()
	without := []string{provider.DecodeSourceFeature, provider.EntryKeysFeature}

	unlisted := provider.NegotiatedDescription(d, without)
	listed := provider.NegotiatedDescription(d, append(without, provider.IndexKindsFeature))

	c.Assert(provider.ValidateDescription(unlisted, nil), qt.IsNil)
	c.Assert(unlisted.Engines[0].IndexKinds, qt.HasLen, 0)
	c.Assert(unlisted.Engines[0].Features, qt.Not(qt.Contains), provider.IndexKindsFeature)
	c.Assert(unlisted.Features, qt.Not(qt.Contains), provider.IndexKindsFeature)
	c.Assert(provider.ValidateSourceRequest(unlisted, request), qt.ErrorMatches, `.*index-kind fragment changed its negotiated claim`)
	c.Assert(listed.Engines[0].IndexKinds, qt.HasLen, 1)
	c.Assert(provider.ValidateDescription(listed, nil), qt.IsNil)
	c.Assert(provider.TwoWayFeatures(), qt.Contains, provider.IndexKindsFeature)
}

// indexKindEncodeExchange writes the table's facet as two index-kind parts of
// one group, one for each index the facet holds, and reports, in a loss of
// the same value, what the parts do not write.
func indexKindEncodeExchange() (*protocol.Description, *protocol.EncodeSourceRequest, *protocol.EncodedSource) {
	d, decodeRequest, decoded := indexKindExchange()
	d.Features = append(d.Features, provider.EncodeSourceFeature, provider.EncodeGroupsFeature)
	d.Engines[0].Features = append(d.Engines[0].Features, provider.EncodeSourceFeature, provider.EncodeGroupsFeature)
	value := decoded.Documents[0].Fragments[0].Values[0].Value
	binding := decodeRequest.Documents[0].Bindings[0]
	request := &protocol.EncodeSourceRequest{Engine: "fixture", TargetFeatures: decodeRequest.TargetFeatures, Format: protocol.SourceDocument_YAML,
		Bindings: []*protocol.SourceBinding{binding},
		Values:   []*protocol.EncodeSourceValue{{Placement: protocol.SourceModelValue_FACET, Value: value, BindingId: binding.Id}}}
	part := func(name string) *protocol.EncodedSourcePart {
		fragment := proto.CloneOf(decodeRequest.Documents[0].Fragments[0])
		fragment.Id, fragment.Subject.Name = name, name
		withoutSpans(fragment)
		return &protocol.EncodedSourcePart{Fragment: fragment}
	}
	result := &protocol.EncodedSource{Complete: true,
		Groups: []*protocol.EncodedSourceGroup{{ValueIds: []string{value.Id}, Parts: []*protocol.EncodedSourcePart{part("by_emb"), part("by_title")}}},
		Losses: []*protocol.EncodeLoss{{ValueId: value.Id, Message: "the storage of the table has no YAML spelling"}}}
	return d, request, result
}

// withoutSpans clears every position of a fragment, as an encoded fragment
// carries none.
func withoutSpans(fragment *protocol.SourceFragment) {
	fragment.Span = nil
	var strip func(*protocol.SourceValue)
	strip = func(value *protocol.SourceValue) {
		value.Span = nil
		for _, item := range value.GetSequence().GetValues() {
			strip(item)
		}
		for _, entry := range value.GetMapping().GetEntries() {
			entry.KeySpan = nil
			strip(entry.Value)
		}
	}
	for _, entry := range fragment.GetInheritedAttributes().GetEntries() {
		entry.KeySpan = nil
		strip(entry.Value)
	}
	strip(fragment.GetValue())
}

// An index-kind part writes a facet of its table, and a loss may name the
// value it writes in part.
func TestIndexKindEncoding_HappyPath(t *testing.T) {
	c := qt.New(t)
	d, request, result := indexKindEncodeExchange()

	c.Assert(provider.ValidateDescription(d, nil), qt.IsNil)
	c.Assert(provider.ValidateEncodedSource(d, request, result), qt.IsNil)
	c.Assert(provider.EncodedSourceParts(result), qt.HasLen, 2)
}

func TestIndexKindEncoding_FailurePath(t *testing.T) {
	part := func(r *protocol.EncodedSource) *protocol.SourceFragment { return r.Groups[0].Parts[0].Fragment }
	for _, test := range []struct {
		name   string
		want   string
		mutate func(*protocol.EncodeSourceRequest, *protocol.EncodedSource)
	}{
		{"a part of another table", "an encoding part names no group value with its model, binding and subject", func(r *protocol.EncodeSourceRequest, e *protocol.EncodedSource) {
			r.Bindings = append(r.Bindings, &protocol.SourceBinding{Id: "other", Object: &protocol.Reference{Engine: "fixture", Kind: "table", Name: "other"}})
			part(e).BindingId = "other"
			part(e).Subject = &protocol.Reference{Engine: "fixture", Kind: "index", Path: []string{"other"}, Name: "by_emb"}
		}},
		{"a part without its type", "index-kind fragment needs its type and its columns", func(_ *protocol.EncodeSourceRequest, e *protocol.EncodedSource) {
			part(e).InheritedAttributes.Entries = part(e).InheritedAttributes.Entries[1:]
		}},
		{"a value lost twice", "a loss names a value of the request once, in one line", func(_ *protocol.EncodeSourceRequest, e *protocol.EncodedSource) {
			e.Losses = append(e.Losses, proto.CloneOf(e.Losses[0]))
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			d, request, result := indexKindEncodeExchange()
			test.mutate(request, result)

			err := provider.ValidateEncodedSource(d, request, result)

			c.Assert(err, qt.ErrorIs, provider.ErrProtocol)
			c.Assert(err, qt.ErrorMatches, ".*"+regexp.QuoteMeta(test.want))
		})
	}
}

// Without the claim, an index-kind part is one the description does not hold.
func TestIndexKindPartsNeedTheNegotiatedClaim(t *testing.T) {
	c := qt.New(t)
	d, request, result := indexKindEncodeExchange()
	engine := d.Engines[0]
	engine.Features = []string{"retention", provider.EntryKeysFeature, provider.EncodeSourceFeature, provider.EncodeGroupsFeature}
	engine.IndexKinds = nil

	err := provider.ValidateEncodedSource(d, request, result)

	c.Assert(err, qt.ErrorIs, provider.ErrProtocol)
	c.Assert(err, qt.ErrorMatches, `.*index-kind fragment changed its negotiated claim`)
}

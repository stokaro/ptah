package provider_test

import (
	"errors"
	"regexp"
	"testing"

	qt "github.com/frankban/quicktest"
	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
	"github.com/stokaro/ptah/provider-sdk/provider"
	"google.golang.org/protobuf/proto"
)

func textEntry(key, value string) *protocol.SourceEntry {
	return &protocol.SourceEntry{Key: key, KeySpan: sourceSpan(), Value: &protocol.SourceValue{Span: sourceSpan(), Value: &protocol.SourceValue_Text{Text: value}}}
}

func indexSubject() *protocol.Reference {
	return &protocol.Reference{Engine: "fixture", Kind: "index", Schema: "c", Path: []string{"a.b"}, Name: "by_name"}
}

// entryKeyExchange claims a YAML key of an index entry that reads the
// entry's own type.
func entryKeyExchange() (*protocol.Description, *protocol.DecodeSourceRequest) {
	d, request, _ := sourceExchange()
	d.Features = append(d.Features, provider.EntryKeysFeature)
	d.Engines[0].Features = append(d.Engines[0].Features, provider.EntryKeysFeature)
	claim := d.Engines[0].YamlSourceKeys[0]
	claim.Scope, claim.Inherited = protocol.YAMLSourceKey_INDEX, []string{"type", "ops"}
	fragment := request.Documents[0].Fragments[0]
	fragment.YamlKey = proto.CloneOf(claim)
	fragment.Subject = indexSubject()
	fragment.InheritedAttributes = &protocol.SourceMapping{Entries: []*protocol.SourceEntry{textEntry("type", "vector")}}
	return d, request
}

func TestYAMLEntryKeys_HappyPath(t *testing.T) {
	c := qt.New(t)
	d, request := entryKeyExchange()
	c.Assert(provider.ValidateDescription(d, nil), qt.IsNil)
	c.Assert(provider.ValidateSourceRequest(d, request), qt.IsNil)
	column := proto.CloneOf(d.Engines[0].YamlSourceKeys[0])
	column.Scope, column.Key, column.Inherited = protocol.YAMLSourceKey_COLUMN, "family", nil
	d.Engines[0].YamlSourceKeys = append(d.Engines[0].YamlSourceKeys, column)
	fragment := proto.CloneOf(request.Documents[0].Fragments[0])
	// A claim without an inherited list keeps the rule of contract 25.2 and
	// reads any common key beside its own.
	fragment.Id, fragment.YamlKey = "family", proto.CloneOf(column)
	fragment.InheritedAttributes = &protocol.SourceMapping{Entries: []*protocol.SourceEntry{textEntry("comment", "The family")}}
	fragment.Subject = &protocol.Reference{Engine: "fixture", Kind: "column", Schema: "c", Path: []string{"a.b"}, Name: "id"}
	request.Documents[0].Fragments = append(request.Documents[0].Fragments, fragment)
	c.Assert(provider.ValidateDescription(d, nil), qt.IsNil)
	c.Assert(provider.ValidateSourceRequest(d, request), qt.IsNil)
}

func TestYAMLEntryKeyClaims_FailurePath(t *testing.T) {
	for _, test := range []struct {
		name   string
		mutate func(*protocol.Description)
	}{
		{"the engine does not list the feature", func(d *protocol.Description) { d.Engines[0].Features = d.Engines[0].Features[:1] }},
		{"the provider does not list the feature", func(d *protocol.Description) { d.Features = d.Features[:len(d.Features)-1] }},
		{"a table key that inherits", func(d *protocol.Description) { d.Engines[0].YamlSourceKeys[0].Scope = protocol.YAMLSourceKey_TABLE }},
		{"an inherited key named twice", func(d *protocol.Description) {
			d.Engines[0].YamlSourceKeys[0].Inherited = []string{"type", "type"}
		}},
		{"an inherited key the claim owns", func(d *protocol.Description) {
			d.Engines[0].YamlSourceKeys[0].Inherited = []string{"retention"}
		}},
		{"an inherited key with a space", func(d *protocol.Description) { d.Engines[0].YamlSourceKeys[0].Inherited = []string{"a b"} }},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			d, _ := entryKeyExchange()
			test.mutate(d)
			c.Assert(provider.ValidateDescription(d, nil), qt.ErrorIs, provider.ErrProtocol)
		})
	}
}

func TestYAMLEntryKeyFragments_FailurePath(t *testing.T) {
	for _, test := range []struct {
		name   string
		mutate func(*protocol.SourceFragment, *protocol.SourceDocument)
	}{
		{"no subject", func(f *protocol.SourceFragment, _ *protocol.SourceDocument) { f.Subject = nil }},
		{"a subject of another kind", func(f *protocol.SourceFragment, _ *protocol.SourceDocument) { f.Subject.Kind = "column" }},
		{"a subject without its table", func(f *protocol.SourceFragment, _ *protocol.SourceDocument) { f.Subject.Path = nil }},
		{"no binding", func(f *protocol.SourceFragment, _ *protocol.SourceDocument) { f.BindingId = "" }},
		{"a binding without an entry key", func(_ *protocol.SourceFragment, d *protocol.SourceDocument) { d.Bindings[0].EntryKey = "" }},
		{"a binding that is no table", func(_ *protocol.SourceFragment, d *protocol.SourceDocument) { d.Bindings[0].Object.Kind = "view" }},
		{"an inherited key outside the claim", func(f *protocol.SourceFragment, _ *protocol.SourceDocument) {
			f.InheritedAttributes.Entries = append(f.InheritedAttributes.Entries, textEntry("unique", "true"))
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			d, request := entryKeyExchange()
			test.mutate(request.Documents[0].Fragments[0], request.Documents[0])
			c.Assert(provider.ValidateSourceRequest(d, request), qt.ErrorIs, provider.ErrProtocol)
		})
	}
}

// A host that does not list the feature gets no entry-scope claim, and a
// model left with no YAML claim no longer takes YAML, so the description
// stays valid for that host.
func TestYAMLEntryKeysAreTwoWay(t *testing.T) {
	c := qt.New(t)
	d, request := entryKeyExchange()
	listed := provider.NegotiatedDescription(d, []string{provider.EntryKeysFeature})
	c.Assert(listed.Engines[0].YamlSourceKeys, qt.HasLen, 1)
	unlisted := provider.NegotiatedDescription(d, nil)
	c.Assert(unlisted.Engines[0].YamlSourceKeys, qt.HasLen, 0)
	c.Assert(unlisted.Engines[0].ValueModels[0].SourceFormats, qt.HasLen, 0)
	d.Engines[0].YamlSourceKeys[0].Scope, d.Engines[0].YamlSourceKeys[0].Inherited = protocol.YAMLSourceKey_COLUMN, nil
	c.Assert(provider.NegotiatedDescription(d, nil).Engines[0].YamlSourceKeys, qt.HasLen, 0)
	d.Engines[0].YamlSourceKeys[0].Scope, d.Engines[0].YamlSourceKeys[0].Inherited = protocol.YAMLSourceKey_INDEX, []string{"type", "ops"}
	c.Assert(provider.ValidateDescription(unlisted, nil), qt.IsNil)
	c.Assert(provider.ValidateSourceRequest(unlisted, request), qt.ErrorIs, provider.ErrProtocol)
	table := proto.CloneOf(d.Engines[0].YamlSourceKeys[0])
	table.Scope, table.Key, table.Inherited = protocol.YAMLSourceKey_TABLE, "settings", nil
	document := proto.CloneOf(table)
	document.Scope, document.Key = protocol.YAMLSourceKey_DOCUMENT, "retentions"
	d.Engines[0].YamlSourceKeys = append(d.Engines[0].YamlSourceKeys, table, document)
	kept := provider.NegotiatedDescription(d, nil)
	c.Assert(kept.Engines[0].YamlSourceKeys, qt.HasLen, 2)
	c.Assert(kept.Engines[0].ValueModels[0].SourceFormats, qt.DeepEquals, []protocol.SourceDocument_Format{protocol.SourceDocument_YAML})
}

func inheritingGoExchange() (*protocol.Description, *protocol.DecodeSourceRequest) {
	d, request, _ := goSourceExchange()
	d.Features = append(d.Features, provider.EntryKeysFeature)
	d.Engines[0].Features = append(d.Engines[0].Features, provider.EntryKeysFeature)
	d.Engines[0].GoSourceDirectives[0].Inherited = []string{"type"}
	fragment := request.Documents[0].Fragments[0]
	fragment.GoDirective = proto.CloneOf(d.Engines[0].GoSourceDirectives[0])
	fragment.InheritedAttributes = &protocol.SourceMapping{Entries: []*protocol.SourceEntry{textEntry("type", "vector")}}
	return d, request
}

func TestGoInheritedAttributes_HappyPath(t *testing.T) {
	c := qt.New(t)
	d, request := inheritingGoExchange()
	c.Assert(provider.ValidateDescription(d, nil), qt.IsNil)
	c.Assert(provider.ValidateSourceRequest(d, request), qt.IsNil)
	unlisted := provider.NegotiatedDescription(d, nil)
	c.Assert(unlisted.Engines[0].GoSourceDirectives, qt.HasLen, 0)
	c.Assert(provider.ValidateDescription(unlisted, nil), qt.IsNil)
}

func TestGoInheritedAttributes_FailurePath(t *testing.T) {
	for _, test := range []struct {
		name   string
		mutate func(*protocol.Description, *protocol.DecodeSourceRequest)
	}{
		{"the engine does not list the feature", func(d *protocol.Description, _ *protocol.DecodeSourceRequest) {
			d.Engines[0].Features = d.Engines[0].Features[:1]
		}},
		{"an inherited attribute the claim owns", func(d *protocol.Description, r *protocol.DecodeSourceRequest) {
			d.Engines[0].GoSourceDirectives[0].Inherited = []string{"seconds"}
			r.Documents[0].Fragments[0].GoDirective = proto.CloneOf(d.Engines[0].GoSourceDirectives[0])
			r.Documents[0].Fragments[0].InheritedAttributes.Entries[0].Key = "seconds"
		}},
		{"an inherited attribute outside the claim", func(_ *protocol.Description, r *protocol.DecodeSourceRequest) {
			r.Documents[0].Fragments[0].InheritedAttributes.Entries[0].Key = "ops"
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			d, request := inheritingGoExchange()
			test.mutate(d, request)
			c.Assert(errors.Join(provider.ValidateDescription(d, nil), provider.ValidateSourceRequest(d, request)), qt.ErrorIs, provider.ErrProtocol)
		})
	}
}

// hclExchange claims a top-level block with a child and the attributes of the
// frontend's index block, and decodes one block.
func hclExchange() (*protocol.Description, *protocol.DecodeSourceRequest, *protocol.DecodedSource) {
	d, request, result := sourceExchange()
	d.Features = append(d.Features, provider.HCLSourceFeature)
	engine := d.Engines[0]
	engine.Features = append(engine.Features, provider.HCLSourceFeature)
	model := engine.ValueModels[0]
	model.SourceFormats = []protocol.SourceDocument_Format{protocol.SourceDocument_HCL}
	engine.YamlSourceKeys = nil
	block := &protocol.HCLSourceBlock{Site: protocol.HCLSourceBlock_TOP_LEVEL, Type: "retention", Naming: protocol.HCLSourceBlock_SCHEMA_AND_NAME,
		Selector: "retention", Models: []string{model.Kind}, GrantKind: "RETENTION",
		Attributes: []*protocol.HCLSourceAttribute{{Name: "seconds", Kind: protocol.HCLSourceAttribute_TEXT},
			{Name: "on", Kind: protocol.HCLSourceAttribute_REFERENCE, Refers: "table", Optional: true}},
		Children: []*protocol.HCLSourceChild{{Type: "tier", Unlabeled: true, Attributes: []*protocol.HCLSourceAttribute{{Name: "after", Kind: protocol.HCLSourceAttribute_TEXT}}}}}
	index := &protocol.HCLSourceBlock{Site: protocol.HCLSourceBlock_INDEX_ATTRIBUTES, Type: "index", Selector: "index", Models: []string{model.Kind},
		Attributes: []*protocol.HCLSourceAttribute{{Name: "partitions", Kind: protocol.HCLSourceAttribute_TEXT}}, Inherited: []string{"type"}}
	engine.HclSource = &protocol.HCLSourceDeclaration{Blocks: []*protocol.HCLSourceBlock{block, index}, HeaderKinds: []string{"retention"}}
	document := request.Documents[0]
	document.Format, document.File = protocol.SourceDocument_HCL, "schema.hcl"
	document.Models = []*protocol.ValueModel{proto.CloneOf(model)}
	document.Limits = []*protocol.SchemaCoverageLimit{{Kind: "retention", Name: "old"}}
	settings := document.Fragments[0]
	settings.YamlKey, settings.BindingId, settings.HclBlock = nil, "", proto.CloneOf(block)
	settings.Content = &protocol.SourceFragment_Block{Block: &protocol.SourceBlock{Type: "retention", Span: sourceSpan(),
		Labels:     []*protocol.SourceLabel{{Value: "keep", Span: sourceSpan()}},
		Attributes: &protocol.SourceMapping{Entries: []*protocol.SourceEntry{textEntry("seconds", "60")}},
		Children:   []*protocol.SourceBlock{{Type: "tier", Span: sourceSpan(), Attributes: &protocol.SourceMapping{Entries: []*protocol.SourceEntry{textEntry("after", "1d")}}}}}}
	document.Fragments = append(document.Fragments, &protocol.SourceFragment{Id: "index", Selector: "index", BindingId: "table", Span: sourceSpan(),
		HclBlock: proto.CloneOf(index), Subject: indexSubject(),
		InheritedAttributes: &protocol.SourceMapping{Entries: []*protocol.SourceEntry{textEntry("type", "vector")}},
		Content: &protocol.SourceFragment_Value{Value: &protocol.SourceValue{Span: sourceSpan(), Value: &protocol.SourceValue_Mapping{
			Mapping: &protocol.SourceMapping{Entries: []*protocol.SourceEntry{textEntry("partitions", "4")}}}}}})
	decoded := result.Documents[0]
	decoded.Coverage.Kinds[0].Model = proto.CloneOf(model)
	decoded.Fragments = append(decoded.Fragments, &protocol.DecodedSourceFragment{Id: "index"})
	return d, request, result
}

func TestHCLClaims_HappyPath(t *testing.T) {
	c := qt.New(t)
	d, request, result := hclExchange()
	before := proto.CloneOf(request)
	c.Assert(provider.ValidateDescription(d, nil), qt.IsNil)
	c.Assert(provider.ValidateSourceRequest(d, request), qt.IsNil)
	c.Assert(provider.ValidateDecodedSource(d, request, result), qt.IsNil)
	c.Assert(proto.Equal(before, request), qt.IsTrue)
	// A model that takes no HCL needs no HCL claim.
	other := proto.CloneOf(d.Engines[0].ValueModels[0])
	other.Kind, other.SourceFormats = "example.org/fixture/other", nil
	d.Engines[0].ValueModels = append(d.Engines[0].ValueModels, other)
	c.Assert(provider.ValidateDescription(d, nil), qt.IsNil)
}

func TestHCLClaimDeclarations_FailurePath(t *testing.T) {
	block := func(d *protocol.Description) *protocol.HCLSourceBlock { return d.Engines[0].HclSource.Blocks[0] }
	index := func(d *protocol.Description) *protocol.HCLSourceBlock { return d.Engines[0].HclSource.Blocks[1] }
	for _, test := range []struct {
		name   string
		want   string
		mutate func(*protocol.Description)
	}{
		{"the engine lists the feature without claims", "an engine declares HCL claims exactly when it lists source.hcl", func(d *protocol.Description) {
			d.Engines[0].HclSource = nil
		}},
		{"claims without the engine's feature", "an engine declares HCL claims exactly when it lists source.hcl", func(d *protocol.Description) {
			d.Engines[0].Features = d.Engines[0].Features[:1]
		}},
		{"claims without the provider's feature", "HCL claims need the provider's source.hcl and source decoding", func(d *protocol.Description) {
			d.Features = d.Features[:len(d.Features)-1]
		}},
		{"an unknown site", "unknown HCL block site", func(d *protocol.Description) { block(d).Site = 99 }},
		{"no site", "unknown HCL block site", func(d *protocol.Description) { block(d).Site = protocol.HCLSourceBlock_SITE_UNSPECIFIED }},
		{"no type", "incomplete HCL block claim", func(d *protocol.Description) { block(d).Type = "" }},
		{"a type that is no identifier", "incomplete HCL block claim", func(d *protocol.Description) { block(d).Type = "1st" }},
		{"no selector", "incomplete HCL block claim", func(d *protocol.Description) { block(d).Selector = " " }},
		{"no models", "incomplete HCL block claim", func(d *protocol.Description) { block(d).Models = nil }},
		{"an unknown model", "HCL block names an unknown, duplicate, or unsupported desired model", func(d *protocol.Description) {
			block(d).Models = []string{"example.org/other"}
		}},
		{"a model named twice", "HCL block names an unknown, duplicate, or unsupported desired model", func(d *protocol.Description) {
			block(d).Models = append(block(d).Models, block(d).Models[0])
		}},
		{"a model that takes no HCL", "HCL block names an unknown, duplicate, or unsupported desired model", func(d *protocol.Description) {
			d.Engines[0].ValueModels[0].SourceFormats = []protocol.SourceDocument_Format{protocol.SourceDocument_GO_ANNOTATIONS}
		}},
		{"an HCL model no claim names", "HCL model has no block claim", func(d *protocol.Description) {
			other := proto.CloneOf(d.Engines[0].ValueModels[0])
			other.Kind = "example.org/fixture/other"
			d.Engines[0].ValueModels = append(d.Engines[0].ValueModels, other)
		}},
		{"a top-level block without a naming", "a top-level HCL block needs a naming", func(d *protocol.Description) {
			block(d).Naming = protocol.HCLSourceBlock_NAMING_UNSPECIFIED
		}},
		{"an unknown naming", "unknown HCL block naming", func(d *protocol.Description) { block(d).Naming = 99 }},
		{"a naming at an attribute site", "only a top-level HCL block takes a naming", func(d *protocol.Description) { index(d).Naming = protocol.HCLSourceBlock_UNLABELED }},
		{"children at an attribute site", "children, a declared type or a grant kind at a site that takes none", func(d *protocol.Description) {
			index(d).Children = block(d).Children
		}},
		{"a grant kind at an attribute site", "children, a declared type or a grant kind at a site that takes none", func(d *protocol.Description) {
			index(d).GrantKind = "INDEX"
		}},
		{"a declared type at an attribute site", "children, a declared type or a grant kind at a site that takes none", func(d *protocol.Description) {
			index(d).DeclaresType = true
		}},
		{"an inherited attribute of an owner's own block", "only an HCL attribute claim inherits", func(d *protocol.Description) { block(d).Inherited = []string{"type"} }},
		{"an inherited attribute the claim owns", "invalid, repeated or owned inherited attribute", func(d *protocol.Description) { index(d).Inherited = []string{"partitions"} }},
		{"an attribute claim of another block", "an HCL attribute claim names another block than its site's", func(d *protocol.Description) { index(d).Type = "table" }},
		{"two claims of one site and type", "duplicate HCL block claim", func(d *protocol.Description) {
			d.Engines[0].HclSource.Blocks = append(d.Engines[0].HclSource.Blocks, proto.CloneOf(index(d)))
		}},
		{"an attribute without a name", "invalid or repeated HCL attribute", func(d *protocol.Description) { block(d).Attributes[0].Name = "" }},
		{"an attribute named twice", "invalid or repeated HCL attribute", func(d *protocol.Description) { block(d).Attributes[1].Name = "seconds" }},
		{"an attribute without a kind", "unknown HCL attribute kind", func(d *protocol.Description) {
			block(d).Attributes[0].Kind = protocol.HCLSourceAttribute_KIND_UNSPECIFIED
		}},
		{"an unknown attribute kind", "unknown HCL attribute kind", func(d *protocol.Description) { block(d).Attributes[0].Kind = 99 }},
		{"a reference that refers to nothing", "an HCL reference attribute names the block it refers to, and no other attribute does", func(d *protocol.Description) {
			block(d).Attributes[1].Refers = ""
		}},
		{"a text that refers to a block", "an HCL reference attribute names the block it refers to, and no other attribute does", func(d *protocol.Description) {
			block(d).Attributes[0].Refers = "table"
		}},
		{"a text that refers to a name no block takes", "an HCL reference attribute names the block it refers to, and no other attribute does", func(d *protocol.Description) {
			block(d).Attributes[0].Refers = "1st"
		}},
		{"an invalid grant kind", "invalid HCL grant kind", func(d *protocol.Description) { block(d).GrantKind = "RETENTION\nNODE" }},
		{"an optional text", "only an HCL reference attribute is optional", func(d *protocol.Description) { block(d).Attributes[0].Optional = true }},
		{"a child named twice", "invalid or repeated HCL child block", func(d *protocol.Description) {
			block(d).Children = append(block(d).Children, proto.CloneOf(block(d).Children[0]))
		}},
		{"a child without a type", "invalid or repeated HCL child block", func(d *protocol.Description) { block(d).Children[0].Type = "" }},
		{"a child attribute without a kind", "unknown HCL attribute kind", func(d *protocol.Description) { block(d).Children[0].Attributes[0].Kind = 0 }},
		{"a header kind named twice", "invalid or duplicate HCL header kind", func(d *protocol.Description) {
			d.Engines[0].HclSource.HeaderKinds = []string{"retention", "retention"}
		}},
		{"an empty header kind", "invalid or duplicate HCL header kind", func(d *protocol.Description) { d.Engines[0].HclSource.HeaderKinds = []string{""} }},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			d, _, _ := hclExchange()
			test.mutate(d)
			err := provider.ValidateDescription(d, nil)
			c.Assert(err, qt.ErrorIs, provider.ErrProtocol)
			c.Assert(err, qt.ErrorMatches, ".*"+regexp.QuoteMeta(test.want))
		})
	}
}

func TestHCLFragments_FailurePath(t *testing.T) {
	settings := func(r *protocol.DecodeSourceRequest) *protocol.SourceFragment { return r.Documents[0].Fragments[0] }
	index := func(r *protocol.DecodeSourceRequest) *protocol.SourceFragment { return r.Documents[0].Fragments[1] }
	for _, test := range []struct {
		name   string
		want   string
		mutate func(*protocol.DecodeSourceRequest)
	}{
		{"a fragment without its claim", "HCL fragment changed its negotiated block claim", func(r *protocol.DecodeSourceRequest) { settings(r).HclBlock = nil }},
		{"a changed claim", "HCL fragment changed its negotiated block claim", func(r *protocol.DecodeSourceRequest) { settings(r).HclBlock.Type = "other" }},
		{"a changed selector", "HCL fragment changed its negotiated block claim", func(r *protocol.DecodeSourceRequest) { settings(r).Selector = "other" }},
		{"a claim model outside the document", "HCL fragment model is outside document scope", func(r *protocol.DecodeSourceRequest) { r.Documents[0].Models = nil }},
		{"a block of another type", "HCL block claim content must be a block of its type", func(r *protocol.DecodeSourceRequest) { settings(r).GetBlock().Type = "other" }},
		{"an attribute outside the claim", "HCL attribute is outside its claim", func(r *protocol.DecodeSourceRequest) {
			settings(r).GetBlock().Attributes.Entries = append(settings(r).GetBlock().Attributes.Entries, textEntry("minutes", "1"))
		}},
		{"a child outside the claim", "HCL child block is outside its claim", func(r *protocol.DecodeSourceRequest) { settings(r).GetBlock().Children[0].Type = "stage" }},
		{"a child attribute outside the claim", "HCL attribute is outside its claim", func(r *protocol.DecodeSourceRequest) {
			settings(r).GetBlock().Children[0].Attributes.Entries[0].Key = "before"
		}},
		{"a grandchild block", "HCL child block is outside its claim", func(r *protocol.DecodeSourceRequest) {
			child := settings(r).GetBlock().Children[0]
			child.Children = []*protocol.SourceBlock{{Type: "tier", Span: sourceSpan()}}
		}},
		{"a top-level block bound to a table", "HCL fragment binding or subject does not match its site", func(r *protocol.DecodeSourceRequest) {
			settings(r).BindingId = "table"
		}},
		{"a top-level block with a subject", "HCL fragment binding or subject does not match its site", func(r *protocol.DecodeSourceRequest) {
			settings(r).Subject = indexSubject()
		}},
		{"a top-level block as a mapping", "value content does not match source format", func(r *protocol.DecodeSourceRequest) { settings(r).Content = index(r).Content }},
		{"index attributes as a block", "HCL attribute claim content must be a mapping", func(r *protocol.DecodeSourceRequest) { index(r).Content = settings(r).Content }},
		{"index attributes as text", "HCL attribute claim content must be a mapping", func(r *protocol.DecodeSourceRequest) {
			index(r).Content = &protocol.SourceFragment_Value{Value: &protocol.SourceValue{Span: sourceSpan(), Value: &protocol.SourceValue_Text{Text: "4"}}}
		}},
		{"an index attribute outside the claim", "HCL attribute is outside its claim", func(r *protocol.DecodeSourceRequest) {
			index(r).GetValue().GetMapping().Entries[0].Key = "replicas"
		}},
		{"index attributes without the index", "HCL fragment binding or subject does not match its site", func(r *protocol.DecodeSourceRequest) { index(r).Subject = nil }},
		{"index attributes of an index without its table", "HCL fragment binding or subject does not match its site", func(r *protocol.DecodeSourceRequest) {
			index(r).Subject.Path = nil
		}},
		{"index attributes of a column", "HCL fragment binding or subject does not match its site", func(r *protocol.DecodeSourceRequest) { index(r).Subject.Kind = "column" }},
		{"index attributes without the table", "HCL fragment binding or subject does not match its site", func(r *protocol.DecodeSourceRequest) { index(r).BindingId = "" }},
		{"an inherited attribute outside the claim", "inherited attribute is outside its claim", func(r *protocol.DecodeSourceRequest) {
			index(r).InheritedAttributes.Entries[0].Key = "ops"
		}},
		{"a limit of an undeclared kind", "HCL limit kind is not a declared header kind", func(r *protocol.DecodeSourceRequest) { r.Documents[0].Limits[0].Kind = "topic" }},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			d, request, _ := hclExchange()
			test.mutate(request)
			err := provider.ValidateSourceRequest(d, request)
			c.Assert(err, qt.ErrorIs, provider.ErrProtocol)
			c.Assert(err, qt.ErrorMatches, ".*"+regexp.QuoteMeta(test.want))
		})
	}
}

// An engine that declares no HCL claim keeps the rule of contract 25.2: its
// HCL fragments carry no claim, and a document carries no header limit.
func TestHCLFragmentsOfAnEngineWithoutClaims(t *testing.T) {
	c := qt.New(t)
	d, request, _ := hclExchange()
	unlisted := provider.NegotiatedDescription(d, nil)
	c.Assert(unlisted.Engines[0].HclSource, qt.IsNil)
	c.Assert(provider.ValidateDescription(unlisted, nil), qt.IsNil)
	c.Assert(provider.ValidateSourceRequest(unlisted, request), qt.ErrorMatches, `.*HCL limit kind is not a declared header kind`)
	request.Documents[0].Limits = nil
	c.Assert(provider.ValidateSourceRequest(unlisted, request), qt.ErrorMatches, `.*claims a block on an engine that declares none`)
	settings := request.Documents[0].Fragments[0]
	settings.HclBlock = nil
	request.Documents[0].Fragments = request.Documents[0].Fragments[:1]
	c.Assert(provider.ValidateSourceRequest(unlisted, request), qt.IsNil)
	settings.Content = &protocol.SourceFragment_Value{Value: &protocol.SourceValue{Span: sourceSpan(), Value: &protocol.SourceValue_Text{Text: "60"}}}
	c.Assert(provider.ValidateSourceRequest(unlisted, request), qt.ErrorMatches, `.*value content does not match source format`)
	yaml := sourceDocument()
	yaml.Limits = []*protocol.SchemaCoverageLimit{{Kind: "retention"}}
	yaml.Models = []*protocol.ValueModel{proto.CloneOf(d.Engines[0].ValueModels[0])}
	request.Documents = []*protocol.SourceDocument{yaml}
	c.Assert(provider.ValidateSourceRequest(d, request), qt.ErrorMatches, `.*header limits on a document that is not HCL`)
}

// A decoded value of an HCL fragment belongs to a model its block claim
// names.
func TestHCLResultCannotBorrowAnotherBlocksModel(t *testing.T) {
	c := qt.New(t)
	d, request, result := hclExchange()
	second := proto.CloneOf(d.Engines[0].ValueModels[0])
	second.Kind = "example.org/fixture/another"
	d.Engines[0].ValueModels = append(d.Engines[0].ValueModels, second)
	d.Engines[0].HclSource.Blocks[1].Models = []string{second.Kind}
	request.Documents[0].Fragments[1].HclBlock = proto.CloneOf(d.Engines[0].HclSource.Blocks[1])
	request.Documents[0].Models = append(request.Documents[0].Models, proto.CloneOf(second))
	result.Documents[0].Coverage.Kinds = append(result.Documents[0].Coverage.Kinds, &protocol.ModelKindCoverage{
		Model: proto.CloneOf(second), Knowledge: &protocol.ModelKnowledge{State: protocol.ModelKnowledge_COMPLETE},
	})
	c.Assert(provider.ValidateDescription(d, nil), qt.IsNil)
	c.Assert(provider.ValidateDecodedSource(d, request, result), qt.IsNil)
	result.Documents[0].Fragments[0].Values[0].Value.Value.Kind = second.Kind
	result.Documents[0].Coverage.Subjects[0].Kind = second.Kind
	c.Assert(provider.ValidateDecodedSource(d, request, result), qt.ErrorMatches, `.*decoded HCL value is outside its block claim`)
}

// A Go document of an engine with no Go claim takes fragments without one,
// and still refuses an HCL block claim.
func TestHCLClaimOnAnotherFormat(t *testing.T) {
	c := qt.New(t)
	d, request, _ := hclExchange()
	model := d.Engines[0].ValueModels[0]
	model.SourceFormats = append(model.SourceFormats, protocol.SourceDocument_GO_ANNOTATIONS)
	document := request.Documents[0]
	document.Format, document.Limits, document.Models = protocol.SourceDocument_GO_ANNOTATIONS, nil, []*protocol.ValueModel{proto.CloneOf(model)}
	document.Fragments = document.Fragments[1:]
	c.Assert(provider.ValidateSourceRequest(d, request), qt.ErrorMatches, `.*HCL block claim on another source format`)
	document.Fragments[0].HclBlock = nil
	c.Assert(provider.ValidateSourceRequest(d, request), qt.IsNil)
}

// A model an HCL claim names is enrolled by the document the fragment is in.
func TestHCLClaimModelOutsideTheDocument(t *testing.T) {
	c := qt.New(t)
	d, request, _ := hclExchange()
	second := proto.CloneOf(d.Engines[0].ValueModels[0])
	second.Kind = "example.org/fixture/another"
	d.Engines[0].ValueModels = append(d.Engines[0].ValueModels, second)
	index := d.Engines[0].HclSource.Blocks[1]
	index.Models = append(index.Models, second.Kind)
	request.Documents[0].Fragments[1].HclBlock = proto.CloneOf(index)
	c.Assert(provider.ValidateDescription(d, nil), qt.IsNil)
	c.Assert(provider.ValidateSourceRequest(d, request), qt.ErrorMatches, `.*HCL fragment model is outside document scope`)
	request.Documents[0].Models = append(request.Documents[0].Models, proto.CloneOf(second))
	c.Assert(provider.ValidateSourceRequest(d, request), qt.IsNil)
}

// Table attributes attach to the table their fragment is bound to.
func TestHCLTableAttributes(t *testing.T) {
	c := qt.New(t)
	d, request, _ := hclExchange()
	table := &protocol.HCLSourceBlock{Site: protocol.HCLSourceBlock_TABLE_ATTRIBUTES, Type: "table", Selector: "table",
		Models: []string{d.Engines[0].ValueModels[0].Kind}, Attributes: []*protocol.HCLSourceAttribute{{Name: "keep", Kind: protocol.HCLSourceAttribute_TEXT}}}
	d.Engines[0].HclSource.Blocks = append(d.Engines[0].HclSource.Blocks, table)
	fragment := &protocol.SourceFragment{Id: "table", Selector: "table", BindingId: "table", Span: sourceSpan(), HclBlock: proto.CloneOf(table),
		Content: &protocol.SourceFragment_Value{Value: &protocol.SourceValue{Span: sourceSpan(), Value: &protocol.SourceValue_Mapping{
			Mapping: &protocol.SourceMapping{Entries: []*protocol.SourceEntry{textEntry("keep", "1d")}}}}}}
	request.Documents[0].Fragments = append(request.Documents[0].Fragments, fragment)
	c.Assert(provider.ValidateDescription(d, nil), qt.IsNil)
	c.Assert(provider.ValidateSourceRequest(d, request), qt.IsNil)
	fragment.BindingId = ""
	c.Assert(provider.ValidateSourceRequest(d, request), qt.ErrorMatches, `.*HCL fragment binding or subject does not match its site`)
}

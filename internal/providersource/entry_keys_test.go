package providersource_test

import (
	"context"
	"testing"

	qt "github.com/frankban/quicktest"
	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/testing/protocmp"

	"ptah.run/core/annotation"
	"ptah.run/core/coverage"
	"ptah.run/core/goschema"
	"ptah.run/core/schemaext"
	"ptah.run/core/yamlext"
	"ptah.run/core/yamlschema"
	"ptah.run/internal/providersource"
)

// captureEntries parses source with an owner of the column key shade and the
// index key shade, which reads the index's type with it, and returns what the
// frontend hands the owner.
func captureEntries(c *qt.C, source string) yamlext.Document {
	c.Helper()
	var document yamlext.Document
	owners, err := yamlext.NewSet(yamlext.Extension{Owner: "example.org/test", EntrySections: []yamlext.EntrySection{
		{Entry: yamlext.EntryColumn, Key: "shade"}, {Entry: yamlext.EntryIndex, Key: "shade", Reads: []string{"type"}},
	}, Document: func(_ context.Context, input yamlext.Document) (yamlext.DocumentResult, error) {
		document = input
		return yamlext.DocumentResult{}, nil
	}})
	c.Assert(err, qt.IsNil)
	_, err = yamlschema.Parse(c.Context(), owners, []byte(source))
	c.Assert(err, qt.IsNil)
	return document
}

func entryEngine() *protocol.Engine {
	engine := sourceEngine()
	engine.YamlSourceKeys = []*protocol.YAMLSourceKey{
		{Scope: protocol.YAMLSourceKey_COLUMN, Key: "shade", Selector: "column"},
		{Scope: protocol.YAMLSourceKey_INDEX, Key: "shade", Selector: "index", Inherited: []string{"type"}},
	}
	return engine
}

// TestYAML_EntryFragments sends a column and an index key with the table's
// binding, the column or the index as the subject, and the index type the
// index key inherits beside its own value.
func TestYAML_EntryFragments(t *testing.T) {
	c := qt.New(t)
	document := captureEntries(c, "tables:\n  items:\n    schema: app\n    columns:\n      id: {type: INTEGER, shade: teal}\n"+
		"    indexes:\n      by_id: {fields: [id], type: btree, shade: blue}\n")

	request, err := providersource.YAML(c.Context(), entryEngine(), document)

	c.Assert(err, qt.IsNil)
	fragments := request.GetDocuments()[0].GetFragments()
	c.Assert(fragments, qt.HasLen, 2)
	c.Assert(fragments[0].GetBindingId(), qt.Equals, "items")
	c.Assert(fragments[0].GetSubject(), qt.CmpEquals(protocmp.Transform()), &protocol.Reference{Engine: "example", Kind: "column", Schema: "app", Path: []string{"items"}, Name: "id"})
	c.Assert(fragments[0].GetInheritedAttributes(), qt.IsNil)
	c.Assert(fragments[1].GetSubject(), qt.CmpEquals(protocmp.Transform()), &protocol.Reference{Engine: "example", Kind: "index", Schema: "app", Path: []string{"items"}, Name: "by_id"})
	inherited := fragments[1].GetInheritedAttributes().GetEntries()
	c.Assert(inherited, qt.HasLen, 1)
	c.Assert(inherited[0].GetKey(), qt.Equals, "type")
	c.Assert(inherited[0].GetValue().GetText(), qt.Equals, "btree")
	c.Assert(inherited[0].GetValue().GetSpan().GetStart().GetLine(), qt.Equals, uint32(7))
	c.Assert(fragments[1].GetValue().GetText(), qt.Equals, "blue")
}

// TestYAML_EntryFragments_FailurePath refuses a fragment that carries a key
// of its entry its claim does not inherit.
func TestYAML_EntryFragments_FailurePath(t *testing.T) {
	c := qt.New(t)
	document := captureEntries(c, "tables:\n  items:\n    columns:\n      id: {type: INTEGER}\n    indexes:\n      by_id: {fields: [id], type: btree, shade: blue}\n")
	engine := entryEngine()
	engine.YamlSourceKeys[1].Inherited = nil

	request, err := providersource.YAML(c.Context(), engine, document)

	c.Assert(err, qt.ErrorMatches, `YAML .* at \[tables items indexes by_id shade\]: YAML fragment carries the key "type" of its entry, which its claim does not inherit`)
	c.Assert(request, qt.IsNil)
}

// TestGoAnnotations_InheritedAttributes sends the attributes of the
// frontend's index directive a claim inherits apart from the claim's own.
func TestGoAnnotations_InheritedAttributes(t *testing.T) {
	c := qt.New(t)
	var (
		declarations []annotation.Declaration
		tables       annotation.Tables
	)
	owners, err := annotation.NewSet(annotation.Extension{Owner: "example.org/test",
		Attributes: []annotation.DirectiveAttributes{{Directive: "ptah:schema:index", Attributes: []annotation.Attribute{{Name: "bloom", Value: "string"}},
			Reads: []string{"type"}, PerFile: true}},
		Kinds:    []schemaext.Kind{retentionModel},
		File:     func() annotation.FileDecoder { return goCapture{declarations: &declarations, tables: &tables} },
		Coverage: func([]coverage.Object) (schemaext.Coverage, error) { return schemaext.Coverage{}, nil }})
	c.Assert(err, qt.IsNil)
	_, err = goschema.ParseSource(owners, "records.go",
		"package p\n\n//ptah:schema:table name=\"records\"\ntype Record struct {\n\t//ptah:schema:field name=\"id\" type=\"INTEGER\"\n\tID int\n\n"+
			"\t//ptah:schema:index name=\"by_id\" fields=\"id\" type=\"btree\" bloom=on\n\t_ int\n}\n")
	c.Assert(err, qt.IsNil)
	engine := goEngine()
	engine.GoSourceDirectives = []*protocol.GoSourceDirective{{Scope: protocol.GoSourceDirective_INDEX, Directive: "ptah:schema:index", Selector: "index",
		Attributes: []string{"bloom"}, Inherited: []string{"type"}, Models: []string{retentionModel}}}

	request, err := providersource.GoAnnotations(c.Context(), engine, tables, declarations)

	c.Assert(err, qt.IsNil)
	fragment := request.GetDocuments()[0].GetFragments()[0]
	keys := func(mapping *protocol.SourceMapping) []string {
		var names []string
		for _, entry := range mapping.GetEntries() {
			names = append(names, entry.GetKey())
		}
		return names
	}
	c.Assert(keys(fragment.GetValue().GetMapping()), qt.DeepEquals, []string{"bloom"})
	c.Assert(keys(fragment.GetInheritedAttributes()), qt.DeepEquals, []string{"type"})
	c.Assert(fragment.GetInheritedAttributes().GetEntries()[0].GetValue().GetText(), qt.Equals, "btree")
}

// TestGoClaims_InheritedFailurePath refuses an inherited list on a provider's
// own directive, and an inherited attribute the frontend's directive does not
// declare.
func TestGoClaims_InheritedFailurePath(t *testing.T) {
	inheriting := func(directive string, scope protocol.GoSourceDirective_Scope, attributes, inherited []string) *protocol.Engine {
		engine := proto.CloneOf(goEngine())
		claim := engine.GetGoSourceDirectives()[0]
		claim.Directive, claim.Scope, claim.Attributes, claim.Inherited = directive, scope, attributes, inherited
		return engine
	}
	for _, row := range []struct {
		name   string
		engine *protocol.Engine
		want   string
	}{
		{"a provider's own directive", inheriting("ptah:test:retention", protocol.GoSourceDirective_TABLE, []string{"seconds"}, []string{"name"}),
			`Go directive "ptah:test:retention" of engine "example" inherits attributes; only a claim on ptah:schema:table, ptah:schema:field or ptah:schema:index reads the attributes of the directive it extends`},
		{"an attribute the directive does not declare", inheriting("ptah:schema:index", protocol.GoSourceDirective_INDEX, []string{"bloom"}, []string{"tint"}),
			`Go directive "ptah:schema:index" of engine "example" inherits attribute "tint", which the directive does not declare`},
	} {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)

			claims, err := providersource.GoClaims([]*protocol.Engine{row.engine})

			c.Assert(err, qt.ErrorMatches, row.want)
			c.Assert(claims, qt.IsNil)
		})
	}
}

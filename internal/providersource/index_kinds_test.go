package providersource_test

import (
	"context"
	"testing"

	qt "github.com/frankban/quicktest"
	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
	"google.golang.org/protobuf/testing/protocmp"

	"ptah.run/core/annotation"
	"ptah.run/core/hclext"
	"ptah.run/core/schemaext"
	"ptah.run/core/yamlext"
	"ptah.run/core/yamlschema"
	"ptah.run/internal/providersource"
)

// treeClaim is the claim of the test engine's "tree" index type.
func treeClaim() *protocol.SourceIndexKind {
	return &protocol.SourceIndexKind{Types: []string{"tree"}, Selector: "tree", Models: []string{retentionModel}}
}

// treeEngine reads the tree indexes of every source format in place of the
// frontend, and claims a fanout on each format's index declaration.
func treeEngine() *protocol.Engine {
	models := []string{retentionModel}
	return &protocol.Engine{Id: "example", IndexKinds: []*protocol.SourceIndexKind{treeClaim()},
		ValueModels: []*protocol.ValueModel{{Kind: retentionModel, Representation: protocol.ValueInput_DESIRED,
			SourceFormats: []protocol.SourceDocument_Format{protocol.SourceDocument_YAML, protocol.SourceDocument_HCL, protocol.SourceDocument_GO_ANNOTATIONS}}},
		GoSourceDirectives: []*protocol.GoSourceDirective{{Scope: protocol.GoSourceDirective_INDEX, Directive: "ptah:schema:index", Selector: "fanout",
			Attributes: []string{"fanout"}, Models: models}},
		YamlSourceKeys: []*protocol.YAMLSourceKey{{Scope: protocol.YAMLSourceKey_INDEX, Key: "fanout", Selector: "fanout", Models: models}},
		HclSource: &protocol.HCLSourceDeclaration{Blocks: []*protocol.HCLSourceBlock{{Site: protocol.HCLSourceBlock_INDEX_ATTRIBUTES, Type: "index",
			Selector: "fanout", Models: models, Attributes: []*protocol.HCLSourceAttribute{{Name: "fanout", Kind: protocol.HCLSourceAttribute_TEXT}}}}}}
}

// treeInherited is the inherited part of the tree index the sources of these
// tests declare: its type, columns, covered column and uniqueness.
func treeInherited(written string) []string {
	return []string{"type=" + written, "columns=[a b]", "include=[c]", "unique=true"}
}

// inherited writes a fragment's inherited part as key=value lines.
func inherited(fragment *protocol.SourceFragment) []string {
	var lines []string
	for _, entry := range fragment.GetInheritedAttributes().GetEntries() {
		value := entry.GetValue()
		switch {
		case value.GetSequence() != nil:
			text := "["
			for i, item := range value.GetSequence().GetValues() {
				if i > 0 {
					text += " "
				}
				text += item.GetText()
			}
			lines = append(lines, entry.GetKey()+"="+text+"]")
		case entry.GetKey() == "unique":
			lines = append(lines, "unique=true")
		default:
			lines = append(lines, entry.GetKey()+"="+value.GetText())
		}
	}
	return lines
}

// treeSubject is the index of table app.items the sources declare.
func treeSubject(schema string) *protocol.Reference {
	return &protocol.Reference{Engine: "example", Kind: "index", Schema: schema, Path: []string{"items"}, Name: "by_ab"}
}

func goIndexDeclaration(attributes map[string]string) annotation.Declaration {
	return annotation.Declaration{Directive: "ptah:schema:index", Attributes: attributes, Struct: "Item", File: "items.go", FileSize: 100,
		Start: annotation.SourcePosition{Offset: 10, Line: 2, Column: 1}, End: annotation.SourcePosition{Offset: 60, Line: 2, Column: 51},
		Subject: annotation.Subject{Kind: annotation.SubjectIndex, Struct: "Item", Name: attributes["name"]}}
}

// TestGoAnnotations_SendsAnIndexOfAClaimedTypeWhole sends an index directive
// of a claimed type as one index-kind fragment: bound to its table, the index
// as its subject, the frontend's attributes inherited and the engine's as
// content. An index directive of another type keeps its attribute claim.
func TestGoAnnotations_SendsAnIndexOfAClaimedTypeWhole(t *testing.T) {
	c := qt.New(t)
	tables := annotation.Tables{{Name: "items", Struct: "Item"}}
	declarations := []annotation.Declaration{
		goIndexDeclaration(map[string]string{"name": "by_ab", "fields": "a, b", "include": "c", "type": "Tree", "unique": "true", "fanout": "4"}),
		goIndexDeclaration(map[string]string{"name": "plain", "fanout": "2"}),
	}

	request, err := providersource.GoAnnotations(c.Context(), treeEngine(), tables, declarations)

	c.Assert(err, qt.IsNil)
	fragments := request.GetDocuments()[0].GetFragments()
	c.Assert(fragments, qt.HasLen, 2)
	tree := fragments[0]
	c.Assert(tree.GetIndexKind(), qt.CmpEquals(protocmp.Transform()), treeClaim())
	c.Assert(tree.GetGoDirective(), qt.IsNil)
	c.Assert(tree.GetSelector(), qt.Equals, "tree")
	c.Assert(tree.GetBindingId(), qt.Equals, "table-0")
	c.Assert(tree.GetSubject(), qt.CmpEquals(protocmp.Transform()), treeSubject(""))
	c.Assert(inherited(tree), qt.DeepEquals, treeInherited("Tree"))
	c.Assert(tree.GetValue().GetMapping().GetEntries(), qt.HasLen, 1)
	c.Assert(tree.GetValue().GetMapping().GetEntries()[0].GetValue().GetText(), qt.Equals, "4")
	c.Assert(fragments[1].GetIndexKind(), qt.IsNil)
	c.Assert(fragments[1].GetGoDirective().GetDirective(), qt.Equals, "ptah:schema:index")
}

func TestGoAnnotations_RefusesWhatAnIndexKindFragmentCannotCarry(t *testing.T) {
	for _, test := range []struct {
		name       string
		attributes map[string]string
		engine     func() *protocol.Engine
		want       string
	}{
		{"a condition", map[string]string{"name": "by_ab", "fields": "a", "type": "tree", "condition": "a > 0"}, treeEngine,
			`index "by_ab" is a "tree" index, which engine "example" reads in place of the frontend; the provider receives no "condition" attribute of it`},
		{"no fields", map[string]string{"name": "by_ab", "type": "tree"}, treeEngine, `index "by_ab" names no fields, or an empty one`},
		{"an empty field", map[string]string{"name": "by_ab", "fields": "a,,b", "type": "tree"}, treeEngine, `index "by_ab" names no fields, or an empty one`},
		{"a model a Go source does not take", map[string]string{"name": "by_ab", "fields": "a", "type": "tree"}, func() *protocol.Engine {
			engine := treeEngine()
			engine.ValueModels[0].SourceFormats = []protocol.SourceDocument_Format{protocol.SourceDocument_YAML}
			return engine
		}, `index "by_ab" is a "tree" index, which engine "example" reads into model "example.org/test/retention", and a go_annotations document does not take that model`},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			request, err := providersource.GoAnnotations(c.Context(), test.engine(), annotation.Tables{{Name: "items", Struct: "Item"}},
				[]annotation.Declaration{goIndexDeclaration(test.attributes)})

			c.Assert(err, qt.ErrorMatches, test.want)
			c.Assert(request, qt.IsNil)
		})
	}
}

// captureIndexes reads a YAML source with an owner that reads tree index
// entries, and returns the document the owner is handed.
func captureIndexes(c *qt.C, source string) yamlext.Document {
	c.Helper()
	var document yamlext.Document
	owners, err := yamlext.NewSet(yamlext.Extension{Owner: "example.org/test", Kinds: []schemaext.Kind{retentionModel},
		EntrySections: []yamlext.EntrySection{{Entry: yamlext.EntryIndex, Key: "fanout"}},
		ReadsIndex:    func(indexType string) bool { return indexType == "Tree" },
		Document: func(_ context.Context, input yamlext.Document) (yamlext.DocumentResult, error) {
			document = input
			return yamlext.DocumentResult{}, nil
		}})
	c.Assert(err, qt.IsNil)
	_, err = yamlschema.Parse(c.Context(), owners, []byte(source))
	c.Assert(err, qt.IsNil)
	return document
}

// TestYAML_SendsAnIndexEntryOfAClaimedTypeWhole sends a YAML index entry of a
// claimed type as one index-kind fragment, its fanout as the number written.
func TestYAML_SendsAnIndexEntryOfAClaimedTypeWhole(t *testing.T) {
	c := qt.New(t)
	document := captureIndexes(c, "tables:\n  items:\n    schema: app\n    columns:\n      a: {type: INTEGER}\n"+
		"    indexes:\n      by_ab: {fields: [a, b], include: [c], type: Tree, unique: true, fanout: 4}\n")

	request, err := providersource.YAML(c.Context(), treeEngine(), document)

	c.Assert(err, qt.IsNil)
	fragments := request.GetDocuments()[0].GetFragments()
	c.Assert(fragments, qt.HasLen, 1)
	c.Assert(fragments[0].GetIndexKind(), qt.CmpEquals(protocmp.Transform()), treeClaim())
	c.Assert(fragments[0].GetBindingId(), qt.Equals, "items")
	c.Assert(fragments[0].GetSubject(), qt.CmpEquals(protocmp.Transform()), treeSubject("app"))
	c.Assert(inherited(fragments[0]), qt.DeepEquals, treeInherited("Tree"))
	c.Assert(fragments[0].GetValue().GetMapping().GetEntries()[0].GetValue().GetNumber(), qt.Equals, "4")
}

// TestHCL_SendsAnIndexBlockOfAClaimedTypeWhole sends an index block the
// frontend claimed whole as one index-kind fragment, and an index block of
// another type as index attributes.
func TestHCL_SendsAnIndexBlockOfAClaimedTypeWhole(t *testing.T) {
	c := qt.New(t)
	claims := []hclext.Claim{
		{Block: "index", Name: "by_ab", Range: hclRange(0, 40), Attributes: map[string]hclext.Value{"fanout": {Kind: hclext.Text, Text: "4"}},
			Index: &hclext.Index{Table: "app.items", Type: "Tree", Owned: true, Columns: []string{"a", "b"}, Include: []string{"c"}, Unique: true}},
		{Block: "index", Name: "plain", Range: hclRange(50, 60), Index: &hclext.Index{Table: "app.items", Type: "btree"}},
	}

	request, err := providersource.HCL(c.Context(), treeEngine(), claims, nil)

	c.Assert(err, qt.IsNil)
	fragments := request.GetDocuments()[0].GetFragments()
	c.Assert(fragments, qt.HasLen, 2)
	c.Assert(fragments[0].GetIndexKind(), qt.CmpEquals(protocmp.Transform()), treeClaim())
	c.Assert(fragments[0].GetHclBlock(), qt.IsNil)
	c.Assert(fragments[0].GetBindingId(), qt.Equals, "table-0")
	c.Assert(fragments[0].GetSubject(), qt.CmpEquals(protocmp.Transform()), treeSubject("app"))
	c.Assert(inherited(fragments[0]), qt.DeepEquals, treeInherited("Tree"))
	c.Assert(fragments[0].GetValue().GetMapping().GetEntries()[0].GetValue().GetText(), qt.Equals, "4")
	c.Assert(fragments[1].GetIndexKind(), qt.IsNil)
	c.Assert(fragments[1].GetHclBlock().GetSite(), qt.Equals, protocol.HCLSourceBlock_INDEX_ATTRIBUTES)
}

// TestReadIndexDeclaration_HappyPath reads back the frontend's part of an
// index-kind fragment, as an encoder's part carries it.
func TestReadIndexDeclaration_HappyPath(t *testing.T) {
	c := qt.New(t)
	request, err := providersource.GoAnnotations(c.Context(), treeEngine(), annotation.Tables{{Name: "items", Struct: "Item"}},
		[]annotation.Declaration{goIndexDeclaration(map[string]string{"name": "by_ab", "fields": "a,b", "include": "c", "type": "tree", "unique": "true",
			"comment": "ab", "ops": "int_ops"})})
	c.Assert(err, qt.IsNil)

	declaration, err := providersource.ReadIndexDeclaration(request.GetDocuments()[0].GetFragments()[0])

	c.Assert(err, qt.IsNil)
	c.Assert(declaration, qt.DeepEquals, providersource.IndexDeclaration{Name: "by_ab", Type: "tree", Columns: []string{"a", "b"}, Include: []string{"c"},
		Unique: true, Comment: "ab", Ops: "int_ops"})
}

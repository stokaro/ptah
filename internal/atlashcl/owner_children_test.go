package atlashcl_test

import (
	"context"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/hclext"
	"ptah.run/core/schemaext"
	"ptah.run/engine"
	"ptah.run/internal/atlashcl"
)

// shapeDecoder declares nothing for each claim and records the requests it is
// given.
type shapeDecoder struct {
	requests *[]hclext.Request
}

func (d shapeDecoder) DecodeHCL(_ context.Context, request hclext.Request) (hclext.Result, error) {
	*d.requests = append(*d.requests, request)
	return hclext.Result{Declarations: make([]hclext.Declaration, len(request.Claims))}, nil
}

// shapeOwners registers the owner example.org/custom through the runtime. Its
// `shape` block declares a type, takes a type, a SQL default and a note, and
// nests `member` children, each taking a type and a string note. Its `widget`
// block declares no type.
func shapeOwners(requests *[]hclext.Request) hclext.Set {
	return must.Must(engine.New(engine.Provider{
		ID:     "example.org/custom",
		Codecs: []schemaext.Codec{gadgetCodec()},
		HCL: []hclext.Extension{{
			Owner: "example.org/custom",
			Kinds: []schemaext.Kind{gadgetKind},
			Blocks: []hclext.Block{
				{Site: hclext.TopLevel, Type: "shape", Naming: hclext.SchemaAndName, DeclaresType: true,
					Attributes: []hclext.Attribute{{Name: "base", Kind: hclext.TypeName}, {Name: "default", Kind: hclext.SQL}, {Name: "note", Kind: hclext.Text}},
					Children: []hclext.Child{{Type: "member", Attributes: []hclext.Attribute{
						{Name: "kind", Kind: hclext.TypeName}, {Name: "note", Kind: hclext.String}}}}},
				{Site: hclext.TopLevel, Type: "widget", Naming: hclext.SchemaAndName},
			},
			Decoder: shapeDecoder{requests: requests},
		}},
	})).HCL()
}

// TestParseWithOptions_ReadsAnOwnersChildrenInTheirOrder hands the owner the
// children of its block in the document's order, which is not sorted, each
// with its label, where it stands, and its attributes read as their kinds. A
// child that writes no attribute reaches the owner too.
func TestParseWithOptions_ReadsAnOwnersChildrenInTheirOrder(t *testing.T) {
	c := qt.New(t)
	var requests []hclext.Request
	document := "shape \"s\" {\n  note = \"n\"\n" +
		"  member \"zeta\" {\n    kind = sql(\"int4\")\n    note = \"last\"\n  }\n" +
		"  member \"alpha\" {\n    kind = enum.mood\n  }\n" +
		"  member \"mid\" {}\n}\n"

	_, err := atlashcl.ParseWithOptions([]byte(document), "schema.hcl", atlashcl.Options{HCL: shapeOwners(&requests)})

	c.Assert(err, qt.IsNil)
	c.Assert(requests, qt.DeepEquals, []hclext.Request{{Claims: []hclext.Claim{{Block: "shape", Name: "s",
		Attributes: map[string]hclext.Value{"note": {Kind: hclext.Text, Text: "n"}},
		Children: []hclext.ClaimChild{
			{Type: "member", Name: "zeta", At: "schema.hcl:3", Attributes: map[string]hclext.Value{
				"kind": {Kind: hclext.TypeName, Text: "int4"}, "note": {Kind: hclext.String, Text: "last"}}},
			{Type: "member", Name: "alpha", At: "schema.hcl:7", Attributes: map[string]hclext.Value{"kind": {Kind: hclext.TypeName, Text: "mood"}}},
			{Type: "member", Name: "mid", At: "schema.hcl:10", Attributes: make(map[string]hclext.Value)},
		},
		At: "schema.hcl:1"}}}})
}

// TestParseWithOptions_RefusesAnOwnersChildAsItsOwn refuses a child the way
// the reader refuses a block of its own, positioned at the child: its labels,
// a second child of one name, an attribute its shape does not list, a value
// of the wrong kind, and a block nested in it. A nested block that is no
// child is refused at it as an unknown name.
func TestParseWithOptions_RefusesAnOwnersChildAsItsOwn(t *testing.T) {
	tests := []struct {
		name     string
		document string
		wantErr  string
	}{
		{name: "a child without a label", document: "shape \"s\" {\n  member {}\n}\n",
			wantErr: `parse HCL schema at schema.hcl:2,3-9: shape member requires exactly one name label`},
		{name: "a child with two labels", document: "shape \"s\" {\n  member \"a\" \"b\" {}\n}\n",
			wantErr: `parse HCL schema at schema.hcl:2,3-9: shape member requires exactly one name label`},
		{name: "a child twice", document: "shape \"s\" {\n  member \"a\" {}\n  member \"a\" {}\n}\n",
			wantErr: `parse HCL schema at schema.hcl:3,3-9: shape "s" declares member "a" twice`},
		{name: "an attribute the child does not take", document: "shape \"s\" {\n  member \"a\" {\n    color = \"red\"\n  }\n}\n",
			wantErr: `parse HCL schema at schema.hcl:2,3-9: unsupported shape member attribute "color"`},
		{name: "a child's string of another kind", document: "shape \"s\" {\n  member \"a\" {\n    note = 5\n  }\n}\n",
			wantErr: `parse HCL schema at schema.hcl:2,3-9: shape member attribute "note" must be a string`},
		{name: "a block nested in a child", document: "shape \"s\" {\n  member \"a\" {\n    inner {}\n  }\n}\n",
			wantErr: `parse HCL schema at schema.hcl:3,5-10: unsupported shape member block "inner"`},
		{name: "a nested block that is no child", document: "shape \"s\" {\n  other \"x\" {}\n}\n",
			wantErr: `parse HCL schema at schema.hcl:2,3-8: unsupported shape block "other"`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			var requests []hclext.Request

			db, err := atlashcl.ParseWithOptions([]byte(test.document), "schema.hcl", atlashcl.Options{HCL: shapeOwners(&requests)})

			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(db, qt.IsNil)
		})
	}
}

// TestParseWithOptions_ReadsATypeNameAsAColumnTypeIsRead drives a column's
// `type` and an owner's TypeName attribute with the same spelling and gets the
// same type from both, so the two positions cannot drift apart. A reference
// whose root declares types reads as the rest of it, whether the reader's own
// block or an owner's declares it, and whether or not the document declares
// the type; anything else reads as text.
func TestParseWithOptions_ReadsATypeNameAsAColumnTypeIsRead(t *testing.T) {
	tests := []struct {
		name     string
		spelling string
		want     string
	}{
		{name: "the reader's own enum", spelling: "enum.mood", want: "mood"},
		{name: "a type no block declares", spelling: "enum.missing", want: "missing"},
		{name: "a type in a schema", spelling: "enum.app.mood", want: "app.mood"},
		{name: "an owner's block that declares types", spelling: "shape.s", want: "s"},
		{name: "an owner's block that declares no type", spelling: "widget.w", want: "widget.w"},
		{name: "a string that spells a reference", spelling: `"enum.mood"`, want: "enum.mood"},
		{name: "an sql() call", spelling: `sql("int4")`, want: "int4"},
		{name: "a schema-qualified type", spelling: "pg_catalog.int4", want: "pg_catalog.int4"},
		{name: "a bare block keyword", spelling: "enum", want: "enum"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			var requests []hclext.Request
			document := "table \"t\" {\n  column \"c\" {\n    type = " + test.spelling + "\n  }\n}\n" +
				"shape \"s\" {\n  base = " + test.spelling + "\n}\n"

			db, err := atlashcl.ParseWithOptions([]byte(document), "schema.hcl", atlashcl.Options{HCL: shapeOwners(&requests)})

			c.Assert(err, qt.IsNil)
			c.Assert(db.Fields, qt.HasLen, 1)
			c.Assert(db.Fields[0].Type, qt.Equals, test.want)
			c.Assert(requests[0].Claims[0].Attributes["base"], qt.DeepEquals, hclext.Value{Kind: hclext.TypeName, Text: test.want})
		})
	}
}

// TestParseWithOptions_ReadsAnSQLAttributeAsWritten reads an sql() call as
// its SQL, of kind SQL, and every other value as text reads it, of kind Text,
// so the owner can tell an expression from a literal.
func TestParseWithOptions_ReadsAnSQLAttributeAsWritten(t *testing.T) {
	tests := []struct {
		name     string
		spelling string
		want     hclext.Value
	}{
		{name: "an sql() call", spelling: `sql("now()")`, want: hclext.Value{Kind: hclext.SQL, Text: "now()"}},
		{name: "a string", spelling: `"now()"`, want: hclext.Value{Kind: hclext.Text, Text: "now()"}},
		{name: "a number", spelling: "5", want: hclext.Value{Kind: hclext.Text, Text: "5"}},
		{name: "a variable", spelling: "var.d", want: hclext.Value{Kind: hclext.Text, Text: "from a variable"}},
		{name: "a traversal", spelling: "foo.bar", want: hclext.Value{Kind: hclext.Text, Text: "foo.bar"}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			var requests []hclext.Request
			document := "variable \"d\" {\n  type    = string\n  default = \"from a variable\"\n}\n\n" +
				"shape \"s\" {\n  default = " + test.spelling + "\n}\n"

			_, err := atlashcl.ParseWithOptions([]byte(document), "schema.hcl", atlashcl.Options{HCL: shapeOwners(&requests)})

			c.Assert(err, qt.IsNil)
			c.Assert(requests[0].Claims[0].Attributes["default"], qt.DeepEquals, test.want)
		})
	}
}

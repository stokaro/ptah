package atlashcl_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/hclext"
	"ptah.run/internal/atlashcl"
)

func dialectShapes(pg, oracle *[]hclext.Request) hclext.Set {
	return must.Must(hclext.NewSet(
		hclext.Extension{Owner: "example.org/pg", Decoder: shapeDecoder{requests: pg}, Blocks: []hclext.Block{
			{Site: hclext.TopLevel, Type: "shape", Naming: hclext.SchemaAndName, DeclaresType: true,
				Dialects: []string{"postgres"}, Unscoped: true, Attributes: []hclext.Attribute{{Name: "pg", Kind: hclext.Text}}},
		}},
		hclext.Extension{Owner: "example.org/oracle", Decoder: shapeDecoder{requests: oracle}, Blocks: []hclext.Block{
			{Site: hclext.TopLevel, Type: "shape", Naming: hclext.SchemaAndName, DeclaresType: true,
				Dialects: []string{"oracle"}, Attributes: []hclext.Attribute{{Name: "ora", Kind: hclext.Text}}},
		}},
	))
}

func TestParseWithOptions_BlockDialectOverridesTheCaller(t *testing.T) {
	for _, test := range []struct {
		name, dialect, selector, attribute, selected string
		pg, oracle                                   int
	}{
		{name: "unscoped", attribute: "pg", pg: 1},
		{name: "caller", dialect: "oracle", attribute: "ora", selected: "oracle", oracle: 1},
		{name: "explicit override", dialect: "postgres", selector: `dialect = "oracle"`, attribute: "ora", selected: "oracle", oracle: 1},
		{name: "alias", dialect: "oracle", selector: `dialect = "pgx"`, attribute: "pg", selected: "postgres", pg: 1},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			var pg, oracle []hclext.Request
			document := "shape \"s\" {\n " + test.attribute + " = \"value\"" + "\n" + test.selector + "\n}"
			parsed, err := atlashcl.ParseWithOptions([]byte(document), "types.hcl", atlashcl.Options{
				HCL: dialectShapes(&pg, &oracle), Dialect: test.dialect,
			})
			c.Assert(err, qt.IsNil)
			c.Assert(parsed, qt.IsNotNil)
			c.Assert(pg, qt.HasLen, test.pg)
			c.Assert(oracle, qt.HasLen, test.oracle)
			requests := append(pg, oracle...)
			c.Assert(requests[0].Claims[0].Dialect, qt.Equals, test.selected)
			c.Assert(requests[0].Claims[0].Attributes, qt.DeepEquals, map[string]hclext.Value{test.attribute: {Kind: hclext.Text, Text: "value"}})
		})
	}
}

func TestParseWithOptions_KeepsDifferentBlockDialectsInOneDocument(t *testing.T) {
	c := qt.New(t)
	var pg, oracle []hclext.Request
	source := `shape "d" {
 dialect = "oracle"
 ora = "value"
}
shape "p" {
 dialect = "postgres"
 pg = "value"
}
table "t" {
 column "value" { type = shape.d }
}`
	parsed, err := atlashcl.ParseWithOptions([]byte(source), "types.hcl", atlashcl.Options{HCL: dialectShapes(&pg, &oracle), Dialect: "mysql"})
	c.Assert(err, qt.IsNil)
	c.Assert(pg, qt.HasLen, 1)
	c.Assert(oracle, qt.HasLen, 1)
	c.Assert(parsed.Fields, qt.HasLen, 1)
	c.Assert(parsed.Fields[0].Type, qt.Equals, "d")
}

func TestParseWithOptions_RefusesInvalidBlockDialect(t *testing.T) {
	for _, test := range []struct{ name, body, want string }{
		{"unavailable", `dialect = "mysql"`, `.*shape has no owner for dialect "mysql".*`},
		{"unknown", `dialect = "typo"`, `.*shape has no owner for dialect "typo".*`},
		{"empty", `dialect = ""`, `.*shape dialect must not be empty.*`},
		{"non-string", `dialect = 5`, `.*shape attribute "dialect" must be a string.*`},
		{"other owner's attribute", "dialect = \"oracle\"\n pg = \"x\"", `.*unsupported shape attribute "pg".*`},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			var pg, oracle []hclext.Request
			parsed, err := atlashcl.ParseWithOptions([]byte("shape \"s\" {\n"+test.body+"\n}"), "types.hcl", atlashcl.Options{HCL: dialectShapes(&pg, &oracle)})
			c.Assert(err, qt.ErrorMatches, test.want)
			c.Assert(parsed, qt.IsNil)
		})
	}
}

func TestParseWithOptions_AnUnavailableDialectIsNotIgnored(t *testing.T) {
	c := qt.New(t)
	var pg, oracle []hclext.Request
	parsed, err := atlashcl.ParseWithOptions([]byte(`shape "s" { dialect = "typo" }`), "types.hcl", atlashcl.Options{
		HCL: dialectShapes(&pg, &oracle), IgnoreUnknownNames: true,
	})
	c.Assert(err, qt.ErrorMatches, `.*shape has no owner for dialect "typo".*`)
	c.Assert(parsed, qt.IsNil)
}

func TestParseWithOptions_UsesTheSelectedSetsDialect(t *testing.T) {
	c := qt.New(t)
	var pg, oracle []hclext.Request
	owners := dialectShapes(&pg, &oracle).ForDialect("oracle")
	db, err := atlashcl.ParseWithOptions([]byte(`shape "s" { ora = "value" }`), "types.hcl", atlashcl.Options{HCL: owners})
	c.Assert(err, qt.IsNil)
	c.Assert(db, qt.IsNotNil)
	c.Assert(pg, qt.HasLen, 0)
	c.Assert(oracle, qt.HasLen, 1)
	c.Assert(oracle[0].Claims[0].Dialect, qt.Equals, "oracle")
}

package atlashclrender_test

import (
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/goschema"
	"go.5x5.cz/ptah/core/platform"
	"go.5x5.cz/ptah/core/renderer"
	"go.5x5.cz/ptah/internal/atlashcl"
	"go.5x5.cz/ptah/internal/atlashclrender"
	"go.5x5.cz/ptah/internal/convert/fromschema"
)

// This is the round trip issue #1138 reported: an Atlas HCL schema whose column
// type is written with the sql() escape hatch has to come back out of Ptah
// spelled the same way.
//
// The reduction issue #1106 introduced is a READ-side reduction, so the DDL Ptah
// renders is `c USER_DEFINED` rather than `c sql("USER_DEFINED")`. Writing the
// reduced text back out bare is a different loss: measured on the pinned Atlas
// community binary v1.3.0, `type = USER_DEFINED` is refused with `Unknown
// column.type; There is no type named "USER_DEFINED"` (exit 1) while
// `type = sql("USER_DEFINED")` plans at exit 0. A round trip through Ptah must
// not turn a file that binary plans into one it refuses.
//
// Without the fix each row renders `type = USER_DEFINED` -- the assertion on the
// call fails and the "still parses to the same type" assertion still passes,
// which is why the loss was invisible to the existing round-trip tests.
func TestRenderWritesSQLRawExpressionColumnTypeBackAsTheCall(t *testing.T) {

	tests := []struct {
		name     string
		source   string
		wantType string
	}{
		{
			name: "type the dialect does not model",
			source: `
table "t" {
  column "c" {
    null = true
    type = sql("USER_DEFINED")
  }
}
`,
			wantType: `type = sql("USER_DEFINED")`,
		},
		{
			// The case of the argument survives; the community binary does not
			// canonicalize it either.
			name: "lower-case argument",
			source: `
table "t" {
  column "c" {
    null = true
    type = sql("user_defined")
  }
}
`,
			wantType: `type = sql("user_defined")`,
		},
		{
			name: "argument carrying parentheses",
			source: `
table "t" {
  column "c" {
    null = true
    type = sql("varchar(10)")
  }
}
`,
			wantType: `type = sql("varchar(10)")`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			parsed, err := atlashcl.Parse([]byte(test.source), "schema.hcl")
			c.Assert(err, qt.IsNil)

			rendered, err := atlashclrender.Render(parsed)
			c.Assert(err, qt.IsNil)
			c.Assert(rendered.Diagnostics, qt.HasLen, 0)
			c.Assert(string(rendered.Data), qt.Contains, test.wantType)

			// Re-parsing has to land on the same IR, or the round trip is
			// stable in text and lossy in meaning.
			reparsed, err := atlashcl.Parse(rendered.Data, "schema.hcl")
			c.Assert(err, qt.IsNil)
			c.Assert(reparsed.Fields, qt.HasLen, 1)
			c.Assert(reparsed.Fields[0].Type, qt.Equals, parsed.Fields[0].Type)
			c.Assert(reparsed.Fields[0].TypeRawSQL, qt.IsTrue)
		})
	}
}

// The two write-backs disagree on purpose, and this is the test that pins the
// disagreement: the SQL side gets the reduced text, the HCL side gets the call.
//
// It is what rules out the other candidate fix for issue #1138 -- carrying
// `sql("USER_DEFINED")` in the type itself, the way Ptah did before issue #1106.
// That spelling makes the HCL round trip correct too, but it was measured to
// render `CREATE TABLE "main"."t" ("c" sql("USER_DEFINED"))`, which sqlite
// rejects: `ptah-compat schema apply` then exits 1 where the pinned Atlas
// community binary v1.3.0 exits 0. Under that fix the DDL assertion here fails.
func TestSQLRawExpressionColumnTypeRendersReducedInDDL(t *testing.T) {
	c := qt.New(t)

	parsed, err := atlashcl.Parse([]byte(`
table "t" {
  column "c" {
    null = true
    type = sql("USER_DEFINED")
  }
}
`), "schema.hcl")
	c.Assert(err, qt.IsNil)

	sql, err := renderer.RenderSQL(platform.SQLite, fromschema.FromDatabase(*parsed, platform.SQLite))
	c.Assert(err, qt.IsNil)
	c.Assert(sql, qt.Contains, "USER_DEFINED")
	c.Assert(sql, qt.Not(qt.Contains), "sql(")

	rendered, err := atlashclrender.Render(parsed)
	c.Assert(err, qt.IsNil)
	c.Assert(string(rendered.Data), qt.Contains, `type = sql("USER_DEFINED")`)
}

// Negative control for the test above: a column type that never went through
// the escape hatch must keep rendering as the bare type. A renderer that wrapped
// every type in sql() would pass the round-trip test above and fail here, and it
// would also make every Ptah-authored schema unreadable as a plain type.
func TestRenderKeepsOrdinaryColumnTypesBare(t *testing.T) {
	c := qt.New(t)

	db := &goschema.Database{
		Tables: []goschema.Table{{StructName: "T", Name: "t"}},
		Fields: []goschema.Field{
			{StructName: "T", Name: "plain", Type: "text"},
			{StructName: "T", Name: "sized", Type: "varchar(10)"},
			{StructName: "T", Name: "hatched", Type: "USER_DEFINED", TypeRawSQL: true},
		},
	}

	rendered, err := atlashclrender.Render(db)
	c.Assert(err, qt.IsNil)
	c.Assert(rendered.Diagnostics, qt.HasLen, 0)

	out := string(rendered.Data)
	c.Assert(out, qt.Contains, "type = text")
	c.Assert(out, qt.Contains, "type = varchar(10)")
	c.Assert(out, qt.Contains, `type = sql("USER_DEFINED")`)
	c.Assert(strings.Count(out, "sql("), qt.Equals, 1)
}

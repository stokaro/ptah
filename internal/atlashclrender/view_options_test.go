package atlashclrender_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/schemamodel"
	"ptah.run/internal/atlashcl"
	"ptah.run/internal/atlashclrender"
	"ptah.run/internal/builtintest"
)

// renderViewOptions renders db as an inspected PostgreSQL schema.
func renderViewOptions(c *qt.C, db *schemamodel.Database) atlashclrender.Result {
	c.Helper()
	result, err := atlashclrender.RenderInspected(db, "public", atlashclrender.Options{HCL: builtintest.Runtime().HCL(), Dialect: "postgres"})
	c.Assert(err, qt.IsNil)
	return result
}

// TestRender_WritesViewSecurity pins that a view's security_invoker and
// security_barrier are written as the security block, each as the bool the
// stored spelling means, and that the document declares them again. Without
// it, a schema inspected to HCL and applied would remove them.
func TestRender_WritesViewSecurity(t *testing.T) {
	c := qt.New(t)
	db := &schemamodel.Database{Views: []schemamodel.View{{
		Name: "public.report", Body: "SELECT 1 AS v", Options: []string{"security_barrier=yes", "security_invoker=on"},
	}}}

	result := renderViewOptions(c, db)

	c.Assert(string(result.Data), qt.Contains, "  security {\n    invoker = true\n    barrier = true\n  }\n")
	c.Assert(result.Diagnostics, qt.HasLen, 0)
	parsed, err := atlashcl.Parse(c.Context(), result.Data, "schema.hcl")
	c.Assert(err, qt.IsNil)
	c.Assert(parsed.Views, qt.HasLen, 1)
	c.Assert(parsed.Views[0].Options, qt.DeepEquals, []string{"security_invoker=true", "security_barrier=true"})
}

// TestRender_ReportsOptionsTheDocumentCannotHold pins the options the HCL has
// no attribute for: each is named in a diagnostic rather than left out
// without a word. A materialized view's storage parameters are among them,
// and a document read back keeps the ones the database holds.
func TestRender_ReportsOptionsTheDocumentCannotHold(t *testing.T) {
	tests := []struct {
		name    string
		db      *schemamodel.Database
		path    string
		message string
	}{
		{
			name: "a view option that is not a security option",
			db: &schemamodel.Database{Views: []schemamodel.View{{
				Name: "public.report", Body: "SELECT 1 AS v", Options: []string{"security_invoker=true", "custom_option=1"},
			}}},
			path:    "views.public.report",
			message: "view option(s) custom_option=1 have no attribute in HCL schema output and are omitted",
		},
		{
			name: "a materialized view's storage parameters",
			db: &schemamodel.Database{MaterializedViews: []schemamodel.MaterializedView{{
				Name: "public.totals", Body: "SELECT 1 AS v", Options: []string{"fillfactor=70"},
			}}},
			path: "materialized_views.public.totals",
			message: "storage parameter(s) fillfactor=70 have no attribute in HCL schema output and are omitted; " +
				"a comparison against the document keeps the ones the database holds",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			result := renderViewOptions(c, test.db)

			c.Assert(result.Diagnostics, qt.HasLen, 1)
			c.Assert(result.Diagnostics[0].Path, qt.Equals, test.path)
			c.Assert(result.Diagnostics[0].Message, qt.Equals, test.message)
		})
	}
}

// TestRender_RerenderedDocumentGainsNoHeaderLine pins that a document rendered
// from its own parse keeps its bytes. The reader records each materialized
// view's storage parameters as not described, and the record stays out of the
// header because the reader makes it again. The export from Go annotations
// refuses a document that changes on this round trip.
func TestRender_RerenderedDocumentGainsNoHeaderLine(t *testing.T) {
	c := qt.New(t)
	options := atlashcl.Options{HCL: builtintest.Runtime().HCL()}
	parsed, err := atlashcl.ParseWithOptions(c.Context(), []byte(`schema "public" {}

materialized "totals" {
  schema = schema.public
  as     = "SELECT 1 AS v"
}
`), "schema.hcl", options)
	c.Assert(err, qt.IsNil)

	first, err := atlashclrender.Render(parsed, atlashclrender.Options{HCL: builtintest.Runtime().HCL()})
	c.Assert(err, qt.IsNil)
	again, err := atlashcl.ParseWithOptions(c.Context(), first.Data, "schema.hcl", options)
	c.Assert(err, qt.IsNil)
	second, err := atlashclrender.Render(again, atlashclrender.Options{HCL: builtintest.Runtime().HCL()})
	c.Assert(err, qt.IsNil)

	c.Assert(coverageHeaderLines(string(first.Data)), qt.HasLen, 0)
	c.Assert(string(second.Data), qt.Equals, string(first.Data))
	c.Assert(again.NotDescribed, qt.DeepEquals, parsed.NotDescribed)
}

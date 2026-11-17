package atlashclrender_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/schemamodel"
)

// TestRender_ReportsAnOwnerTheDocumentCannotHold pins that a declared owner is
// named in a diagnostic when a schema is written as Atlas HCL, which has no
// owner attribute, rather than left out without a word. The document then
// leaves the owner unmanaged.
func TestRender_ReportsAnOwnerTheDocumentCannotHold(t *testing.T) {
	tests := []struct {
		name string
		db   *schemamodel.Database
		path string
	}{
		{
			name: "a view",
			db:   &schemamodel.Database{Views: []schemamodel.View{{Name: "public.report", Body: "SELECT 1 AS v", Owner: "reporting"}}},
			path: "views.public.report",
		},
		{
			name: "a materialized view",
			db: &schemamodel.Database{MaterializedViews: []schemamodel.MaterializedView{{
				Name: "public.totals", Body: "SELECT 1 AS v", Owner: "reporting",
			}}},
			path: "materialized_views.public.totals",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			result := renderViewOptions(c, test.db)

			c.Assert(result.Diagnostics, qt.HasLen, 1)
			c.Assert(result.Diagnostics[0].Path, qt.Equals, test.path)
			c.Assert(result.Diagnostics[0].Message, qt.Equals, `owner "reporting" is left out: Atlas HCL has no owner attribute`)
			c.Assert(string(result.Data), qt.Not(qt.Contains), "reporting")
		})
	}
}

// TestRender_NoOwnerNoDiagnostic is the control: a view that declares no
// owner renders without one.
func TestRender_NoOwnerNoDiagnostic(t *testing.T) {
	c := qt.New(t)

	result := renderViewOptions(c, &schemamodel.Database{Views: []schemamodel.View{{Name: "public.report", Body: "SELECT 1 AS v"}}})

	c.Assert(result.Diagnostics, qt.HasLen, 0)
}

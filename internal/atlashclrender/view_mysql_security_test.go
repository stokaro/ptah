package atlashclrender_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/schemamodel"
	"ptah.run/internal/atlashcl"
	"ptah.run/internal/atlashclrender"
	"ptah.run/internal/builtintest"
)

func TestRenderInspectedPreservesMySQLViewSQLSecurity(t *testing.T) {
	c := qt.New(t)
	db := &schemamodel.Database{Views: []schemamodel.View{{
		Name: "app.report", Body: "SELECT 1 AS v", Security: "INVOKER",
	}}}
	result, err := atlashclrender.RenderInspected(db, "app", atlashclrender.Options{
		HCL: builtintest.Runtime().HCL(), Dialect: "mysql",
	})
	c.Assert(err, qt.IsNil)
	c.Assert(result.Diagnostics, qt.HasLen, 0)
	c.Assert(string(result.Data), qt.Contains, "  security = INVOKER\n")
	parsed, err := atlashcl.Parse(c.Context(), result.Data, "schema.hcl")
	c.Assert(err, qt.IsNil)
	c.Assert(parsed.Views, qt.HasLen, 1)
	c.Assert(parsed.Views[0].Security, qt.Equals, "INVOKER")
}

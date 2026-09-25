package atlashclrender_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/schemamodel"
	"ptah.run/internal/atlashcl"
	"ptah.run/internal/atlashclrender"
)

// The planner attributes and the settings survive an export to HCL and back
// (stokaro/ptah#3630). The renderer wrote none of them, and the reader did not
// know strict, so a function read back from the export lost all four.
func TestRender_RoutineAttributesRoundTrip(t *testing.T) {
	c := qt.New(t)
	database := &schemamodel.Database{Functions: []schemamodel.Function{{
		Name: "public.same_tenant", Parameters: "t uuid", Returns: "boolean",
		Language: "sql", Volatility: "STABLE",
		Leakproof: true, Parallel: "SAFE", Strict: true,
		Settings: []string{"app.tenant=x", "search_path=pg_catalog, pg_temp"},
		Body:     "SELECT t = current_setting('app.tenant')::uuid",
	}}}

	result, err := atlashclrender.RenderForDialect(database, "postgres")
	c.Assert(err, qt.IsNil)
	parsed, err := atlashcl.Parse(result.Data, "schema.hcl")

	c.Assert(err, qt.IsNil, qt.Commentf("the exported HCL does not parse:\n%s", result.Data))
	c.Assert(parsed.Functions, qt.HasLen, 1)
	function := parsed.Functions[0]
	c.Assert([]any{function.Leakproof, function.Parallel, function.Strict}, qt.DeepEquals, []any{true, "SAFE", true})
	c.Assert(function.Settings, qt.DeepEquals, []string{"app.tenant=x", "search_path=pg_catalog, pg_temp"})
}

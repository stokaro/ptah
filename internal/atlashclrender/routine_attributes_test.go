package atlashclrender_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/dialect/postgres/pgroutine"
	"ptah.run/internal/atlashcl"
	"ptah.run/internal/atlashclrender"
	"ptah.run/internal/builtintest"
)

// The planner attributes and the settings survive an export to HCL and back
// (stokaro/ptah#3630). The renderer wrote none of them, and the reader did not
// know strict, so a function read back from the export lost all four.
func TestRender_RoutineAttributesRoundTrip(t *testing.T) {
	c := qt.New(t)
	database := &schemamodel.Database{Functions: []schemamodel.Function{{
		Name: "public.same_tenant", Parameters: "t uuid", Returns: "boolean",
		Language: "sql", Volatility: "STABLE",
		Facets:   must.Must(pgroutine.WithProperties(schemaext.Facets{}, pgroutine.Properties{Leakproof: true, Parallel: "SAFE", Strict: true})),
		Settings: []string{"app.tenant=x", "search_path=pg_catalog, pg_temp"},
		Body:     "SELECT t = current_setting('app.tenant')::uuid",
	}}}

	result, err := atlashclrender.Render(database, atlashclrender.Options{HCL: builtintest.Runtime().HCL(), Dialect: "postgres"})
	c.Assert(err, qt.IsNil)
	parsed, err := atlashcl.ParseWithOptions(result.Data, "schema.hcl", atlashcl.Options{HCL: builtintest.Runtime().HCL()})

	c.Assert(err, qt.IsNil, qt.Commentf("the exported HCL does not parse:\n%s", result.Data))
	c.Assert(parsed.Functions, qt.HasLen, 1)
	function := parsed.Functions[0]
	properties, _, propertyErr := pgroutine.PropertiesOf(function.Facets)
	c.Assert(propertyErr, qt.IsNil)
	c.Assert([]any{properties.Leakproof, properties.Parallel, properties.Strict}, qt.DeepEquals, []any{true, "SAFE", true})
	c.Assert(function.Settings, qt.DeepEquals, []string{"app.tenant=x", "search_path=pg_catalog, pg_temp"})
}

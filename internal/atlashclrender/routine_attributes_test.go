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
	parsed, err := atlashcl.ParseWithOptions(t.Context(), result.Data, "schema.hcl", atlashcl.Options{HCL: builtintest.Runtime().HCL()})

	c.Assert(err, qt.IsNil, qt.Commentf("the exported HCL does not parse:\n%s", result.Data))
	c.Assert(parsed.Functions, qt.HasLen, 1)
	function := parsed.Functions[0]
	properties, _, propertyErr := pgroutine.PropertiesOf(function.Facets)
	c.Assert(propertyErr, qt.IsNil)
	c.Assert([]any{properties.Leakproof, properties.Parallel, properties.Strict}, qt.DeepEquals, []any{true, "SAFE", true})
	c.Assert(function.Settings, qt.DeepEquals, []string{"app.tenant='x'", "search_path='pg_catalog', 'pg_temp'"})
}

// TestRender_RoutineSettingsWriteTheCatalogSpelling pins the document a routine
// read from the server exports to, and that reading the document back holds the
// routine read from the server. Each setting is spelled as the PostgreSQL reader
// spells it; the values are what a `set` holds, as pg_proc.proconfig does.
func TestRender_RoutineSettingsWriteTheCatalogSpelling(t *testing.T) {
	rows := []struct {
		name    string
		setting string
		want    string
	}{
		{name: "a unit", setting: "work_mem='8MB'", want: `"work_mem" = "8MB"`},
		{name: "a name that needs a dot", setting: "app.tenant='x'", want: `"app.tenant" = "x"`},
		{name: "a name in its own case", setting: "TimeZone='Europe/Prague'", want: `"TimeZone" = "Europe/Prague"`},
		{name: "a list of two", setting: "search_path='pg_catalog', 'pg_temp'", want: `"search_path" = "pg_catalog, pg_temp"`},
		{name: "a name the server quotes", setting: "search_path='$user', 'public'", want: `"search_path" = "\"$user\", public"`},
		{name: "one name with a comma", setting: "search_path='a, b'", want: `"search_path" = "\"a, b\""`},
		{name: "a value with a quote", setting: "application_name='it''s'", want: `"application_name" = "it's"`},
		{name: "white space around a value", setting: "application_name=' a '", want: `"application_name" = " a "`},
		{name: "an empty name", setting: "search_path=''", want: `"search_path" = "\"\""`},
	}

	for _, row := range rows {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)
			database := &schemamodel.Database{Functions: []schemamodel.Function{{
				Name: "public.pinned", Returns: "integer", Language: "sql",
				Settings: []string{row.setting}, Body: "SELECT 1",
			}}}
			options := atlashclrender.Options{HCL: builtintest.Runtime().HCL(), Dialect: "postgres"}

			result, err := atlashclrender.Render(database, options)
			c.Assert(err, qt.IsNil)
			parsed, err := atlashcl.ParseWithOptions(t.Context(), result.Data, "schema.hcl", atlashcl.Options{HCL: options.HCL})

			c.Assert(string(result.Data), qt.Contains, row.want)
			c.Assert(err, qt.IsNil, qt.Commentf("the exported HCL does not parse:\n%s", result.Data))
			c.Assert(parsed.Functions, qt.HasLen, 1)
			c.Assert(parsed.Functions[0].Settings, qt.DeepEquals, []string{row.setting})
		})
	}
}

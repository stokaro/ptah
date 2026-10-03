package compare_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/config"
	"ptah.run/core/platform"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemamodel"
	"ptah.run/internal/exprkey"
	"ptah.run/migration/schemadiff/difftypes"
	"ptah.run/migration/schemadiff/internal/compare"
)

// routineWithResult compares one declared function that takes no arguments
// against a catalog function of the same name, with the spellings a server
// answered.
func routineWithResult(
	declared, observed string,
	spellings map[string]config.RoutineArguments,
) *difftypes.SchemaDiff {
	diff := &difftypes.SchemaDiff{}
	compare.FunctionsWithSemantics(
		&schemamodel.Database{Functions: []schemamodel.Function{{Name: "all_items", Returns: declared, Language: "sql"}}},
		&catalog.Database{Functions: []catalog.Function{{
			Schema: "public", Name: "all_items", Returns: observed,
			Language: "sql", Security: "INVOKER", Volatility: "VOLATILE",
		}}},
		diff, platform.Postgres, identifier.ForDialect(platform.Postgres), spellings,
	)
	return diff
}

// resultSpelling is the answer a server gives for a function that takes no
// arguments and declares the result, keyed by the declaration
// [routineWithResult] compares.
func resultSpelling(declared, spelled string) map[string]config.RoutineArguments {
	return map[string]config.RoutineArguments{
		exprkey.RoutineArguments(schemamodel.Function{Returns: declared, Language: "sql"}): {Result: spelled, Resolved: true},
	}
}

// Where a server spelled the declared result, the spelling is compared with
// the catalog's. pg_get_function_result leaves out a schema the search path
// reaches, so `SETOF public.items` reads back as `SETOF items`; compared as
// text, the function was dropped and created again on every plan
// (stokaro/ptah#4038). The spellings below are what PostgreSQL 18.6 printed
// with public on the search path.
func TestFunctionsWithSemantics_ComparesTheServersSpellingOfTheResult(t *testing.T) {
	tests := []struct {
		name     string
		declared string
		spelled  string
	}{
		{name: "a set of a qualified row type", declared: "SETOF public.items", spelled: "SETOF items"},
		{name: "a qualified row type", declared: "public.items", spelled: "items"},
		{name: "a table column of a qualified row type", declared: "TABLE (r public.items)", spelled: "TABLE(r items)"},
		{name: "a type outside the search path", declared: "SETOF other.t2", spelled: "SETOF other.t2"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			diff := routineWithResult(test.declared, test.spelled, resultSpelling(test.declared, test.spelled))

			c.Assert(diff.FunctionsModified, qt.HasLen, 0)
			c.Assert(diff.FunctionsAdded, qt.HasLen, 0)
			c.Assert(diff.FunctionsRemoved, qt.HasLen, 0)
		})
	}
}

// The controls. The server's spelling still differs from a catalog function
// returning another type, and the plan writes the declaration as written.
// Without an answer, or with one given for another declaration, the result is
// compared as text, which cannot drop a qualifier without the search path:
// `SETOF other.t2` and `SETOF t2` name two types when public is the search
// path and one when other is.
func TestFunctionsWithSemantics_TheServersSpellingOfTheResultStillSeesAChange(t *testing.T) {
	declared := "SETOF public.items"
	tests := []struct {
		name      string
		observed  string
		spellings map[string]config.RoutineArguments
	}{
		{
			name:      "a resolved spelling against another type",
			observed:  "SETOF other.items",
			spellings: resultSpelling(declared, "SETOF items"),
		},
		{
			name:      "an unresolved spelling",
			observed:  "SETOF items",
			spellings: map[string]config.RoutineArguments{exprkey.RoutineArguments(schemamodel.Function{Returns: declared, Language: "sql"}): {}},
		},
		{
			name:      "a spelling of another result",
			observed:  "SETOF items",
			spellings: resultSpelling("SETOF public.other", "SETOF items"),
		},
		{
			name:     "no server",
			observed: "SETOF items",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			diff := routineWithResult(declared, test.observed, test.spellings)

			c.Assert(diff.FunctionsModified, qt.HasLen, 1)
			c.Assert(diff.FunctionsModified[0].Changes["returns"], qt.Not(qt.Equals), "")
			c.Assert(diff.FunctionsModified[0].Desired.Returns, qt.Equals, declared)
		})
	}
}

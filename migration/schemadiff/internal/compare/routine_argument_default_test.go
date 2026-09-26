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

// Without a server's spelling the arguments are compared as text, and the
// default in each is kept as written apart from the keyword. Read as part of
// the type, `lower('A B')` would lose the spaces inside its literal, so two
// different defaults would compare equal, and the keyword's case differs
// between a declaration and the catalog, so an argument would compare unequal
// to itself (stokaro/ptah#3673).
func TestFunctionDefinitions_ADefaultIsComparedAsWritten(t *testing.T) {
	tests := []struct {
		name     string
		declared string
		observed string
	}{
		{name: "the keyword's case", declared: "b text default 'x'::text", observed: "b text DEFAULT 'x'::text"},
		{name: "the equals sign", declared: "b text = 'x'::text", observed: "b text DEFAULT 'x'::text"},
		{name: "a type alias before a default", declared: "c int4 DEFAULT 1", observed: "c integer DEFAULT 1"},
		{name: "a comma inside a default", declared: "d text DEFAULT 'a,b'::text, e integer", observed: "d text DEFAULT 'a,b'::text, e integer"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			diff := compare.FunctionDefinitionsWithDialect(
				schemamodel.Function{Name: "f", Parameters: test.declared},
				catalog.Function{Name: "f", Parameters: test.observed},
				platform.Postgres,
			)

			c.Assert(diff.Changes["parameters"], qt.Equals, "")
		})
	}
}

// The control: a default that differs is a change, including in the case of a
// literal and in the spaces inside one.
func TestFunctionDefinitions_ADifferentDefaultIsAChange(t *testing.T) {
	tests := []struct {
		name     string
		declared string
		observed string
	}{
		{name: "the case of a literal", declared: "b text DEFAULT 'X'::text", observed: "b text DEFAULT 'x'::text"},
		{name: "the spaces inside a literal", declared: "b text DEFAULT lower('A B'::text)", observed: "b text DEFAULT lower('AB'::text)"},
		{name: "a default added", declared: "b text DEFAULT 'x'::text", observed: "b text"},
		{name: "a space after a comma inside a literal", declared: "d text DEFAULT 'a,b'::text", observed: "d text DEFAULT 'a, b'::text"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			diff := compare.FunctionDefinitionsWithDialect(
				schemamodel.Function{Name: "f", Parameters: test.declared},
				catalog.Function{Name: "f", Parameters: test.observed},
				platform.Postgres,
			)

			c.Assert(diff.Changes["parameters"], qt.Not(qt.Equals), "")
		})
	}
}

// routineWithArguments compares one declared routine against a catalog
// routine of the same name, with the argument spellings a server answered.
func routineWithArguments(
	declared, observed string,
	spellings map[string]config.RoutineArguments,
) *difftypes.SchemaDiff {
	diff := &difftypes.SchemaDiff{}
	compare.FunctionsWithSemantics(
		&schemamodel.Database{Functions: []schemamodel.Function{{Name: "score", Parameters: declared, Returns: "integer"}}},
		&catalog.Database{Functions: []catalog.Function{{
			Schema: "public", Name: "score", Parameters: observed, Returns: "integer",
			Language: "plpgsql", Security: "INVOKER", Volatility: "VOLATILE",
		}}},
		diff, platform.Postgres, identifier.ForDialect(platform.Postgres), spellings,
	)
	return diff
}

// Where a server spelled the declared arguments, the spelling is compared with
// the catalog's rather than the declaration's text: `'X'` is stored as
// `'X'::text`, `numeric(10,2)` as `numeric`, and no fold over the text can say
// so. The planner still writes the declaration. The spelling is found by the
// declaration as the resolver was given it, before the comparison folds its
// case.
func TestFunctionsWithSemantics_ComparesTheServersSpellingOfTheArguments(t *testing.T) {
	c := qt.New(t)
	declared := "A VARCHAR(50), B TEXT DEFAULT 'X'"
	spellings := map[string]config.RoutineArguments{
		exprkey.RoutineArguments(schemamodel.Function{Parameters: declared}): {Arguments: "a character varying, b text DEFAULT 'X'::text", Resolved: true},
	}

	diff := routineWithArguments(declared, "a character varying, b text DEFAULT 'X'::text", spellings)

	c.Assert(diff.FunctionsModified, qt.HasLen, 0)
	c.Assert(diff.FunctionsAdded, qt.HasLen, 0)
	c.Assert(diff.FunctionsRemoved, qt.HasLen, 0)
}

// An answer the server could not give leaves the text comparison in place,
// which finds a declaration spelled the way the catalog prints it unchanged.
func TestFunctionsWithSemantics_AnUnresolvedSpellingFallsBackToTheText(t *testing.T) {
	c := qt.New(t)
	declared := "b text DEFAULT 'x'::text"
	spellings := map[string]config.RoutineArguments{exprkey.RoutineArguments(schemamodel.Function{Parameters: declared}): {}}

	diff := routineWithArguments(declared, "b text DEFAULT 'x'::text", spellings)

	c.Assert(diff.FunctionsModified, qt.HasLen, 0)
}

// The controls. The server's spelling of a different default is a change, and
// the planned declaration is the one written, with its literal's case. An
// answer the server could not give, and one for another argument list, leave
// the text comparison in place.
func TestFunctionsWithSemantics_TheServersSpellingStillSeesAChange(t *testing.T) {
	declared := "b text DEFAULT 'X'"
	tests := []struct {
		name      string
		spellings map[string]config.RoutineArguments
	}{
		{
			name:      "a resolved spelling",
			spellings: map[string]config.RoutineArguments{exprkey.RoutineArguments(schemamodel.Function{Parameters: declared}): {Arguments: "b text DEFAULT 'X'::text", Resolved: true}},
		},
		{
			name:      "an unresolved spelling",
			spellings: map[string]config.RoutineArguments{exprkey.RoutineArguments(schemamodel.Function{Parameters: declared}): {}},
		},
		{
			name:      "a spelling of a procedure's arguments",
			spellings: map[string]config.RoutineArguments{exprkey.RoutineArguments(schemamodel.Function{Kind: schemamodel.FunctionKindProcedure, Parameters: declared}): {Arguments: "b text DEFAULT 'x'::text", Resolved: true}},
		},
		{
			name: "no server",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			diff := routineWithArguments(declared, "b text DEFAULT 'x'::text", test.spellings)

			c.Assert(diff.FunctionsModified, qt.HasLen, 1)
			c.Assert(diff.FunctionsModified[0].Changes["parameters"], qt.Not(qt.Equals), "")
			c.Assert(diff.FunctionsModified[0].Desired.Parameters, qt.Equals, "b text DEFAULT 'X'")
		})
	}
}

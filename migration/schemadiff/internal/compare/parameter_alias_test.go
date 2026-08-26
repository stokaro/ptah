package compare_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/catalog"
	"go.5x5.cz/ptah/core/goschema"
	"go.5x5.cz/ptah/core/platform"
	"go.5x5.cz/ptah/migration/schemadiff/internal/compare"
)

// TestFunctionDefinitions_AParameterTypeIsComparedByWhatItMeans pins that the
// two sides may spell a type differently and still describe one function.
//
// PostgreSQL accepts `float8` and reports `double precision`. Compared as text,
// such a declaration plans CREATE OR REPLACE on every run, applies it, changes
// nothing -- the function is already what the statement says -- and plans it
// again. Measured on PostgreSQL 18.6 (stokaro/ptah#2273).
func TestFunctionDefinitions_AParameterTypeIsComparedByWhatItMeans(t *testing.T) {
	tests := []struct {
		name     string
		declared string
		reported string
	}{
		{
			name:     "the alias a person writes against what the catalog prints",
			declared: "float8, float8",
			reported: "double precision, double precision",
		},
		{
			name:     "a named argument keeps its name and canonicalizes its type",
			declared: "a float8",
			reported: "a double precision",
		},
		{
			name:     "an alias with a modifier",
			declared: "a decimal(10, 2)",
			reported: "a numeric(10,2)",
		},
		{
			name:     "the integer aliases",
			declared: "a int4, b int8, c int2",
			reported: "a integer, b bigint, c smallint",
		},
		{
			name:     "a type that is already what the catalog prints",
			declared: "a double precision",
			reported: "a double precision",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			diff := compare.FunctionDefinitionsWithDialect(
				goschema.Function{Name: "f", Parameters: test.declared, Body: "SELECT 1"},
				catalog.Function{Name: "f", Parameters: test.reported, Body: "SELECT 1"},
				platform.Postgres,
			)

			c.Assert(diff.Changes["parameters"], qt.Equals, "")
		})
	}
}

// TestFunctionDefinitions_AParameterNameIsNotATypeAlias is the control that
// keeps the rule from renaming an argument.
//
// The canonicalization rewrites the last word before any modifier, because in
// PostgreSQL's parameter syntax that word is always part of the type. A rule
// that mapped any alias-looking token would rewrite `float8 integer` -- an
// argument NAMED float8 -- into a different argument.
func TestFunctionDefinitions_AParameterNameIsNotATypeAlias(t *testing.T) {
	c := qt.New(t)

	diff := compare.FunctionDefinitionsWithDialect(
		goschema.Function{Name: "f", Parameters: "float8 integer", Body: "SELECT 1"},
		catalog.Function{Name: "f", Parameters: "float8 integer", Body: "SELECT 1"},
		platform.Postgres,
	)

	c.Assert(diff.Changes["parameters"], qt.Equals, "")
}

// TestFunctionDefinitions_AGenuinelyDifferentParameterTypeIsStillAChange is the
// other control: folding aliases must not fold types together.
func TestFunctionDefinitions_AGenuinelyDifferentParameterTypeIsStillAChange(t *testing.T) {
	tests := []struct {
		name     string
		declared string
		reported string
	}{
		{name: "a different width", declared: "a int4", reported: "a bigint"},
		{name: "a different type entirely", declared: "a float8", reported: "a text"},
		{name: "a different modifier", declared: "a numeric(10,2)", reported: "a numeric(12,2)"},
		{name: "an argument added", declared: "a float8, b text", reported: "a double precision"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			diff := compare.FunctionDefinitionsWithDialect(
				goschema.Function{Name: "f", Parameters: test.declared, Body: "SELECT 1"},
				catalog.Function{Name: "f", Parameters: test.reported, Body: "SELECT 1"},
				platform.Postgres,
			)

			c.Assert(diff.Changes["parameters"], qt.Not(qt.Equals), "")
		})
	}
}

// TestFunctionDefinitions_TheAliasFoldIsPostgresOnly keeps the rule where it
// was measured. `float8` is not a MySQL type, and mapping it there would answer
// a different question than the caller asked.
func TestFunctionDefinitions_TheAliasFoldIsPostgresOnly(t *testing.T) {
	c := qt.New(t)

	diff := compare.FunctionDefinitionsWithDialect(
		goschema.Function{Name: "f", Parameters: "a float8", Body: "SELECT 1"},
		catalog.Function{Name: "f", Parameters: "a double precision", Body: "SELECT 1"},
		platform.MySQL,
	)

	c.Assert(diff.Changes["parameters"], qt.Not(qt.Equals), "")
}

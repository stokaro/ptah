package compare_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/core/platform"
	"ptah.run/core/schemamodel"
	"ptah.run/migration/schemadiff/internal/compare"
)

// A PostgreSQL type has aliases and the two sides do not use the same ones, so
// a return clause is folded before it is matched (stokaro/ptah#3155).
//
// The arguments beside it already went through this canonicalization. The
// return type did not, so `RETURNS int` against a catalog reporting `integer`
// planned a CREATE OR REPLACE on every run, applied it, changed nothing, and
// planned it again.
func TestFunctionDefinitions_AReturnAliasIsTheSameType(t *testing.T) {
	tests := []struct {
		name     string
		declared string
		observed string
	}{
		{name: "int against integer", declared: "int", observed: "integer"},
		{name: "int4 against integer", declared: "int4", observed: "integer"},
		{name: "int8 against bigint", declared: "int8", observed: "bigint"},
		{name: "bool against boolean", declared: "bool", observed: "boolean"},
		{name: "float8 against double precision", declared: "float8", observed: "double precision"},
		{name: "varchar against character varying", declared: "varchar", observed: "character varying"},
		{name: "decimal against numeric", declared: "decimal", observed: "numeric"},
		{name: "timestamptz against its long form", declared: "timestamptz", observed: "timestamp with time zone"},
		{name: "a set of an alias", declared: "SETOF int", observed: "SETOF integer"},
		{name: "a table of aliases", declared: "TABLE(a int, b varchar)", observed: "TABLE(a integer, b character varying)"},
		{name: "a type with no alias", declared: "text", observed: "text"},
		{name: "the same spelling on both sides", declared: "integer", observed: "integer"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			diff := compare.FunctionDefinitionsWithDialect(
				schemamodel.Function{Name: "f", Returns: test.declared},
				catalog.Function{Name: "f", Returns: test.observed},
				platform.Postgres,
			)

			c.Assert(diff.Changes["returns"], qt.Equals, "")
		})
	}
}

// TestFunctionDefinitions_ARealReturnDifferenceSurvivesTheFold is the control.
//
// The fold maps spellings of one type onto each other, and must not reach a
// second type. An over-wide fold would report a routine as unchanged while the
// server returns something else, which is worse than the churn it removes.
func TestFunctionDefinitions_ARealReturnDifferenceSurvivesTheFold(t *testing.T) {
	tests := []struct {
		name     string
		declared string
		observed string
	}{
		{name: "a wider integer", declared: "bigint", observed: "integer"},
		{name: "a narrower integer", declared: "smallint", observed: "integer"},
		{name: "a different type entirely", declared: "text", observed: "integer"},
		{name: "a set against a scalar", declared: "SETOF integer", observed: "integer"},
		{name: "a table of a different shape", declared: "TABLE(a integer)", observed: "TABLE(a text)"},
		{name: "a table against a set", declared: "TABLE(a integer)", observed: "SETOF integer"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			diff := compare.FunctionDefinitionsWithDialect(
				schemamodel.Function{Name: "f", Returns: test.declared},
				catalog.Function{Name: "f", Returns: test.observed},
				platform.Postgres,
			)

			c.Assert(diff.Changes["returns"], qt.Not(qt.Equals), "")
		})
	}
}

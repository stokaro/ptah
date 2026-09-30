package compare_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/core/platform"
	"ptah.run/core/schemamodel"
	"ptah.run/migration/schemadiff/internal/compare"
)

// CREATE FUNCTION discards type modifiers in both arguments and results.
// A comparison must retain the authored declaration for a real change while
// treating the catalog's unmodified type as the same function.
func TestFunctionDefinitions_PostgresDiscardsRoutineTypeModifiers(t *testing.T) {
	tests := []struct {
		name               string
		declaredParameters string
		recordedParameters string
		declaredReturns    string
		recordedReturns    string
	}{
		{name: "HCI case ID", declaredReturns: "varchar(20)", recordedReturns: "character varying"},
		{name: "numeric result", declaredReturns: "numeric(10, 2)", recordedReturns: "numeric"},
		{name: "set result", declaredReturns: "SETOF varchar(20)", recordedReturns: "SETOF character varying"},
		{name: "array result", declaredReturns: "varchar(20)[]", recordedReturns: "character varying[]"},
		{name: "time zone result", declaredReturns: "timestamp(3) with time zone", recordedReturns: "timestamp with time zone"},
		{name: "table result", declaredReturns: "TABLE(code varchar(20), amount numeric(10, 2))", recordedReturns: "TABLE(code character varying, amount numeric)"},
		{name: "table result with whitespace", declaredReturns: "TABLE (code varchar(20), amount numeric(10, 2))", recordedReturns: "TABLE(code character varying, amount numeric)"},
		{name: "quoted result", declaredReturns: `"Type(20)"`, recordedReturns: `"Type(20)"`},
		{name: "quoted argument", declaredParameters: `code "Type(20)"`, recordedParameters: `code "Type(20)"`, declaredReturns: "text", recordedReturns: "text"},
		{name: "argument modifier", declaredParameters: "code varchar(20)", recordedParameters: "code character varying", declaredReturns: "text", recordedReturns: "text"},
		{name: "argument default", declaredParameters: "code varchar(20) DEFAULT lower('X')", recordedParameters: "code character varying DEFAULT lower('X')", declaredReturns: "text", recordedReturns: "text"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			declared := schemamodel.Function{
				Name: "f", Parameters: test.declaredParameters, Returns: test.declaredReturns,
			}
			diff := compare.FunctionDefinitionsWithDialect(declared, catalog.Function{
				Name: "f", Parameters: test.recordedParameters, Returns: test.recordedReturns,
			}, platform.Postgres)
			c.Assert(diff.Changes["returns"], qt.Equals, "")
			c.Assert(diff.Changes["parameters"], qt.Equals, "")
			c.Assert(diff.Desired, qt.DeepEquals, declared)
		})
	}
}

func TestFunctionDefinitions_RoutineModifierNormalizationKeepsRealChanges(t *testing.T) {
	tests := []struct {
		name     string
		dialect  string
		declared string
		recorded string
	}{
		{name: "another base type", dialect: platform.Postgres, declared: "varchar(20)", recorded: "text"},
		{name: "an array against a scalar", dialect: platform.Postgres, declared: "varchar(20)[]", recorded: "character varying"},
		{name: "quoted type name", dialect: platform.Postgres, declared: `"type(20)"`, recorded: `"type"`},
		{name: "quoted type case", dialect: platform.Postgres, declared: `"Type(20)"`, recorded: `"type(20)"`},
		{name: "another table column", dialect: platform.Postgres, declared: "TABLE(a varchar(20))", recorded: "TABLE(b character varying)"},
		{name: "MySQL retains return length", dialect: platform.MySQL, declared: "varchar(20)", recorded: "varchar(64)"},
		{name: "Oracle retains return length", dialect: platform.Oracle, declared: "varchar(20)", recorded: "varchar(64)"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			diff := compare.FunctionDefinitionsWithDialect(
				schemamodel.Function{Name: "f", Returns: test.declared},
				catalog.Function{Name: "f", Returns: test.recorded}, test.dialect,
			)
			c.Assert(diff.Changes["returns"], qt.Not(qt.Equals), "")
		})
	}
}

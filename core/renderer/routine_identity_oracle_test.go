package renderer_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform"
	"ptah.run/core/platform/capability"
	"ptah.run/core/renderer"
	"ptah.run/core/schemamodel"
)

// TestValidateSchema_OracleRefusesTwoRoutinesItCannotTellApart is the check
// that stands between two declarations and one silently discarded body.
//
// Ptah writes Oracle names without quotes, so the server folds them: measured
// on 23.26.2.0.0, `CREATE OR REPLACE FUNCTION zz_case` followed by
// `CREATE OR REPLACE FUNCTION ZZ_CASE` reported "Function created" TWICE and
// left ONE routine in USER_OBJECTS, carrying the SECOND body. Unlike the MySQL
// family, which answers Error 1304 to the second create, Oracle raises nothing
// at all -- so without this check an apply of two declarations succeeds, one of
// them is gone, and nothing names it.
func TestValidateSchema_OracleRefusesTwoRoutinesItCannotTellApart(t *testing.T) {
	c := qt.New(t)
	database := &schemamodel.Database{Functions: []schemamodel.Function{
		oracleCaseFunction("zz_case", "BEGIN\n  RETURN 1;\nEND;"),
		oracleCaseFunction("ZZ_CASE", "BEGIN\n  RETURN 2;\nEND;"),
	}}

	err := renderer.ValidateSchemaWithCapabilities(database, platform.Oracle, capability.Oracle23())

	c.Assert(err, qt.IsNotNil)
	c.Assert(err.Error(), qt.Contains, "differ only by case")
	c.Assert(err.Error(), qt.Contains, "the second silently replaces the first")
}

// TestValidateSchema_OracleAcceptsTwoRoutinesItCanTellApart is the control.
//
// A validator that refused any two routines would pass the test above while
// making every ordinary schema unrenderable.
func TestValidateSchema_OracleAcceptsTwoRoutinesItCanTellApart(t *testing.T) {
	c := qt.New(t)
	database := &schemamodel.Database{Functions: []schemamodel.Function{
		oracleCaseFunction("zz_case", "BEGIN\n  RETURN 1;\nEND;"),
		oracleCaseFunction("zz_other", "BEGIN\n  RETURN 2;\nEND;"),
	}}

	c.Assert(renderer.ValidateSchemaWithCapabilities(database, platform.Oracle, capability.Oracle23()), qt.IsNil)
}

func oracleCaseFunction(name, body string) schemamodel.Function {
	return schemamodel.Function{
		Name:       name,
		Returns:    "NUMBER",
		Language:   "plsql",
		Security:   "DEFINER",
		Volatility: "VOLATILE",
		Body:       body,
	}
}

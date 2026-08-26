package compare_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/goschema"
	"go.5x5.cz/ptah/core/platform"
	dbtypes "go.5x5.cz/ptah/dbschema/types"
	"go.5x5.cz/ptah/migration/schemadiff/difftypes"
	"go.5x5.cz/ptah/migration/schemadiff/internal/compare"
)

// oracleDeclaredFunction is the declaration the cases below compare against
// what the Oracle catalog reports for the routine it created.
func oracleDeclaredFunction(parameters string) goschema.Function {
	return goschema.Function{
		Name:       "fn_double",
		Parameters: parameters,
		Returns:    "NUMBER",
		Language:   "plsql",
		Security:   "DEFINER",
		Volatility: "VOLATILE",
		Body:       "BEGIN\n  RETURN p * 2;\nEND;",
	}
}

// oracleLiveFunction is what the reader builds from ALL_PROCEDURES,
// ALL_ARGUMENTS and ALL_SOURCE for that routine: names and types folded to
// lower case, and the mode spelled as the catalog reports it.
func oracleLiveFunction() dbtypes.DBFunction {
	return dbtypes.DBFunction{
		Name:       "FN_DOUBLE",
		Parameters: "p in number",
		Returns:    "number",
		Language:   "plsql",
		Security:   "DEFINER",
		Volatility: "VOLATILE",
		Body:       "BEGIN\n  RETURN p * 2;\nEND;",
	}
}

// TestFunctionDefinitions_OracleAcceptsEitherSpellingOfTheDefaultMode is the
// property that decides whether an unchanged Oracle routine plans anything.
//
// PL/SQL writes the mode AFTER the name, so the fold that cuts a leading `in `
// never reaches it, and `p NUMBER` and `p IN NUMBER` are the same parameter.
// Without the Oracle fold, a schema declaring one of the two spellings compared
// unequal to the routine it had just created and planned a replacement on every
// run.
func TestFunctionDefinitions_OracleAcceptsEitherSpellingOfTheDefaultMode(t *testing.T) {
	tests := []struct {
		name       string
		parameters string
	}{
		{name: "the mode written out", parameters: "p IN NUMBER"},
		{name: "the mode left implicit", parameters: "p NUMBER"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			diff := compare.FunctionDefinitionsWithDialect(
				oracleDeclaredFunction(test.parameters), oracleLiveFunction(), platform.Oracle)

			c.Assert(diff.Changes, qt.HasLen, 0)
		})
	}
}

// TestFunctionDefinitions_OracleStillReportsARealParameterChange is the control
// the fold needs.
//
// A fold that swallowed the whole parameter list would pass the test above and
// report nothing here either, which is the failure that leaves a changed
// signature unplanned.
func TestFunctionDefinitions_OracleStillReportsARealParameterChange(t *testing.T) {
	c := qt.New(t)
	diff := compare.FunctionDefinitionsWithDialect(
		oracleDeclaredFunction("p IN VARCHAR2"), oracleLiveFunction(), platform.Oracle)

	c.Assert(diff.Changes["parameters"], qt.Equals, "p number -> p varchar2")
}

// TestFunctionDefinitions_OracleKeepsOUTAndINOUT pins the modes that are NOT
// defaults, because folding them would silently change what the routine does
// with its arguments.
func TestFunctionDefinitions_OracleKeepsOUTAndINOUT(t *testing.T) {
	tests := []struct {
		name      string
		declared  string
		live      string
		wantEqual bool
	}{
		{name: "out matches out", declared: "b OUT NUMBER", live: "b out number", wantEqual: true},
		{name: "in out matches in out", declared: "c IN OUT VARCHAR2", live: "c in out varchar2", wantEqual: true},
		{name: "in does not match out", declared: "b IN NUMBER", live: "b out number", wantEqual: false},
		{name: "in out does not match in", declared: "c IN OUT NUMBER", live: "c in number", wantEqual: false},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			live := oracleLiveFunction()
			live.Parameters = test.live
			diff := compare.FunctionDefinitionsWithDialect(
				oracleDeclaredFunction(test.declared), live, platform.Oracle)

			c.Assert(len(diff.Changes) == 0, qt.Equals, test.wantEqual)
		})
	}
}

// TestFunctions_OracleMatchesADeclarationToTheUpperCaseNameItCreated is the
// identity half.
//
// Ptah writes Oracle names without quotes, so the server folds every one of
// them to upper case: `fn_double` in a declaration is FN_DOUBLE in the catalog.
// Keying on the exact spelling made the live routine BOTH added and removed, on
// every run of an unchanged schema.
func TestFunctions_OracleMatchesADeclarationToTheUpperCaseNameItCreated(t *testing.T) {
	c := qt.New(t)
	generated := &goschema.Database{Functions: []goschema.Function{oracleDeclaredFunction("p IN NUMBER")}}
	database := &dbtypes.DBSchema{Functions: []dbtypes.DBFunction{oracleLiveFunction()}}

	diff := &difftypes.SchemaDiff{}
	compare.FunctionsWithDialect(generated, database, diff, platform.Oracle)

	c.Assert(diff.FunctionsAdded, qt.HasLen, 0)
	c.Assert(diff.FunctionsRemoved, qt.HasLen, 0)
	c.Assert(diff.FunctionsModified, qt.HasLen, 0)
}

// TestFunctions_OracleStillReportsAFunctionThatIsNotThere is the control for
// the fold above: a name that differs by more than case is a different routine.
func TestFunctions_OracleStillReportsAFunctionThatIsNotThere(t *testing.T) {
	c := qt.New(t)
	declared := oracleDeclaredFunction("p IN NUMBER")
	declared.Name = "fn_triple"
	generated := &goschema.Database{Functions: []goschema.Function{declared}}
	database := &dbtypes.DBSchema{Functions: []dbtypes.DBFunction{oracleLiveFunction()}}

	diff := &difftypes.SchemaDiff{}
	compare.FunctionsWithDialect(generated, database, diff, platform.Oracle)

	c.Assert(diff.FunctionsAdded, qt.DeepEquals, []string{"fn_triple"})
	c.Assert(diff.FunctionsRemoved, qt.DeepEquals, []string{"FN_DOUBLE"})
}

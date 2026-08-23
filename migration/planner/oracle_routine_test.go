package planner_test

import (
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/goschema"
	"go.5x5.cz/ptah/core/platform"
	"go.5x5.cz/ptah/core/platform/capability"
	"go.5x5.cz/ptah/migration/planner"
	"go.5x5.cz/ptah/migration/schemadiff/types"
)

// TestGenerateSchemaDiffSQLStatements_KeepsAnOracleRoutineInOneStatement is the
// property that made the plan splitter dialect-aware.
//
// The planner renders the whole plan and then cuts it into statements. The
// blind cut treats every semicolon outside a BEGIN block as a boundary, which
// is right for most targets and wrong for the one whose routine body is opened
// by IS: a PL/SQL function with a declaration section came out as four
// fragments -- the header, the BEGIN, the RETURN and the END -- and the server
// refuses each of them on its own.
//
// The function WITHOUT a declaration section is here as the control. It was
// already whole, because BEGIN opens a body in every dialect this splitter
// serves, so a test using only that shape would pass with the fix reverted.
func TestGenerateSchemaDiffSQLStatements_KeepsAnOracleRoutineInOneStatement(t *testing.T) {
	// wantStatement is the WHOLE statement rather than a substring, so a split
	// that produced the header alone cannot satisfy it. The semicolon after END
	// is KEPT, because in PL/SQL it is part of the block: measured on
	// 23.26.2.0.0, a CREATE handed over without it returns no driver error and
	// leaves the routine INVALID.
	tests := []struct {
		name          string
		body          string
		wantStatement string
	}{
		{
			name: "a body opened by BEGIN",
			body: "BEGIN\n  RETURN p * 2;\nEND;",
			wantStatement: "CREATE OR REPLACE FUNCTION fn_double(p IN NUMBER) RETURN NUMBER AUTHID DEFINER IS\n" +
				"BEGIN\n  RETURN p * 2;\nEND;",
		},
		{
			name: "a body with a declaration section",
			body: "  x NUMBER := 0;\nBEGIN\n  x := p;\n  RETURN x;\nEND;",
			wantStatement: "CREATE OR REPLACE FUNCTION fn_double(p IN NUMBER) RETURN NUMBER AUTHID DEFINER IS\n" +
				"x NUMBER := 0;\nBEGIN\n  x := p;\n  RETURN x;\nEND;",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			generated := &goschema.Database{Functions: []goschema.Function{{
				StructName: "F",
				Name:       "fn_double",
				Parameters: "p IN NUMBER",
				Returns:    "NUMBER",
				Language:   "plsql",
				Security:   "DEFINER",
				Volatility: "VOLATILE",
				Body:       test.body,
			}}}
			diff := &types.SchemaDiff{FunctionsAdded: []string{"fn_double"}}

			statements, err := planner.GenerateSchemaDiffSQLStatementsWithCapabilities(
				diff, generated, platform.Oracle, capability.Oracle23())

			c.Assert(err, qt.IsNil)
			c.Assert(statements, qt.HasLen, 1)
			c.Assert(statements[0], qt.Equals, test.wantStatement)
		})
	}
}

// TestGenerateSchemaDiffSQLStatements_OracleReplacesARoutineWithBothHalves pins
// the pair, because half a replacement is a deletion.
//
// The planner emits a guarded DROP and then the CREATE. Oracle's own
// CREATE OR REPLACE would make the drop unnecessary, and it is still emitted
// because a routine whose KIND changed cannot be replaced in place -- and the
// guard is what keeps the drop harmless when there is nothing to drop.
func TestGenerateSchemaDiffSQLStatements_OracleReplacesARoutineWithBothHalves(t *testing.T) {
	c := qt.New(t)
	generated := &goschema.Database{Functions: []goschema.Function{{
		StructName: "F",
		Name:       "fn_double",
		Parameters: "p IN NUMBER",
		Returns:    "NUMBER",
		Language:   "plsql",
		Security:   "DEFINER",
		Volatility: "VOLATILE",
		Body:       "BEGIN\n  RETURN p * 3;\nEND;",
	}}}
	diff := &types.SchemaDiff{FunctionsModified: []types.FunctionDiff{{
		FunctionName: "fn_double",
		Changes:      map[string]string{"body": "old -> new"},
	}}}

	statements, err := planner.GenerateSchemaDiffSQLStatementsWithCapabilities(
		diff, generated, platform.Oracle, capability.Oracle23())

	c.Assert(err, qt.IsNil)
	c.Assert(statements, qt.HasLen, 2)
	c.Assert(statements[0], qt.Contains, "DROP FUNCTION IF EXISTS fn_double")
	c.Assert(statements[1], qt.Contains, "CREATE OR REPLACE FUNCTION fn_double")
}

// TestGenerateSchemaDiffSQLStatements_OracleDropsNothingItCannotRecreate is the
// coupling the MySQL family paid for once already.
//
// The renderer answers a routine whose language this target does not run with a
// named skip and no DDL. If the planner emitted the DROP anyway, an apply would
// execute it, create nothing, and report success -- the operator asked for a
// change and got a deletion. plpgsql is the value that actually arrives, because
// an annotation without `language=` is defaulted to it.
func TestGenerateSchemaDiffSQLStatements_OracleDropsNothingItCannotRecreate(t *testing.T) {
	c := qt.New(t)
	generated := &goschema.Database{Functions: []goschema.Function{{
		StructName: "F",
		Name:       "fn_double",
		Parameters: "p integer",
		Returns:    "integer",
		Language:   "plpgsql",
		Security:   "DEFINER",
		Volatility: "VOLATILE",
		Body:       "BEGIN RETURN p * 3; END;",
	}}}
	diff := &types.SchemaDiff{FunctionsModified: []types.FunctionDiff{{
		FunctionName: "fn_double",
		Changes:      map[string]string{"body": "old -> new"},
	}}}

	statements, err := planner.GenerateSchemaDiffSQLStatementsWithCapabilities(
		diff, generated, platform.Oracle, capability.Oracle23())

	c.Assert(err, qt.IsNil)
	c.Assert(strings.Join(statements, "\n"), qt.Not(qt.Contains), "DROP FUNCTION")
}

//go:build integration

package dbschema_test

import (
	"fmt"
	"strings"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/goschema"
	"go.5x5.cz/ptah/core/platform"
	"go.5x5.cz/ptah/core/renderer"
	"go.5x5.cz/ptah/dbschema"
	"go.5x5.cz/ptah/internal/dbtarget"
	"go.5x5.cz/ptah/migration/schemadiff"
)

// TestSQLServerLiveProcedureRoundTrip is the test capability.Procedures could
// not have been turned on for this target without.
//
// The key was false for one reason -- nothing read a procedure back -- and the
// failure that reason describes is an apply loop planning the same CREATE
// forever, which no offline test can see: the fixture on both sides is written
// by the same hand (stokaro/ptah#1784).
func TestSQLServerLiveProcedureRoundTrip(t *testing.T) {
	dbURL := dbtarget.URL(t, dbtarget.SQLServer)
	c := qt.New(t)
	ctx := t.Context()

	conn, err := dbschema.ConnectToDatabase(ctx, dbURL)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)

	schemaName := fmt.Sprintf("ptah_proc_%d", time.Now().UnixNano())
	quoted := quoteSQLServerIdentifier(schemaName)
	_, err = conn.ExecContext(ctx, "EXEC('CREATE SCHEMA "+quoted+"')")
	c.Assert(err, qt.IsNil)
	defer func() {
		for _, name := range []string{"p_report", "p_owner"} {
			_, _ = conn.ExecContext(ctx, "DROP PROCEDURE IF EXISTS "+quoted+"."+quoteSQLServerIdentifier(name))
		}
		_, _ = conn.ExecContext(ctx, "DROP SCHEMA IF EXISTS "+quoted)
	}()

	description := sqlServerProcedureSchema(schemaName)

	// 1. The renderer's statements are what the server is given. A procedure
	// takes its parameters without parentheses and has no RETURNS, so borrowing
	// the function spelling fails here rather than being corrected by hand.
	statements, err := renderer.GetOrderedCreateStatements(description, platform.SQLServer)
	c.Assert(err, qt.IsNil)
	joined := strings.Join(statements, "\n")
	c.Assert(joined, qt.Contains, "CREATE OR ALTER PROCEDURE")
	c.Assert(joined, qt.Not(qt.Contains), "PROCEDURE ["+schemaName+"].[p_report](")
	for _, statement := range statements {
		_, execErr := conn.ExecContext(ctx, statement)
		c.Assert(execErr, qt.IsNil, qt.Commentf("statement:\n%s", statement))
	}

	// 2. The signature comes back from sys.parameters exactly as a function's
	// does, which is the finding that settled whether a T-SQL header parser was
	// needed: it is not. The body is the half that comes out of the statement
	// text, and p_owner is the one that pins where it starts.
	live, err := dbschema.ReadSchemaWithSchemas(conn, []string{schemaName})
	c.Assert(err, qt.IsNil)
	c.Assert(live.Functions, qt.HasLen, 2)
	report := sqlServerFunctionNamed(live.Functions, "p_report")
	c.Assert(report.Kind, qt.Equals, goschema.FunctionKindProcedure)
	c.Assert(report.Parameters, qt.Equals, "@id int, @label varchar(50)")
	c.Assert(report.Returns, qt.Equals, "")
	owner := sqlServerFunctionNamed(live.Functions, "p_owner")
	c.Assert(owner.Body, qt.Equals, "BEGIN SELECT @n AS n; END")
	c.Assert(owner.Body, qt.Not(qt.Contains), "OWNER")

	// 3. The convergence assertion. Comparing the same description against what
	// the server now holds must produce nothing to do.
	settled := schemadiff.CompareWithDialect(description, live, platform.SQLServer)
	c.Assert(settled.FunctionsAdded, qt.HasLen, 0)
	c.Assert(settled.FunctionsRemoved, qt.HasLen, 0)
	c.Assert(settled.FunctionsModified, qt.HasLen, 0)
	c.Assert(settled.ProceduresRemoved, qt.HasLen, 0)
}

// TestSQLServerLiveProcedureReplacementUsesTheMatchingVerb is the half a live
// run found and no offline test would have.
//
// A modified procedure is planned as a drop and a create. The drop half was
// built without the kind, so it rendered DROP FUNCTION for a procedure, and the
// server answers that with "does not exist" about an object that is right
// there -- the apply fails and the old body stays.
func TestSQLServerLiveProcedureReplacementUsesTheMatchingVerb(t *testing.T) {
	dbURL := dbtarget.URL(t, dbtarget.SQLServer)
	c := qt.New(t)
	ctx := t.Context()

	conn, err := dbschema.ConnectToDatabase(ctx, dbURL)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)

	schemaName := fmt.Sprintf("ptah_procrep_%d", time.Now().UnixNano())
	quoted := quoteSQLServerIdentifier(schemaName)
	_, err = conn.ExecContext(ctx, "EXEC('CREATE SCHEMA "+quoted+"')")
	c.Assert(err, qt.IsNil)
	defer func() {
		_, _ = conn.ExecContext(ctx, "DROP PROCEDURE IF EXISTS "+quoted+".[p_report]")
		_, _ = conn.ExecContext(ctx, "DROP SCHEMA IF EXISTS "+quoted)
	}()

	before := sqlServerProcedureSchema(schemaName)
	statements, err := renderer.GetOrderedCreateStatements(before, platform.SQLServer)
	c.Assert(err, qt.IsNil)
	for _, statement := range statements {
		_, execErr := conn.ExecContext(ctx, statement)
		c.Assert(execErr, qt.IsNil, qt.Commentf("statement:\n%s", statement))
	}

	after := sqlServerProcedureSchema(schemaName)
	after.Functions[0].Body = "BEGIN SET NOCOUNT ON; SELECT @id AS id, 1 AS extra; END"
	afterStatements, err := renderer.GetOrderedCreateStatements(after, platform.SQLServer)
	c.Assert(err, qt.IsNil)
	for _, statement := range afterStatements {
		_, execErr := conn.ExecContext(ctx, statement)
		c.Assert(execErr, qt.IsNil, qt.Commentf("statement:\n%s", statement))
	}

	live, err := dbschema.ReadSchemaWithSchemas(conn, []string{schemaName})
	c.Assert(err, qt.IsNil)
	report := sqlServerFunctionNamed(live.Functions, "p_report")
	c.Assert(report.Body, qt.Contains, "1 AS extra")
	settled := schemadiff.CompareWithDialect(after, live, platform.SQLServer)
	c.Assert(settled.FunctionsModified, qt.HasLen, 0)
}

// sqlServerProcedureSchema describes two procedures: one ordinary, and one
// carrying WITH EXECUTE AS OWNER, whose definition keeps both an `EXECUTE AS`
// and the `AS` that opens the body. Without the second, a walk taking the first
// standalone AS looks correct.
func sqlServerProcedureSchema(schemaName string) *goschema.Database {
	return &goschema.Database{
		Functions: []goschema.Function{{
			StructName: "Report", Name: schemaName + ".p_report",
			Kind:       goschema.FunctionKindProcedure,
			Parameters: "@id int, @label varchar(50)",
			Language:   "sql",
			Body:       "BEGIN SET NOCOUNT ON; SELECT @id AS id, @label AS label; END",
		}, {
			StructName: "Owner", Name: schemaName + ".p_owner",
			Kind:       goschema.FunctionKindProcedure,
			Parameters: "@n int",
			Language:   "sql",
			Security:   "DEFINER",
			Body:       "BEGIN SELECT @n AS n; END",
		}},
	}
}

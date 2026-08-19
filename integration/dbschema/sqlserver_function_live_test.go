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
	dbschematypes "go.5x5.cz/ptah/dbschema/types"
	"go.5x5.cz/ptah/internal/dbtarget"
	"go.5x5.cz/ptah/migration/schemadiff"
)

// TestSQLServerLiveFunctionRoundTrip is the test the Functions capability could
// not have been flipped without.
//
// The key was false for one reason -- nothing read a function back -- and the
// failure that reason describes is not a compile error but an apply loop
// planning the same CREATE forever. Only a live read can show it is gone.
//
// It also settles the question the issue got wrong. #1720 assumed the signature
// would have to be parsed out of sys.sql_modules.definition; the catalog
// publishes it as rows, and step 2 asserts the recovered parameter list and
// return type character for character.
func TestSQLServerLiveFunctionRoundTrip(t *testing.T) {
	dbURL := dbtarget.URL(t, dbtarget.SQLServer)
	c := qt.New(t)
	ctx := t.Context()

	conn, err := dbschema.ConnectToDatabase(ctx, dbURL)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)

	schemaName := fmt.Sprintf("ptah_fn_%d", time.Now().UnixNano())
	quoted := quoteSQLServerIdentifier(schemaName)
	_, err = conn.ExecContext(ctx, "EXEC('CREATE SCHEMA "+quoted+"')")
	c.Assert(err, qt.IsNil)
	defer func() {
		for _, name := range []string{"fn_label", "fn_answer", "fn_rows"} {
			_, _ = conn.ExecContext(ctx, "DROP FUNCTION IF EXISTS "+quoted+"."+quoteSQLServerIdentifier(name))
		}
		_, _ = conn.ExecContext(ctx, "DROP SCHEMA IF EXISTS "+quoted)
	}()

	description := sqlServerFunctionSchema(schemaName)

	// 1. The renderer's statements are what the server is given, so a statement
	// this engine refuses fails here rather than being corrected by hand.
	statements, err := renderer.GetOrderedCreateStatements(description, platform.SQLServer)
	c.Assert(err, qt.IsNil)
	c.Assert(strings.Join(statements, "\n"), qt.Contains, "CREATE OR ALTER FUNCTION")
	for _, statement := range statements {
		_, execErr := conn.ExecContext(ctx, statement)
		c.Assert(execErr, qt.IsNil, qt.Commentf("statement:\n%s", statement))
	}

	// 2. The signature comes back from the catalog, not from the statement text.
	live, err := dbschema.ReadSchemaWithSchemas(conn, []string{schemaName})
	c.Assert(err, qt.IsNil)
	c.Assert(live.Functions, qt.HasLen, 3)
	label := sqlServerFunctionNamed(live.Functions, "fn_label")
	c.Assert(label.Parameters, qt.Equals, "@id int, @tag nvarchar(50)")
	c.Assert(label.Returns, qt.Equals, "varchar(100)")
	answer := sqlServerFunctionNamed(live.Functions, "fn_answer")
	c.Assert(answer.Parameters, qt.Equals, "")
	c.Assert(answer.Returns, qt.Equals, "int")
	// The table-valued row is the one that pins the body boundary.
	rows := sqlServerFunctionNamed(live.Functions, "fn_rows")
	c.Assert(rows.Body, qt.Equals, "RETURN SELECT 1 AS ok WHERE @t = 1")

	// 3. The convergence assertion. Comparing the same description against what
	// the server now holds must produce nothing to do.
	settled := schemadiff.CompareWithDialect(description, live, platform.SQLServer)
	c.Assert(settled.FunctionsAdded, qt.HasLen, 0)
	c.Assert(settled.FunctionsRemoved, qt.HasLen, 0)
	c.Assert(settled.FunctionsModified, qt.HasLen, 0)
}

// TestSQLServerLiveFunctionRefusesWhatTheRendererDeclines pins that the clauses
// the renderer answers with a sentence are ones the engine refuses too, and
// that the one it cannot read back really is invisible.
func TestSQLServerLiveFunctionRefusesWhatTheRendererDeclines(t *testing.T) {
	dbURL := dbtarget.URL(t, dbtarget.SQLServer)
	c := qt.New(t)
	ctx := t.Context()

	conn, err := dbschema.ConnectToDatabase(ctx, dbURL)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)

	schemaName := fmt.Sprintf("ptah_fnref_%d", time.Now().UnixNano())
	quoted := quoteSQLServerIdentifier(schemaName)
	_, err = conn.ExecContext(ctx, "EXEC('CREATE SCHEMA "+quoted+"')")
	c.Assert(err, qt.IsNil)
	defer func() {
		for _, name := range []string{"fn_ok", "fn_default"} {
			_, _ = conn.ExecContext(ctx, "DROP FUNCTION IF EXISTS "+quoted+"."+quoteSQLServerIdentifier(name))
		}
		_, _ = conn.ExecContext(ctx, "DROP SCHEMA IF EXISTS "+quoted)
	}()

	rows := []struct {
		name      string
		statement string
		refusal   string
	}{{
		name: "IF NOT EXISTS is not a clause here",
		statement: "CREATE FUNCTION IF NOT EXISTS " + quoted +
			".fn_x() RETURNS int AS BEGIN RETURN 1; END",
		// The parser names whichever token it stopped on, and that varies with
		// the statement's shape: the same clause draws `Incorrect syntax near
		// the keyword 'IF'` inside an EXEC and `A RETURN statement with a
		// return value cannot be used in this context` submitted directly.
		// What is stable is that it never runs.
		refusal: "",
	}, {
		name:      "there is no LANGUAGE clause",
		statement: "CREATE FUNCTION " + quoted + ".fn_y() RETURNS int LANGUAGE SQL AS BEGIN RETURN 1; END",
		refusal:   "Incorrect syntax near 'LANGUAGE'",
	}}

	for _, row := range rows {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)

			_, execErr := conn.ExecContext(ctx, row.statement)
			c.Assert(execErr, qt.IsNotNil, qt.Commentf("statement:\n%s", row.statement))
			c.Assert(execErr.Error(), qt.Contains, row.refusal,
				qt.Commentf("statement:\n%s", row.statement))
		})
	}

	t.Run("the rendered create form is accepted, twice", func(t *testing.T) {
		c := qt.New(t)

		create := "CREATE OR ALTER FUNCTION " + quoted + ".fn_ok(@a int)\nRETURNS int\nAS\nBEGIN RETURN @a; END;"
		_, execErr := conn.ExecContext(ctx, create)
		c.Assert(execErr, qt.IsNil)
		// The second run is the point: CREATE OR ALTER is what a declaration
		// asking for IF NOT EXISTS gets, so it has to be idempotent.
		_, execErr = conn.ExecContext(ctx, create)
		c.Assert(execErr, qt.IsNil)
	})

	t.Run("a parameter default is accepted and then unreadable", func(t *testing.T) {
		c := qt.New(t)

		_, execErr := conn.ExecContext(ctx,
			"CREATE FUNCTION "+quoted+".fn_default(@a int, @b varchar(50) = 'x') RETURNS int AS BEGIN RETURN @a; END")
		c.Assert(execErr, qt.IsNil)

		// This is why the renderer refuses the shape rather than emitting it:
		// the engine takes the default and the catalog does not report it, so a
		// function created with one differs from its own declaration forever.
		live, readErr := dbschema.ReadSchemaWithSchemas(conn, []string{schemaName})
		c.Assert(readErr, qt.IsNil)
		defaulted := sqlServerFunctionNamed(live.Functions, "fn_default")
		c.Assert(defaulted.Parameters, qt.Equals, "@a int, @b varchar(50)")
		c.Assert(defaulted.Parameters, qt.Not(qt.Contains), "'x'")
	})
}

// sqlServerFunctionSchema declares two functions: one with arguments and a
// sized return type, one with neither.
func sqlServerFunctionSchema(schemaName string) *goschema.Database {
	return &goschema.Database{
		Functions: []goschema.Function{{
			// nvarchar is here on purpose: sys.parameters reports max_length in
			// BYTES, so this one is stored as 100 and has to be rendered back as
			// 50. A fixture using only varchar leaves that halving unmeasured.
			// The body carries its own depth-zero `AS`, which is what separates
			// taking the first one after RETURNS from taking the last.
			StructName: "Label", Name: schemaName + ".fn_label",
			Parameters: "@id int, @tag nvarchar(50)", Returns: "varchar(100)",
			Language: "sql",
			Body:     "BEGIN RETURN (SELECT CONVERT(varchar(100), @id) + CONVERT(varchar(50), @tag) AS label); END",
		}, {
			StructName: "Answer", Name: schemaName + ".fn_answer",
			Returns: "int", Language: "sql", Body: "BEGIN RETURN 42; END",
		}, {
			// An inline table-valued function is what separates "first AS after
			// RETURNS" from "last AS": its body carries a depth-zero `AS` of its
			// own, and the last-match rule would return `ok` as the whole body.
			StructName: "Rows", Name: schemaName + ".fn_rows",
			Parameters: "@t int", Returns: "TABLE", Language: "sql",
			Body: "RETURN SELECT 1 AS ok WHERE @t = 1",
		}},
	}
}

// sqlServerFunctionNamed returns the function a catalog read reports under a
// name.
func sqlServerFunctionNamed(functions []dbschematypes.DBFunction, name string) dbschematypes.DBFunction {
	for _, function := range functions {
		if function.Name == name {
			return function
		}
	}
	return dbschematypes.DBFunction{}
}

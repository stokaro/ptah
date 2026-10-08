//go:build integration

package generator_test

import (
	"context"
	"fmt"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/platform"
	"ptah.run/core/schemamodel"
	"ptah.run/dbschema"
	"ptah.run/engine/builtin"
	"ptah.run/internal/dbtarget"
	"ptah.run/migration/planner"
	"ptah.run/migration/schemadiff"
)

// A plan says whether it creates a PostgreSQL routine or replaces one. A routine
// the database does not have is a plain CREATE FUNCTION, and one it has and the
// schema changes is CREATE OR REPLACE FUNCTION. A trigger's own function follows
// its trigger. Without the plain form, every plan that adds a function reads as
// one that rewrites code already in use, and a plan made for a database without
// the function overwrites one that appeared since instead of failing.

// routineDeclaration is a table with one declared function and one trigger
// with a body, which Ptah gives a function of its own. version changes both
// bodies.
func routineDeclaration(version int) *schemamodel.Database {
	return &schemamodel.Database{
		Tables: []schemamodel.Table{{StructName: "User", Name: "users"}},
		Fields: []schemamodel.Field{
			{StructName: "User", Name: "id", Type: "INTEGER", Primary: true},
			{StructName: "User", Name: "email", Type: "TEXT"},
		},
		Functions: []schemamodel.Function{{
			Name: "bump", Parameters: "n integer", Returns: "integer", Language: "sql",
			Body: fmt.Sprintf("SELECT n + %d", version),
		}},
		Triggers: []schemamodel.Trigger{{
			Name: "owned", Table: "users", Timing: "BEFORE", Event: "UPDATE", ForEach: "ROW",
			Body: fmt.Sprintf("NEW.email := NEW.email || '%d'; RETURN NEW;", version),
		}},
	}
}

// planRoutines compares a declaration with the schema the connection reads and
// plans the change.
func planRoutines(c *qt.C, conn *dbschema.DatabaseConnection, schemaName string, version int) []string {
	c.Helper()
	live, err := dbschema.ReadSchemaWithSchemasContext(c.Context(), conn, []string{schemaName})
	c.Assert(err, qt.IsNil)
	diff := must.Must(schemadiff.CompareWithDialect(c.Context(), routineDeclaration(version), live, platform.Postgres, must.Must(builtin.New())))
	statements, err := planner.GenerateSchemaDiffSQLStatements(
		context.Background(), must.Must(builtin.New()),
		diff, platform.Postgres,
	)
	c.Assert(err, qt.IsNil)
	return statements
}

// executeAll runs planned statements in order, naming the one the server
// refuses.
func executeAll(c *qt.C, conn *dbschema.DatabaseConnection, statements []string) {
	c.Helper()
	for _, statement := range statements {
		_, err := conn.ExecContext(c.Context(), statement)
		c.Assert(err, qt.IsNil, qt.Commentf("statement:\n%s", statement))
	}
}

// createdRoutines reports how each planned statement creates a function, as
// its verb and name: `CREATE FUNCTION "bump"`.
func createdRoutines(statements []string) []string {
	var created []string
	for _, statement := range statements {
		for line := range strings.Lines(statement) {
			if strings.HasPrefix(line, "CREATE FUNCTION ") || strings.HasPrefix(line, "CREATE OR REPLACE FUNCTION ") {
				header, _, _ := strings.Cut(line, "(")
				created = append(created, header)
			}
		}
	}
	return created
}

// statementCreating returns the planned statement that creates the named
// function.
func statementCreating(c *qt.C, statements []string, name string) string {
	c.Helper()
	for _, statement := range statements {
		if strings.Contains(statement, "FUNCTION "+name+"(") {
			return statement
		}
	}
	c.Fatalf("no statement creates %s in:\n%s", name, strings.Join(statements, "\n"))
	return ""
}

// TestPostgresLiveRoutinesAreCreatedThenReplaced applies the plan that adds
// the routines, then the plan that changes them, and reads the result back.
func TestPostgresLiveRoutinesAreCreatedThenReplaced(t *testing.T) {
	dbURL := dbtarget.URL(t, dbtarget.PostgreSQL)
	c := qt.New(t)
	conn, schemaName := triggerDropConnection(c, dbURL)

	added := planRoutines(c, conn, schemaName, 1)
	c.Assert(createdRoutines(added), qt.ContentEquals,
		[]string{`CREATE FUNCTION "bump"`, `CREATE FUNCTION "ptah_trigger_users_owned"`})
	executeAll(c, conn, added)

	// The plan for a database without the function does not overwrite one:
	// run again, its CREATE fails with duplicate_function.
	_, err := conn.ExecContext(c.Context(), statementCreating(c, added, `"bump"`))
	c.Assert(err, qt.ErrorMatches, `(?s).*42723.*`)

	changed := planRoutines(c, conn, schemaName, 2)
	c.Assert(createdRoutines(changed), qt.ContentEquals,
		[]string{`CREATE OR REPLACE FUNCTION "bump"`, `CREATE OR REPLACE FUNCTION "ptah_trigger_users_owned"`})
	executeAll(c, conn, changed)

	var bumped int
	c.Assert(conn.QueryRowContext(c.Context(), `SELECT bump(1)`).Scan(&bumped), qt.IsNil)
	c.Assert(bumped, qt.Equals, 3)
	c.Assert(planRoutines(c, conn, schemaName, 2), qt.HasLen, 0)
}

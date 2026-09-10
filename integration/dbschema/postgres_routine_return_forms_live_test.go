//go:build integration

package dbschema_test

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/core/platform"
	"ptah.run/core/renderer"
	"ptah.run/dbschema"
	"ptah.run/internal/atlashcl"
	"ptah.run/internal/dbtarget"
	"ptah.run/migration/schemadiff"
)

// returnFormsDocument declares one set-returning function and one returning a
// table, in the format whose reader this test drives.
//
// The table's columns are written in reverse alphabetical order, and the body
// returns them in that order, so a parser that sorted the columns would declare
// a row shape the body does not produce and the server would refuse it.
func returnFormsDocument(schemaName string) []byte {
	return []byte(`
schema "` + schemaName + `" {}

function "fs" {
  schema     = schema.` + schemaName + `
  lang       = SQL
  return     = integer
  return_set = true
  as         = "SELECT 1"
}

function "ft" {
  schema = schema.` + schemaName + `
  lang   = SQL
  as     = "SELECT 'x'::text, 1"
  return_table = {
    zz = text
    aa = integer
  }
}
`)
}

// TestPostgresLiveRoutineReturnFormsConverge is the test these two clauses
// could not have been added without.
//
// Each step is one only a server answers:
//
//  1. `RETURNS SETOF integer` and `RETURNS TABLE(zz text, aa integer)` are
//     statements this engine accepts, in the column order the document wrote.
//     A parser that sorted the table's columns would declare a row the body
//     does not produce, and the refusal names a type mismatch rather than an
//     ordering, so no offline assertion would have located it.
//  2. pg_get_function_result reports both clauses back. It prints the keyword
//     uppercase and the type names lowercase, which is the spelling the model
//     has to hold.
//  3. Comparing the document against what the server now holds finds nothing
//     to do. The comparison is a string equality, so a lowercased keyword on
//     either side is an apply loop rather than a cosmetic difference.
func TestPostgresLiveRoutineReturnFormsConverge(t *testing.T) {
	dbURL := dbtarget.URL(t, dbtarget.PostgreSQL)
	c := qt.New(t)
	ctx := t.Context()

	conn, err := dbschema.ConnectToDatabase(ctx, dbURL)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)

	schemaName := fmt.Sprintf("ptah_retform_%d", time.Now().UnixNano())
	_, err = conn.ExecContext(ctx, `CREATE SCHEMA "`+schemaName+`"`)
	c.Assert(err, qt.IsNil)
	defer func() {
		_, _ = conn.ExecContext(context.Background(), `DROP SCHEMA IF EXISTS "`+schemaName+`" CASCADE`)
	}()

	description, err := atlashcl.Parse(returnFormsDocument(schemaName), "schema.hcl")
	c.Assert(err, qt.IsNil)

	statements, err := renderer.GetOrderedCreateStatements(description, platform.Postgres)
	c.Assert(err, qt.IsNil)
	joined := strings.Join(statements, "\n")
	c.Assert(joined, qt.Contains, "RETURNS SETOF integer")
	c.Assert(joined, qt.Contains, "RETURNS TABLE(zz text, aa integer)")
	for _, statement := range statements {
		_, execErr := conn.ExecContext(ctx, statement)
		c.Assert(execErr, qt.IsNil, qt.Commentf("statement:\n%s", statement))
	}

	live, err := dbschema.ReadSchemaWithSchemasContext(ctx, conn, []string{schemaName})
	c.Assert(err, qt.IsNil)
	c.Assert(live.Functions, qt.HasLen, 2)
	c.Assert(liveReturnsOf(c, live.Functions, "fs"), qt.Equals, "SETOF integer")
	c.Assert(liveReturnsOf(c, live.Functions, "ft"), qt.Equals, "TABLE(zz text, aa integer)")

	settled := schemadiff.CompareWithDialect(description, live, platform.Postgres)
	c.Assert(settled.FunctionsAdded, qt.HasLen, 0)
	c.Assert(settled.FunctionsModified, qt.HasLen, 0)
	c.Assert(settled.FunctionsRemoved, qt.HasLen, 0)
}

// liveReturnsOf returns the read-back return clause of the named routine.
func liveReturnsOf(c *qt.C, functions []catalog.Function, name string) string {
	for _, function := range functions {
		if function.Name == name {
			return function.Returns
		}
	}
	c.Fatalf("no function named %q in the read schema", name)
	return ""
}

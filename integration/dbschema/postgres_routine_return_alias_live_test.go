//go:build integration

package dbschema_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform"
	"ptah.run/core/renderer"
	"ptah.run/dbschema"
	"ptah.run/internal/atlashcl"
	"ptah.run/internal/dbtarget"
	"ptah.run/migration/schemadiff"
)

// returnAliasDocument declares routines whose return types are written with the
// aliases a server accepts rather than the spellings it reports back.
func returnAliasDocument(schemaName string) []byte {
	return []byte(`
schema "` + schemaName + `" {}

function "scalar" {
  schema = schema.` + schemaName + `
  lang   = SQL
  return = int
  as     = "SELECT 1"
}

function "wide" {
  schema = schema.` + schemaName + `
  lang   = SQL
  return = float8
  as     = "SELECT 1.0::float8"
}

function "rows" {
  schema     = schema.` + schemaName + `
  lang       = SQL
  return     = int4
  return_set = true
  as         = "SELECT 1"
}
`)
}

// TestPostgresLiveRoutineReturnAliasConverges is the test this fold could not
// have been added without.
//
// The declaration and the catalog do not use the same spellings: `int4` is what
// a server accepts and `integer` is what pg_get_function_result reports. The
// comparison is a string equality, so such a routine was planned for
// replacement on every run -- the plan applied, changed nothing because the
// routine was already what the statement said, and was planned again
// (stokaro/ptah#3155).
//
// Only a server can produce the second spelling, which is why an offline test
// cannot see this at all: both sides of an offline comparison come from the
// same declaration.
func TestPostgresLiveRoutineReturnAliasConverges(t *testing.T) {
	dbURL := dbtarget.URL(t, dbtarget.PostgreSQL)
	c := qt.New(t)
	ctx := t.Context()

	conn, err := dbschema.ConnectToDatabase(ctx, dbURL)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)

	schemaName := fmt.Sprintf("ptah_retalias_%d", time.Now().UnixNano())
	_, err = conn.ExecContext(ctx, `CREATE SCHEMA "`+schemaName+`"`)
	c.Assert(err, qt.IsNil)
	defer func() {
		_, _ = conn.ExecContext(context.Background(), `DROP SCHEMA IF EXISTS "`+schemaName+`" CASCADE`)
	}()

	description, err := atlashcl.Parse(returnAliasDocument(schemaName), "schema.hcl")
	c.Assert(err, qt.IsNil)

	statements, err := renderer.GetOrderedCreateStatements(description, platform.Postgres)
	c.Assert(err, qt.IsNil)
	for _, statement := range statements {
		_, execErr := conn.ExecContext(ctx, statement)
		c.Assert(execErr, qt.IsNil, qt.Commentf("statement:\n%s", statement))
	}

	live, err := dbschema.ReadSchemaWithSchemasContext(ctx, conn, []string{schemaName})
	c.Assert(err, qt.IsNil)
	c.Assert(live.Functions, qt.HasLen, 3)

	settled := schemadiff.CompareWithDialect(description, live, platform.Postgres)
	c.Assert(settled.FunctionsAdded, qt.HasLen, 0)
	c.Assert(settled.FunctionsModified, qt.HasLen, 0)
	c.Assert(settled.FunctionsRemoved, qt.HasLen, 0)
}

// TestPostgresLiveRoutineReturnDifferenceIsStillPlanned is the control.
//
// A fold wide enough to hide a real change would report a routine as unchanged
// while the server returns something else, which is worse than the churn it
// removes. `bigint` and `integer` are two types, not two spellings of one.
func TestPostgresLiveRoutineReturnDifferenceIsStillPlanned(t *testing.T) {
	dbURL := dbtarget.URL(t, dbtarget.PostgreSQL)
	c := qt.New(t)
	ctx := t.Context()

	conn, err := dbschema.ConnectToDatabase(ctx, dbURL)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)

	schemaName := fmt.Sprintf("ptah_retwiden_%d", time.Now().UnixNano())
	_, err = conn.ExecContext(ctx, `CREATE SCHEMA "`+schemaName+`"`)
	c.Assert(err, qt.IsNil)
	defer func() {
		_, _ = conn.ExecContext(context.Background(), `DROP SCHEMA IF EXISTS "`+schemaName+`" CASCADE`)
	}()

	_, err = conn.ExecContext(ctx,
		`CREATE FUNCTION "`+schemaName+`".scalar() RETURNS integer LANGUAGE sql AS 'SELECT 1'`)
	c.Assert(err, qt.IsNil)

	wanted, err := atlashcl.Parse([]byte(`
schema "`+schemaName+`" {}

function "scalar" {
  schema = schema.`+schemaName+`
  lang   = SQL
  return = bigint
  as     = "SELECT 1"
}
`), "schema.hcl")
	c.Assert(err, qt.IsNil)

	live, err := dbschema.ReadSchemaWithSchemasContext(ctx, conn, []string{schemaName})
	c.Assert(err, qt.IsNil)

	diff := schemadiff.CompareWithDialect(wanted, live, platform.Postgres)

	c.Assert(diff.FunctionsModified, qt.HasLen, 1)
	c.Assert(diff.FunctionsModified[0].Changes["returns"], qt.Equals, "integer -> bigint")
}

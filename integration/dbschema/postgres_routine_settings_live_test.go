//go:build integration

package dbschema_test

import (
	"context"
	"fmt"
	"strings"
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

// routineSettingsDocument renders a DEFINER function pinning its search_path,
// written in the format whose reader this test drives.
func routineSettingsDocument(schemaName string) []byte {
	return []byte(`
schema "` + schemaName + `" {}

function "pinned" {
  schema   = schema.` + schemaName + `
  lang     = SQL
  return   = integer
  as       = "SELECT 1"
  security = DEFINER
  set = {
    search_path = "` + schemaName + `"
  }
}
`)
}

// TestPostgresLiveRoutineSettingsConverge is the test the `set` reader could
// not have been wired without.
//
// A SECURITY DEFINER routine without a pinned search_path resolves unqualified
// names through whatever the caller set, so the clause being dropped was a
// hazard rather than a cosmetic loss. Three steps only a server answers:
//
//  1. The rendered SET clause is one PostgreSQL takes. It sits after the
//     language and security clauses and before the body, and an offline test
//     comparing strings cannot tell a valid position from an invalid one.
//  2. pg_proc.proconfig reports the setting back. The catalog stores it as
//     `name=value`, which is the spelling the model holds, so this is where the
//     two representations meet.
//  3. Comparing the same document against what the server now holds finds
//     nothing to do.
func TestPostgresLiveRoutineSettingsConverge(t *testing.T) {
	dbURL := dbtarget.URL(t, dbtarget.PostgreSQL)
	c := qt.New(t)
	ctx := t.Context()

	conn, err := dbschema.ConnectToDatabase(ctx, dbURL)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)

	schemaName := fmt.Sprintf("ptah_fnset_%d", time.Now().UnixNano())
	_, err = conn.ExecContext(ctx, `CREATE SCHEMA "`+schemaName+`"`)
	c.Assert(err, qt.IsNil)
	defer func() {
		_, _ = conn.ExecContext(context.Background(), `DROP SCHEMA IF EXISTS "`+schemaName+`" CASCADE`)
	}()

	description, err := atlashcl.Parse(routineSettingsDocument(schemaName), "schema.hcl")
	c.Assert(err, qt.IsNil)

	// 1. The rendered statements are the ones the server is given.
	statements, err := renderer.GetOrderedCreateStatements(description, platform.Postgres)
	c.Assert(err, qt.IsNil)
	c.Assert(strings.Join(statements, "\n"), qt.Contains, "SET search_path = "+schemaName)
	for _, statement := range statements {
		_, execErr := conn.ExecContext(ctx, statement)
		c.Assert(execErr, qt.IsNil, qt.Commentf("statement:\n%s", statement))
	}

	// 2. The catalog reports the setting back.
	live, err := dbschema.ReadSchemaWithSchemasContext(ctx, conn, []string{schemaName})
	c.Assert(err, qt.IsNil)
	c.Assert(live.Functions, qt.HasLen, 1)
	c.Assert(live.Functions[0].Settings, qt.DeepEquals, []string{"search_path=" + schemaName})

	// 3. The convergence assertion.
	settled := schemadiff.CompareWithDialect(description, live, platform.Postgres)
	c.Assert(settled.FunctionsAdded, qt.HasLen, 0)
	c.Assert(settled.FunctionsModified, qt.HasLen, 0)
	c.Assert(settled.FunctionsRemoved, qt.HasLen, 0)
}

// TestPostgresLiveRoutineWithoutSettingsCarriesNone is the control for the read
// above.
//
// A reader reporting a setting for every routine would satisfy the assertions
// there while telling the comparison nothing, and a routine that declared no
// SET clause has to come back with none.
func TestPostgresLiveRoutineWithoutSettingsCarriesNone(t *testing.T) {
	dbURL := dbtarget.URL(t, dbtarget.PostgreSQL)
	c := qt.New(t)
	ctx := t.Context()

	conn, err := dbschema.ConnectToDatabase(ctx, dbURL)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)

	schemaName := fmt.Sprintf("ptah_fnnoset_%d", time.Now().UnixNano())
	_, err = conn.ExecContext(ctx, `CREATE SCHEMA "`+schemaName+`"`)
	c.Assert(err, qt.IsNil)
	defer func() {
		_, _ = conn.ExecContext(context.Background(), `DROP SCHEMA IF EXISTS "`+schemaName+`" CASCADE`)
	}()

	document := []byte(`
schema "` + schemaName + `" {}

function "plain" {
  schema   = schema.` + schemaName + `
  lang     = SQL
  return   = integer
  as       = "SELECT 1"
  security = DEFINER
}
`)
	description, err := atlashcl.Parse(document, "schema.hcl")
	c.Assert(err, qt.IsNil)

	statements, err := renderer.GetOrderedCreateStatements(description, platform.Postgres)
	c.Assert(err, qt.IsNil)
	c.Assert(strings.Join(statements, "\n"), qt.Not(qt.Contains), "SET ")
	for _, statement := range statements {
		_, execErr := conn.ExecContext(ctx, statement)
		c.Assert(execErr, qt.IsNil, qt.Commentf("statement:\n%s", statement))
	}

	live, err := dbschema.ReadSchemaWithSchemasContext(ctx, conn, []string{schemaName})
	c.Assert(err, qt.IsNil)
	c.Assert(live.Functions, qt.HasLen, 1)
	c.Assert(live.Functions[0].Settings, qt.HasLen, 0)

	settled := schemadiff.CompareWithDialect(description, live, platform.Postgres)
	c.Assert(settled.FunctionsModified, qt.HasLen, 0)
}

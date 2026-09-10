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

// triggerExecuteDocument declares one function and two triggers that share it,
// which is the arrangement a per-trigger body cannot express.
func triggerExecuteDocument(schemaName string) []byte {
	return []byte(`
schema "` + schemaName + `" {}

table "t" {
  schema = schema.` + schemaName + `
  column "id" { type = int }
}

function "shared_touch" {
  schema = schema.` + schemaName + `
  lang   = PLpgSQL
  return = trigger
  as     = "BEGIN RETURN NEW; END;"
}

trigger "t_before" {
  on = table.t
  before {
    update = true
  }
  foreach = ROW
  execute {
    function = function.shared_touch
  }
}

trigger "t_after" {
  on = table.t
  after {
    insert = true
  }
  foreach = ROW
  execute {
    function = function.shared_touch
  }
}
`)
}

// TestPostgresLiveTriggerExecuteConverges is the test this reader could not
// have been wired without.
//
// Two triggers share one function, which a per-trigger body cannot express: a
// body makes Ptah generate a private function per trigger. Three steps only a
// server answers:
//
//  1. The rendered statements bind both triggers to the declared function, and
//     the server accepts them. A trigger naming a function that does not exist
//     is refused there, so this also proves the ordering puts the function
//     first.
//  2. pg_trigger reports the function each trigger executes, which is what the
//     comparison reads.
//  3. Comparing the document against what the server holds finds nothing to do.
func TestPostgresLiveTriggerExecuteConverges(t *testing.T) {
	dbURL := dbtarget.URL(t, dbtarget.PostgreSQL)
	c := qt.New(t)
	ctx := t.Context()

	conn, err := dbschema.ConnectToDatabase(ctx, dbURL)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)

	schemaName := fmt.Sprintf("ptah_trigexec_%d", time.Now().UnixNano())
	_, err = conn.ExecContext(ctx, `CREATE SCHEMA "`+schemaName+`"`)
	c.Assert(err, qt.IsNil)
	defer func() {
		_, _ = conn.ExecContext(context.Background(), `DROP SCHEMA IF EXISTS "`+schemaName+`" CASCADE`)
	}()

	description, err := atlashcl.Parse(triggerExecuteDocument(schemaName), "schema.hcl")
	c.Assert(err, qt.IsNil)

	statements, err := renderer.GetOrderedCreateStatements(description, platform.Postgres)
	c.Assert(err, qt.IsNil)
	joined := strings.Join(statements, "\n")
	// Qualified, which is the half a local run against `public` cannot see: an
	// unqualified name is resolved through search_path, and a schema the
	// document just created is not on it.
	c.Assert(joined, qt.Contains,
		`EXECUTE FUNCTION "`+schemaName+`"."shared_touch"()`)
	// One function, not one per trigger. A body would have produced two.
	c.Assert(joined, qt.Not(qt.Contains), "ptah_trigger_")
	c.Assert(strings.Count(joined, "RETURNS trigger"), qt.Equals, 1)
	for _, statement := range statements {
		_, execErr := conn.ExecContext(ctx, statement)
		c.Assert(execErr, qt.IsNil, qt.Commentf("statement:\n%s", statement))
	}

	live, err := dbschema.ReadSchemaWithSchemasContext(ctx, conn, []string{schemaName})
	c.Assert(err, qt.IsNil)
	c.Assert(live.Triggers, qt.HasLen, 2)
	c.Assert(live.Functions, qt.HasLen, 1)

	settled := schemadiff.CompareWithDialect(description, live, platform.Postgres)
	c.Assert(settled.TriggersAdded, qt.HasLen, 0)
	c.Assert(settled.TriggersModified, qt.HasLen, 0)
	c.Assert(settled.TriggersRemoved, qt.HasLen, 0)
	c.Assert(settled.FunctionsAdded, qt.HasLen, 0)
	c.Assert(settled.FunctionsRemoved, qt.HasLen, 0)
}

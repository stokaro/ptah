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

// plannerPropertiesDocument renders a function carrying the given attribute
// lines, in the format whose reader this test drives.
func plannerPropertiesDocument(schemaName, attrs string) []byte {
	return []byte(`
schema "` + schemaName + `" {}

function "planned" {
  schema = schema.` + schemaName + `
  lang   = SQL
  return = integer
  as     = "SELECT 1"
  ` + attrs + `
}
`)
}

// TestPostgresLiveRoutinePlannerPropertiesConverge is the test the two
// attributes could not have been added without.
//
// Each step is one only a server answers:
//
//  1. The clauses are accepted where the renderer puts them. PostgreSQL takes
//     the routine's attributes in any order but refuses an unknown one, and an
//     offline test comparing strings cannot tell an accepted statement from a
//     rejected one.
//  2. pg_proc reports both back. proleakproof is a boolean and proparallel is a
//     single character the reader maps to a word, so this is where the catalog's
//     spelling and the model's meet.
//  3. Comparing the same document against what the server now holds finds
//     nothing to do -- the apply loop this would otherwise create.
func TestPostgresLiveRoutinePlannerPropertiesConverge(t *testing.T) {
	dbURL := dbtarget.URL(t, dbtarget.PostgreSQL)
	c := qt.New(t)
	ctx := t.Context()

	conn, err := dbschema.ConnectToDatabase(ctx, dbURL)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)

	schemaName := fmt.Sprintf("ptah_fnplan_%d", time.Now().UnixNano())
	_, err = conn.ExecContext(ctx, `CREATE SCHEMA "`+schemaName+`"`)
	c.Assert(err, qt.IsNil)
	defer func() {
		_, _ = conn.ExecContext(context.Background(), `DROP SCHEMA IF EXISTS "`+schemaName+`" CASCADE`)
	}()

	description, err := atlashcl.Parse(plannerPropertiesDocument(schemaName, `
  leakproof = true
  parallel  = SAFE`), "schema.hcl")
	c.Assert(err, qt.IsNil)

	statements, err := renderer.GetOrderedCreateStatements(description, platform.Postgres)
	c.Assert(err, qt.IsNil)
	joined := strings.Join(statements, "\n")
	c.Assert(joined, qt.Contains, "LEAKPROOF")
	c.Assert(joined, qt.Contains, "PARALLEL SAFE")
	for _, statement := range statements {
		_, execErr := conn.ExecContext(ctx, statement)
		c.Assert(execErr, qt.IsNil, qt.Commentf("statement:\n%s", statement))
	}

	live, err := dbschema.ReadSchemaWithSchemasContext(ctx, conn, []string{schemaName})
	c.Assert(err, qt.IsNil)
	c.Assert(live.Functions, qt.HasLen, 1)
	c.Assert(live.Functions[0].Leakproof, qt.IsTrue)
	c.Assert(live.Functions[0].Parallel, qt.Equals, "SAFE")

	settled := schemadiff.CompareWithDialect(description, live, platform.Postgres)
	c.Assert(settled.FunctionsAdded, qt.HasLen, 0)
	c.Assert(settled.FunctionsModified, qt.HasLen, 0)
	c.Assert(settled.FunctionsRemoved, qt.HasLen, 0)
}

// TestPostgresLiveRoutineWithoutPlannerPropertiesConverges is the control for
// the read above, and the one that proves the UNSAFE fold is needed.
//
// A routine stating no level renders no clause, and the catalog reports UNSAFE
// for it regardless. Without the fold those two are different strings and the
// routine is planned for replacement on every run; with it the comparison is
// empty. A reader reporting both properties as set would also pass the test
// above while telling the comparison nothing.
func TestPostgresLiveRoutineWithoutPlannerPropertiesConverges(t *testing.T) {
	dbURL := dbtarget.URL(t, dbtarget.PostgreSQL)
	c := qt.New(t)
	ctx := t.Context()

	conn, err := dbschema.ConnectToDatabase(ctx, dbURL)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)

	schemaName := fmt.Sprintf("ptah_fnnoplan_%d", time.Now().UnixNano())
	_, err = conn.ExecContext(ctx, `CREATE SCHEMA "`+schemaName+`"`)
	c.Assert(err, qt.IsNil)
	defer func() {
		_, _ = conn.ExecContext(context.Background(), `DROP SCHEMA IF EXISTS "`+schemaName+`" CASCADE`)
	}()

	description, err := atlashcl.Parse(plannerPropertiesDocument(schemaName, `volatility = STABLE`), "schema.hcl")
	c.Assert(err, qt.IsNil)

	statements, err := renderer.GetOrderedCreateStatements(description, platform.Postgres)
	c.Assert(err, qt.IsNil)
	joined := strings.Join(statements, "\n")
	c.Assert(joined, qt.Not(qt.Contains), "LEAKPROOF")
	c.Assert(joined, qt.Not(qt.Contains), "PARALLEL")
	for _, statement := range statements {
		_, execErr := conn.ExecContext(ctx, statement)
		c.Assert(execErr, qt.IsNil, qt.Commentf("statement:\n%s", statement))
	}

	live, err := dbschema.ReadSchemaWithSchemasContext(ctx, conn, []string{schemaName})
	c.Assert(err, qt.IsNil)
	c.Assert(live.Functions, qt.HasLen, 1)
	c.Assert(live.Functions[0].Leakproof, qt.IsFalse)
	// The server names the default even though the declaration did not.
	c.Assert(live.Functions[0].Parallel, qt.Equals, "UNSAFE")

	settled := schemadiff.CompareWithDialect(description, live, platform.Postgres)
	c.Assert(settled.FunctionsModified, qt.HasLen, 0)
}

// TestPostgresLiveRoutinePlannerPropertyDifferenceIsPlanned pins that a
// property the server does not have is a difference rather than silence.
//
// The convergence tests pass just as well when nothing is compared, so this
// drives the opposite case: a database holding the defaults against a
// declaration asking for both properties has to produce a change.
func TestPostgresLiveRoutinePlannerPropertyDifferenceIsPlanned(t *testing.T) {
	dbURL := dbtarget.URL(t, dbtarget.PostgreSQL)
	c := qt.New(t)
	ctx := t.Context()

	conn, err := dbschema.ConnectToDatabase(ctx, dbURL)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)

	schemaName := fmt.Sprintf("ptah_fnplandiff_%d", time.Now().UnixNano())
	_, err = conn.ExecContext(ctx, `CREATE SCHEMA "`+schemaName+`"`)
	c.Assert(err, qt.IsNil)
	defer func() {
		_, _ = conn.ExecContext(context.Background(), `DROP SCHEMA IF EXISTS "`+schemaName+`" CASCADE`)
	}()

	plain, err := atlashcl.Parse(plannerPropertiesDocument(schemaName, `volatility = STABLE`), "schema.hcl")
	c.Assert(err, qt.IsNil)
	statements, err := renderer.GetOrderedCreateStatements(plain, platform.Postgres)
	c.Assert(err, qt.IsNil)
	for _, statement := range statements {
		_, execErr := conn.ExecContext(ctx, statement)
		c.Assert(execErr, qt.IsNil, qt.Commentf("statement:\n%s", statement))
	}

	live, err := dbschema.ReadSchemaWithSchemasContext(ctx, conn, []string{schemaName})
	c.Assert(err, qt.IsNil)

	wanted, err := atlashcl.Parse(plannerPropertiesDocument(schemaName, `
  volatility = STABLE
  leakproof  = true
  parallel   = SAFE`), "schema.hcl")
	c.Assert(err, qt.IsNil)

	diff := schemadiff.CompareWithDialect(wanted, live, platform.Postgres)

	c.Assert(diff.FunctionsModified, qt.HasLen, 1)
	c.Assert(diff.FunctionsModified[0].Changes["leakproof"], qt.Equals, "false -> true")
	c.Assert(diff.FunctionsModified[0].Changes["parallel"], qt.Equals, "UNSAFE -> SAFE")
}

//go:build integration

package gonative_test

import (
	"database/sql"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"
	_ "github.com/jackc/pgx/v5/stdlib"

	"github.com/stokaro/ptah/core/goschema"
	"github.com/stokaro/ptah/internal/dbschema/postgres"
	"github.com/stokaro/ptah/migration/planner"
	"github.com/stokaro/ptah/migration/schemadiff"
)

// TestFunctionDiff_BodyAndSecurityChange_Integration covers GitHub issue #89.
//
// The bug: changes to a PL/pgSQL function's body or SECURITY qualifier were
// being silently dropped by the migration generator — diff comparison populated
// FunctionsModified, but the planner had no handler so no SQL was emitted and
// the caller reported "no schema changes detected".
//
// This test installs an initial function, runs the full schemadiff +
// planner pipeline against a target schema with a changed body and changed
// SECURITY qualifier, applies the generated SQL, and confirms the function's
// live definition in the database has actually changed.
func TestFunctionDiff_BodyAndSecurityChange_Integration(t *testing.T) {
	dsn := skipIfNoPostgreSQL(t)
	c := qt.New(t)

	db, err := sql.Open("pgx", dsn)
	c.Assert(err, qt.IsNil)
	defer db.Close()

	// Install the "before" function: SECURITY DEFINER + session-scoped set_config.
	_, err = db.Exec(`
		CREATE OR REPLACE FUNCTION ptah_test_set_group_context(group_id_param TEXT)
		RETURNS VOID AS $$
		BEGIN PERFORM set_config('app.current_group_id', group_id_param, false); END;
		$$ LANGUAGE plpgsql SECURITY DEFINER;
	`)
	c.Assert(err, qt.IsNil)
	defer func() { _, _ = db.Exec("DROP FUNCTION IF EXISTS ptah_test_set_group_context(TEXT)") }()

	// Read the live DB schema and confirm the function landed as expected.
	reader := postgres.NewPostgreSQLReader(db, "public")
	before, err := reader.ReadSchema()
	c.Assert(err, qt.IsNil)

	var beforeFn *struct{ Body, Security string }
	for _, fn := range before.Functions {
		if fn.Name == "ptah_test_set_group_context" {
			beforeFn = &struct{ Body, Security string }{Body: fn.Body, Security: fn.Security}
			break
		}
	}
	c.Assert(beforeFn, qt.IsNotNil, qt.Commentf("seeded function should be readable from DB"))
	c.Assert(beforeFn.Security, qt.Equals, "DEFINER")
	c.Assert(strings.Contains(beforeFn.Body, "false"), qt.IsTrue)

	// Define the "after" target: same name/signature, but the body switches to
	// transaction-local set_config and SECURITY DEFINER is dropped.
	target := &goschema.Database{
		Functions: []goschema.Function{
			{
				Name:       "ptah_test_set_group_context",
				Parameters: "group_id_param text",
				Returns:    "void",
				Language:   "plpgsql",
				// No security= → default INVOKER (drops SECURITY DEFINER).
				Body: "\nBEGIN PERFORM set_config('app.current_group_id', group_id_param, true); END;\n",
			},
		},
	}

	// Run the diff over the live DB schema.
	diff := schemadiff.Compare(target, before)
	c.Assert(diff.HasChanges(), qt.IsTrue, qt.Commentf("body+security change must be detected"))
	c.Assert(diff.FunctionsModified, qt.HasLen, 1)
	mod := diff.FunctionsModified[0]
	c.Assert(mod.FunctionName, qt.Equals, "ptah_test_set_group_context")
	c.Assert(mod.Changes["body"], qt.Not(qt.Equals), "")
	c.Assert(mod.Changes["security"], qt.Equals, "DEFINER -> INVOKER")

	// Plan, render, and apply the up migration.
	statements, err := planner.GenerateSchemaDiffSQLStatements(diff, target, "postgres")
	c.Assert(err, qt.IsNil)
	c.Assert(len(statements) > 0, qt.IsTrue,
		qt.Commentf("planner must emit at least one SQL statement for FunctionsModified"))

	sqlText := strings.Join(statements, ";\n") + ";"
	sqlForAssert := legacyRenderedSQL(sqlText)
	c.Assert(strings.Contains(sqlForAssert, "CREATE OR REPLACE FUNCTION ptah_test_set_group_context"), qt.IsTrue)
	c.Assert(strings.Contains(sqlForAssert, "set_config('app.current_group_id', group_id_param, true)"), qt.IsTrue)
	c.Assert(strings.Contains(sqlForAssert, "SECURITY DEFINER"), qt.IsFalse,
		qt.Commentf("the rewritten definition must not carry the dropped SECURITY DEFINER"))

	_, err = db.Exec(sqlText)
	c.Assert(err, qt.IsNil, qt.Commentf("generated migration SQL must execute: %s", sqlText))

	// Verify the live function actually flipped both attributes.
	after, err := reader.ReadSchema()
	c.Assert(err, qt.IsNil)
	var afterFn *struct{ Body, Security string }
	for _, fn := range after.Functions {
		if fn.Name == "ptah_test_set_group_context" {
			afterFn = &struct{ Body, Security string }{Body: fn.Body, Security: fn.Security}
			break
		}
	}
	c.Assert(afterFn, qt.IsNotNil)
	c.Assert(afterFn.Security, qt.Equals, "INVOKER",
		qt.Commentf("SECURITY DEFINER must be dropped"))
	c.Assert(strings.Contains(afterFn.Body, "true"), qt.IsTrue,
		qt.Commentf("function body must reflect transaction-local set_config; got: %s", afterFn.Body))
	c.Assert(strings.Contains(afterFn.Body, "false"), qt.IsFalse,
		qt.Commentf("function body must no longer contain the previous flag value"))

	// Idempotency: a second diff against the freshly updated DB must be empty.
	idempDiff := schemadiff.Compare(target, after)
	c.Assert(idempDiff.HasChanges(), qt.IsFalse,
		qt.Commentf("re-running diff after applying the migration must report no changes"))
}

// TestFunctionDiff_VolatilityChange_Integration covers the planner-hint case
// from issue #89 (VOLATILE → STABLE). The new schema reader exposes
// p.provolatile, so the diff must catch it and emit CREATE OR REPLACE.
func TestFunctionDiff_VolatilityChange_Integration(t *testing.T) {
	dsn := skipIfNoPostgreSQL(t)
	c := qt.New(t)

	db, err := sql.Open("pgx", dsn)
	c.Assert(err, qt.IsNil)
	defer db.Close()

	// Seed a VOLATILE function.
	_, err = db.Exec(`
		CREATE OR REPLACE FUNCTION ptah_test_get_current_group_id()
		RETURNS TEXT AS $$
		BEGIN RETURN current_setting('app.current_group_id', true); END;
		$$ LANGUAGE plpgsql VOLATILE;
	`)
	c.Assert(err, qt.IsNil)
	defer func() { _, _ = db.Exec("DROP FUNCTION IF EXISTS ptah_test_get_current_group_id()") }()

	reader := postgres.NewPostgreSQLReader(db, "public")
	before, err := reader.ReadSchema()
	c.Assert(err, qt.IsNil)

	// Target schema marks the same function as STABLE.
	target := &goschema.Database{
		Functions: []goschema.Function{
			{
				Name:       "ptah_test_get_current_group_id",
				Parameters: "",
				Returns:    "text",
				Language:   "plpgsql",
				Volatility: "STABLE",
				Body:       "\nBEGIN RETURN current_setting('app.current_group_id', true); END;\n",
			},
		},
	}

	diff := schemadiff.Compare(target, before)
	c.Assert(diff.HasChanges(), qt.IsTrue)
	c.Assert(diff.FunctionsModified, qt.HasLen, 1)
	c.Assert(diff.FunctionsModified[0].Changes["volatility"], qt.Equals, "VOLATILE -> STABLE")

	statements, err := planner.GenerateSchemaDiffSQLStatements(diff, target, "postgres")
	c.Assert(err, qt.IsNil)
	c.Assert(len(statements), qt.Not(qt.Equals), 0)
	sqlText := strings.Join(statements, ";\n") + ";"
	c.Assert(strings.Contains(sqlText, "STABLE"), qt.IsTrue)

	_, err = db.Exec(sqlText)
	c.Assert(err, qt.IsNil, qt.Commentf("generated migration SQL must execute: %s", sqlText))

	after, err := reader.ReadSchema()
	c.Assert(err, qt.IsNil)
	var afterFn *struct{ Volatility string }
	for _, fn := range after.Functions {
		if fn.Name == "ptah_test_get_current_group_id" {
			afterFn = &struct{ Volatility string }{Volatility: fn.Volatility}
			break
		}
	}
	c.Assert(afterFn, qt.IsNotNil, qt.Commentf("function must still exist after the modify migration"))
	c.Assert(afterFn.Volatility, qt.Equals, "STABLE")

	// Idempotency: re-diffing the freshly-updated DB against the same target
	// must report no changes.
	idempDiff := schemadiff.Compare(target, after)
	c.Assert(idempDiff.HasChanges(), qt.IsFalse,
		qt.Commentf("post-migration diff must be clean"))
}

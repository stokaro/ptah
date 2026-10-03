//go:build integration

package integration_test

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"
	_ "github.com/jackc/pgx/v5/stdlib" // registers the pgx driver for database/sql

	"ptah.run/core/platform"
	"ptah.run/core/renderer"
	"ptah.run/internal/dbtarget"
	"ptah.run/internal/sqlschema"
)

// routineBeforeBareTable declares a routine returning `SETOF public.t` before
// the table `t` it names. CockroachDB resolves the bare `t` in `public`, so the
// table has to be created first: created the other way round, the routine is
// refused with `type "public.t" does not exist` (stokaro/ptah#4065).
const routineBeforeBareTable = `CREATE FUNCTION public.all_t() RETURNS SETOF public.t LANGUAGE SQL STABLE AS $$ SELECT * FROM public.t $$;
CREATE TABLE t (id INT8 PRIMARY KEY);`

// TestSchemaRenderCreatesARoutineAfterABareTableOnCockroachDBLive runs the
// rendered DDL of routineBeforeBareTable on an empty CockroachDB database, and
// reads the routine back.
func TestSchemaRenderCreatesARoutineAfterABareTableOnCockroachDBLive(t *testing.T) {
	c := qt.New(t)
	ctx, cancel := context.WithTimeout(c.Context(), 5*time.Minute)
	defer cancel()
	_, db := newCockroachRoutineOrderDatabase(c, ctx)
	database, _, err := sqlschema.Read([]byte(routineBeforeBareTable), platform.CockroachDB)
	c.Assert(err, qt.IsNil)
	statements, err := renderer.GetOrderedCreateStatements(&database, platform.CockroachDB)
	c.Assert(err, qt.IsNil)

	for _, statement := range statements {
		_, execErr := db.ExecContext(ctx, statement)
		c.Assert(execErr, qt.IsNil, qt.Commentf("%s", statement))
	}

	c.Assert(cockroachRoutineCount(c, ctx, db, "all_t"), qt.Equals, 1)
}

// TestSchemaApplyCreatesARoutineAfterABareTableOnCockroachDBLive applies
// routineBeforeBareTable with the native apply, which places routines by the
// same rule as the render.
func TestSchemaApplyCreatesARoutineAfterABareTableOnCockroachDBLive(t *testing.T) {
	c := qt.New(t)
	ctx, cancel := context.WithTimeout(c.Context(), 5*time.Minute)
	defer cancel()
	dbURL, db := newCockroachRoutineOrderDatabase(c, ctx)
	schema := filepath.Join(c.TempDir(), "schema.sql")
	c.Assert(os.WriteFile(schema, []byte(routineBeforeBareTable+"\n"), 0o600), qt.IsNil)

	out, err := runPtahNativeWithError("schema", "apply", "--db-url", dbURL, "--schema-file", schema, "--auto-approve")

	c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
	c.Assert(cockroachRoutineCount(c, ctx, db, "all_t"), qt.Equals, 1)
}

// newCockroachRoutineOrderDatabase creates an empty database of its own on the
// CockroachDB test server and returns its URL with a connection to it.
func newCockroachRoutineOrderDatabase(c *qt.C, ctx context.Context) (string, *sql.DB) {
	c.Helper()
	adminURL := dbtarget.URL(c, dbtarget.CockroachDB)
	admin, err := sql.Open("pgx", postgresFamilyDriverURL(c, adminURL))
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { c.Check(admin.Close(), qt.IsNil) })
	name := fmt.Sprintf("ptah_routine_order_%d", time.Now().UnixNano())
	createE2EDatabase(c, ctx, admin, name)
	c.Cleanup(func() { dropPostgresFamilyE2EDatabase(c, admin, name) })

	dbURL := replaceDatabaseName(c, adminURL, name)
	db, err := sql.Open("pgx", postgresFamilyDriverURL(c, dbURL))
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { c.Check(db.Close(), qt.IsNil) })
	return dbURL, db
}

// cockroachRoutineCount counts the routines named name in the public schema.
func cockroachRoutineCount(c *qt.C, ctx context.Context, db *sql.DB, name string) int {
	c.Helper()
	var count int
	c.Assert(db.QueryRowContext(ctx, `
		SELECT count(*) FROM pg_proc p
		JOIN pg_namespace n ON n.oid = p.pronamespace
		WHERE n.nspname = 'public' AND p.proname = $1`, name).Scan(&count), qt.IsNil)
	return count
}

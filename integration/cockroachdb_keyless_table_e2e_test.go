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

	"ptah.run/internal/dbtarget"
)

// TestCockroachDBKeylessTableE2E_HappyPath compares and applies a schema file
// declaring a table with no primary key against the CockroachDB table it
// describes.
//
// CockroachDB gives such a table a hidden rowid column and a key over it,
// `t_pkey`. A read that keeps the key while it leaves the column out makes the
// comparison plan `ALTER TABLE "t" DROP CONSTRAINT IF EXISTS "t_pkey"` on every
// run, and the server refuses it: v26 because the table is schema_locked, v25.4
// because a key cannot be dropped without adding another (stokaro/ptah#3738).
//
// The second half is the control that the engine's key is still replaced when
// the file does declare one: the plan adds the key, CockroachDB swaps it for
// the one over rowid, and the table loses the hidden column.
func TestCockroachDBKeylessTableE2E_HappyPath(t *testing.T) {
	c := qt.New(t)
	ctx, cancel := context.WithTimeout(c.Context(), 5*time.Minute)
	defer cancel()
	dbURL, db := newKeylessTableDatabase(c, ctx)
	dir := c.TempDir()
	keyless := filepath.Join(dir, "keyless.sql")
	c.Assert(os.WriteFile(keyless, []byte("CREATE TABLE t (id bigint);\n"), 0o600), qt.IsNil)
	keyed := filepath.Join(dir, "keyed.sql")
	c.Assert(os.WriteFile(keyed, []byte("CREATE TABLE t (id bigint NOT NULL, CONSTRAINT t_pkey PRIMARY KEY (id));\n"), 0o600), qt.IsNil)
	c.Assert(keylessTableKey(c, ctx, db), qt.Equals, "rowid",
		qt.Commentf("the engine did not give the table its hidden key, so nothing below is measured"))

	compared, comparedErr, err := runPtahSplitStreams(ctx, []string{
		"schema", "compare", "--schema-file", keyless, "--db-url", dbURL, "--exit-code",
	})
	c.Assert(err, qt.IsNil, qt.Commentf("stdout:\n%s\nstderr:\n%s", compared, comparedErr))
	c.Assert(compared, qt.Contains, "No schema differences detected.")

	applied, appliedErr, err := runPtahSplitStreams(ctx, []string{
		"schema", "apply", "--schema-file", keyless, "--db-url", dbURL, "--auto-approve",
	})
	c.Assert(err, qt.IsNil, qt.Commentf("stdout:\n%s\nstderr:\n%s", applied, appliedErr))
	c.Assert(applied, qt.Contains, "Schema is synced")

	keyedApply, keyedApplyErr, err := runPtahSplitStreams(ctx, []string{
		"schema", "apply", "--schema-file", keyed, "--db-url", dbURL, "--auto-approve",
	})
	c.Assert(err, qt.IsNil, qt.Commentf("stdout:\n%s\nstderr:\n%s", keyedApply, keyedApplyErr))
	c.Assert(keylessTableKey(c, ctx, db), qt.Equals, "id")

	settled, settledErr, err := runPtahSplitStreams(ctx, []string{
		"schema", "compare", "--schema-file", keyed, "--db-url", dbURL, "--exit-code",
	})
	c.Assert(err, qt.IsNil, qt.Commentf("stdout:\n%s\nstderr:\n%s", settled, settledErr))
}

// newKeylessTableDatabase creates a database of its own on the CockroachDB
// server, holding one table that declares no primary key, and drops it when the
// test ends. It returns the address ptah connects with and a driver handle for
// reading the catalog back.
func newKeylessTableDatabase(c *qt.C, ctx context.Context) (string, *sql.DB) {
	c.Helper()
	adminURL := dbtarget.URL(c, dbtarget.CockroachDB)
	admin, err := sql.Open("pgx", postgresFamilyDriverURL(c, adminURL))
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { c.Check(admin.Close(), qt.IsNil) })
	name := fmt.Sprintf("ptah_keyless_%d", time.Now().UnixNano())
	createE2EDatabase(c, ctx, admin, name)
	c.Cleanup(func() { dropPostgresFamilyE2EDatabase(c, admin, name) })

	dbURL := replaceDatabaseName(c, adminURL, name)
	db, err := sql.Open("pgx", postgresFamilyDriverURL(c, dbURL))
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { c.Check(db.Close(), qt.IsNil) })
	_, err = db.ExecContext(ctx, "CREATE TABLE t (id INT8)")
	c.Assert(err, qt.IsNil)
	return dbURL, db
}

// keylessTableKey names the columns of table t's primary key as pg_constraint
// records it, which is where the hidden key shows and the description does not.
func keylessTableKey(c *qt.C, ctx context.Context, db *sql.DB) string {
	c.Helper()
	var columns string
	c.Assert(db.QueryRowContext(ctx, `
		SELECT string_agg(a.attname, ',' ORDER BY k.ordinality)
		FROM pg_constraint con
		JOIN pg_class cls ON cls.oid = con.conrelid
		CROSS JOIN LATERAL unnest(con.conkey) WITH ORDINALITY AS k(attnum, ordinality)
		JOIN pg_attribute a ON a.attrelid = con.conrelid AND a.attnum = k.attnum
		WHERE cls.relname = 't' AND con.contype = 'p'`,
	).Scan(&columns), qt.IsNil)
	return columns
}

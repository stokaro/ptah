//go:build integration

package integration_test

import (
	"context"
	"database/sql"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"ptah.run/dbschema"
	"ptah.run/internal/dblock"
)

// TestMigrationsUpWritesNothingBeforeTheMigrationLockE2E runs `ptah migrations
// up` against an empty PostgreSQL database while another session holds the
// migration lock. The run times out on the lock and leaves the database empty.
// A run that creates schema_migrations outside the lock races every other first
// run on that CREATE, and on PostgreSQL 18 the loser fails with a duplicate key
// on pg_type_typname_nsp_index.
func TestMigrationsUpWritesNothingBeforeTheMigrationLockE2E(t *testing.T) {
	c := qt.New(t)
	url := postgresScratchDevURL(c, "ptah_nup_lock")
	conn, err := dbschema.ConnectToDatabase(c.Context(), url)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { c.Check(conn.Close(), qt.IsNil) })
	lock, err := dblock.Acquire(c.Context(), conn, "ptah_migrate", time.Second)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { c.Check(lock.Release(context.Background()), qt.IsNil) })

	out, err := runPtahNativeWithError("migrations", "up", "--db-url", url,
		"--migrations-dir", nativeStatusMigrationDir(c), "--migration-lock-timeout", "100ms")

	c.Assert(err, qt.ErrorMatches, "(?s).*migration lock.*", qt.Commentf("%s", out))
	c.Assert(userTableCount(c, url), qt.Equals, 0)
}

// TestMigrationsUpWithNoMigrationsWritesNothingE2E runs `ptah migrations up`
// with an empty migrations directory against an empty PostgreSQL database.
// Nothing is pending, so the run never takes the migration lock, and every read
// it makes on the way to that answer is outside the lock as well.
func TestMigrationsUpWithNoMigrationsWritesNothingE2E(t *testing.T) {
	c := qt.New(t)
	url := postgresScratchDevURL(c, "ptah_nup_empty")

	out, err := runPtahNativeWithError("migrations", "up", "--db-url", url,
		"--migrations-dir", c.TempDir())

	c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
	c.Assert(userTableCount(c, url), qt.Equals, 0)
}

func userTableCount(c *qt.C, url string) int {
	c.Helper()
	db, err := sql.Open("pgx", url)
	c.Assert(err, qt.IsNil)
	defer func() { c.Check(db.Close(), qt.IsNil) }()
	var tables int
	err = db.QueryRowContext(c.Context(),
		`SELECT count(*) FROM pg_tables WHERE schemaname NOT IN ('pg_catalog', 'information_schema')`).Scan(&tables)
	c.Assert(err, qt.IsNil)
	return tables
}

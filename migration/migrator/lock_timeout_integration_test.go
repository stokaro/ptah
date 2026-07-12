package migrator_test

import (
	"context"
	"database/sql"
	"os"
	"strings"
	"testing"
	"testing/fstest"
	"time"

	qt "github.com/frankban/quicktest"

	"github.com/stokaro/ptah/dbschema"
	"github.com/stokaro/ptah/migration/migrator"
)

func TestMigrateUp_PostgresLockTimeoutIntegration(t *testing.T) {
	dbURL := os.Getenv("POSTGRES_TEST_DSN")
	if dbURL == "" {
		dbURL = os.Getenv("TEST_DATABASE_URL")
	}
	if dbURL == "" {
		t.Skip("POSTGRES_TEST_DSN or TEST_DATABASE_URL not set")
	}
	if !strings.HasPrefix(dbURL, "postgres://") && !strings.HasPrefix(dbURL, "postgresql://") {
		t.Skip("PostgreSQL URL required for lock timeout integration test")
	}

	c := qt.New(t)
	ctx := context.Background()

	conn, err := dbschema.ConnectToDatabase(ctx, dbURL)
	c.Assert(err, qt.IsNil)
	defer func() { _ = conn.Close() }()

	_, _ = conn.ExecContext(ctx, "DROP TABLE IF EXISTS schema_migrations")
	_, _ = conn.ExecContext(ctx, "DROP TABLE IF EXISTS ptah_lock_timeout_items")
	_, err = conn.ExecContext(ctx, "CREATE TABLE ptah_lock_timeout_items (id INTEGER PRIMARY KEY)")
	c.Assert(err, qt.IsNil)
	defer func() {
		_, _ = conn.ExecContext(ctx, "DROP TABLE IF EXISTS schema_migrations")
		_, _ = conn.ExecContext(ctx, "DROP TABLE IF EXISTS ptah_lock_timeout_items")
	}()

	lockDB, err := sql.Open("pgx", dbURL)
	c.Assert(err, qt.IsNil)
	defer func() { _ = lockDB.Close() }()

	lockTx, err := lockDB.BeginTx(ctx, nil)
	c.Assert(err, qt.IsNil)
	defer func() { _ = lockTx.Rollback() }()

	_, err = lockTx.ExecContext(ctx, "SELECT * FROM ptah_lock_timeout_items")
	c.Assert(err, qt.IsNil)

	fsys := fstest.MapFS{
		"0000000001_add_blocked_column.up.sql": &fstest.MapFile{
			Data: []byte("-- +ptah lock_timeout=200ms\nALTER TABLE ptah_lock_timeout_items ADD COLUMN blocked_value TEXT;"),
		},
		"0000000001_add_blocked_column.down.sql": &fstest.MapFile{
			Data: []byte("ALTER TABLE ptah_lock_timeout_items DROP COLUMN blocked_value;"),
		},
	}

	mig, err := migrator.NewFSMigrator(conn, fsys)
	c.Assert(err, qt.IsNil)

	start := time.Now()
	err = mig.MigrateUp(ctx)
	elapsed := time.Since(start)

	c.Assert(err, qt.IsNotNil)
	c.Assert(err.Error(), qt.Contains, "lock timeout")
	c.Assert(elapsed < 2*time.Second, qt.IsTrue, qt.Commentf("migration took %s", elapsed))
}

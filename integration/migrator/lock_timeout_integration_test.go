//go:build integration

package migrator_test

import (
	"context"
	"database/sql"
	"strings"
	"testing"
	"testing/fstest"
	"time"

	qt "github.com/frankban/quicktest"

	"ptah.run/dbschema"
	"ptah.run/internal/dbtarget"
	"ptah.run/migration/migrator"
)

func TestMigrateUp_PostgresLockTimeoutIntegration(t *testing.T) {
	dbURL := postgresTestURL(t)
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

func TestMigrateUp_MySQLMetadataLockTimeoutIntegration(t *testing.T) {
	runMySQLFamilyMetadataLockTimeoutIntegration(t, "mysql", dbtarget.MySQLAdmin)
}

func TestMigrateUp_MariaDBMetadataLockTimeoutIntegration(t *testing.T) {
	runMySQLFamilyMetadataLockTimeoutIntegration(t, "mariadb", dbtarget.MariaDBAdmin)
}

func runMySQLFamilyMetadataLockTimeoutIntegration(t *testing.T, dialect string, engine dbtarget.Engine) {
	t.Helper()
	c := qt.New(t)
	ctx := context.Background()
	dbURL := mySQLFamilyScratchDatabaseURL(t, dialect, engine, "ptah_metadata_lock_timeout")

	conn, err := dbschema.ConnectToDatabase(ctx, dbURL)
	c.Assert(err, qt.IsNil)
	defer func() { _ = conn.Close() }()

	_, err = conn.ExecContext(ctx, "CREATE TABLE ptah_lock_timeout_items (id INTEGER PRIMARY KEY)")
	c.Assert(err, qt.IsNil)

	lockDB, err := sql.Open("mysql", strings.TrimPrefix(dbURL, dialect+"://"))
	c.Assert(err, qt.IsNil)
	defer func() { _ = lockDB.Close() }()

	lockTx, err := lockDB.BeginTx(ctx, nil)
	c.Assert(err, qt.IsNil)
	defer func() { _ = lockTx.Rollback() }()
	_, err = lockTx.ExecContext(ctx, "SELECT * FROM ptah_lock_timeout_items")
	c.Assert(err, qt.IsNil)

	releaseLock := time.AfterFunc(2500*time.Millisecond, func() { _ = lockTx.Rollback() })
	defer releaseLock.Stop()

	fsys := fstest.MapFS{
		"0000000001_add_blocked_column.up.sql": &fstest.MapFile{
			Data: []byte("-- +ptah lock_timeout=1s\nALTER TABLE ptah_lock_timeout_items ADD COLUMN blocked_value TEXT;"),
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
	c.Assert(err.Error(), qt.Contains, "Lock wait timeout exceeded")
	c.Assert(elapsed < 2*time.Second, qt.IsTrue, qt.Commentf("migration took %s", elapsed))

	var blockedColumnCount int
	err = conn.QueryRowContext(ctx, `SELECT COUNT(*) FROM information_schema.columns
		WHERE table_schema = DATABASE()
		AND table_name = 'ptah_lock_timeout_items'
		AND column_name = 'blocked_value'`).Scan(&blockedColumnCount)
	c.Assert(err, qt.IsNil)
	c.Assert(blockedColumnCount, qt.Equals, 0)
}

//go:build integration

package migrator_test

import (
	"context"
	"database/sql"
	"testing"
	"testing/fstest"
	"time"

	qt "github.com/frankban/quicktest"

	"ptah.run/dbschema"
	"ptah.run/internal/revisiontable"
	"ptah.run/migration/migrator"
)

func TestMigrateUp_PostgresLockTimeoutIntegration(t *testing.T) {
	dbURL := postgresTestURL(t)
	c := qt.New(t)
	ctx := context.Background()

	conn, err := dbschema.ConnectToDatabase(ctx, dbURL)
	c.Assert(err, qt.IsNil)
	defer func() { _ = conn.Close() }()

	dropLockTimeoutFixture(ctx, conn)
	_, err = conn.ExecContext(ctx, "CREATE TABLE ptah_lock_timeout_items (id INTEGER PRIMARY KEY)")
	c.Assert(err, qt.IsNil)
	defer dropLockTimeoutFixture(ctx, conn)

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

// dropLockTimeoutFixture drops the items table and the default revision table
// with every table the migrator keeps beside it. A table that is not there is
// the state this wants, so the error is not read.
func dropLockTimeoutFixture(ctx context.Context, conn *dbschema.DatabaseConnection) {
	for _, statement := range append(dropRevisionTableStatements(revisiontable.Ptah),
		"DROP TABLE IF EXISTS ptah_lock_timeout_items") {
		_, _ = conn.ExecContext(ctx, statement)
	}
}

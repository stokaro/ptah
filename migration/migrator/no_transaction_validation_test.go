package migrator_test

import (
	"context"
	"path/filepath"
	"testing"
	"testing/fstest"
	"time"

	qt "github.com/frankban/quicktest"

	"github.com/stokaro/ptah/dbschema"
	"github.com/stokaro/ptah/migration/migrator"
)

func TestNoTransactionTimeoutValidation_UpLeavesNoRevision(t *testing.T) {
	c := qt.New(t)
	ctx := t.Context()
	conn := openNoTransactionValidationSQLite(c)
	defer dbschema.CloseAndWarn(conn)

	mig, err := migrator.NewFSMigrator(conn, fstest.MapFS{
		"000001_create_users.up.sql": {
			Data: []byte("-- +ptah no_transaction lock_timeout=1s\nCREATE TABLE users (id INTEGER PRIMARY KEY);"),
		},
		"000001_create_users.down.sql": {
			Data: []byte("DROP TABLE users;"),
		},
	})
	c.Assert(err, qt.IsNil)

	err = mig.MigrateUp(ctx)
	c.Assert(err, qt.ErrorMatches, `migration 1 is marked no_transaction, so migration timeouts cannot be applied safely`)
	c.Assert(noTransactionRevisionCount(c, conn), qt.Equals, int64(0))
	c.Assert(noTransactionTableExists(c, conn, "users"), qt.IsFalse)

	status, err := mig.GetMigrationStatus(ctx)
	c.Assert(err, qt.IsNil)
	c.Assert(status.CurrentVersion, qt.Equals, int64(0))
	c.Assert(status.DirtyRevision, qt.IsNil)
}

func TestNoTransactionTimeoutValidation_DownPreservesAppliedRevision(t *testing.T) {
	c := qt.New(t)
	ctx := t.Context()
	conn := openNoTransactionValidationSQLite(c)
	defer dbschema.CloseAndWarn(conn)

	mig, err := migrator.NewFSMigrator(conn, fstest.MapFS{
		"000001_create_users.up.sql": {
			Data: []byte("CREATE TABLE users (id INTEGER PRIMARY KEY);"),
		},
		"000001_create_users.down.sql": {
			Data: []byte("-- +ptah no_transaction statement_timeout=1s\nDROP TABLE users;"),
		},
	})
	c.Assert(err, qt.IsNil)
	c.Assert(mig.MigrateUp(ctx), qt.IsNil)

	err = mig.MigrateDownTo(ctx, 0)
	c.Assert(err, qt.ErrorMatches, `migration 1 is marked no_transaction, so migration timeouts cannot be applied safely`)
	c.Assert(noTransactionRevisionCount(c, conn), qt.Equals, int64(1))
	c.Assert(noTransactionRevisionState(c, conn), qt.Equals, "applied")
	c.Assert(noTransactionTableExists(c, conn, "users"), qt.IsTrue)

	status, err := mig.GetMigrationStatus(ctx)
	c.Assert(err, qt.IsNil)
	c.Assert(status.CurrentVersion, qt.Equals, int64(1))
	c.Assert(status.DirtyRevision, qt.IsNil)
}

func TestNoTransactionTimeoutValidation_DefaultTimeoutCanBeFixedAndRetried(t *testing.T) {
	c := qt.New(t)
	ctx := t.Context()
	conn := openNoTransactionValidationSQLite(c)
	defer dbschema.CloseAndWarn(conn)

	mig, err := migrator.NewFSMigrator(conn, fstest.MapFS{
		"000001_create_users.up.sql": {
			Data: []byte("-- +ptah no_transaction\nCREATE TABLE users (id INTEGER PRIMARY KEY);"),
		},
		"000001_create_users.down.sql": {
			Data: []byte("DROP TABLE users;"),
		},
	})
	c.Assert(err, qt.IsNil)
	mig = mig.WithDefaultTimeouts(migrator.MigrationTimeouts{
		LockTimeout:    time.Second,
		HasLockTimeout: true,
	})

	err = mig.MigrateUp(ctx)
	c.Assert(err, qt.ErrorMatches, `migration 1 is marked no_transaction, so migration timeouts cannot be applied safely`)
	c.Assert(noTransactionRevisionCount(c, conn), qt.Equals, int64(0))
	c.Assert(noTransactionTableExists(c, conn, "users"), qt.IsFalse)

	mig = mig.WithDefaultTimeouts(migrator.MigrationTimeouts{})
	c.Assert(mig.MigrateUp(ctx), qt.IsNil)
	c.Assert(noTransactionRevisionCount(c, conn), qt.Equals, int64(1))
	c.Assert(noTransactionRevisionState(c, conn), qt.Equals, "applied")
	c.Assert(noTransactionTableExists(c, conn, "users"), qt.IsTrue)
}

func TestProgrammaticMigration_UpNoTransactionUsesAutocommit(t *testing.T) {
	c := qt.New(t)
	ctx := t.Context()
	conn := openNoTransactionValidationSQLite(c)
	defer dbschema.CloseAndWarn(conn)

	migration := migrator.CreateMigrationFromSQL(
		1,
		"create users",
		"CREATE TABLE users (id INTEGER PRIMARY KEY); INSERT INTO missing_table (id) VALUES (1);",
		"DROP TABLE users;",
	)
	migration.UpNoTransaction = true
	mig := migrator.NewMigrator(conn, migrator.NewRegisteredMigrationProvider(migration))

	err := mig.MigrateUp(ctx)
	c.Assert(err, qt.ErrorMatches, `(?s).*failed to execute SQL.*no such table: missing_table.*`)
	c.Assert(noTransactionTableExists(c, conn, "users"), qt.IsTrue)
	c.Assert(noTransactionRevisionState(c, conn), qt.Equals, "failed")
	var applied, total int
	c.Assert(
		conn.QueryRow("SELECT applied, total FROM schema_migrations WHERE version = 1").Scan(&applied, &total),
		qt.IsNil,
	)
	c.Assert(applied, qt.Equals, 1)
	c.Assert(total, qt.Equals, 2)
}

func openNoTransactionValidationSQLite(c *qt.C) *dbschema.DatabaseConnection {
	c.Helper()
	conn, err := dbschema.ConnectToDatabase(
		context.Background(),
		"sqlite://"+filepath.Join(c.TempDir(), "no-transaction-validation.db"),
	)
	c.Assert(err, qt.IsNil)
	return conn
}

func noTransactionRevisionCount(c *qt.C, conn *dbschema.DatabaseConnection) int64 {
	c.Helper()
	var count int64
	c.Assert(conn.QueryRow("SELECT COUNT(*) FROM schema_migrations").Scan(&count), qt.IsNil)
	return count
}

func noTransactionRevisionState(c *qt.C, conn *dbschema.DatabaseConnection) string {
	c.Helper()
	var state string
	c.Assert(conn.QueryRow("SELECT state FROM schema_migrations WHERE version = 1").Scan(&state), qt.IsNil)
	return state
}

func noTransactionTableExists(c *qt.C, conn *dbschema.DatabaseConnection, table string) bool {
	c.Helper()
	var count int64
	c.Assert(
		conn.QueryRow("SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = ?", table).Scan(&count),
		qt.IsNil,
	)
	return count == 1
}

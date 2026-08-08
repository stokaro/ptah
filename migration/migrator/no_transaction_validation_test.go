package migrator_test

import (
	"context"
	"path/filepath"
	"testing"
	"testing/fstest"
	"time"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/dbschema"
	"go.5x5.cz/ptah/migration/migrator"
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

func TestNoTransactionControlValidation_UpLeavesNoRevision(t *testing.T) {
	c := qt.New(t)
	tests := []struct {
		name      string
		statement string
	}{
		{name: "begin", statement: "BEGIN"},
		{name: "start transaction", statement: "START TRANSACTION"},
		{name: "commit", statement: "COMMIT"},
		{name: "rollback", statement: "ROLLBACK"},
		{name: "savepoint", statement: "SAVEPOINT ptah_test"},
		{name: "release savepoint", statement: "RELEASE SAVEPOINT ptah_test"},
	}

	for _, test := range tests {
		c.Run(test.name, func(c *qt.C) {
			conn := openNoTransactionValidationSQLite(c)
			c.Cleanup(func() { dbschema.CloseAndWarn(conn) })
			migration := migrator.CreateMigrationFromSQL(
				1,
				"transaction control",
				"-- +ptah no_transaction\n"+test.statement+";\nCREATE TABLE users (id INTEGER PRIMARY KEY);",
				"DROP TABLE users;",
			)
			mig := migrator.NewMigrator(conn, migrator.NewRegisteredMigrationProvider(migration))

			err := mig.MigrateUp(c.Context())
			c.Assert(err, qt.ErrorMatches, `migration 1 cannot run up statement 1 without a transaction because .* controls transaction state;.*`)
			c.Assert(noTransactionRevisionCount(c, conn), qt.Equals, int64(0))
			c.Assert(noTransactionTableExists(c, conn, "users"), qt.IsFalse)
		})
	}
}

func TestNoTransactionControlValidation_DownPreservesAppliedRevision(t *testing.T) {
	c := qt.New(t)
	conn := openNoTransactionValidationSQLite(c)
	defer dbschema.CloseAndWarn(conn)
	migration := migrator.CreateMigrationFromSQL(
		1,
		"transaction control",
		"CREATE TABLE users (id INTEGER PRIMARY KEY);",
		"-- +ptah no_transaction\nBEGIN;\nDROP TABLE users;\nCOMMIT;",
	)
	mig := migrator.NewMigrator(conn, migrator.NewRegisteredMigrationProvider(migration))
	c.Assert(mig.MigrateUp(c.Context()), qt.IsNil)

	err := mig.MigrateDownTo(c.Context(), 0)
	c.Assert(err, qt.ErrorMatches, `migration 1 cannot run down statement 1 without a transaction because .* controls transaction state;.*`)
	c.Assert(noTransactionRevisionCount(c, conn), qt.Equals, int64(1))
	c.Assert(noTransactionRevisionState(c, conn), qt.Equals, "applied")
	c.Assert(noTransactionTableExists(c, conn, "users"), qt.IsTrue)
}

func TestNoTransactionControlValidation_RepairPreservesDirtyRevision(t *testing.T) {
	c := qt.New(t)
	conn := openNoTransactionValidationSQLite(c)
	defer dbschema.CloseAndWarn(conn)
	initial, err := migrator.NewFSMigrator(conn, fstest.MapFS{
		"000001_repair.up.sql": {Data: []byte(
			"-- +ptah no_transaction\n" +
				"CREATE TABLE users (id INTEGER PRIMARY KEY);\n" +
				"INSERT INTO missing_table (id) VALUES (1);",
		)},
		"000001_repair.down.sql": {Data: []byte("DROP TABLE users;")},
	})
	c.Assert(err, qt.IsNil)
	c.Assert(initial.MigrateUp(c.Context()), qt.IsNotNil)
	var originalError string
	c.Assert(conn.QueryRow("SELECT error FROM schema_migrations WHERE version = 1").Scan(&originalError), qt.IsNil)

	repair, err := migrator.NewFSMigrator(conn, fstest.MapFS{
		"000001_repair.up.sql": {Data: []byte(
			"-- +ptah no_transaction\n" +
				"CREATE TABLE users (id INTEGER PRIMARY KEY);\n" +
				"BEGIN;\n" +
				"INSERT INTO missing_table (id) VALUES (1);",
		)},
		"000001_repair.down.sql": {Data: []byte("DROP TABLE users;")},
	})
	c.Assert(err, qt.IsNil)
	err = repair.RepairMigration(c.Context(), migrator.RepairMigrationOptions{Version: 1, ResumeFrom: 2})
	c.Assert(err, qt.ErrorMatches, `.*cannot be repaired safely: migration 1 cannot run up statement 2 without a transaction because .* controls transaction state;.*`)
	status, err := repair.GetMigrationStatus(c.Context())
	c.Assert(err, qt.IsNil)
	c.Assert(status.DirtyRevision, qt.IsNotNil)
	c.Assert(status.DirtyRevision.Applied, qt.Equals, 1)
	c.Assert(status.DirtyRevision.Total, qt.Equals, 2)
	c.Assert(status.DirtyRevision.Error, qt.Equals, originalError)
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
	migration.UpTxMode = migrator.MigrationFileTxModeNone
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

func TestNoTransaction_InMemorySQLiteUsesPinnedBookkeeping(t *testing.T) {
	c := qt.New(t)
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()
	conn, err := dbschema.ConnectToDatabase(ctx, "sqlite:///:memory:")
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)

	migration := migrator.CreateMigrationFromSQL(
		1,
		"create users",
		"-- +ptah no_transaction\nCREATE TABLE users (id INTEGER PRIMARY KEY); INSERT INTO users (id) VALUES (1);",
		"DROP TABLE users;",
	)
	mig := migrator.NewMigrator(conn, migrator.NewRegisteredMigrationProvider(migration))

	c.Assert(mig.MigrateUp(ctx), qt.IsNil)
	c.Assert(noTransactionRevisionState(c, conn), qt.Equals, "applied")
	c.Assert(noTransactionTableExists(c, conn, "users"), qt.IsTrue)
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

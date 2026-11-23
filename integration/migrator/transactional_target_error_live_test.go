//go:build integration

package migrator_test

import (
	"context"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/dbschema"
	"ptah.run/internal/dbtarget"
	"ptah.run/migration/migrator"
)

// transactionalTargetEngines are the engines that ask what the database holds
// before the first statement of a migration in transaction mode file.
var transactionalTargetEngines = []struct {
	dialect string
	admin   dbtarget.Engine
}{
	{dialect: "mysql", admin: dbtarget.MySQLAdmin},
	{dialect: "mariadb", admin: dbtarget.MariaDBAdmin},
}

// transactionalTargetRefusals are databases that hold something a body in
// transaction mode file cannot run beside, with the body. Only the server's
// catalog says what the body writes to.
var transactionalTargetRefusals = []struct {
	name    string
	setup   []string
	body    string
	wantErr string
}{
	{
		name:    "a MyISAM table",
		setup:   []string{"CREATE TABLE t (id int PRIMARY KEY) ENGINE=MyISAM"},
		body:    "CREATE TABLE u (id int PRIMARY KEY);",
		wantErr: `.*tx-mode file requires InnoDB target tables on MySQL-family databases; table \S+\.t uses MyISAM`,
	},
	{
		name:    "a view the body writes through",
		setup:   []string{"CREATE TABLE t (id int PRIMARY KEY, n int)", "CREATE VIEW v AS SELECT id, n FROM t"},
		body:    "INSERT INTO v VALUES (1, 1);",
		wantErr: `.*migration 1 cannot run tx-mode file statement 1 because relation v has indirect behavior .*`,
	},
}

// transactionalTargetConnection connects to a new database of the engine whose
// administrative address is admin, after setup runs on it, and closes the
// connection when the test ends.
func transactionalTargetConnection(t *testing.T, dialect string, admin dbtarget.Engine, setup []string) *dbschema.DatabaseConnection {
	t.Helper()
	c := qt.New(t)
	dbURL := mySQLFamilyScratchDatabaseURL(t, dialect, admin, "ptah_tx_target")
	conn, err := dbschema.ConnectToDatabase(t.Context(), dbURL)
	c.Assert(err, qt.IsNil)
	t.Cleanup(func() { dbschema.CloseAndWarn(conn) })
	for _, statement := range setup {
		_, err := conn.ExecContext(t.Context(), statement)
		c.Assert(err, qt.IsNil)
	}
	return conn
}

// TestTransactionalTargetErrorLive_FailurePath asks MySQL and MariaDB about a
// body beside what transaction mode file cannot run with, and the refusal is a
// TransactionalTargetError, from FileModeRefusal and from a run alike. The run
// applies nothing.
func TestTransactionalTargetErrorLive_FailurePath(t *testing.T) {
	for _, engine := range transactionalTargetEngines {
		for _, test := range transactionalTargetRefusals {
			t.Run(engine.dialect+"/"+test.name+"/FileModeRefusal", func(t *testing.T) {
				c := qt.New(t)
				conn := transactionalTargetConnection(t, engine.dialect, engine.admin, test.setup)

				err := migrator.FileModeRefusal(c.Context(), conn, test.body)

				var refusal *migrator.TransactionalTargetError
				c.Assert(err, qt.ErrorAs, &refusal)
				c.Assert(refusal, qt.ErrorMatches, test.wantErr)
			})
			t.Run(engine.dialect+"/"+test.name+"/MigrateUp", func(t *testing.T) {
				c := qt.New(t)
				conn := transactionalTargetConnection(t, engine.dialect, engine.admin, test.setup)
				migration := migrator.CreateMigrationFromSQL(1, "refused", test.body, "SELECT 1;")

				err := migrator.NewMigrator(conn, migrator.NewRegisteredMigrationProvider(migration)).MigrateUp(c.Context())

				var refusal *migrator.TransactionalTargetError
				c.Assert(err, qt.ErrorAs, &refusal)
				c.Assert(refusal, qt.ErrorMatches, test.wantErr)
				var tables int
				c.Assert(conn.QueryRowContext(c.Context(),
					"SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = DATABASE() AND table_name = 'u'",
				).Scan(&tables), qt.IsNil)
				c.Assert(tables, qt.Equals, 0)
			})
		}
	}
}

// TestTransactionalTargetErrorLive_AnUnreadDatabaseIsNotARefusal asks with a
// context canceled before the question, so the session is never read. The
// failure is the context's, and it is not a TransactionalTargetError: it says
// nothing about what the database holds.
func TestTransactionalTargetErrorLive_AnUnreadDatabaseIsNotARefusal(t *testing.T) {
	for _, engine := range transactionalTargetEngines {
		t.Run(engine.dialect, func(t *testing.T) {
			c := qt.New(t)
			conn := transactionalTargetConnection(t, engine.dialect, engine.admin, nil)
			ctx, cancel := context.WithCancel(c.Context())
			cancel()

			err := migrator.FileModeRefusal(ctx, conn, "CREATE TABLE u (id int PRIMARY KEY);")

			c.Assert(err, qt.ErrorIs, context.Canceled)
			c.Assert(err, qt.Not(qt.ErrorAs), new(*migrator.TransactionalTargetError))
		})
	}
}

package migrator_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/dbschema"
	"ptah.run/migration/migrator"
)

func TestManagedTransactionControlRefusesUpBeforeChangingSchema(t *testing.T) {
	for _, mode := range []migrator.MigrationTxMode{migrator.MigrationTxModeFile, migrator.MigrationTxModeAll} {
		t.Run(string(mode), func(t *testing.T) {
			c := qt.New(t)
			conn := openNoTransactionValidationSQLite(c)
			c.Cleanup(func() { dbschema.CloseAndWarn(conn) })
			migration := migrator.CreateMigrationFromSQL(1, "unsafe commit",
				"CREATE TABLE users (id INTEGER PRIMARY KEY); COMMIT; INSERT INTO absent (id) VALUES (1);",
				"DROP TABLE users;")
			mig := migrator.NewMigrator(conn, migrator.NewRegisteredMigrationProvider(migration)).WithTransactionMode(mode)

			err := mig.MigrateUp(c.Context())
			c.Assert(err, qt.ErrorMatches, `migration 1 cannot run tx-mode `+string(mode)+` up statement 2 because it controls transaction state; .*`)
			c.Assert(noTransactionTableExists(c, conn, "users"), qt.IsFalse)
			c.Assert(noTransactionRevisionCount(c, conn), qt.Equals, int64(0))
		})
	}
}

func TestManagedTransactionControlRefusesDownBeforeChangingSchema(t *testing.T) {
	c := qt.New(t)
	conn := openNoTransactionValidationSQLite(c)
	defer dbschema.CloseAndWarn(conn)
	migration := migrator.CreateMigrationFromSQL(1, "unsafe rollback",
		"CREATE TABLE users (id INTEGER PRIMARY KEY);",
		"DROP TABLE users; COMMIT; INSERT INTO absent (id) VALUES (1);")
	mig := migrator.NewMigrator(conn, migrator.NewRegisteredMigrationProvider(migration))
	c.Assert(mig.MigrateUp(c.Context()), qt.IsNil)

	err := mig.MigrateDownTo(c.Context(), 0)
	c.Assert(err, qt.ErrorMatches, `migration 1 cannot run tx-mode file down statement 2 because it controls transaction state; .*`)
	c.Assert(noTransactionTableExists(c, conn, "users"), qt.IsTrue)
	c.Assert(noTransactionRevisionCount(c, conn), qt.Equals, int64(1))
	c.Assert(noTransactionRevisionState(c, conn), qt.Equals, "applied")
}

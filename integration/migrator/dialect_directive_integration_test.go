//go:build integration

package migrator_test

import (
	"context"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/dbschema"
	"go.5x5.cz/ptah/migration/migrator"
)

func TestSQLMigrationDirectiveBoundariesUseTargetDialect(t *testing.T) {
	c := qt.New(t)
	conn := openDirectiveBoundarySQLite(c)
	defer dbschema.CloseAndWarn(conn)

	upSQL := sqliteDirectiveBoundarySQL("up_probe")
	downSQL := sqliteDirectiveBoundarySQL("down_probe")
	migration := migrator.CreateMigrationFromSQL(1, "dialect directives", upSQL, downSQL)

	err := migration.Up(t.Context(), conn)
	c.Assert(err, qt.ErrorMatches,
		`invalid up migration directives: invalid \+ptah no_transaction value "maybe": expected true or false`)
	c.Assert(directiveBoundaryTableExists(c, conn, "up_probe"), qt.IsFalse)

	err = migration.Down(t.Context(), conn)
	c.Assert(err, qt.ErrorMatches,
		`invalid down migration directives: invalid \+ptah no_transaction value "maybe": expected true or false`)
	c.Assert(directiveBoundaryTableExists(c, conn, "down_probe"), qt.IsFalse)

	c.Assert(migration.UpForReplay(t.Context(), conn), qt.IsNil)
	c.Assert(directiveBoundaryTableExists(c, conn, "up_probe"), qt.IsTrue)
}

func TestMigratorResolvesTransactionDirectiveWithTargetDialect(t *testing.T) {
	c := qt.New(t)
	conn := openDirectiveBoundarySQLite(c)
	defer dbschema.CloseAndWarn(conn)

	migration := migrator.CreateMigrationFromSQL(
		1,
		"dialect directives",
		sqliteDirectiveBoundarySQL("migrator_probe"),
		"DROP TABLE migrator_probe;\n",
	)
	mig := migrator.NewMigrator(conn, migrator.NewRegisteredMigrationProvider(migration))

	err := mig.MigrateUp(t.Context())
	c.Assert(err, qt.ErrorMatches, `invalid \+ptah no_transaction value "maybe": expected true or false`)
	c.Assert(directiveBoundaryTableExists(c, conn, "migrator_probe"), qt.IsFalse)
}

func openDirectiveBoundarySQLite(c *qt.C) *dbschema.DatabaseConnection {
	c.Helper()
	conn, err := dbschema.ConnectToDatabase(context.Background(), "sqlite:///:memory:")
	c.Assert(err, qt.IsNil)
	return conn
}

func sqliteDirectiveBoundarySQL(table string) string {
	// SQLite closes the quote after the backslash and sees a real directive;
	// MySQL treats that quote as escaped and keeps the same bytes inside a
	// string. The SQL remains executable on SQLite when replay deliberately
	// ignores an unreadable transaction-mode directive.
	return "SELECT 'prefix \\'\n-- +ptah no_transaction=maybe\n;\nCREATE TABLE " + table + " (id integer);\n"
}

func directiveBoundaryTableExists(c *qt.C, conn *dbschema.DatabaseConnection, table string) bool {
	c.Helper()
	var count int
	err := conn.QueryRowContext(
		context.Background(),
		"SELECT count(*) FROM sqlite_master WHERE type = 'table' AND name = ?",
		table,
	).Scan(&count)
	c.Assert(err, qt.IsNil)
	return count == 1
}

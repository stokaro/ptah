//go:build integration

package migrator_test

import (
	"context"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/dbschema"
	"go.5x5.cz/ptah/migration/migrator"
)

// directiveBoundaryValueError is the refusal a malformed `-- +ptah` VALUE earns
// wherever the line sits.
//
// The trailing clause is the second half of the diagnosis and is asserted, not
// tolerated: position and value are independent facts about the line, and an
// operator told only "the value is nonsense" would fix it, re-run, and only
// then learn the line was below the statement it claims to govern. The
// `-- atlas:` spelling deliberately has no equivalent refusal -- measured on
// the pinned community binary, `-- atlas:txmode bogus` exits 1 in the header
// and 0 below the statement, so refusing it here would exit non-zero where the
// binary accepts.
const directiveBoundaryValueError = `invalid \+ptah no_transaction value "maybe": expected true or false ` +
	`\(on line 2, below the first SQL statement, where it would not have been honored\)`

func TestSQLMigrationDirectiveBoundariesUseTargetDialect(t *testing.T) {
	c := qt.New(t)
	conn := openDirectiveBoundarySQLite(c)
	defer dbschema.CloseAndWarn(conn)

	upSQL := sqliteDirectiveBoundarySQL("up_probe")
	downSQL := sqliteDirectiveBoundarySQL("down_probe")
	migration := migrator.CreateMigrationFromSQL(1, "dialect directives", upSQL, downSQL)

	err := migration.Up(t.Context(), conn)
	c.Assert(err, qt.ErrorMatches, `invalid up migration directives: `+directiveBoundaryValueError)
	c.Assert(directiveBoundaryTableExists(c, conn, "up_probe"), qt.IsFalse)

	err = migration.Down(t.Context(), conn)
	c.Assert(err, qt.ErrorMatches, `invalid down migration directives: `+directiveBoundaryValueError)
	c.Assert(directiveBoundaryTableExists(c, conn, "down_probe"), qt.IsFalse)

	c.Assert(migration.UpForReplay(t.Context(), conn), qt.IsNil)
	c.Assert(directiveBoundaryTableExists(c, conn, "up_probe"), qt.IsTrue)
}

// TestSQLMigrationDirectiveBoundaryIsDecidedByTheTargetDialect is the control
// that keeps the test above from passing for the wrong reason.
//
// The refusal is only correct because the SQLite lexer closes the string on the
// backslashed quote and sees a directive. Without a dialect the same bytes must
// stay SQL: the conservative scan keeps a marker only when every supported
// dialect agrees on it, and MySQL does not. If the refusal ever stopped being
// dialect-decided, this assertion is what notices -- the test above would keep
// passing.
func TestSQLMigrationDirectiveBoundaryIsDecidedByTheTargetDialect(t *testing.T) {
	c := qt.New(t)

	parsed, err := migrator.ParseMigrationUp("1_ambiguous.sql", sqliteDirectiveBoundarySQL("dialect_probe"))

	c.Assert(err, qt.IsNil)
	c.Assert(parsed.TxMode, qt.Equals, migrator.MigrationFileTxModeUnspecified)
}

// TestSQLMigrationWellFormedDirectiveBelowTheStatementIsNotHonored separates
// the two verdicts a misplaced directive can earn.
//
// A malformed value is refused; a WELL-FORMED directive in the same position is
// not, because refusing it would remove behavior this tree shipped. It is
// reported instead, and the migration runs with the global mode. Without this
// row the refusal above could be read as "anything below the statement fails",
// which is not the rule.
func TestSQLMigrationWellFormedDirectiveBelowTheStatementIsNotHonored(t *testing.T) {
	c := qt.New(t)
	conn := openDirectiveBoundarySQLite(c)
	defer dbschema.CloseAndWarn(conn)

	upSQL := "CREATE TABLE well_formed_probe (id integer);\n-- +ptah no_transaction\n"
	migration := migrator.CreateMigrationFromSQL(1, "well formed", upSQL, "DROP TABLE well_formed_probe;\n")

	c.Assert(migration.Up(t.Context(), conn), qt.IsNil)
	c.Assert(directiveBoundaryTableExists(c, conn, "well_formed_probe"), qt.IsTrue)
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
	c.Assert(err, qt.ErrorMatches, directiveBoundaryValueError)
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

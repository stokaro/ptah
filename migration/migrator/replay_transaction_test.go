package migrator_test

import (
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/dbschema"
	"ptah.run/migration/migrator"
)

// replayTransactionSQL creates a table, fills it, and then fails on a table
// that does not exist, so what the failure leaves behind says whether the
// statements before it ran in one transaction.
const replayTransactionSQL = `CREATE TABLE replay_tx (id INTEGER PRIMARY KEY);
INSERT INTO replay_tx (id) VALUES (1);
INSERT INTO replay_missing (id) VALUES (1);
`

func openReplayTransactionDatabase(c *qt.C) *dbschema.DatabaseConnection {
	c.Helper()
	conn, err := dbschema.ConnectToDatabase(c.Context(), "sqlite://"+filepath.Join(c.TempDir(), "replay.db"))
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { dbschema.CloseAndWarn(conn) })
	return conn
}

func replayTransactionTableExists(c *qt.C, conn *dbschema.DatabaseConnection) bool {
	c.Helper()
	var count int
	c.Assert(conn.QueryRowContext(c.Context(),
		"SELECT count(*) FROM sqlite_master WHERE type = 'table' AND name = 'replay_tx'",
	).Scan(&count), qt.IsNil)
	return count == 1
}

// TestUpForReplayRunsATransactionalMigrationInOneTransaction pins that replay
// runs a migration the way MigrateUp does. A failure rolls the whole migration
// back, so the table its first statement created is gone.
func TestUpForReplayRunsATransactionalMigrationInOneTransaction(t *testing.T) {
	c := qt.New(t)
	conn := openReplayTransactionDatabase(c)
	migration := migrator.CreateMigrationFromSQL(1, "replay transaction", replayTransactionSQL, "")

	err := migration.UpForReplay(c.Context(), conn)

	c.Assert(err, qt.ErrorMatches, `(?s).*no such table: replay_missing.*`)
	c.Assert(replayTransactionTableExists(c, conn), qt.IsFalse)
}

// TestUpForReplayRunsANoTransactionMigrationStatementByStatement is the
// control: a migration that opts out of the transaction runs statement by
// statement, as MigrateUp runs it, and keeps what ran before the failure.
func TestUpForReplayRunsANoTransactionMigrationStatementByStatement(t *testing.T) {
	c := qt.New(t)
	conn := openReplayTransactionDatabase(c)
	migration := migrator.CreateMigrationFromSQL(1, "replay transaction",
		"-- +ptah no_transaction\n"+replayTransactionSQL, "")

	err := migration.UpForReplay(c.Context(), conn)

	c.Assert(err, qt.ErrorMatches, `(?s).*no such table: replay_missing.*`)
	c.Assert(replayTransactionTableExists(c, conn), qt.IsTrue)
}

// TestUpForReplayCommitsASuccessfulMigration pins that the transaction is
// committed before UpForReplay returns, so a later read sees the rows.
func TestUpForReplayCommitsASuccessfulMigration(t *testing.T) {
	c := qt.New(t)
	conn := openReplayTransactionDatabase(c)
	migration := migrator.CreateMigrationFromSQL(1, "replay transaction",
		"CREATE TABLE replay_tx (id INTEGER PRIMARY KEY);\nINSERT INTO replay_tx (id) VALUES (1);\n", "")

	c.Assert(migration.UpForReplay(c.Context(), conn), qt.IsNil)

	var rows int
	c.Assert(conn.QueryRowContext(c.Context(), "SELECT count(*) FROM replay_tx").Scan(&rows), qt.IsNil)
	c.Assert(rows, qt.Equals, 1)
}

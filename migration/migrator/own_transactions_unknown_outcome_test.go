package migrator_test

import (
	"path/filepath"
	"testing"
	"testing/fstest"

	qt "github.com/frankban/quicktest"

	"ptah.run/dbschema"
	"ptah.run/migration/migrator"
)

// TestMigrateUpOwnTransactions_UnknownOutcome_FailurePath refuses to resume
// a migration of a directory written for a runner that wraps nothing, whose
// second statement has an unknown outcome, and ends the advice with the
// repair `ptah migrations status` prints for the same row. SQLite reads no
// transaction of such a body, so the refusal names the statement, and the
// repair carries no runner flag, as the status hint for the row carries none.
func TestMigrateUpOwnTransactions_UnknownOutcome_FailurePath(t *testing.T) {
	c := qt.New(t)
	conn, err := dbschema.ConnectToDatabase(c.Context(), "sqlite://"+filepath.Join(c.TempDir(), "own.db"))
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { c.Check(conn.Close(), qt.IsNil) })
	files := fstest.MapFS{
		"0000000001_users.up.sql":   {Data: []byte("CREATE TABLE users (id INTEGER PRIMARY KEY);\n")},
		"0000000001_users.down.sql": {Data: []byte("DROP TABLE users;\n")},
		"0000000002_pets.up.sql": {
			Data: []byte("CREATE TABLE pets (id INTEGER PRIMARY KEY);\nINSERT INTO missing VALUES (1);\n"),
		},
		"0000000002_pets.down.sql": {Data: []byte("DROP TABLE pets;\n")},
	}
	failing, err := migrator.NewFSMigrator(conn, files, migrator.WithOwnTransactions())
	c.Assert(err, qt.IsNil)
	c.Assert(failing.MigrateUp(c.Context()), qt.ErrorMatches, `(?s).*no such table: missing.*`)
	markStatementOutcomeUnknown(c, conn, 2)
	markAppliedStatements(c, conn, 2, 1)
	retry, err := migrator.NewFSMigrator(conn, files, migrator.WithOwnTransactions())
	c.Assert(err, qt.IsNil)

	err = retry.MigrateUpWithOptions(c.Context(), migrator.MigrateUpOptions{AllowDirty: true})

	c.Assert(err, qt.ErrorMatches, `(?s).*migration 2 cannot resume automatically: the outcome of statement 2 is `+
		`unknown after an interrupted run; inspect the database: if it took effect, nothing is left to run; if it `+
		`did not, run statements 2 to 2 by hand; then record the migration applied with `+
		`'ptah migrations repair --version 2 --force'$`)
}

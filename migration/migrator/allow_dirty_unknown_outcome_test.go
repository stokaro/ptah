package migrator_test

import (
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/dbschema"
	"ptah.run/migration/migrator"
)

// markAppliedStatements records that a dirty revision got as far as applied,
// which is the row an interruption leaves after some statements committed.
func markAppliedStatements(c *qt.C, conn *dbschema.DatabaseConnection, version int64, applied int) {
	c.Helper()
	_, err := conn.ExecContext(c.Context(),
		`UPDATE schema_migrations SET applied = ? WHERE version = ?`, applied, version)
	c.Assert(err, qt.IsNil)
}

// A run that died while its first statement was executing records applied=0 and
// the unknown-outcome failure. The zero says no checkpoint was written, not
// that nothing ran: on a dialect where each statement commits by itself the
// statement may have committed. So a retry must not restart at statement 1,
// which is the one statement whose outcome nothing knows
// (stokaro/ptah#3454).
func TestMigrateUpAllowDirty_FailurePath(t *testing.T) {
	t.Run("an interrupted first statement is not replayed", func(t *testing.T) {
		c := qt.New(t)
		dbPath := filepath.Join(t.TempDir(), "unknown-outcome.db")
		conn, failing := newDirtyRetryMigrator(
			c, dbPath, dirtyRetryFailingUp,
			migrator.MigrationTxModeFile, migrator.RevisionTableFormatPtah,
		)
		c.Assert(failing.MigrateUp(c.Context()), qt.IsNotNil)
		markStatementOutcomeUnknown(c, conn, 2)

		_, fixed := newDirtyRetryMigrator(
			c, dbPath, dirtyRetryFixedUp,
			migrator.MigrationTxModeFile, migrator.RevisionTableFormatPtah,
		)
		err := fixed.MigrateUpWithOptions(c.Context(), migrator.MigrateUpOptions{AllowDirty: true})

		c.Assert(err, qt.ErrorMatches,
			`(?s).*migration 2 cannot resume automatically: the outcome of statement 1 is unknown.*`)
		c.Assert(dirtyRetryTableExists(c, conn, "pets"), qt.IsFalse)
	})

	// The refusal reads the recorded failure, not the counter, so the row a
	// partially applied run leaves is refused the same way. This row already
	// was; it is here as the control that the reordering did not move the
	// rule off it.
	t.Run("an interrupted later statement is still refused", func(t *testing.T) {
		c := qt.New(t)
		dbPath := filepath.Join(t.TempDir(), "unknown-outcome-partial.db")
		conn, failing := newDirtyRetryMigrator(
			c, dbPath, dirtyRetryFailingUp,
			migrator.MigrationTxModeFile, migrator.RevisionTableFormatPtah,
		)
		c.Assert(failing.MigrateUp(c.Context()), qt.IsNotNil)
		markStatementOutcomeUnknown(c, conn, 2)
		markAppliedStatements(c, conn, 2, 1)

		_, fixed := newDirtyRetryMigrator(
			c, dbPath, dirtyRetryFixedUp,
			migrator.MigrationTxModeFile, migrator.RevisionTableFormatPtah,
		)
		err := fixed.MigrateUpWithOptions(c.Context(), migrator.MigrateUpOptions{AllowDirty: true})

		c.Assert(err, qt.ErrorMatches,
			`(?s).*migration 2 cannot resume automatically: the outcome of statement 2 is unknown.*`)
		c.Assert(dirtyRetryTableExists(c, conn, "pets"), qt.IsFalse)
	})
}

func TestMigrateUpAllowDirty_HappyPath(t *testing.T) {
	// The control the refusal must not swallow: a dirty row with an ordinary
	// recorded failure and no checkpoint is the one a retry is for, and it
	// still applies.
	t.Run("a zero-progress row with a known failure still retries", func(t *testing.T) {
		c := qt.New(t)
		dbPath := filepath.Join(t.TempDir(), "known-failure.db")
		_, failing := newDirtyRetryMigrator(
			c, dbPath, dirtyRetryFailingUp,
			migrator.MigrationTxModeFile, migrator.RevisionTableFormatPtah,
		)
		c.Assert(failing.MigrateUp(c.Context()), qt.IsNotNil)

		conn, fixed := newDirtyRetryMigrator(
			c, dbPath, dirtyRetryFixedUp,
			migrator.MigrationTxModeFile, migrator.RevisionTableFormatPtah,
		)
		err := fixed.MigrateUpWithOptions(c.Context(), migrator.MigrateUpOptions{AllowDirty: true})

		c.Assert(err, qt.IsNil)
		c.Assert(dirtyRetryTableExists(c, conn, "pets"), qt.IsTrue)
	})
}

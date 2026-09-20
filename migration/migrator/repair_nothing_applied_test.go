package migrator_test

import (
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/dbschema"
	"ptah.run/migration/migrator"
)

// A dirty revision that recorded no applied statement is a migration that did
// not reach the database: a transaction that rolled back, or a run that never
// got its lock. Recording it applied signs off SQL that never ran, and nothing
// would apply it afterwards, so the repair refuses and names the retry instead
// (stokaro/ptah#3452).

func TestRepairMigration_FailurePath(t *testing.T) {
	t.Run("the dirty revision recorded no applied statement", func(t *testing.T) {
		c := qt.New(t)
		dbPath := filepath.Join(t.TempDir(), "nothing-applied.db")
		conn, mig := newDirtyRetryMigrator(
			c, dbPath, dirtyRetryFailingUp,
			migrator.MigrationTxModeFile, migrator.RevisionTableFormatPtah,
		)
		c.Assert(mig.MigrateUp(c.Context()), qt.IsNotNil)

		err := mig.RepairMigration(c.Context(), migrator.RepairMigrationOptions{Version: 2})

		c.Assert(err, qt.ErrorMatches,
			`migration 2 recorded no applied statement, so there is nothing to finish.*`)
		var refusal *migrator.RepairNothingAppliedError
		c.Assert(err, qt.ErrorAs, &refusal)
		c.Assert(refusal.Version, qt.Equals, int64(2))
		revisions, revisionsErr := mig.GetRevisions(c.Context())
		c.Assert(revisionsErr, qt.IsNil)
		c.Assert(revisions[1].Dirty, qt.IsTrue)
		c.Assert(dirtyRetryTableExists(c, conn, "pets"), qt.IsFalse)
	})
}

// markStatementOutcomeUnknown rewrites the revision the failed run left into
// the row an interrupted statement leaves: applied is 0 there too, and only the
// recorded failure says the statement may have committed.
func markStatementOutcomeUnknown(c *qt.C, conn *dbschema.DatabaseConnection, version int64) {
	c.Helper()
	_, err := conn.ExecContext(c.Context(),
		`UPDATE schema_migrations
		 SET error = 'statement execution outcome is unknown after process interruption'
		 WHERE version = ?`,
		version)
	c.Assert(err, qt.IsNil)
}

func TestRepairMigration_HappyPath(t *testing.T) {
	// The row an interrupted statement leaves reads applied=0 as well, and the
	// refusal must not claim it: the statement may have committed, so a rerun
	// would repeat it. Repairing it by hand is what the operator is left with,
	// and that is the path this keeps open (stokaro/ptah#3452).
	t.Run("a repair still ends a revision whose statement outcome is unknown", func(t *testing.T) {
		c := qt.New(t)
		dbPath := filepath.Join(t.TempDir(), "unknown-outcome.db")
		conn, mig := newDirtyRetryMigrator(
			c, dbPath, dirtyRetryFailingUp,
			migrator.MigrationTxModeFile, migrator.RevisionTableFormatPtah,
		)
		c.Assert(mig.MigrateUp(c.Context()), qt.IsNotNil)
		markStatementOutcomeUnknown(c, conn, 2)

		err := mig.RepairMigration(c.Context(), migrator.RepairMigrationOptions{Version: 2})

		c.Assert(err, qt.IsNil)
		revisions, revisionsErr := mig.GetRevisions(c.Context())
		c.Assert(revisionsErr, qt.IsNil)
		c.Assert(revisions[1].Dirty, qt.IsFalse)
	})

	// --force is the operator saying they applied the migration themselves, so
	// it records what the refusal would not decide on its own.
	t.Run("force records a migration the operator applied by hand", func(t *testing.T) {
		c := qt.New(t)
		dbPath := filepath.Join(t.TempDir(), "nothing-applied-force.db")
		_, mig := newDirtyRetryMigrator(
			c, dbPath, dirtyRetryFailingUp,
			migrator.MigrationTxModeFile, migrator.RevisionTableFormatPtah,
		)
		c.Assert(mig.MigrateUp(c.Context()), qt.IsNotNil)

		err := mig.RepairMigration(c.Context(), migrator.RepairMigrationOptions{Version: 2, Force: true})

		c.Assert(err, qt.IsNil)
		revisions, revisionsErr := mig.GetRevisions(c.Context())
		c.Assert(revisionsErr, qt.IsNil)
		c.Assert(revisions[1].Dirty, qt.IsFalse)
	})

	// The control the refusal exists for: retrying is what that state wants,
	// and it leaves the table the failed run never created.
	t.Run("the retry the refusal names applies the migration", func(t *testing.T) {
		c := qt.New(t)
		dbPath := filepath.Join(t.TempDir(), "nothing-applied-retry.db")
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

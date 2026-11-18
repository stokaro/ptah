package migrator_test

import (
	"path/filepath"
	"testing"
	"testing/fstest"

	qt "github.com/frankban/quicktest"

	"ptah.run/dbschema"
	"ptah.run/migration/migrator"
)

// repeatableRowCount counts the rows of the default repeatable migration
// record at dbPath.
func repeatableRowCount(c *qt.C, dbPath string) int {
	c.Helper()
	conn, err := dbschema.ConnectToDatabase(c.Context(), "sqlite://"+dbPath)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)
	var count int
	c.Assert(conn.QueryRowContext(c.Context(), "SELECT COUNT(*) FROM schema_migrations_repeatable").Scan(&count), qt.IsNil)
	return count
}

// removeRepeatable runs one repair of identity against dbPath over fsys.
func removeRepeatable(
	c *qt.C,
	dbPath string,
	fsys fstest.MapFS,
	opts migrator.RemoveRepeatableRecordOptions,
) (*migrator.RepeatableRecord, error) {
	c.Helper()
	_, mig := openRepeatableMigrator(c, dbPath, fsys)
	return mig.RemoveRepeatableRecord(c.Context(), opts)
}

func TestRemoveRepeatableRecord_HappyPath(t *testing.T) {
	t.Run("the row of a deleted file is removed, and a rollback then proceeds", func(t *testing.T) {
		c := qt.New(t)
		dbPath := filepath.Join(c.TempDir(), "app.db")
		_, err := runRepeatableUp(c, dbPath, repeatableFiles(map[string]string{"R__user_names.sql": repeatableViewUp}),
			migrator.MigrateUpOptions{})
		c.Assert(err, qt.IsNil)
		deleted := repeatableFiles(nil)

		removed, err := removeRepeatable(c, dbPath, deleted, migrator.RemoveRepeatableRecordOptions{Identity: "user_names"})

		c.Assert(err, qt.IsNil)
		c.Assert(removed.Identity, qt.Equals, "user_names")
		c.Assert(removed.State, qt.Equals, migrator.RepeatableStateOrphaned)
		c.Assert(removed.Orphaned, qt.IsTrue)
		c.Assert(repeatableRowCount(c, dbPath), qt.Equals, 0)
		c.Assert(repeatableStatus(c, dbPath, deleted).Repeatables, qt.HasLen, 0)
		_, mig := openRepeatableMigrator(c, dbPath, deleted)
		c.Assert(mig.MigrateTo(c.Context(), 0), qt.IsNil)
		c.Assert(repeatableTableCount(c, dbPath, "users"), qt.Equals, 0)
	})

	t.Run("the row of a dirty record no file carries is removed, and up runs again", func(t *testing.T) {
		c := qt.New(t)
		dbPath := filepath.Join(c.TempDir(), "app.db")
		_, err := runRepeatableUp(c, dbPath, repeatableFiles(map[string]string{"R__user_names.sql": repeatableFailingUp}),
			migrator.MigrateUpOptions{})
		c.Assert(err, qt.ErrorAs, new(*migrator.RepeatableMigrationError))
		deleted := repeatableFiles(map[string]string{"R__other.sql": repeatableAuditUp})

		removed, err := removeRepeatable(c, dbPath, deleted, migrator.RemoveRepeatableRecordOptions{Identity: "user_names"})

		c.Assert(err, qt.IsNil)
		c.Assert(removed.State, qt.Equals, migrator.RepeatableStateFailed)
		c.Assert(removed.Orphaned, qt.IsTrue)
		_, err = runRepeatableUp(c, dbPath, deleted, migrator.MigrateUpOptions{})
		c.Assert(err, qt.IsNil)
		c.Assert(repeatableAudit(c, dbPath), qt.DeepEquals, []string{"a_first"})
	})

	t.Run("a forced removal of a row whose file is still here makes the next up run execute it again", func(t *testing.T) {
		c := qt.New(t)
		dbPath := filepath.Join(c.TempDir(), "app.db")
		fsys := repeatableFiles(map[string]string{"R__user_names.sql": repeatableViewUp})
		_, err := runRepeatableUp(c, dbPath, fsys, migrator.MigrateUpOptions{})
		c.Assert(err, qt.IsNil)

		removed, err := removeRepeatable(c, dbPath, fsys,
			migrator.RemoveRepeatableRecordOptions{Identity: "user_names", Force: true})

		c.Assert(err, qt.IsNil)
		c.Assert(removed.State, qt.Equals, migrator.RepeatableStateApplied)
		c.Assert(removed.Orphaned, qt.IsFalse)
		c.Assert(repeatableStatus(c, dbPath, fsys).Repeatables[0].State, qt.Equals, migrator.RepeatableStatePending)
		_, err = runRepeatableUp(c, dbPath, fsys, migrator.MigrateUpOptions{})
		c.Assert(err, qt.IsNil)
		c.Assert(repeatableAudit(c, dbPath), qt.DeepEquals, []string{"user_names", "user_names"})
	})
}

func TestRemoveRepeatableRecord_FailurePath(t *testing.T) {
	t.Run("a row whose file is still in the directory is kept without force", func(t *testing.T) {
		c := qt.New(t)
		dbPath := filepath.Join(c.TempDir(), "app.db")
		fsys := repeatableFiles(map[string]string{"R__user_names.sql": repeatableViewUp})
		_, err := runRepeatableUp(c, dbPath, fsys, migrator.MigrateUpOptions{})
		c.Assert(err, qt.IsNil)

		removed, err := removeRepeatable(c, dbPath, fsys, migrator.RemoveRepeatableRecordOptions{Identity: "user_names"})

		c.Assert(err, qt.ErrorIs, migrator.ErrRepeatableStillInDirectory)
		c.Assert(err, qt.ErrorMatches, `.*: R__user_names.sql carries user_names, .*rerun with --force to remove the row anyway`)
		c.Assert(removed, qt.IsNil)
		c.Assert(repeatableRowCount(c, dbPath), qt.Equals, 1)
	})

	t.Run("a migrator that does not load repeatable migrations keeps the row without force", func(t *testing.T) {
		c := qt.New(t)
		dbPath := filepath.Join(c.TempDir(), "app.db")
		_, err := runRepeatableUp(c, dbPath, repeatableFiles(map[string]string{"R__user_names.sql": repeatableViewUp}),
			migrator.MigrateUpOptions{})
		c.Assert(err, qt.IsNil)
		conn, err := dbschema.ConnectToDatabase(c.Context(), "sqlite://"+dbPath)
		c.Assert(err, qt.IsNil)
		c.Cleanup(func() { _ = conn.Close() })
		mig, err := migrator.NewFSMigrator(conn, repeatableFiles(nil))
		c.Assert(err, qt.IsNil)

		removed, err := mig.RemoveRepeatableRecord(c.Context(), migrator.RemoveRepeatableRecordOptions{Identity: "user_names"})

		c.Assert(err, qt.ErrorIs, migrator.ErrRepeatableStillInDirectory)
		c.Assert(removed, qt.IsNil)
		c.Assert(repeatableRowCount(c, dbPath), qt.Equals, 1)
	})

	t.Run("an identity the record holds no row for", func(t *testing.T) {
		c := qt.New(t)
		dbPath := filepath.Join(c.TempDir(), "app.db")
		fsys := repeatableFiles(map[string]string{"R__user_names.sql": repeatableViewUp})
		_, err := runRepeatableUp(c, dbPath, fsys, migrator.MigrateUpOptions{})
		c.Assert(err, qt.IsNil)

		removed, err := removeRepeatable(c, dbPath, fsys,
			migrator.RemoveRepeatableRecordOptions{Identity: "other", Force: true})

		c.Assert(err, qt.ErrorIs, migrator.ErrRepeatableNotRecorded)
		c.Assert(err, qt.ErrorMatches, `.*: "schema_migrations_repeatable" has no row for "other"`)
		c.Assert(removed, qt.IsNil)
		c.Assert(repeatableRowCount(c, dbPath), qt.Equals, 1)
	})

	t.Run("a database that never ran a repeatable migration has no row to remove", func(t *testing.T) {
		c := qt.New(t)
		dbPath := filepath.Join(c.TempDir(), "app.db")

		removed, err := removeRepeatable(c, dbPath, repeatableFiles(nil),
			migrator.RemoveRepeatableRecordOptions{Identity: "user_names", Force: true})

		c.Assert(err, qt.ErrorIs, migrator.ErrRepeatableNotRecorded)
		c.Assert(removed, qt.IsNil)
		c.Assert(repeatableTableCount(c, dbPath, "schema_migrations_repeatable"), qt.Equals, 0)
	})

	t.Run("an empty identity", func(t *testing.T) {
		c := qt.New(t)
		removed, err := removeRepeatable(c, filepath.Join(c.TempDir(), "app.db"), repeatableFiles(nil),
			migrator.RemoveRepeatableRecordOptions{Force: true})

		c.Assert(err, qt.ErrorMatches, "a repeatable migration identity is required")
		c.Assert(removed, qt.IsNil)
	})

	t.Run("an identity no repeatable file name could carry", func(t *testing.T) {
		c := qt.New(t)
		dbPath := filepath.Join(c.TempDir(), "app.db")

		removed, err := removeRepeatable(c, dbPath, repeatableFiles(nil),
			migrator.RemoveRepeatableRecordOptions{Identity: "user.names", Force: true})

		c.Assert(err, qt.ErrorMatches, `invalid repeatable migration identity "user.names": .*`)
		c.Assert(err, qt.Not(qt.ErrorIs), migrator.ErrRepeatableNotRecorded)
		c.Assert(removed, qt.IsNil)
		c.Assert(repeatableTableCount(c, dbPath, "schema_migrations_repeatable"), qt.Equals, 0)
	})

	t.Run("the Atlas-compatible revision format reads the record beside atlas_schema_revisions", func(t *testing.T) {
		c := qt.New(t)
		_, mig := openRepeatableMigrator(c, filepath.Join(c.TempDir(), "app.db"), repeatableFiles(nil))

		removed, err := mig.WithRevisionTableFormat(migrator.RevisionTableFormatAtlas).RemoveRepeatableRecord(
			c.Context(), migrator.RemoveRepeatableRecordOptions{Identity: "user_names", Force: true})

		c.Assert(err, qt.ErrorIs, migrator.ErrRepeatableNotRecorded)
		c.Assert(err, qt.ErrorMatches, `.*"atlas_schema_revisions_repeatable" has no row for "user_names"`)
		c.Assert(removed, qt.IsNil)
	})
}

package migrator_test

import (
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/dbschema"
	"ptah.run/migration/migrator"
)

// appliedRepeatableShape is what a history says of one repeatable migration,
// without the body, which the replay measures instead.
type appliedRepeatableShape struct {
	Identity, Path, Checksum string
}

func appliedRepeatableShapes(repeatables []migrator.AppliedRepeatable) []appliedRepeatableShape {
	shapes := make([]appliedRepeatableShape, 0, len(repeatables))
	for _, repeatable := range repeatables {
		shapes = append(shapes, appliedRepeatableShape{repeatable.Identity, repeatable.Path, repeatable.Checksum})
	}
	return shapes
}

// replayInto runs the replay of history on a new SQLite database and returns
// its path and the migrator that replayed it.
func replayInto(c *qt.C, history migrator.AppliedHistory) (string, *migrator.Migrator) {
	c.Helper()
	provider, err := history.ReplayProvider()
	c.Assert(err, qt.IsNil)
	shadowPath := filepath.Join(c.TempDir(), "shadow.db")
	conn, err := dbschema.ConnectToDatabase(c.Context(), "sqlite://"+shadowPath)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { _ = conn.Close() })
	mig := migrator.NewMigrator(conn, provider)
	c.Assert(mig.MigrateUp(c.Context()), qt.IsNil)
	return shadowPath, mig
}

func TestAppliedHistoryRepeatables_HappyPath(t *testing.T) {
	t.Run("an applied repeatable migration is replayed after the versioned history", func(t *testing.T) {
		c := qt.New(t)
		dbPath := filepath.Join(c.TempDir(), "app.db")
		fsys := repeatableFiles(map[string]string{
			"R__user_names.sql": repeatableViewUp,
			"R__a_first.sql":    repeatableAuditUp,
		})
		_, err := runRepeatableUp(c, dbPath, fsys, migrator.MigrateUpOptions{})
		c.Assert(err, qt.IsNil)
		_, mig := openRepeatableMigrator(c, dbPath, fsys)

		history, err := mig.AppliedHistory(c.Context())

		c.Assert(err, qt.IsNil)
		c.Assert(history.Versions(), qt.DeepEquals, []int64{1})
		c.Assert(appliedRepeatableShapes(history.Repeatables), qt.DeepEquals, []appliedRepeatableShape{
			{Identity: "a_first", Path: "R__a_first.sql", Checksum: h1(repeatableAuditUp)},
			{Identity: "user_names", Path: "R__user_names.sql", Checksum: h1(repeatableViewUp)},
		})
		shadowPath, shadow := replayInto(c, history)
		c.Assert(repeatableAudit(c, shadowPath), qt.DeepEquals, []string{"a_first", "user_names"})
		c.Assert(shadow.RepeatableTableIdentifier(), qt.Equals, `"schema_migrations_repeatable"`)
		c.Assert(repeatableTableCount(c, shadowPath, "schema_migrations_repeatable"), qt.Equals, 1)
	})

	t.Run("a repeatable migration the database never ran is not part of the history", func(t *testing.T) {
		c := qt.New(t)
		dbPath := filepath.Join(c.TempDir(), "app.db")
		_, err := runRepeatableUp(c, dbPath, repeatableFiles(nil), migrator.MigrateUpOptions{})
		c.Assert(err, qt.IsNil)
		_, mig := openRepeatableMigrator(c, dbPath, repeatableFiles(map[string]string{"R__user_names.sql": repeatableViewUp}))

		history, err := mig.AppliedHistory(c.Context())

		c.Assert(err, qt.IsNil)
		c.Assert(history.Versions(), qt.DeepEquals, []int64{1})
		c.Assert(history.Repeatables, qt.HasLen, 0)
	})

	t.Run("a provider that does not load repeatable migrations reads no record", func(t *testing.T) {
		c := qt.New(t)
		dbPath := filepath.Join(c.TempDir(), "app.db")
		fsys := repeatableFiles(map[string]string{"R__user_names.sql": repeatableViewUp})
		_, err := runRepeatableUp(c, dbPath, fsys, migrator.MigrateUpOptions{})
		c.Assert(err, qt.IsNil)
		conn, err := dbschema.ConnectToDatabase(c.Context(), "sqlite://"+dbPath)
		c.Assert(err, qt.IsNil)
		c.Cleanup(func() { _ = conn.Close() })
		mig, err := migrator.NewFSMigrator(conn, fsys)
		c.Assert(err, qt.IsNil)

		history, err := mig.AppliedHistory(c.Context())

		c.Assert(err, qt.IsNil)
		c.Assert(history.Repeatables, qt.HasLen, 0)
	})
}

func TestAppliedHistoryRepeatables_FailurePath(t *testing.T) {
	for _, test := range []struct {
		name string
		// applied is the directory the target ran; reader is the one the
		// history is read against.
		applied, reader map[string]string
		reason, message string
	}{
		{
			name:    "a repeatable migration whose file changed after it applied",
			applied: map[string]string{"R__user_names.sql": repeatableViewUp},
			reader:  map[string]string{"R__user_names.sql": repeatableViewChangedUp},
			reason:  migrator.UnreplayableChanged,
			message: `the applied history cannot be replayed: repeatable migration user_names: ` +
				`R__user_names\.sql changed after it applied, and the content the database ran is not in the directory`,
		},
		{
			name:    "an applied repeatable migration whose file is gone",
			applied: map[string]string{"R__user_names.sql": repeatableViewUp},
			reader:  nil,
			reason:  migrator.UnreplayableMissingFile,
			message: `the applied history cannot be replayed: repeatable migration user_names: ` +
				`the database records it as applied and the migration directory has no file for it`,
		},
		{
			name:    "a repeatable migration whose last attempt failed",
			applied: map[string]string{"R__user_names.sql": repeatableFailingUp},
			reader:  map[string]string{"R__user_names.sql": repeatableFailingUp},
			reason:  migrator.UnreplayableDirty,
			message: `the applied history cannot be replayed: repeatable migration user_names: ` +
				`its last attempt is failed, not cleanly applied`,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			dbPath := filepath.Join(c.TempDir(), "app.db")
			_, _ = runRepeatableUp(c, dbPath, repeatableFiles(test.applied), migrator.MigrateUpOptions{})
			_, mig := openRepeatableMigrator(c, dbPath, repeatableFiles(test.reader))

			history, err := mig.AppliedHistory(c.Context())

			c.Assert(history, qt.DeepEquals, migrator.AppliedHistory{})
			var unreplayable *migrator.UnreplayableHistoryError
			c.Assert(err, qt.ErrorAs, &unreplayable)
			c.Assert(unreplayable.Reason, qt.Equals, test.reason)
			c.Assert(unreplayable.Repeatable, qt.Equals, "user_names")
			c.Assert(unreplayable.Version, qt.Equals, int64(0))
			c.Assert(err, qt.ErrorMatches, test.message)
		})
	}

	t.Run("a replay refuses a repeatable migration with no body", func(t *testing.T) {
		c := qt.New(t)
		history := migrator.AppliedHistory{Repeatables: []migrator.AppliedRepeatable{{Identity: "user_names"}}}

		provider, err := history.ReplayProvider()

		c.Assert(err, qt.ErrorMatches, `repeatable migration user_names has no body to replay`)
		c.Assert(provider, qt.IsNil)
	})
}

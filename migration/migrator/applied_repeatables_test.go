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

// viewColumns lists the columns of the user_names view in the SQLite
// database at dbPath, which say which body of the repeatable migration ran.
func viewColumns(c *qt.C, dbPath string) []string {
	c.Helper()
	conn, err := dbschema.ConnectToDatabase(c.Context(), "sqlite://"+dbPath)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)
	rows, err := conn.QueryContext(c.Context(), "SELECT name FROM pragma_table_info('user_names') ORDER BY cid")
	c.Assert(err, qt.IsNil)
	defer rows.Close()
	columns := make([]string, 0)
	for rows.Next() {
		var column string
		c.Assert(rows.Scan(&column), qt.IsNil)
		columns = append(columns, column)
	}
	c.Assert(rows.Err(), qt.IsNil)
	return columns
}

// execOn runs each statement on the SQLite database at dbPath outside any
// migration.
func execOn(c *qt.C, dbPath string, statements ...string) {
	c.Helper()
	conn, err := dbschema.ConnectToDatabase(c.Context(), "sqlite://"+dbPath)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)
	for _, statement := range statements {
		_, err := conn.ExecContext(c.Context(), statement)
		c.Assert(err, qt.IsNil)
	}
}

// repeatableFailingNoTransactionUp fails after its first statement outside a
// transaction, so the database keeps part of it.
const repeatableFailingNoTransactionUp = "-- +ptah no_transaction\n" + repeatableFailingUp

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

// The record keeps the body of the content that applied, so a replay
// reproduces a repeatable migration whose file changed, is gone, or whose last
// attempt rolled back. The view's columns say which body the replay ran.
func TestAppliedHistoryRecordedRepeatables_HappyPath(t *testing.T) {
	for _, test := range []struct {
		name string
		// applied are the directories the target ran, in order; reader is the
		// one the history is read against.
		applied []map[string]string
		reader  map[string]string
		want    []appliedRepeatableShape
		columns []string
	}{
		{
			name:    "a file that changed after it applied replays the body that applied",
			applied: []map[string]string{{"R__user_names.sql": repeatableViewUp}},
			reader:  map[string]string{"R__user_names.sql": repeatableViewChangedUp},
			want:    []appliedRepeatableShape{{Identity: "user_names", Path: "R__user_names.sql", Checksum: h1(repeatableViewUp)}},
			columns: []string{"name"},
		},
		{
			name:    "a file that is gone replays the body that applied",
			applied: []map[string]string{{"R__user_names.sql": repeatableViewChangedUp}},
			reader:  nil,
			want:    []appliedRepeatableShape{{Identity: "user_names", Checksum: h1(repeatableViewChangedUp)}},
			columns: []string{"id", "name"},
		},
		{
			name: "an attempt that failed in a transaction replays the content that applied before it",
			applied: []map[string]string{
				{"R__user_names.sql": repeatableViewUp},
				{"R__user_names.sql": repeatableFailingUp},
			},
			reader:  map[string]string{"R__user_names.sql": repeatableFailingUp},
			want:    []appliedRepeatableShape{{Identity: "user_names", Path: "R__user_names.sql", Checksum: h1(repeatableViewUp)}},
			columns: []string{"name"},
		},
		{
			name:    "a first attempt that failed in a transaction leaves nothing to replay",
			applied: []map[string]string{{"R__user_names.sql": repeatableFailingUp}},
			reader:  map[string]string{"R__user_names.sql": repeatableFailingUp},
			want:    make([]appliedRepeatableShape, 0),
			columns: make([]string, 0),
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			dbPath := filepath.Join(c.TempDir(), "app.db")
			for _, directory := range test.applied {
				_, _ = runRepeatableUp(c, dbPath, repeatableFiles(directory), migrator.MigrateUpOptions{})
			}
			_, mig := openRepeatableMigrator(c, dbPath, repeatableFiles(test.reader))

			history, err := mig.AppliedHistory(c.Context())

			c.Assert(err, qt.IsNil)
			c.Assert(appliedRepeatableShapes(history.Repeatables), qt.DeepEquals, test.want)
			shadowPath, _ := replayInto(c, history)
			c.Assert(viewColumns(c, shadowPath), qt.DeepEquals, test.columns)
		})
	}

	// An orphaned record sorts among the files by identity, which is the
	// order the run that applied them executed them in.
	t.Run("a recorded body replays in identity order among the files", func(t *testing.T) {
		c := qt.New(t)
		dbPath := filepath.Join(c.TempDir(), "app.db")
		applied := map[string]string{
			"R__a_first.sql":    repeatableAuditUp,
			"R__b_second.sql":   "INSERT INTO repeatable_runs (name) VALUES ('b_second');\n",
			"R__user_names.sql": repeatableViewUp,
		}
		_, err := runRepeatableUp(c, dbPath, repeatableFiles(applied), migrator.MigrateUpOptions{})
		c.Assert(err, qt.IsNil)
		delete(applied, "R__b_second.sql")
		_, mig := openRepeatableMigrator(c, dbPath, repeatableFiles(applied))

		history, err := mig.AppliedHistory(c.Context())

		c.Assert(err, qt.IsNil)
		shadowPath, _ := replayInto(c, history)
		c.Assert(repeatableAudit(c, shadowPath), qt.DeepEquals, []string{"a_first", "b_second", "user_names"})
	})

	// A record created without the body column reads as one that keeps no
	// body, and the next run that executes repeatable work adds the column
	// and fills it.
	t.Run("a record created without the body column gets it from the next run", func(t *testing.T) {
		c := qt.New(t)
		dbPath := filepath.Join(c.TempDir(), "app.db")
		_, err := runRepeatableUp(c, dbPath, repeatableFiles(map[string]string{"R__user_names.sql": repeatableViewUp}),
			migrator.MigrateUpOptions{})
		c.Assert(err, qt.IsNil)
		execOn(c, dbPath, "ALTER TABLE schema_migrations_repeatable DROP COLUMN body")
		changed := repeatableFiles(map[string]string{"R__user_names.sql": repeatableViewChangedUp})
		c.Assert(repeatableStatus(c, dbPath, changed).Repeatables[0].State, qt.Equals, migrator.RepeatableStateChanged)

		_, err = runRepeatableUp(c, dbPath, changed, migrator.MigrateUpOptions{})
		c.Assert(err, qt.IsNil)
		_, mig := openRepeatableMigrator(c, dbPath, repeatableFiles(map[string]string{"R__user_names.sql": repeatableViewUp}))
		history, err := mig.AppliedHistory(c.Context())

		c.Assert(err, qt.IsNil)
		c.Assert(appliedRepeatableShapes(history.Repeatables), qt.DeepEquals, []appliedRepeatableShape{
			{Identity: "user_names", Path: "R__user_names.sql", Checksum: h1(repeatableViewChangedUp)},
		})
		shadowPath, _ := replayInto(c, history)
		c.Assert(viewColumns(c, shadowPath), qt.DeepEquals, []string{"id", "name"})
	})
}

func TestAppliedHistoryRepeatables_FailurePath(t *testing.T) {
	for _, test := range []struct {
		name string
		// applied is the directory the target ran; tamper runs on the target
		// afterwards; reader is the directory the history is read against.
		applied, reader map[string]string
		tamper          []string
		reason, message string
	}{
		{
			name:    "an attempt outside a transaction that failed",
			applied: map[string]string{"R__user_names.sql": repeatableFailingNoTransactionUp},
			reader:  map[string]string{"R__user_names.sql": repeatableFailingNoTransactionUp},
			reason:  migrator.UnreplayableDirty,
			message: `the applied history cannot be replayed: repeatable migration user_names: ` +
				`its last attempt is failed and ran outside a transaction, so what it left in the database is unknown`,
		},
		{
			name:    "a changed file whose record keeps no body",
			applied: map[string]string{"R__user_names.sql": repeatableViewUp},
			reader:  map[string]string{"R__user_names.sql": repeatableViewChangedUp},
			tamper:  []string{"UPDATE schema_migrations_repeatable SET body = NULL"},
			reason:  migrator.UnreplayableChanged,
			message: `the applied history cannot be replayed: repeatable migration user_names: ` +
				`R__user_names\.sql changed after it applied, and its record keeps no body for the content that applied`,
		},
		{
			name:    "a missing file whose recorded body does not hash to the applied digest",
			applied: map[string]string{"R__user_names.sql": repeatableViewUp},
			reader:  nil,
			tamper:  []string{"UPDATE schema_migrations_repeatable SET body = X'00'"},
			reason:  migrator.UnreplayableMissingFile,
			message: `the applied history cannot be replayed: repeatable migration user_names: ` +
				`the database records it as applied and the migration directory has no file for it, ` +
				`and the body its record keeps does not hash to the digest recorded as applied`,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			dbPath := filepath.Join(c.TempDir(), "app.db")
			_, _ = runRepeatableUp(c, dbPath, repeatableFiles(test.applied), migrator.MigrateUpOptions{})
			execOn(c, dbPath, test.tamper...)
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

package migrator_test

import (
	"path/filepath"
	"testing"
	"testing/fstest"

	qt "github.com/frankban/quicktest"

	"ptah.run/dbschema"
	"ptah.run/migration/migrationfile"
	"ptah.run/migration/migrator"
)

var appliedHistoryFixture = fstest.MapFS{
	"0000000001_users.up.sql":    {Data: []byte("CREATE TABLE users (id INTEGER PRIMARY KEY);")},
	"0000000001_users.down.sql":  {Data: []byte("DROP TABLE users;")},
	"0000000002_orders.up.sql":   {Data: []byte("CREATE TABLE orders (id INTEGER PRIMARY KEY);")},
	"0000000002_orders.down.sql": {Data: []byte("DROP TABLE orders;")},
	"0000000003_notes.up.sql":    {Data: []byte("CREATE TABLE notes (id INTEGER PRIMARY KEY);")},
	"0000000003_notes.down.sql":  {Data: []byte("DROP TABLE notes;")},
}

// appliedHistoryTarget migrates a new SQLite database with fsys to version
// and runs each statement in edits on it afterwards.
func appliedHistoryTarget(c *qt.C, fsys fstest.MapFS, version int64, edits ...string) *dbschema.DatabaseConnection {
	c.Helper()
	conn, err := dbschema.ConnectToDatabase(c.Context(), "sqlite://"+filepath.Join(c.TempDir(), "history.db"))
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { _ = conn.Close() })
	mig, err := migrator.NewFSMigrator(conn, fsys)
	c.Assert(err, qt.IsNil)
	c.Assert(mig.MigrateTo(c.Context(), version), qt.IsNil)
	for _, edit := range edits {
		_, err := conn.ExecContext(c.Context(), edit)
		c.Assert(err, qt.IsNil)
	}
	return conn
}

func TestAppliedHistoryHappyPath(t *testing.T) {
	for _, test := range []struct {
		name    string
		version int64
		want    []int64
	}{
		{name: "every migration applied", version: 3, want: []int64{1, 2, 3}},
		{name: "a pending migration left out", version: 2, want: []int64{1, 2}},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			conn := appliedHistoryTarget(c, appliedHistoryFixture, test.version)
			mig, err := migrator.NewFSMigrator(conn, appliedHistoryFixture)
			c.Assert(err, qt.IsNil)

			history, err := mig.AppliedHistory(t.Context())

			c.Assert(err, qt.IsNil)
			c.Assert(history.Versions(), qt.DeepEquals, test.want)
			c.Assert(history.Revisions, qt.HasLen, len(test.want))
		})
	}
}

func TestAppliedHistoryFailurePathRefusesAHistoryThatCannotBeReplayed(t *testing.T) {
	withoutOrders := fstest.MapFS{}
	for name, file := range appliedHistoryFixture {
		withoutOrders[name] = file
	}
	delete(withoutOrders, "0000000002_orders.up.sql")
	delete(withoutOrders, "0000000002_orders.down.sql")

	for _, test := range []struct {
		name string
		// edits run on the target after it migrated to version 2.
		edits []string
		// reader is the directory the history is read against.
		reader  fstest.MapFS
		options []migrator.FSProviderOption
		reason  string
		version int64
	}{
		{
			name:   "a failed revision",
			edits:  []string{"UPDATE schema_migrations SET state = 'failed' WHERE version = 2"},
			reader: appliedHistoryFixture, reason: migrator.UnreplayableDirty, version: 2,
		},
		{
			name:   "an applied revision whose file is gone",
			reader: withoutOrders, reason: migrator.UnreplayableMissingFile, version: 2,
		},
		{
			name: "a lower version applied after a higher one",
			edits: []string{
				"UPDATE schema_migrations SET applied_at = '2026-11-13 10:00:00' WHERE version = 2",
				"UPDATE schema_migrations SET applied_at = '2026-11-13 11:00:00' WHERE version = 1",
			},
			reader: appliedHistoryFixture, reason: migrator.UnreplayableOutOfOrder, version: 1,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			conn := appliedHistoryTarget(c, appliedHistoryFixture, 2, test.edits...)
			mig, err := migrator.NewFSMigrator(conn, test.reader, test.options...)
			c.Assert(err, qt.IsNil)

			history, err := mig.AppliedHistory(t.Context())

			c.Assert(history, qt.DeepEquals, migrator.AppliedHistory{})
			var unreplayable *migrator.UnreplayableHistoryError
			c.Assert(err, qt.ErrorAs, &unreplayable)
			c.Assert(unreplayable.Reason, qt.Equals, test.reason)
			c.Assert(unreplayable.Version, qt.Equals, test.version)
			c.Assert(err, qt.ErrorMatches, `the applied history cannot be replayed: version \d+: .+`)
		})
	}
}

// An applied repeatable records only its last run, so its history is not one
// a replay reproduces.
func TestAppliedHistoryFailurePathRefusesAnAppliedRepeatable(t *testing.T) {
	c := qt.New(t)
	atlasDir := fstest.MapFS{
		"0000000001_users.sql": {Data: []byte("CREATE TABLE users (id INTEGER PRIMARY KEY);\n")},
		"0000000002_view.sql":  {Data: []byte("CREATE VIEW user_ids AS SELECT id FROM users;\n")},
	}
	options := []migrator.FSProviderOption{
		migrator.WithMigrationDirFormat(migrationfile.DirFormatAtlas),
		migrator.WithAtlasRepeatableVersions([]int64{2}),
	}
	conn, err := dbschema.ConnectToDatabase(c.Context(), "sqlite://"+filepath.Join(c.TempDir(), "repeatable.db"))
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { _ = conn.Close() })
	mig, err := migrator.NewFSMigrator(conn, atlasDir, options...)
	c.Assert(err, qt.IsNil)
	mig = mig.WithRevisionTableFormat(migrator.RevisionTableFormatPtah)
	c.Assert(mig.MigrateUp(c.Context()), qt.IsNil)

	history, err := mig.AppliedHistory(c.Context())

	c.Assert(history, qt.DeepEquals, migrator.AppliedHistory{})
	var unreplayable *migrator.UnreplayableHistoryError
	c.Assert(err, qt.ErrorAs, &unreplayable)
	c.Assert(unreplayable.Reason, qt.Equals, migrator.UnreplayableRepeatable)
	c.Assert(unreplayable.Version, qt.Equals, int64(2))
}

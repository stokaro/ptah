package shadowdb_test

import (
	"testing"
	"testing/fstest"

	qt "github.com/frankban/quicktest"

	"ptah.run/dbschema"
	"ptah.run/internal/devclean/replay"
	"ptah.run/migration/internal/shadowdb"
	"ptah.run/migration/migrationfile"
	"ptah.run/migration/migrator"
)

// replayWraps are the two ways a replay into a claimed database builds its
// migrator, each of which installs the lease's replay guard.
var replayWraps = []struct {
	name string
	wrap func(*shadowdb.Lease, *migrator.Migrator) *migrator.Migrator
}{
	{name: "ReplayMigrator", wrap: shadowdb.ReplayMigrator},
	{name: "GuardedMigrator", wrap: shadowdb.GuardedMigrator},
}

// historyOn loads files as an Atlas directory.
func historyOn(c *qt.C, files fstest.MapFS) *migrator.FSMigrationProvider {
	c.Helper()
	provider, err := migrator.NewFSMigrationProvider(files, migrator.WithMigrationDirFormat(migrationfile.DirFormatAtlas))
	c.Assert(err, qt.IsNil)
	return provider
}

// A history whose second statement attaches another database file is refused
// before its first statement runs, by both wrappers: the table the first
// statement creates is not there, and the refusal is the replay guard's.
func TestGuardedMigrators_FailurePath(t *testing.T) {
	for _, test := range replayWraps {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			conn, lease := claimedSQLite(c)
			provider := historyOn(c, fstest.MapFS{
				"1_a.sql": {Data: []byte("CREATE TABLE a (id INTEGER PRIMARY KEY);\nATTACH DATABASE ':memory:' AS other;\n")},
			})

			err := test.wrap(lease, migrator.NewMigrator(conn, provider).WithAppliedHistoryReplay()).MigrateUp(c.Context())

			c.Assert(err, qt.ErrorMatches, `(?s).*sqlite migration replay rejects ATTACH because its effects cannot be confined to the disposable database realm.*`)
			c.Assert(err, qt.ErrorAs, new(*replay.Refusal))
			c.Assert(tableCount(c, conn, "a"), qt.Equals, 0)
		})
	}
}

// The control: a history the guard accepts replays through both wrappers.
func TestGuardedMigrators_HappyPath(t *testing.T) {
	for _, test := range replayWraps {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			conn, lease := claimedSQLite(c)
			provider := historyOn(c, fstest.MapFS{
				"1_a.sql": {Data: []byte("CREATE TABLE a (id INTEGER PRIMARY KEY);\nINSERT INTO a (id) VALUES (1);\n")},
			})

			err := test.wrap(lease, migrator.NewMigrator(conn, provider).WithAppliedHistoryReplay()).MigrateUp(c.Context())

			c.Assert(err, qt.IsNil)
			c.Assert(tableCount(c, conn, "a"), qt.Equals, 1)
		})
	}
}

// tableCount counts the tables named name in the SQLite database conn reaches.
func tableCount(c *qt.C, conn *dbschema.DatabaseConnection, name string) int {
	c.Helper()
	var count int
	c.Assert(conn.QueryRowContext(c.Context(), "SELECT count(*) FROM sqlite_master WHERE type = 'table' AND name = ?", name).Scan(&count), qt.IsNil)
	return count
}

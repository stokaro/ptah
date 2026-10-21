//go:build integration

package providercli_test

import (
	"path/filepath"
	"testing"
	"testing/fstest"

	qt "github.com/frankban/quicktest"

	"ptah.run/dbschema"
	"ptah.run/internal/atlasurl"
	"ptah.run/migration/migrationfile"
	"ptah.run/migration/migrator"
)

func providerMappedSetRunner(c *qt.C, conn *dbschema.DatabaseConnection) *migrator.Migrator {
	c.Helper()
	runner, err := migrator.NewFSMigrator(conn, fstest.MapFS{
		"10_repeat.sql": {Data: []byte("CREATE TABLE set_body_must_not_run (id INTEGER);\n")},
	}, migrator.WithMigrationDirFormat(migrationfile.DirFormatAtlas),
		migrator.WithAtlasRevisionVersions(map[int64]string{10: "R"}))
	c.Assert(err, qt.IsNil)
	return runner.WithMigrationsTable("main", "_revisions").WithMigrationLog(false).WithoutMigrationLock()
}

func TestNativeHistorySetReportsStoredNumericIdentityForMappedSource(t *testing.T) {
	c := qt.New(t)
	conn, err := dbschema.ConnectToDatabase(c.Context(), atlasurl.SQLiteURLFromPath(filepath.Join(c.TempDir(), "set.db")))
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { c.Check(conn.Close(), qt.IsNil) })
	runner := providerMappedSetRunner(c, conn)
	result, err := runner.SetRevision(c.Context(), 10)
	c.Assert(err, qt.IsNil)
	c.Assert(result.Set, qt.HasLen, 1)
	c.Assert(result.Set[0].RevisionVersion, qt.Equals, "10")
	result, err = runner.SetRevision(c.Context(), 10)
	c.Assert(err, qt.IsNil)
	c.Assert(result.Set, qt.HasLen, 0)
	c.Assert(result.Removed, qt.HasLen, 0)
}

func TestNativeHistorySetZeroRemovesNonpositiveStoredRevisions(t *testing.T) {
	c := qt.New(t)
	conn, err := dbschema.ConnectToDatabase(c.Context(), atlasurl.SQLiteURLFromPath(filepath.Join(c.TempDir(), "zero.db")))
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { c.Check(conn.Close(), qt.IsNil) })
	runner := providerMappedSetRunner(c, conn)
	c.Assert(runner.Initialize(c.Context()), qt.IsNil)
	for _, version := range []int64{-1, 0, 10} {
		_, err := conn.ExecContext(c.Context(), `INSERT INTO main._revisions
(version, description, applied_at, state, applied, total, execution_time_ms, checksum)
VALUES (?, 'manual', '2024-01-01T00:00:00Z', 'applied', 0, 0, 0, '')`, version)
		c.Assert(err, qt.IsNil)
	}
	result, err := runner.SetRevision(c.Context(), 0)
	c.Assert(err, qt.IsNil)
	c.Assert(result.Removed, qt.HasLen, 3)
	rows, err := runner.GetRevisions(c.Context())
	c.Assert(err, qt.IsNil)
	c.Assert(rows, qt.HasLen, 0)
}

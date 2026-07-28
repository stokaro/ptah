package migrator_test

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"path/filepath"
	"testing"
	"testing/fstest"

	qt "github.com/frankban/quicktest"

	"github.com/stokaro/ptah/dbschema"
	"github.com/stokaro/ptah/migration/migrator"
)

// The migrator treats atlas.sum hashes as opaque identities (content
// validation lives in internal/migratesum), so fixture values stand in for
// real h1 hashes.
const (
	atlasSumFixtureVersion  = int64(20240101120000)
	atlasSumFixtureFileHash = "h1:widgetsfixturehash="
)

// newSQLiteAtlasSumMigrator builds a migrator over an Atlas-format directory
// that carries an atlas.sum file, using the default ptah revision table
// format on an ephemeral SQLite database.
func newSQLiteAtlasSumMigrator(t *testing.T) (*dbschema.DatabaseConnection, *migrator.Migrator) {
	t.Helper()
	ctx := context.Background()
	conn, err := dbschema.ConnectToDatabase(ctx, "sqlite://"+filepath.Join(t.TempDir(), "atlas-sum.db"))
	qt.Assert(t, err, qt.IsNil)
	t.Cleanup(func() { _ = conn.Close() })

	fsys := fstest.MapFS{
		"atlas.sum": &fstest.MapFile{Data: []byte(
			"h1:directoryfixturehash=\n" +
				"20240101120000_widgets.sql " + atlasSumFixtureFileHash + "\n",
		)},
		"20240101120000_widgets.sql": &fstest.MapFile{Data: []byte(`-- atlas:txtar

-- migration.sql --
CREATE TABLE atlas_sum_widgets (id INTEGER PRIMARY KEY);

-- down.sql --
DROP TABLE atlas_sum_widgets;
`)},
	}
	m, err := migrator.NewFSMigrator(conn, fsys, migrator.WithMigrationDirFormat(migrator.MigrationDirFormatAtlas))
	qt.Assert(t, err, qt.IsNil)
	return conn, m
}

func storedRevisionChecksum(t *testing.T, conn *dbschema.DatabaseConnection, version int64) string {
	t.Helper()
	var checksum string
	err := conn.QueryRow("SELECT checksum FROM schema_migrations WHERE version = ?", version).Scan(&checksum)
	qt.Assert(t, err, qt.IsNil)
	return checksum
}

func TestAtlasSumPtahRevisions_UpThenDownRoundTrip(t *testing.T) {
	c := qt.New(t)
	ctx := context.Background()
	conn, m := newSQLiteAtlasSumMigrator(t)

	c.Assert(m.MigrateUp(ctx), qt.IsNil)

	// The stored revision checksum must be the same atlas.sum identity the
	// verification side computes: the h1 hash without its prefix.
	c.Assert(storedRevisionChecksum(t, conn, atlasSumFixtureVersion), qt.Equals, "widgetsfixturehash=")

	// Re-entering up with nothing pending verifies applied checksums.
	c.Assert(m.MigrateUp(ctx), qt.IsNil)

	// Down also verifies applied checksums before rolling back.
	c.Assert(m.MigrateDownTo(ctx, 0), qt.IsNil)

	version, err := m.GetCurrentVersion(ctx)
	c.Assert(err, qt.IsNil)
	c.Assert(version, qt.Equals, int64(0))

	var count int
	err = conn.QueryRow("SELECT count(*) FROM sqlite_master WHERE type='table' AND name='atlas_sum_widgets'").Scan(&count)
	c.Assert(err, qt.IsNil)
	c.Assert(count, qt.Equals, 0)
}

func TestAtlasSumPtahRevisions_LegacyHexChecksumRowStillVerifies(t *testing.T) {
	c := qt.New(t)
	ctx := context.Background()
	conn, m := newSQLiteAtlasSumMigrator(t)

	c.Assert(m.MigrateUp(ctx), qt.IsNil)

	// Databases migrated before the fix stored the hex SHA-256 of the up SQL
	// instead of the atlas.sum hash. Rewrite the row to that legacy value.
	migrations := m.MigrationProvider().Migrations()
	c.Assert(migrations, qt.HasLen, 1)
	legacySum := sha256.Sum256([]byte(migrations[0].UpSQL))
	legacyChecksum := hex.EncodeToString(legacySum[:])
	_, err := conn.Exec("UPDATE schema_migrations SET checksum = ? WHERE version = ?", legacyChecksum, atlasSumFixtureVersion)
	c.Assert(err, qt.IsNil)

	// Verification must keep accepting the legacy encoding.
	c.Assert(m.MigrateUp(ctx), qt.IsNil)
	c.Assert(m.MigrateDownTo(ctx, 0), qt.IsNil)

	version, err := m.GetCurrentVersion(ctx)
	c.Assert(err, qt.IsNil)
	c.Assert(version, qt.Equals, int64(0))
}

func TestAtlasSumPtahRevisions_TamperedChecksumStillFails(t *testing.T) {
	c := qt.New(t)
	ctx := context.Background()
	conn, m := newSQLiteAtlasSumMigrator(t)

	c.Assert(m.MigrateUp(ctx), qt.IsNil)

	_, err := conn.Exec("UPDATE schema_migrations SET checksum = ? WHERE version = ?", "deadbeef", atlasSumFixtureVersion)
	c.Assert(err, qt.IsNil)

	var mismatch *migrator.ChecksumMismatchError

	err = m.MigrateUp(ctx)
	c.Assert(err, qt.ErrorAs, &mismatch)
	c.Assert(mismatch.Version, qt.Equals, atlasSumFixtureVersion)
	c.Assert(mismatch.Stored, qt.Equals, "deadbeef")
	c.Assert(mismatch.Computed, qt.Equals, "widgetsfixturehash=")

	err = m.MigrateDownTo(ctx, 0)
	c.Assert(err, qt.ErrorAs, &mismatch)
	c.Assert(mismatch.Version, qt.Equals, atlasSumFixtureVersion)
}

func TestPtahDirPtahRevisions_ChecksumBehaviorUnchanged(t *testing.T) {
	c := qt.New(t)
	ctx := context.Background()
	conn, err := dbschema.ConnectToDatabase(ctx, "sqlite://"+filepath.Join(t.TempDir(), "ptah-dir.db"))
	c.Assert(err, qt.IsNil)
	t.Cleanup(func() { _ = conn.Close() })

	fsys := fstest.MapFS{
		"000001_create_widgets.up.sql": &fstest.MapFile{
			Data: []byte("CREATE TABLE ptah_dir_widgets (id INTEGER PRIMARY KEY);"),
		},
		"000001_create_widgets.down.sql": &fstest.MapFile{
			Data: []byte("DROP TABLE ptah_dir_widgets;"),
		},
	}
	m, err := migrator.NewFSMigrator(conn, fsys)
	c.Assert(err, qt.IsNil)

	c.Assert(m.MigrateUp(ctx), qt.IsNil)

	// Without atlas.sum the stored checksum stays the hex SHA-256 of the up
	// SQL, exactly as before the atlas.sum fix.
	migrations := m.MigrationProvider().Migrations()
	c.Assert(migrations, qt.HasLen, 1)
	sum := sha256.Sum256([]byte(migrations[0].UpSQL))
	c.Assert(storedRevisionChecksum(t, conn, 1), qt.Equals, hex.EncodeToString(sum[:]))

	c.Assert(m.MigrateUp(ctx), qt.IsNil)
	c.Assert(m.MigrateDownTo(ctx, 0), qt.IsNil)

	version, err := m.GetCurrentVersion(ctx)
	c.Assert(err, qt.IsNil)
	c.Assert(version, qt.Equals, int64(0))
}

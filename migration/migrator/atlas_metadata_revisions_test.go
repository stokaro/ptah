package migrator_test

import (
	"context"
	"path/filepath"
	"testing"
	"testing/fstest"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/dbschema"
	"go.5x5.cz/ptah/migration/migrator"
)

// Atlas's `migrate down` inserts a `.atlas_cloud_identifier` metadata row
// into atlas_schema_revisions even in purely local mode (measured). Revision
// readers must skip dot-prefixed
// versions in version math and preserve the row untouched on writes (#957).

const dotRowVersion = ".atlas_cloud_identifier"

// insertAtlasMetadataDotRow inserts the measured Atlas metadata row shape:
// description carries a UUID, applied=0, total=0, an empty hash, error/error_stmt and
// partial_hashes are NULL.
func insertAtlasMetadataDotRow(t *testing.T, conn *dbschema.DatabaseConnection) {
	c := qt.New(t)
	t.Helper()
	_, err := conn.Exec(
		`INSERT INTO atlas_schema_revisions
(version, description, type, applied, total, executed_at, execution_time, error, error_stmt, hash, partial_hashes, operator_version)
VALUES (?, '472fecf4-5a9c-431f-8ff1-8e1facd1d50b', 2, 0, 0, '2026-08-01 12:04:21.291103+02:00', 0, NULL, NULL, '', NULL, 'Atlas')`,
		dotRowVersion,
	)
	c.Assert(err, qt.IsNil)
}

// dotRowLiteral returns the metadata row as one sqlite quote()-rendered tuple,
// so survival comparisons are byte-precise, including NULL versus empty-string
// distinctions.
func dotRowLiteral(t *testing.T, conn *dbschema.DatabaseConnection) string {
	c := qt.New(t)
	t.Helper()
	var literal string
	err := conn.QueryRow(
		`SELECT quote(version) || '|' || quote(description) || '|' || quote(type) || '|' ||
quote(applied) || '|' || quote(total) || '|' || quote(executed_at) || '|' ||
quote(execution_time) || '|' || quote(error) || '|' || quote(error_stmt) || '|' ||
quote(hash) || '|' || quote(partial_hashes) || '|' || quote(operator_version)
FROM atlas_schema_revisions WHERE version = ?`,
		dotRowVersion,
	).Scan(&literal)
	c.Assert(err, qt.IsNil)
	return literal
}

func newSQLiteAtlasFormatMigrator(t *testing.T) (*dbschema.DatabaseConnection, *migrator.Migrator) {
	c := qt.New(t)
	t.Helper()
	ctx := context.Background()
	conn, err := dbschema.ConnectToDatabase(ctx, "sqlite://"+filepath.Join(t.TempDir(), "dot-rows.db"))
	c.Assert(err, qt.IsNil)
	t.Cleanup(func() { _ = conn.Close() })

	m, err := migrator.NewFSMigrator(
		conn,
		fstest.MapFS{
			"1_create_users.sql": &fstest.MapFile{Data: []byte("CREATE TABLE users (id INTEGER PRIMARY KEY);\n")},
			"2_create_posts.sql": &fstest.MapFile{Data: []byte(`-- atlas:txtar

-- migration.sql --
CREATE TABLE posts (id INTEGER PRIMARY KEY);

-- down.sql --
DROP TABLE posts;
`)},
		},
		migrator.WithMigrationDirFormat(migrator.MigrationDirFormatAtlas),
	)
	c.Assert(err, qt.IsNil)
	m = m.WithRevisionTableFormat(migrator.RevisionTableFormatAtlas)
	return conn, m
}

func TestAtlasMetadataRow_StatusSkipsDotRow(t *testing.T) {
	c := qt.New(t)
	ctx := context.Background()
	conn, m := newSQLiteAtlasFormatMigrator(t)
	c.Assert(m.MigrateUp(ctx), qt.IsNil)
	insertAtlasMetadataDotRow(t, conn)
	before := dotRowLiteral(t, conn)

	snapshot, err := m.GetMigrationStatusSnapshot(ctx)

	c.Assert(err, qt.IsNil)
	c.Assert(snapshot.Status.CurrentVersion, qt.Equals, int64(2))
	c.Assert(snapshot.Status.AppliedMigrations, qt.DeepEquals, []int64{1, 2})
	c.Assert(snapshot.Status.PendingMigrations, qt.HasLen, 0)
	c.Assert(snapshot.Status.DirtyRevision, qt.IsNil)
	c.Assert(snapshot.Revisions, qt.HasLen, 2) // the metadata row is not a revision

	version, err := m.GetCurrentVersion(ctx)
	c.Assert(err, qt.IsNil)
	c.Assert(version, qt.Equals, int64(2))
	c.Assert(dotRowLiteral(t, conn), qt.Equals, before)
}

func TestAtlasMetadataRow_ApplyProceedsAndPreservesDotRow(t *testing.T) {
	c := qt.New(t)
	ctx := context.Background()
	conn, m := newSQLiteAtlasFormatMigrator(t)
	c.Assert(m.MigrateUpWithOptions(ctx, migrator.MigrateUpOptions{TargetVersion: 1}), qt.IsNil)
	insertAtlasMetadataDotRow(t, conn)
	before := dotRowLiteral(t, conn)

	c.Assert(m.MigrateUp(ctx), qt.IsNil)

	version, err := m.GetCurrentVersion(ctx)
	c.Assert(err, qt.IsNil)
	c.Assert(version, qt.Equals, int64(2))
	c.Assert(dotRowLiteral(t, conn), qt.Equals, before)
}

func TestAtlasMetadataRow_DownWorksAndPreservesDotRow(t *testing.T) {
	c := qt.New(t)
	ctx := context.Background()
	conn, m := newSQLiteAtlasFormatMigrator(t)
	c.Assert(m.MigrateUp(ctx), qt.IsNil)
	insertAtlasMetadataDotRow(t, conn)
	before := dotRowLiteral(t, conn)

	c.Assert(m.MigrateDownTo(ctx, 1), qt.IsNil)

	version, err := m.GetCurrentVersion(ctx)
	c.Assert(err, qt.IsNil)
	c.Assert(version, qt.Equals, int64(1))
	c.Assert(dotRowLiteral(t, conn), qt.Equals, before)
}

func TestAtlasMetadataRow_SetRevisionPreservesDotRow(t *testing.T) {
	c := qt.New(t)
	ctx := context.Background()
	conn, m := newSQLiteAtlasFormatMigrator(t)
	c.Assert(m.MigrateUp(ctx), qt.IsNil)
	insertAtlasMetadataDotRow(t, conn)
	before := dotRowLiteral(t, conn)

	result, err := m.SetAtlasRevision(ctx, 1)

	c.Assert(err, qt.IsNil)
	c.Assert(result.CurrentVersion, qt.Equals, int64(1))
	version, err := m.GetCurrentVersion(ctx)
	c.Assert(err, qt.IsNil)
	c.Assert(version, qt.Equals, int64(1))
	// The rows-above delete removed revision 2 but never the metadata row.
	c.Assert(dotRowLiteral(t, conn), qt.Equals, before)
}

func TestAtlasMetadataRow_DryRunReadsRealVersion(t *testing.T) {
	c := qt.New(t)
	ctx := context.Background()
	conn, m := newSQLiteAtlasFormatMigrator(t)
	c.Assert(m.MigrateUp(ctx), qt.IsNil)
	insertAtlasMetadataDotRow(t, conn)

	// Regression (#957): `migrate down --dry-run` misread an existing Atlas
	// revision table as version 0 because dry-run read paths skipped the
	// metadata query entirely.
	conn.SchemaWriter().SetDryRun(true)
	t.Cleanup(func() { conn.SchemaWriter().SetDryRun(false) })

	version, err := m.GetCurrentVersion(ctx)
	c.Assert(err, qt.IsNil)
	c.Assert(version, qt.Equals, int64(2))

	status, err := m.GetMigrationStatus(ctx)
	c.Assert(err, qt.IsNil)
	c.Assert(status.CurrentVersion, qt.Equals, int64(2))
	c.Assert(status.PendingMigrations, qt.HasLen, 0)
}

func TestAtlasMetadataRow_DryRunOnFreshDatabaseReportsZero(t *testing.T) {
	c := qt.New(t)
	ctx := context.Background()
	conn, m := newSQLiteAtlasFormatMigrator(t)

	// No Initialize ran for real: the metadata table does not exist, and the
	// dry run must still report the empty state without erroring.
	conn.SchemaWriter().SetDryRun(true)
	t.Cleanup(func() { conn.SchemaWriter().SetDryRun(false) })

	version, err := m.GetCurrentVersion(ctx)
	c.Assert(err, qt.IsNil)
	c.Assert(version, qt.Equals, int64(0))

	status, err := m.GetMigrationStatus(ctx)
	c.Assert(err, qt.IsNil)
	c.Assert(status.CurrentVersion, qt.Equals, int64(0))
	c.Assert(status.PendingMigrations, qt.DeepEquals, []int64{1, 2})
}

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

// Atlas-format revisions keep the upstream table shape, but Ptah does not copy
// the upstream behavior that hides a failed rollback behind an applied row.
// Rollback direction is encoded in operator_version so the existing schema can
// expose the failure to status and route repair to the down body.

const failingDownTxtar = `-- atlas:txtar

-- migration.sql --
CREATE TABLE child (id INTEGER PRIMARY KEY);

-- down.sql --
DROP TABLE child;
THIS IS A FAILING STATEMENT;
`

const succeedingDownTxtar = `-- atlas:txtar

-- migration.sql --
CREATE TABLE child (id INTEGER PRIMARY KEY);

-- down.sql --
DROP TABLE child;
`

const succeedingNoTransactionDownTxtar = `-- atlas:txtar

-- migration.sql --
CREATE TABLE child (id INTEGER PRIMARY KEY);

-- down.sql --
-- atlas:txmode none

DROP TABLE child;
`

// failingNoTransactionDownTxtar opts the down body out of the migration
// transaction, so a mid-body failure leaves the completed statements applied.
const failingNoTransactionDownTxtar = `-- atlas:txtar

-- migration.sql --
CREATE TABLE child1 (id INTEGER PRIMARY KEY);
CREATE TABLE child2 (id INTEGER PRIMARY KEY);

-- down.sql --
-- atlas:txmode none

DROP TABLE child2;
THIS IS A FAILING STATEMENT;
`

func newAtlasDownMigrator(t *testing.T, secondMigration string) (*dbschema.DatabaseConnection, *migrator.Migrator) {
	t.Helper()
	c := qt.New(t)
	ctx := context.Background()
	conn, err := dbschema.ConnectToDatabase(ctx, "sqlite://"+filepath.Join(t.TempDir(), "down.db"))
	c.Assert(err, qt.IsNil)
	t.Cleanup(func() { _ = conn.Close() })

	m, err := migrator.NewFSMigrator(
		conn,
		fstest.MapFS{
			"1_create_parent.sql": &fstest.MapFile{Data: []byte("CREATE TABLE parent (id INTEGER PRIMARY KEY);\n")},
			"2_create_child.sql":  &fstest.MapFile{Data: []byte(secondMigration)},
		},
		migrator.WithMigrationDirFormat(migrator.MigrationDirFormatAtlas),
	)
	c.Assert(err, qt.IsNil)
	m = m.WithRevisionTableFormat(migrator.RevisionTableFormatAtlas)
	c.Assert(m.MigrateUp(ctx), qt.IsNil)
	return conn, m
}

// atlasRevisionTuples renders every Atlas revision row as a quote()-based tuple
// so a comparison is byte-precise, including NULL versus the empty string and
// unchanged execution_time values.
func atlasRevisionTuples(t *testing.T, conn *dbschema.DatabaseConnection) []string {
	t.Helper()
	c := qt.New(t)
	rows, err := conn.Query(
		`SELECT quote(version) || '|' || quote(description) || '|' || quote(type) || '|' ||
quote(applied) || '|' || quote(total) || '|' || quote(executed_at) || '|' ||
quote(execution_time) || '|' || quote(error) || '|' || quote(error_stmt) || '|' ||
quote(hash) || '|' || quote(partial_hashes) || '|' || quote(operator_version)
FROM atlas_schema_revisions ORDER BY version`)
	c.Assert(err, qt.IsNil)
	defer func() { _ = rows.Close() }()
	var tuples []string
	for rows.Next() {
		var tuple string
		c.Assert(rows.Scan(&tuple), qt.IsNil)
		tuples = append(tuples, tuple)
	}
	c.Assert(rows.Err(), qt.IsNil)
	return tuples
}

func TestMigrateDown_AtlasFormatFailedDownRecordsDirection(t *testing.T) {
	c := qt.New(t)
	ctx := context.Background()
	conn, m := newAtlasDownMigrator(t, failingDownTxtar)
	c.Assert(atlasRevisionTuples(t, conn), qt.HasLen, 2)

	err := m.MigrateDownTo(ctx, 1)

	c.Assert(err, qt.IsNotNil)
	c.Assert(sqliteTableExists(t, conn, "child"), qt.IsTrue)
	status, err := m.GetMigrationStatus(ctx)
	c.Assert(err, qt.IsNil)
	c.Assert(status.CurrentVersion, qt.Equals, int64(2))
	c.Assert(status.DirtyRevision, qt.IsNotNil)
	c.Assert(status.DirtyRevision.Direction, qt.Equals, migrator.MigrationDirectionDown)
	c.Assert(status.DirtyRevision.Applied, qt.Equals, 0)
	c.Assert(status.DirtyRevision.Total, qt.Equals, 2)
	c.Assert(status.DirtyRevision.OperatorVersion, qt.Equals, "Ptah/down")
}

// TestMigrateDown_AtlasFormatFailedNoTransactionDownLeavesPartialSchema covers
// the non-transactional down path, where "the body is rolled back" does not
// hold: `-- atlas:txmode none` runs the down outside a transaction, so the
// statements that completed stay applied. The Atlas-shaped table now records
// that progress instead of reporting the half-reverted schema as clean.
func TestMigrateDown_AtlasFormatFailedNoTransactionDownRecordsPartialSchema(t *testing.T) {
	c := qt.New(t)
	ctx := context.Background()
	conn, m := newAtlasDownMigrator(t, failingNoTransactionDownTxtar)
	c.Assert(atlasRevisionTuples(t, conn), qt.HasLen, 2)

	err := m.MigrateDownTo(ctx, 1)

	c.Assert(err, qt.IsNotNil)
	// The first down statement completed and was not rolled back.
	c.Assert(sqliteTableExists(t, conn, "child2"), qt.IsFalse)
	c.Assert(sqliteTableExists(t, conn, "child1"), qt.IsTrue)
	status, err := m.GetMigrationStatus(ctx)
	c.Assert(err, qt.IsNil)
	c.Assert(status.CurrentVersion, qt.Equals, int64(2))
	c.Assert(status.DirtyRevision, qt.IsNotNil)
	c.Assert(status.DirtyRevision.Direction, qt.Equals, migrator.MigrationDirectionDown)
	c.Assert(status.DirtyRevision.Applied, qt.Equals, 1)
	c.Assert(status.DirtyRevision.Total, qt.Equals, 2)
	c.Assert(status.DirtyRevision.OperatorVersion, qt.Equals, "Ptah/down")

	err = m.MigrateUpWithOptions(ctx, migrator.MigrateUpOptions{AllowDirty: true})
	c.Assert(err, qt.ErrorMatches, `migration 2 is dirty from an interrupted rollback; repair the rollback before migrating up`)

	_, err = conn.ExecContext(ctx, `UPDATE atlas_schema_revisions
SET applied = total, error = ''
WHERE version = '2'`)
	c.Assert(err, qt.IsNil)
	status, err = m.GetMigrationStatus(ctx)
	c.Assert(err, qt.IsNil)
	c.Assert(status.DirtyRevision, qt.IsNotNil)
	c.Assert(status.DirtyRevision.Direction, qt.Equals, migrator.MigrationDirectionDown)
	c.Assert(m.MigrateUp(ctx), qt.ErrorMatches, `migration 2 is dirty.*`)
	err = m.MigrateUpWithOptions(ctx, migrator.MigrateUpOptions{AllowDirty: true})
	c.Assert(err, qt.ErrorMatches, `migration 2 is dirty from an interrupted rollback; repair the rollback before migrating up`)
}

func TestRepairMigration_AtlasCompletedDownFinalizesAfterMetadataFailure(t *testing.T) {
	c := qt.New(t)
	ctx := context.Background()
	conn, m := newAtlasDownMigrator(t, succeedingNoTransactionDownTxtar)
	_, err := conn.ExecContext(ctx, `CREATE TRIGGER block_revision_delete
BEFORE DELETE ON atlas_schema_revisions
WHEN OLD.version = '2'
BEGIN
    SELECT RAISE(FAIL, 'blocked revision delete');
END`)
	c.Assert(err, qt.IsNil)

	err = m.MigrateDownTo(ctx, 1)
	c.Assert(err, qt.ErrorMatches, `(?s)failed to record migration reversion 2:.*blocked revision delete.*`)
	c.Assert(sqliteTableExists(t, conn, "child"), qt.IsFalse)
	var applied, total int
	var partialHashes string
	c.Assert(
		conn.QueryRowContext(ctx, "SELECT applied, total, partial_hashes FROM atlas_schema_revisions WHERE version = '2'").
			Scan(&applied, &total, &partialHashes),
		qt.IsNil,
	)
	c.Assert(applied, qt.Equals, 1)
	c.Assert(total, qt.Equals, 1)
	c.Assert(partialHashes, qt.Matches, `\["h1:[A-Za-z0-9+/]+=*"\]`)

	_, err = conn.ExecContext(ctx, "DROP TRIGGER block_revision_delete")
	c.Assert(err, qt.IsNil)
	c.Assert(m.RepairMigration(ctx, migrator.RepairMigrationOptions{Version: 2}), qt.IsNil)
	c.Assert(atlasRevisionTuples(t, conn), qt.HasLen, 1)
}

func sqliteTableExists(t *testing.T, conn *dbschema.DatabaseConnection, table string) bool {
	t.Helper()
	c := qt.New(t)
	var count int
	err := conn.QueryRow(
		"SELECT count(*) FROM sqlite_master WHERE type = 'table' AND name = ?", table).Scan(&count)
	c.Assert(err, qt.IsNil)
	return count == 1
}

// TestMigrateDown_AtlasFormatSuccessfulDownDeletesRow is the other direction:
// deletion on success is how Atlas marks a migration reverted.
func TestMigrateDown_AtlasFormatSuccessfulDownDeletesRow(t *testing.T) {
	c := qt.New(t)
	ctx := context.Background()
	conn, m := newAtlasDownMigrator(t, succeedingDownTxtar)
	c.Assert(atlasRevisionTuples(t, conn), qt.HasLen, 2)

	c.Assert(m.MigrateDownTo(ctx, 1), qt.IsNil)

	remaining := atlasRevisionTuples(t, conn)
	c.Assert(remaining, qt.HasLen, 1)
	c.Assert(remaining[0], qt.Contains, "'1'")
	status, err := m.GetMigrationStatus(ctx)
	c.Assert(err, qt.IsNil)
	c.Assert(status.CurrentVersion, qt.Equals, int64(1))
}

func TestMigrateDown_AtlasFormatRepairAfterFailedDownRunsDownBody(t *testing.T) {
	c := qt.New(t)
	ctx := context.Background()
	conn, err := dbschema.ConnectToDatabase(ctx, "sqlite://"+filepath.Join(t.TempDir(), "retry.db"))
	c.Assert(err, qt.IsNil)
	t.Cleanup(func() { _ = conn.Close() })

	fsys := fstest.MapFS{
		"1_create_parent.sql": &fstest.MapFile{Data: []byte("CREATE TABLE parent (id INTEGER PRIMARY KEY);\n")},
		"2_create_child.sql":  &fstest.MapFile{Data: []byte(failingDownTxtar)},
	}
	broken, err := migrator.NewFSMigrator(conn, fsys, migrator.WithMigrationDirFormat(migrator.MigrationDirFormatAtlas))
	c.Assert(err, qt.IsNil)
	broken = broken.WithRevisionTableFormat(migrator.RevisionTableFormatAtlas)
	c.Assert(broken.MigrateUp(ctx), qt.IsNil)
	c.Assert(broken.MigrateDownTo(ctx, 1), qt.IsNotNil)

	// The operator repairs the down body and explicitly resumes the rollback.
	fsys["2_create_child.sql"] = &fstest.MapFile{Data: []byte(succeedingDownTxtar)}
	repaired, err := migrator.NewFSMigrator(conn, fsys, migrator.WithMigrationDirFormat(migrator.MigrationDirFormatAtlas))
	c.Assert(err, qt.IsNil)
	repaired = repaired.WithRevisionTableFormat(migrator.RevisionTableFormatAtlas)

	c.Assert(repaired.RepairMigration(ctx, migrator.RepairMigrationOptions{Version: 2, ResumeFrom: 1}), qt.IsNil)

	c.Assert(atlasRevisionTuples(t, conn), qt.HasLen, 1)
	c.Assert(sqliteTableExists(t, conn, "child"), qt.IsFalse)
}

// TestMigrateDown_PtahFormatFailedDownStillRecordsDirtyState pins the native
// revision-table encoding independently from the Atlas-layout tests above.
func TestMigrateDown_PtahFormatFailedDownStillRecordsDirtyState(t *testing.T) {
	c := qt.New(t)
	ctx := context.Background()
	conn, err := dbschema.ConnectToDatabase(ctx, "sqlite://"+filepath.Join(t.TempDir(), "ptah-down.db"))
	c.Assert(err, qt.IsNil)
	t.Cleanup(func() { _ = conn.Close() })

	m, err := migrator.NewFSMigrator(conn, fstest.MapFS{
		"0000000001_parent.up.sql":   &fstest.MapFile{Data: []byte("CREATE TABLE parent (id INTEGER PRIMARY KEY);\n")},
		"0000000001_parent.down.sql": &fstest.MapFile{Data: []byte("DROP TABLE parent;\n")},
		"0000000002_child.up.sql":    &fstest.MapFile{Data: []byte("CREATE TABLE child (id INTEGER PRIMARY KEY);\n")},
		"0000000002_child.down.sql":  &fstest.MapFile{Data: []byte("DROP TABLE child;\nTHIS IS A FAILING STATEMENT;\n")},
	})
	c.Assert(err, qt.IsNil)
	c.Assert(m.MigrateUp(ctx), qt.IsNil)

	c.Assert(m.MigrateDownTo(ctx, 1), qt.IsNotNil)

	// Native keeps the failure visible: this is what repair and resume act on.
	// The stored state carries the direction that wrote it, so the row a
	// rollback leaves behind is distinguishable from the one a failed apply
	// leaves behind.
	var state, errText string
	c.Assert(conn.QueryRow(
		"SELECT state, COALESCE(error, '') FROM schema_migrations WHERE version = 2").Scan(&state, &errText), qt.IsNil)
	c.Assert(state, qt.Equals, "failed:down")
	c.Assert(errText, qt.Not(qt.Equals), "")

	status, err := m.GetMigrationStatus(ctx)
	c.Assert(err, qt.IsNil)
	c.Assert(status.DirtyRevision, qt.IsNotNil,
		qt.Commentf("native status must surface the half-finished rollback"))
	c.Assert(status.DirtyRevision.Version, qt.Equals, int64(2))
	c.Assert(status.DirtyRevision.State, qt.Equals, "failed")
	c.Assert(status.DirtyRevision.Direction, qt.Equals, migrator.MigrationDirectionDown)
}

// TestMigrateDown_PtahFormatDirtyDownBlocksUntilRepaired proves the native
// recovery path still works end to end on that dirty state: the next up run
// refuses, and `RepairMigration` clears it.
func TestMigrateDown_PtahFormatDirtyDownBlocksUntilRepaired(t *testing.T) {
	c := qt.New(t)
	ctx := context.Background()
	conn, err := dbschema.ConnectToDatabase(ctx, "sqlite://"+filepath.Join(t.TempDir(), "ptah-repair.db"))
	c.Assert(err, qt.IsNil)
	t.Cleanup(func() { _ = conn.Close() })

	m, err := migrator.NewFSMigrator(conn, fstest.MapFS{
		"0000000001_parent.up.sql":   &fstest.MapFile{Data: []byte("CREATE TABLE parent (id INTEGER PRIMARY KEY);\n")},
		"0000000001_parent.down.sql": &fstest.MapFile{Data: []byte("DROP TABLE parent;\n")},
		"0000000002_child.up.sql":    &fstest.MapFile{Data: []byte("CREATE TABLE child (id INTEGER PRIMARY KEY);\n")},
		"0000000002_child.down.sql":  &fstest.MapFile{Data: []byte("DROP TABLE child;\nTHIS IS A FAILING STATEMENT;\n")},
	})
	c.Assert(err, qt.IsNil)
	c.Assert(m.MigrateUp(ctx), qt.IsNil)
	c.Assert(m.MigrateDownTo(ctx, 1), qt.IsNotNil)

	// failIfDirty refuses to run over the recorded failure.
	upErr := m.MigrateUp(ctx)
	c.Assert(upErr, qt.IsNotNil)
	c.Assert(migrator.IsDirtyMigration(upErr), qt.IsTrue)

	c.Assert(m.RepairMigration(ctx, migrator.RepairMigrationOptions{Version: 2}), qt.IsNil)

	status, err := m.GetMigrationStatus(ctx)
	c.Assert(err, qt.IsNil)
	c.Assert(status.DirtyRevision, qt.IsNil)
}

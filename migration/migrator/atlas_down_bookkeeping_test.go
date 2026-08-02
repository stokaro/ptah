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

// Down bookkeeping is split by surface (see Migrator.reproducesAtlasDownBookkeeping):
// the Atlas-shaped surface must leave exactly what Atlas leaves, while native
// Ptah keeps its richer dirty state for recovery tooling. Measured against
// Atlas: a failed `migrate down` leaves the
// revision row byte-identical and a successful one deletes it (#957).

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
	ctx := context.Background()
	conn, err := dbschema.ConnectToDatabase(ctx, "sqlite://"+filepath.Join(t.TempDir(), "down.db"))
	qt.Assert(t, err, qt.IsNil)
	t.Cleanup(func() { _ = conn.Close() })

	m, err := migrator.NewFSMigrator(
		conn,
		fstest.MapFS{
			"1_create_parent.sql": &fstest.MapFile{Data: []byte("CREATE TABLE parent (id INTEGER PRIMARY KEY);\n")},
			"2_create_child.sql":  &fstest.MapFile{Data: []byte(secondMigration)},
		},
		migrator.WithMigrationDirFormat(migrator.MigrationDirFormatAtlas),
	)
	qt.Assert(t, err, qt.IsNil)
	m = m.WithRevisionTableFormat(migrator.RevisionTableFormatAtlas)
	qt.Assert(t, m.MigrateUp(ctx), qt.IsNil)
	return conn, m
}

// atlasRevisionTuples renders every Atlas revision row as a quote()-based tuple
// so a comparison is byte-precise, including NULL versus the empty string and
// unchanged execution_time values.
func atlasRevisionTuples(t *testing.T, conn *dbschema.DatabaseConnection) []string {
	t.Helper()
	rows, err := conn.Query(
		`SELECT quote(version) || '|' || quote(description) || '|' || quote(type) || '|' ||
quote(applied) || '|' || quote(total) || '|' || quote(executed_at) || '|' ||
quote(execution_time) || '|' || quote(error) || '|' || quote(error_stmt) || '|' ||
quote(hash) || '|' || quote(partial_hashes) || '|' || quote(operator_version)
FROM atlas_schema_revisions ORDER BY version`)
	qt.Assert(t, err, qt.IsNil)
	defer func() { _ = rows.Close() }()
	var tuples []string
	for rows.Next() {
		var tuple string
		qt.Assert(t, rows.Scan(&tuple), qt.IsNil)
		tuples = append(tuples, tuple)
	}
	qt.Assert(t, rows.Err(), qt.IsNil)
	return tuples
}

// TestMigrateDown_AtlasFormatFailedDownLeavesRevisionsByteIdentical is the
// compat-surface contract: Atlas records nothing when a down fails, so
// `atlas migrate status` and `ptah-compat migrate status` keep agreeing.
func TestMigrateDown_AtlasFormatFailedDownLeavesRevisionsByteIdentical(t *testing.T) {
	c := qt.New(t)
	ctx := context.Background()
	conn, m := newAtlasDownMigrator(t, failingDownTxtar)
	before := atlasRevisionTuples(t, conn)
	c.Assert(before, qt.HasLen, 2)

	err := m.MigrateDownTo(ctx, 1)

	c.Assert(err, qt.IsNotNil)
	c.Assert(atlasRevisionTuples(t, conn), qt.DeepEquals, before,
		qt.Commentf("a failed Atlas-format down must not touch the revision table"))

	// Status agrees with Atlas: the version still reads as applied, not dirty.
	status, err := m.GetMigrationStatus(ctx)
	c.Assert(err, qt.IsNil)
	c.Assert(status.CurrentVersion, qt.Equals, int64(2))
	c.Assert(status.DirtyRevision, qt.IsNil)
}

// TestMigrateDown_AtlasFormatFailedNoTransactionDownLeavesPartialSchema covers
// the non-transactional down path, where "the body is rolled back" does not
// hold: `-- atlas:txmode none` runs the down outside a transaction, so the
// statements that completed stay applied. The Atlas-shaped surface still
// records nothing, which means a half-reverted schema is left behind a revision
// row that reads as fully applied, with no dirty state and no repair hook. That
// is Atlas's own behavior, and it is the reason the native surface keeps
// recording (#957).
func TestMigrateDown_AtlasFormatFailedNoTransactionDownLeavesPartialSchema(t *testing.T) {
	c := qt.New(t)
	ctx := context.Background()
	conn, m := newAtlasDownMigrator(t, failingNoTransactionDownTxtar)
	before := atlasRevisionTuples(t, conn)
	c.Assert(before, qt.HasLen, 2)

	err := m.MigrateDownTo(ctx, 1)

	c.Assert(err, qt.IsNotNil)
	// The first down statement completed and was not rolled back.
	c.Assert(sqliteTableExists(t, conn, "child2"), qt.IsFalse)
	c.Assert(sqliteTableExists(t, conn, "child1"), qt.IsTrue)
	// The revision row still claims the migration is fully applied.
	c.Assert(atlasRevisionTuples(t, conn), qt.DeepEquals, before)
	status, err := m.GetMigrationStatus(ctx)
	c.Assert(err, qt.IsNil)
	c.Assert(status.CurrentVersion, qt.Equals, int64(2))
	c.Assert(status.DirtyRevision, qt.IsNil)
}

func sqliteTableExists(t *testing.T, conn *dbschema.DatabaseConnection, table string) bool {
	t.Helper()
	var count int
	err := conn.QueryRow(
		"SELECT count(*) FROM sqlite_master WHERE type = 'table' AND name = ?", table).Scan(&count)
	qt.Assert(t, err, qt.IsNil)
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

// TestMigrateDown_AtlasFormatRetryAfterFailedDownSucceeds mirrors the
// check-failure recovery contract: because nothing was recorded, a repaired
// retry needs no flags and no revision repair.
func TestMigrateDown_AtlasFormatRetryAfterFailedDownSucceeds(t *testing.T) {
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

	// The operator repairs the down body and retries.
	fsys["2_create_child.sql"] = &fstest.MapFile{Data: []byte(succeedingDownTxtar)}
	repaired, err := migrator.NewFSMigrator(conn, fsys, migrator.WithMigrationDirFormat(migrator.MigrationDirFormatAtlas))
	c.Assert(err, qt.IsNil)
	repaired = repaired.WithRevisionTableFormat(migrator.RevisionTableFormatAtlas)

	c.Assert(repaired.MigrateDownTo(ctx, 1), qt.IsNil)

	c.Assert(atlasRevisionTuples(t, conn), qt.HasLen, 1)
}

// TestMigrateDown_PtahFormatFailedDownStillRecordsDirtyState pins the native
// side of the split. This is the regression guard that proves the branch is
// load-bearing: collapsing both surfaces onto Atlas semantics turns this red.
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
	var state, errText string
	c.Assert(conn.QueryRow(
		"SELECT state, COALESCE(error, '') FROM schema_migrations WHERE version = 2").Scan(&state, &errText), qt.IsNil)
	c.Assert(state, qt.Equals, "failed")
	c.Assert(errText, qt.Not(qt.Equals), "")

	status, err := m.GetMigrationStatus(ctx)
	c.Assert(err, qt.IsNil)
	c.Assert(status.DirtyRevision, qt.IsNotNil,
		qt.Commentf("native status must surface the half-finished rollback"))
	c.Assert(status.DirtyRevision.Version, qt.Equals, int64(2))
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

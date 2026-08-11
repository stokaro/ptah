package migrator_test

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"path/filepath"
	"strings"
	"testing"
	"testing/fstest"
	"time"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/dbschema"
	"go.5x5.cz/ptah/internal/migratesum"
	"go.5x5.cz/ptah/migration/migrator"
)

// Static fixture values cover the compatibility path for old callers that
// provide atlas.sum identities without asking Ptah to project the chain.
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

const (
	atlasChainOneVersion   = int64(20240101000000)
	atlasChainTwoVersion   = int64(20240101500000)
	atlasChainThreeVersion = int64(20240102000000)
	atlasChainFourVersion  = int64(20240103000000)
	atlasChainOneFile      = "20240101000000_one.sql"
	atlasChainTwoFile      = "20240101500000_two.sql"
	atlasChainMiddleFile   = "20240101750000_middle.sql"
	atlasChainThreeFile    = "20240102000000_three.sql"
	atlasChainFourFile     = "20240103000000_four.sql"
	atlasChainOneSQL       = "CREATE TABLE atlas_chain_one (id INTEGER PRIMARY KEY);\n"
	atlasChainTwoSQL       = "CREATE TABLE atlas_chain_two (id INTEGER PRIMARY KEY);\n"
	atlasChainMiddleSQL    = "CREATE TABLE atlas_chain_middle (id INTEGER PRIMARY KEY);\n"
	atlasChainThreeSQL     = "CREATE TABLE atlas_chain_three (id INTEGER PRIMARY KEY);\n"
	atlasChainFourSQL      = "CREATE TABLE atlas_chain_four (id INTEGER PRIMARY KEY);\n"
)

func atlasChainFS(t *testing.T, files map[string]string) fstest.MapFS {
	t.Helper()
	fsys := make(fstest.MapFS, len(files)+1)
	for name, body := range files {
		fsys[name] = &fstest.MapFile{Data: []byte(body)}
	}
	sum, err := migratesum.ComputeWithFormat(fsys, migrator.MigrationDirFormatAtlas)
	qt.Assert(t, err, qt.IsNil)
	fsys[migratesum.AtlasFileName] = &fstest.MapFile{Data: sum.Bytes()}
	return fsys
}

func newSQLiteAtlasChainMigrator(
	t *testing.T,
	dbPath string,
	fsys fstest.MapFS,
) (*dbschema.DatabaseConnection, *migrator.Migrator) {
	t.Helper()
	conn, err := dbschema.ConnectToDatabase(t.Context(), "sqlite://"+dbPath)
	qt.Assert(t, err, qt.IsNil)
	t.Cleanup(func() { _ = conn.Close() })
	m, err := migrator.NewFSMigrator(conn, fsys, migrator.WithMigrationDirFormat(migrator.MigrationDirFormatAtlas))
	qt.Assert(t, err, qt.IsNil)
	return conn, m
}

func atlasChainInitialFS(t *testing.T) fstest.MapFS {
	t.Helper()
	return atlasChainFS(t, map[string]string{
		atlasChainOneFile:   atlasChainOneSQL,
		atlasChainThreeFile: atlasChainThreeSQL,
	})
}

func atlasChainExpandedFS(t *testing.T, twoSQL string) fstest.MapFS {
	t.Helper()
	return atlasChainFS(t, map[string]string{
		atlasChainOneFile:   atlasChainOneSQL,
		atlasChainTwoFile:   twoSQL,
		atlasChainThreeFile: atlasChainThreeSQL,
	})
}

func atlasChainEntryHash(t *testing.T, fsys fstest.MapFS, name string) string {
	t.Helper()
	sum, err := migratesum.Parse(fsys[migratesum.AtlasFileName].Data)
	qt.Assert(t, err, qt.IsNil)
	entries := make(map[string]string, len(sum.Entries))
	for _, entry := range sum.Entries {
		entries[entry.Name] = strings.TrimPrefix(entry.Hash, "h1:")
	}
	return entries[name]
}

func atlasChainTableExists(t *testing.T, conn *dbschema.DatabaseConnection, table string) bool {
	t.Helper()
	var count int
	err := conn.QueryRow(
		"SELECT count(*) FROM sqlite_master WHERE type = 'table' AND name = ?",
		table,
	).Scan(&count)
	qt.Assert(t, err, qt.IsNil)
	return count == 1
}

func installSuccessfulAtlasChainTwoRevision(
	t *testing.T,
	conn *dbschema.DatabaseConnection,
	fsys fstest.MapFS,
	appliedAt any,
) {
	t.Helper()
	_, err := conn.Exec(atlasChainTwoSQL)
	qt.Assert(t, err, qt.IsNil)
	_, err = conn.Exec(
		`INSERT INTO schema_migrations
(version, description, applied_at, state, applied, total, execution_time_ms, checksum)
VALUES (?, ?, ?, 'applied', 1, 1, 0, ?)`,
		atlasChainTwoVersion,
		"two",
		appliedAt,
		atlasChainEntryHash(t, fsys, atlasChainTwoFile),
	)
	qt.Assert(t, err, qt.IsNil)
}

func setAtlasChainAppliedAt(
	t *testing.T,
	conn *dbschema.DatabaseConnection,
	version int64,
	appliedAt time.Time,
) {
	t.Helper()
	_, err := conn.Exec(
		"UPDATE schema_migrations SET applied_at = ? WHERE version = ?",
		appliedAt,
		version,
	)
	qt.Assert(t, err, qt.IsNil)
}

func TestAtlasSumPtahRevisions_OutOfOrderInsertionReconcilesTheAppliedChain(t *testing.T) {
	c := qt.New(t)
	ctx := t.Context()
	dbPath := filepath.Join(t.TempDir(), "out-of-order.db")
	initialFS := atlasChainInitialFS(t)
	conn, initial := newSQLiteAtlasChainMigrator(t, dbPath, initialFS)
	t.Cleanup(func() { _ = conn.Close() })
	c.Assert(initial.MigrateUp(ctx), qt.IsNil)
	lateBefore := storedRevisionChecksum(t, conn, atlasChainThreeVersion)

	expandedFS := atlasChainExpandedFS(t, atlasChainTwoSQL)
	_, linear := newSQLiteAtlasChainMigrator(t, dbPath, expandedFS)
	var outOfOrder *migrator.OutOfOrderError
	err := linear.MigrateUp(ctx)
	c.Assert(err, qt.ErrorAs, &outOfOrder)
	c.Assert(outOfOrder.Versions, qt.DeepEquals, []int64{atlasChainTwoVersion})
	c.Assert(storedRevisionChecksum(t, conn, atlasChainThreeVersion), qt.Equals, lateBefore)
	c.Assert(atlasChainTableExists(t, conn, "atlas_chain_two"), qt.IsFalse)

	_, linearSkip := newSQLiteAtlasChainMigrator(t, dbPath, expandedFS)
	linearSkip = linearSkip.WithExecOrder(migrator.ExecOrderLinearSkip)
	c.Assert(linearSkip.MigrateUp(ctx), qt.IsNil)
	c.Assert(storedRevisionChecksum(t, conn, atlasChainThreeVersion), qt.Equals, lateBefore)
	c.Assert(atlasChainTableExists(t, conn, "atlas_chain_two"), qt.IsFalse)

	_, nonLinear := newSQLiteAtlasChainMigrator(t, dbPath, expandedFS)
	nonLinear = nonLinear.WithExecOrder(migrator.ExecOrderNonLinear)
	c.Assert(nonLinear.MigrateUp(ctx), qt.IsNil)
	c.Assert(atlasChainTableExists(t, conn, "atlas_chain_two"), qt.IsTrue)
	c.Assert(
		storedRevisionChecksum(t, conn, atlasChainOneVersion),
		qt.Equals,
		atlasChainEntryHash(t, expandedFS, atlasChainOneFile),
	)
	c.Assert(
		storedRevisionChecksum(t, conn, atlasChainTwoVersion),
		qt.Equals,
		atlasChainEntryHash(t, expandedFS, atlasChainTwoFile),
	)
	c.Assert(
		storedRevisionChecksum(t, conn, atlasChainThreeVersion),
		qt.Equals,
		atlasChainEntryHash(t, expandedFS, atlasChainThreeFile),
	)
	c.Assert(nonLinear.MigrateUp(ctx), qt.IsNil)
}

func TestAtlasSumPtahRevisions_DryRunDoesNotApplyOrReconcileAnInsertion(t *testing.T) {
	c := qt.New(t)
	dbPath := filepath.Join(t.TempDir(), "dry-run.db")
	conn, initial := newSQLiteAtlasChainMigrator(t, dbPath, atlasChainInitialFS(t))
	t.Cleanup(func() { _ = conn.Close() })
	c.Assert(initial.MigrateUp(t.Context()), qt.IsNil)
	lateBefore := storedRevisionChecksum(t, conn, atlasChainThreeVersion)

	dryConn, dryRun := newSQLiteAtlasChainMigrator(t, dbPath, atlasChainExpandedFS(t, atlasChainTwoSQL))
	t.Cleanup(func() { _ = dryConn.Close() })
	dryConn.SchemaWriter().SetDryRun(true)
	dryRun = dryRun.WithExecOrder(migrator.ExecOrderNonLinear)
	c.Assert(dryRun.MigrateUp(t.Context()), qt.IsNil)
	c.Assert(atlasChainTableExists(t, conn, "atlas_chain_two"), qt.IsFalse)
	c.Assert(storedRevisionChecksum(t, conn, atlasChainThreeVersion), qt.Equals, lateBefore)
}

func TestAtlasSumPtahRevisions_AmountExcludesUnselectedPendingContributions(t *testing.T) {
	c := qt.New(t)
	dbPath := filepath.Join(t.TempDir(), "amount.db")
	conn, initial := newSQLiteAtlasChainMigrator(t, dbPath, atlasChainInitialFS(t))
	c.Assert(initial.MigrateUp(t.Context()), qt.IsNil)
	expandedFS := atlasChainFS(t, map[string]string{
		atlasChainOneFile:    atlasChainOneSQL,
		atlasChainTwoFile:    atlasChainTwoSQL,
		atlasChainMiddleFile: atlasChainMiddleSQL,
		atlasChainThreeFile:  atlasChainThreeSQL,
	})
	selectedProjectionFS := atlasChainFS(t, map[string]string{
		atlasChainOneFile:   atlasChainOneSQL,
		atlasChainTwoFile:   atlasChainTwoSQL,
		atlasChainThreeFile: atlasChainThreeSQL,
	})

	_, limited := newSQLiteAtlasChainMigrator(t, dbPath, expandedFS)
	limited = limited.WithExecOrder(migrator.ExecOrderNonLinear)
	c.Assert(limited.MigrateUpWithOptions(t.Context(), migrator.MigrateUpOptions{Amount: 1}), qt.IsNil)
	c.Assert(atlasChainTableExists(t, conn, "atlas_chain_two"), qt.IsTrue)
	c.Assert(atlasChainTableExists(t, conn, "atlas_chain_middle"), qt.IsFalse)
	c.Assert(
		storedRevisionChecksum(t, conn, atlasChainThreeVersion),
		qt.Equals,
		atlasChainEntryHash(t, selectedProjectionFS, atlasChainThreeFile),
	)

	c.Assert(limited.MigrateUp(t.Context()), qt.IsNil)
	c.Assert(atlasChainTableExists(t, conn, "atlas_chain_middle"), qt.IsTrue)
	c.Assert(
		storedRevisionChecksum(t, conn, atlasChainThreeVersion),
		qt.Equals,
		atlasChainEntryHash(t, expandedFS, atlasChainThreeFile),
	)
}

func TestAtlasSumPtahRevisions_AnEditBesideAnInsertionStillFailsClosed(t *testing.T) {
	c := qt.New(t)
	dbPath := filepath.Join(t.TempDir(), "edited.db")
	conn, initial := newSQLiteAtlasChainMigrator(t, dbPath, atlasChainInitialFS(t))
	t.Cleanup(func() { _ = conn.Close() })
	c.Assert(initial.MigrateUp(t.Context()), qt.IsNil)
	lateBefore := storedRevisionChecksum(t, conn, atlasChainThreeVersion)
	editedFS := atlasChainFS(t, map[string]string{
		atlasChainOneFile:   "CREATE TABLE atlas_chain_one (id INTEGER PRIMARY KEY, edited TEXT);\n",
		atlasChainTwoFile:   atlasChainTwoSQL,
		atlasChainThreeFile: atlasChainThreeSQL,
	})

	_, edited := newSQLiteAtlasChainMigrator(t, dbPath, editedFS)
	edited = edited.WithExecOrder(migrator.ExecOrderNonLinear)
	var mismatch *migrator.ChecksumMismatchError
	err := edited.MigrateUp(t.Context())
	c.Assert(err, qt.ErrorAs, &mismatch)
	c.Assert(mismatch.Version, qt.Equals, atlasChainOneVersion)
	c.Assert(atlasChainTableExists(t, conn, "atlas_chain_two"), qt.IsFalse)
	c.Assert(storedRevisionChecksum(t, conn, atlasChainThreeVersion), qt.Equals, lateBefore)
}

func TestAtlasSumPtahRevisions_TransactionalFailureDoesNotProspectivelyReconcile(t *testing.T) {
	c := qt.New(t)
	dbPath := filepath.Join(t.TempDir(), "transaction-failure.db")
	conn, initial := newSQLiteAtlasChainMigrator(t, dbPath, atlasChainInitialFS(t))
	t.Cleanup(func() { _ = conn.Close() })
	c.Assert(initial.MigrateUp(t.Context()), qt.IsNil)
	lateBefore := storedRevisionChecksum(t, conn, atlasChainThreeVersion)
	failingFS := atlasChainExpandedFS(t, "THIS IS NOT SQL;\n")

	_, failing := newSQLiteAtlasChainMigrator(t, dbPath, failingFS)
	failing = failing.WithExecOrder(migrator.ExecOrderNonLinear)
	c.Assert(failing.MigrateUp(t.Context()), qt.IsNotNil)
	c.Assert(storedRevisionChecksum(t, conn, atlasChainThreeVersion), qt.Equals, lateBefore)
	c.Assert(atlasChainTableExists(t, conn, "atlas_chain_two"), qt.IsFalse)
}

func TestAtlasSumPtahRevisions_NoTransactionFailureDoesNotProspectivelyReconcile(t *testing.T) {
	c := qt.New(t)
	dbPath := filepath.Join(t.TempDir(), "no-transaction-failure.db")
	conn, initial := newSQLiteAtlasChainMigrator(t, dbPath, atlasChainInitialFS(t))
	t.Cleanup(func() { _ = conn.Close() })
	c.Assert(initial.MigrateUp(t.Context()), qt.IsNil)
	lateBefore := storedRevisionChecksum(t, conn, atlasChainThreeVersion)
	failingFS := atlasChainExpandedFS(t, "-- atlas:txmode none\n\n"+atlasChainTwoSQL+"THIS IS NOT SQL;\n")

	_, failing := newSQLiteAtlasChainMigrator(t, dbPath, failingFS)
	failing = failing.WithExecOrder(migrator.ExecOrderNonLinear)
	c.Assert(failing.MigrateUp(t.Context()), qt.IsNotNil)
	c.Assert(storedRevisionChecksum(t, conn, atlasChainThreeVersion), qt.Equals, lateBefore)
	c.Assert(atlasChainTableExists(t, conn, "atlas_chain_two"), qt.IsTrue)
}

func TestAtlasSumPtahRevisions_ReconciliationFailureRollsBackEveryChecksumAndRetries(t *testing.T) {
	c := qt.New(t)
	dbPath := filepath.Join(t.TempDir(), "atomic-reconciliation.db")
	initialFS := atlasChainFS(t, map[string]string{
		atlasChainOneFile:   atlasChainOneSQL,
		atlasChainThreeFile: atlasChainThreeSQL,
		atlasChainFourFile:  atlasChainFourSQL,
	})
	conn, initial := newSQLiteAtlasChainMigrator(t, dbPath, initialFS)
	c.Assert(initial.MigrateUp(t.Context()), qt.IsNil)
	threeBefore := storedRevisionChecksum(t, conn, atlasChainThreeVersion)
	fourBefore := storedRevisionChecksum(t, conn, atlasChainFourVersion)
	duplicateAppliedAt := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	setAtlasChainAppliedAt(t, conn, atlasChainOneVersion, duplicateAppliedAt)
	setAtlasChainAppliedAt(t, conn, atlasChainThreeVersion, duplicateAppliedAt)
	setAtlasChainAppliedAt(t, conn, atlasChainFourVersion, duplicateAppliedAt)
	_, err := conn.ExecContext(t.Context(), `CREATE TRIGGER fail_second_checksum_reconciliation
BEFORE UPDATE OF checksum ON schema_migrations
WHEN OLD.version = 20240103000000
BEGIN
    SELECT RAISE(ABORT, 'fail second checksum reconciliation');
END`)
	c.Assert(err, qt.IsNil)

	expandedFS := atlasChainFS(t, map[string]string{
		atlasChainOneFile:   atlasChainOneSQL,
		atlasChainTwoFile:   atlasChainTwoSQL,
		atlasChainThreeFile: atlasChainThreeSQL,
		atlasChainFourFile:  atlasChainFourSQL,
	})
	_, applying := newSQLiteAtlasChainMigrator(t, dbPath, expandedFS)
	applying = applying.WithExecOrder(migrator.ExecOrderNonLinear)
	err = applying.MigrateUp(t.Context())
	c.Assert(err, qt.ErrorMatches, `(?s)failed to reconcile checksum for migration 20240103000000:.*fail second checksum reconciliation.*`)
	c.Assert(atlasChainTableExists(t, conn, "atlas_chain_two"), qt.IsTrue)
	c.Assert(storedRevisionChecksum(t, conn, atlasChainThreeVersion), qt.Equals, threeBefore)
	c.Assert(storedRevisionChecksum(t, conn, atlasChainFourVersion), qt.Equals, fourBefore)

	_, err = conn.ExecContext(t.Context(), "DROP TRIGGER fail_second_checksum_reconciliation")
	c.Assert(err, qt.IsNil)
	c.Assert(applying.MigrateUp(t.Context()), qt.IsNil)
	c.Assert(
		storedRevisionChecksum(t, conn, atlasChainThreeVersion),
		qt.Equals,
		atlasChainEntryHash(t, expandedFS, atlasChainThreeFile),
	)
	c.Assert(
		storedRevisionChecksum(t, conn, atlasChainFourVersion),
		qt.Equals,
		atlasChainEntryHash(t, expandedFS, atlasChainFourFile),
	)
}

func TestAtlasSumPtahRevisions_MixedHistoricalProjectionFailsClosed(t *testing.T) {
	c := qt.New(t)
	dbPath := filepath.Join(t.TempDir(), "mixed-reconciliation.db")
	initialFS := atlasChainFS(t, map[string]string{
		atlasChainOneFile:   atlasChainOneSQL,
		atlasChainThreeFile: atlasChainThreeSQL,
		atlasChainFourFile:  atlasChainFourSQL,
	})
	conn, initial := newSQLiteAtlasChainMigrator(t, dbPath, initialFS)
	c.Assert(initial.MigrateUp(t.Context()), qt.IsNil)
	duplicateAppliedAt := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	setAtlasChainAppliedAt(t, conn, atlasChainOneVersion, duplicateAppliedAt)
	setAtlasChainAppliedAt(t, conn, atlasChainThreeVersion, duplicateAppliedAt)
	setAtlasChainAppliedAt(t, conn, atlasChainFourVersion, duplicateAppliedAt)
	expandedFS := atlasChainFS(t, map[string]string{
		atlasChainOneFile:   atlasChainOneSQL,
		atlasChainTwoFile:   atlasChainTwoSQL,
		atlasChainThreeFile: atlasChainThreeSQL,
		atlasChainFourFile:  atlasChainFourSQL,
	})
	installSuccessfulAtlasChainTwoRevision(
		t,
		conn,
		expandedFS,
		time.Date(2024, 1, 2, 0, 0, 0, 0, time.UTC),
	)
	_, err := conn.ExecContext(
		t.Context(),
		"UPDATE schema_migrations SET checksum = ? WHERE version = ?",
		atlasChainEntryHash(t, expandedFS, atlasChainThreeFile),
		atlasChainThreeVersion,
	)
	c.Assert(err, qt.IsNil)
	fourBefore := storedRevisionChecksum(t, conn, atlasChainFourVersion)

	_, retrying := newSQLiteAtlasChainMigrator(t, dbPath, expandedFS)
	retrying = retrying.WithExecOrder(migrator.ExecOrderNonLinear)
	var mismatch *migrator.ChecksumMismatchError
	err = retrying.MigrateUp(t.Context())
	c.Assert(err, qt.ErrorAs, &mismatch)
	c.Assert(mismatch.Version, qt.Equals, atlasChainFourVersion)
	c.Assert(
		storedRevisionChecksum(t, conn, atlasChainThreeVersion),
		qt.Equals,
		atlasChainEntryHash(t, expandedFS, atlasChainThreeFile),
	)
	c.Assert(storedRevisionChecksum(t, conn, atlasChainFourVersion), qt.Equals, fourBefore)
}

func TestAtlasSumPtahRevisions_StrictTimestampMixedHistoricalProjectionFailsClosed(t *testing.T) {
	c := qt.New(t)
	dbPath := filepath.Join(t.TempDir(), "strict-mixed-reconciliation.db")
	initialFS := atlasChainFS(t, map[string]string{
		atlasChainOneFile:   atlasChainOneSQL,
		atlasChainThreeFile: atlasChainThreeSQL,
		atlasChainFourFile:  atlasChainFourSQL,
	})
	conn, initial := newSQLiteAtlasChainMigrator(t, dbPath, initialFS)
	c.Assert(initial.MigrateUp(t.Context()), qt.IsNil)
	setAtlasChainAppliedAt(t, conn, atlasChainOneVersion, time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC))
	setAtlasChainAppliedAt(t, conn, atlasChainThreeVersion, time.Date(2024, 1, 2, 0, 0, 0, 0, time.UTC))
	setAtlasChainAppliedAt(t, conn, atlasChainFourVersion, time.Date(2024, 1, 3, 0, 0, 0, 0, time.UTC))
	threeBefore := storedRevisionChecksum(t, conn, atlasChainThreeVersion)
	expandedFS := atlasChainFS(t, map[string]string{
		atlasChainOneFile:   atlasChainOneSQL,
		atlasChainTwoFile:   atlasChainTwoSQL,
		atlasChainThreeFile: atlasChainThreeSQL,
		atlasChainFourFile:  atlasChainFourSQL,
	})
	installSuccessfulAtlasChainTwoRevision(
		t,
		conn,
		expandedFS,
		time.Date(2024, 1, 4, 0, 0, 0, 0, time.UTC),
	)
	_, err := conn.ExecContext(
		t.Context(),
		"UPDATE schema_migrations SET checksum = ? WHERE version = ?",
		atlasChainEntryHash(t, expandedFS, atlasChainFourFile),
		atlasChainFourVersion,
	)
	c.Assert(err, qt.IsNil)

	_, retrying := newSQLiteAtlasChainMigrator(t, dbPath, expandedFS)
	retrying = retrying.WithExecOrder(migrator.ExecOrderNonLinear)
	var mismatch *migrator.ChecksumMismatchError
	err = retrying.MigrateUp(t.Context())
	c.Assert(err, qt.ErrorAs, &mismatch)
	c.Assert(mismatch.Version, qt.Equals, atlasChainThreeVersion)
	c.Assert(storedRevisionChecksum(t, conn, atlasChainThreeVersion), qt.Equals, threeBefore)
	c.Assert(
		storedRevisionChecksum(t, conn, atlasChainFourVersion),
		qt.Equals,
		atlasChainEntryHash(t, expandedFS, atlasChainFourFile),
	)
}

func TestAtlasSumPtahRevisions_MultipleLaterInsertionsRetainTheirApplicationProjections(t *testing.T) {
	c := qt.New(t)
	dbPath := filepath.Join(t.TempDir(), "multiple-insertion-recovery.db")
	initialFS := atlasChainFS(t, map[string]string{
		atlasChainOneFile:  atlasChainOneSQL,
		atlasChainFourFile: atlasChainFourSQL,
	})
	conn, initial := newSQLiteAtlasChainMigrator(t, dbPath, initialFS)
	c.Assert(initial.MigrateUp(t.Context()), qt.IsNil)
	setAtlasChainAppliedAt(t, conn, atlasChainOneVersion, time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC))
	setAtlasChainAppliedAt(t, conn, atlasChainFourVersion, time.Date(2024, 1, 2, 0, 0, 0, 0, time.UTC))
	fourBefore := storedRevisionChecksum(t, conn, atlasChainFourVersion)
	expandedFS := atlasChainFS(t, map[string]string{
		atlasChainOneFile:   atlasChainOneSQL,
		atlasChainTwoFile:   atlasChainTwoSQL,
		atlasChainThreeFile: atlasChainThreeSQL,
		atlasChainFourFile:  atlasChainFourSQL,
	})
	installSuccessfulAtlasChainTwoRevision(
		t,
		conn,
		expandedFS,
		time.Date(2024, 1, 3, 0, 0, 0, 0, time.UTC),
	)
	_, err := conn.ExecContext(t.Context(), atlasChainThreeSQL)
	c.Assert(err, qt.IsNil)
	_, err = conn.ExecContext(
		t.Context(),
		`INSERT INTO schema_migrations
(version, description, applied_at, state, applied, total, execution_time_ms, checksum)
VALUES (?, ?, ?, 'applied', 1, 1, 0, ?)`,
		atlasChainThreeVersion,
		"three",
		time.Date(2024, 1, 4, 0, 0, 0, 0, time.UTC),
		atlasChainEntryHash(t, expandedFS, atlasChainThreeFile),
	)
	c.Assert(err, qt.IsNil)

	_, retrying := newSQLiteAtlasChainMigrator(t, dbPath, expandedFS)
	retrying = retrying.WithExecOrder(migrator.ExecOrderNonLinear)
	c.Assert(retrying.MigrateUp(t.Context()), qt.IsNil)
	c.Assert(fourBefore, qt.Not(qt.Equals), atlasChainEntryHash(t, expandedFS, atlasChainFourFile))
	c.Assert(
		storedRevisionChecksum(t, conn, atlasChainFourVersion),
		qt.Equals,
		atlasChainEntryHash(t, expandedFS, atlasChainFourFile),
	)
}

func TestAtlasSumPtahRevisions_TamperedHistoricalProjectionFailsClosed(t *testing.T) {
	c := qt.New(t)
	dbPath := filepath.Join(t.TempDir(), "tampered-reconciliation.db")
	initialFS := atlasChainFS(t, map[string]string{
		atlasChainOneFile:   atlasChainOneSQL,
		atlasChainThreeFile: atlasChainThreeSQL,
		atlasChainFourFile:  atlasChainFourSQL,
	})
	conn, initial := newSQLiteAtlasChainMigrator(t, dbPath, initialFS)
	c.Assert(initial.MigrateUp(t.Context()), qt.IsNil)
	duplicateAppliedAt := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	setAtlasChainAppliedAt(t, conn, atlasChainOneVersion, duplicateAppliedAt)
	setAtlasChainAppliedAt(t, conn, atlasChainThreeVersion, duplicateAppliedAt)
	setAtlasChainAppliedAt(t, conn, atlasChainFourVersion, duplicateAppliedAt)
	expandedFS := atlasChainFS(t, map[string]string{
		atlasChainOneFile:   atlasChainOneSQL,
		atlasChainTwoFile:   atlasChainTwoSQL,
		atlasChainThreeFile: atlasChainThreeSQL,
		atlasChainFourFile:  atlasChainFourSQL,
	})
	installSuccessfulAtlasChainTwoRevision(
		t,
		conn,
		expandedFS,
		time.Date(2024, 1, 2, 0, 0, 0, 0, time.UTC),
	)
	_, err := conn.ExecContext(
		t.Context(),
		"UPDATE schema_migrations SET checksum = 'tampered' WHERE version = ?",
		atlasChainThreeVersion,
	)
	c.Assert(err, qt.IsNil)
	fourBefore := storedRevisionChecksum(t, conn, atlasChainFourVersion)

	_, retrying := newSQLiteAtlasChainMigrator(t, dbPath, expandedFS)
	retrying = retrying.WithExecOrder(migrator.ExecOrderNonLinear)
	var mismatch *migrator.ChecksumMismatchError
	err = retrying.MigrateUp(t.Context())
	c.Assert(err, qt.ErrorAs, &mismatch)
	c.Assert(mismatch.Version, qt.Equals, atlasChainThreeVersion)
	c.Assert(storedRevisionChecksum(t, conn, atlasChainThreeVersion), qt.Equals, "tampered")
	c.Assert(storedRevisionChecksum(t, conn, atlasChainFourVersion), qt.Equals, fourBefore)
}

func TestAtlasSumPtahRevisions_StrictHistoryRecoversAfterSuccessfulInsert(t *testing.T) {
	c := qt.New(t)
	dbPath := filepath.Join(t.TempDir(), "historical-recovery.db")
	conn, initial := newSQLiteAtlasChainMigrator(t, dbPath, atlasChainInitialFS(t))
	t.Cleanup(func() { _ = conn.Close() })
	c.Assert(initial.MigrateUp(t.Context()), qt.IsNil)
	setAtlasChainAppliedAt(t, conn, atlasChainOneVersion, time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC))
	setAtlasChainAppliedAt(t, conn, atlasChainThreeVersion, time.Date(2024, 1, 3, 0, 0, 0, 0, time.UTC))
	expandedFS := atlasChainExpandedFS(t, atlasChainTwoSQL)
	installSuccessfulAtlasChainTwoRevision(
		t,
		conn,
		expandedFS,
		time.Date(2024, 1, 4, 0, 0, 0, 0, time.UTC),
	)

	_, recovering := newSQLiteAtlasChainMigrator(t, dbPath, expandedFS)
	recovering = recovering.WithExecOrder(migrator.ExecOrderNonLinear)
	c.Assert(recovering.MigrateUp(t.Context()), qt.IsNil)
	c.Assert(
		storedRevisionChecksum(t, conn, atlasChainThreeVersion),
		qt.Equals,
		atlasChainEntryHash(t, expandedFS, atlasChainThreeFile),
	)
}

func TestAtlasSumPtahRevisions_InteroperatesWithALaterExternalInsertion(t *testing.T) {
	c := qt.New(t)
	dbPath := filepath.Join(t.TempDir(), "external-insertion.db")
	conn, initial := newSQLiteAtlasChainMigrator(t, dbPath, atlasChainInitialFS(t))
	c.Assert(initial.MigrateUp(t.Context()), qt.IsNil)
	setAtlasChainAppliedAt(t, conn, atlasChainOneVersion, time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC))
	setAtlasChainAppliedAt(t, conn, atlasChainThreeVersion, time.Date(2024, 1, 3, 0, 0, 0, 0, time.UTC))

	firstInsertionFS := atlasChainExpandedFS(t, atlasChainTwoSQL)
	_, firstInsertion := newSQLiteAtlasChainMigrator(t, dbPath, firstInsertionFS)
	firstInsertion = firstInsertion.WithExecOrder(migrator.ExecOrderNonLinear)
	c.Assert(firstInsertion.MigrateUp(t.Context()), qt.IsNil)

	secondInsertionFS := atlasChainFS(t, map[string]string{
		atlasChainOneFile:    atlasChainOneSQL,
		atlasChainTwoFile:    atlasChainTwoSQL,
		atlasChainMiddleFile: atlasChainMiddleSQL,
		atlasChainThreeFile:  atlasChainThreeSQL,
	})
	_, err := conn.Exec(atlasChainMiddleSQL)
	c.Assert(err, qt.IsNil)
	_, err = conn.Exec(
		`INSERT INTO schema_migrations
(version, description, applied_at, state, applied, total, execution_time_ms, checksum)
VALUES (?, ?, ?, 'applied', 1, 1, 0, ?)`,
		int64(20240101750000),
		"middle",
		time.Date(2027, 1, 1, 0, 0, 0, 0, time.UTC),
		atlasChainEntryHash(t, secondInsertionFS, atlasChainMiddleFile),
	)
	c.Assert(err, qt.IsNil)

	_, interoperating := newSQLiteAtlasChainMigrator(t, dbPath, secondInsertionFS)
	interoperating = interoperating.WithExecOrder(migrator.ExecOrderNonLinear)
	c.Assert(interoperating.MigrateUp(t.Context()), qt.IsNil)
	c.Assert(
		storedRevisionChecksum(t, conn, atlasChainThreeVersion),
		qt.Equals,
		atlasChainEntryHash(t, secondInsertionFS, atlasChainThreeFile),
	)
}

func TestAtlasSumPtahRevisions_EqualHistoricalTimestampsFailClosed(t *testing.T) {
	c := qt.New(t)
	dbPath := filepath.Join(t.TempDir(), "equal-time.db")
	conn, initial := newSQLiteAtlasChainMigrator(t, dbPath, atlasChainInitialFS(t))
	t.Cleanup(func() { _ = conn.Close() })
	c.Assert(initial.MigrateUp(t.Context()), qt.IsNil)
	setAtlasChainAppliedAt(t, conn, atlasChainOneVersion, time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC))
	equalTime := time.Date(2024, 1, 3, 0, 0, 0, 0, time.UTC)
	setAtlasChainAppliedAt(t, conn, atlasChainThreeVersion, equalTime)
	expandedFS := atlasChainExpandedFS(t, atlasChainTwoSQL)
	installSuccessfulAtlasChainTwoRevision(t, conn, expandedFS, equalTime)

	_, ambiguous := newSQLiteAtlasChainMigrator(t, dbPath, expandedFS)
	ambiguous = ambiguous.WithExecOrder(migrator.ExecOrderNonLinear)
	var mismatch *migrator.ChecksumMismatchError
	err := ambiguous.MigrateUp(t.Context())
	c.Assert(err, qt.ErrorAs, &mismatch)
	c.Assert(mismatch.Version, qt.Equals, atlasChainThreeVersion)
}

func TestAtlasSumPtahRevisions_ZeroHistoricalTimestampFailsClosed(t *testing.T) {
	c := qt.New(t)
	dbPath := filepath.Join(t.TempDir(), "zero-time.db")
	conn, initial := newSQLiteAtlasChainMigrator(t, dbPath, atlasChainInitialFS(t))
	t.Cleanup(func() { _ = conn.Close() })
	c.Assert(initial.MigrateUp(t.Context()), qt.IsNil)
	setAtlasChainAppliedAt(t, conn, atlasChainOneVersion, time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC))
	setAtlasChainAppliedAt(t, conn, atlasChainThreeVersion, time.Date(2024, 1, 3, 0, 0, 0, 0, time.UTC))
	expandedFS := atlasChainExpandedFS(t, atlasChainTwoSQL)
	installSuccessfulAtlasChainTwoRevision(t, conn, expandedFS, time.Time{})

	_, ambiguous := newSQLiteAtlasChainMigrator(t, dbPath, expandedFS)
	ambiguous = ambiguous.WithExecOrder(migrator.ExecOrderNonLinear)
	var mismatch *migrator.ChecksumMismatchError
	err := ambiguous.MigrateUp(t.Context())
	c.Assert(err, qt.ErrorAs, &mismatch)
	c.Assert(mismatch.Version, qt.Equals, atlasChainThreeVersion)
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

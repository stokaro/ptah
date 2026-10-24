package migrator_test

import (
	"encoding/json"
	"path/filepath"
	"testing"
	"testing/fstest"
	"time"

	qt "github.com/frankban/quicktest"

	"ptah.run/dbschema"
	"ptah.run/migration/migrator"
)

// TestMigrationStatusRecordsWhatAPlanNeeds covers the contract a caller plans
// from: which file, what it hashes to, what the database recorded, and whether
// the two still agree.
func TestMigrationStatusRecordsWhatAPlanNeeds(t *testing.T) {
	c := qt.New(t)
	conn := sqliteConnection(c, "status-records.sqlite")
	mig, err := migrator.NewFSMigrator(conn, twoMigrationDirectory())
	c.Assert(err, qt.IsNil)

	before, err := mig.GetMigrationStatus(c.Context())
	c.Assert(err, qt.IsNil)
	c.Assert(before.ContractVersion, qt.Equals, migrator.StatusContractVersion)
	c.Assert(before.Migrations, qt.HasLen, 2)
	c.Assert(before.Migrations[0].State, qt.Equals, migrator.MigrationStatePending)
	c.Assert(before.Migrations[0].Version, qt.Equals, int64(1))
	c.Assert(before.Migrations[0].Description, qt.Equals, "First")
	c.Assert(before.Migrations[0].Checksum, qt.Not(qt.Equals), "")
	c.Assert(before.Migrations[0].AppliedChecksum, qt.Equals, "")
	c.Assert(before.Migrations[1].State, qt.Equals, migrator.MigrationStatePending)

	c.Assert(mig.MigrateUp(c.Context()), qt.IsNil)

	after, err := mig.GetMigrationStatus(c.Context())
	c.Assert(err, qt.IsNil)
	c.Assert(after.Migrations, qt.HasLen, 2)
	for _, record := range after.Migrations {
		c.Assert(record.State, qt.Equals, migrator.MigrationStateApplied)
		c.Assert(record.AppliedChecksum, qt.Not(qt.Equals), "")
	}
}

// TestMigrationStatusReportsAModifiedAppliedMigration is the refusal a
// versioned workflow depends on, reported as a fact rather than as an error a
// caller has to provoke.
func TestMigrationStatusReportsAModifiedAppliedMigration(t *testing.T) {
	c := qt.New(t)
	path := filepath.Join(c.TempDir(), "status-modified.sqlite")
	conn := sqliteConnectionAt(c, path)
	mig, err := migrator.NewFSMigrator(conn, twoMigrationDirectory())
	c.Assert(err, qt.IsNil)
	c.Assert(mig.MigrateUp(c.Context()), qt.IsNil)

	edited := twoMigrationDirectory()
	edited["0000000001_first.up.sql"] = &fstest.MapFile{
		Data: []byte("CREATE TABLE first (id INTEGER PRIMARY KEY, added TEXT);\n"),
	}
	rereadConn := sqliteConnectionAt(c, path)
	reread, err := migrator.NewFSMigrator(rereadConn, edited)
	c.Assert(err, qt.IsNil)

	status, err := reread.GetMigrationStatus(c.Context())

	c.Assert(err, qt.IsNil)
	c.Assert(status.Migrations[0].State, qt.Equals, migrator.MigrationStateModified)
	c.Assert(status.Migrations[1].State, qt.Equals, migrator.MigrationStateApplied)
}

// TestMigrationStatusReportsTheCheckpointBoundary separates a migration a
// fresh database will never run from one it has not run yet.
func TestMigrationStatusReportsTheCheckpointBoundary(t *testing.T) {
	c := qt.New(t)
	conn := sqliteConnection(c, "status-checkpoint.sqlite")
	mig, err := migrator.NewFSMigrator(conn, fstest.MapFS{
		"0000000001_first.up.sql":   {Data: []byte("CREATE TABLE first (id INTEGER PRIMARY KEY);\n")},
		"0000000001_first.down.sql": {Data: []byte("DROP TABLE first;\n")},
		"0000000002_snapshot.checkpoint.up.sql": {
			Data: []byte("CREATE TABLE first (id INTEGER PRIMARY KEY);\nCREATE TABLE second (id INTEGER PRIMARY KEY);\n"),
		},
		"0000000002_snapshot.checkpoint.down.sql": {
			Data: []byte("DROP TABLE second;\nDROP TABLE first;\n"),
		},
		"0000000003_third.up.sql":   {Data: []byte("CREATE TABLE third (id INTEGER PRIMARY KEY);\n")},
		"0000000003_third.down.sql": {Data: []byte("DROP TABLE third;\n")},
	})
	c.Assert(err, qt.IsNil)

	status, err := mig.GetMigrationStatus(c.Context())

	c.Assert(err, qt.IsNil)
	c.Assert(status.CheckpointVersion, qt.Equals, int64(2))
	c.Assert(status.Migrations[0].State, qt.Equals, migrator.MigrationStateCheckpointCovered)
	c.Assert(status.Migrations[1].State, qt.Equals, migrator.MigrationStatePending)
	c.Assert(status.Migrations[1].Checkpoint, qt.IsTrue)
	c.Assert(status.Migrations[2].State, qt.Equals, migrator.MigrationStatePending)
}

// TestMigrationStatusReportsTheDeclaredTransactionMode reads the mode off the
// file rather than off the migrator's global default, because a plan that has
// to survive a crash needs to know which statements shared a transaction.
func TestMigrationStatusReportsTheDeclaredTransactionMode(t *testing.T) {
	c := qt.New(t)
	conn := sqliteConnection(c, "status-txmode.sqlite")
	mig, err := migrator.NewFSMigrator(conn, fstest.MapFS{
		"0000000001_first.up.sql": {
			Data: []byte("-- atlas:txmode none\nCREATE TABLE first (id INTEGER PRIMARY KEY);\n"),
		},
		"0000000001_first.down.sql":  {Data: []byte("DROP TABLE first;\n")},
		"0000000002_second.up.sql":   {Data: []byte("CREATE TABLE second (id INTEGER PRIMARY KEY);\n")},
		"0000000002_second.down.sql": {Data: []byte("DROP TABLE second;\n")},
	})
	c.Assert(err, qt.IsNil)

	status, err := mig.GetMigrationStatus(c.Context())

	c.Assert(err, qt.IsNil)
	c.Assert(status.Migrations[0].TransactionMode, qt.Equals, "none")
	c.Assert(status.Migrations[1].TransactionMode, qt.Equals, "")
}

// TestMigrationStatusDocumentCarriesItsContractVersion pins what a consumer
// reads first: a document whose version it does not know is one it must refuse
// rather than one it may partially understand.
func TestMigrationStatusDocumentCarriesItsContractVersion(t *testing.T) {
	c := qt.New(t)
	conn := sqliteConnection(c, "status-document.sqlite")
	mig, err := migrator.NewFSMigrator(conn, twoMigrationDirectory())
	c.Assert(err, qt.IsNil)
	status, err := mig.GetMigrationStatus(c.Context())
	c.Assert(err, qt.IsNil)

	encoded, err := json.Marshal(status)

	c.Assert(err, qt.IsNil)
	c.Assert(string(encoded), qt.Contains, `"contract_version":1`)
	c.Assert(string(encoded), qt.Contains, `"state":"pending"`)
	var decoded migrator.MigrationStatus
	c.Assert(json.Unmarshal(encoded, &decoded), qt.IsNil)
	c.Assert(decoded.Migrations, qt.HasLen, 2)
	c.Assert(decoded.Migrations[0].Version, qt.Equals, int64(1))
}

func twoMigrationDirectory() fstest.MapFS {
	return fstest.MapFS{
		"0000000001_first.up.sql":    {Data: []byte("CREATE TABLE first (id INTEGER PRIMARY KEY);\n")},
		"0000000001_first.down.sql":  {Data: []byte("DROP TABLE first;\n")},
		"0000000002_second.up.sql":   {Data: []byte("CREATE TABLE second (id INTEGER PRIMARY KEY);\n")},
		"0000000002_second.down.sql": {Data: []byte("DROP TABLE second;\n")},
	}
}

func sqliteConnection(c *qt.C, name string) *dbschema.DatabaseConnection {
	c.Helper()
	return sqliteConnectionAt(c, filepath.Join(c.TempDir(), name))
}

func sqliteConnectionAt(c *qt.C, path string) *dbschema.DatabaseConnection {
	c.Helper()
	conn, err := dbschema.ConnectToDatabase(c.Context(), "sqlite://"+path)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { dbschema.CloseAndWarn(conn) })
	return conn
}

// TestMigrationStatusKeepsTheCheckpointBoundaryAfterTheBootstrap is
// stokaro/ptah#3356: the document contradicted itself once the checkpoint was
// applied. Its aggregate reported nothing pending, and its records reported the
// two migrations the checkpoint replaces as pending -- a reader that plans from
// the records replays DDL the snapshot already created.
//
// Applying the checkpoint is what the bootstrap did, and nothing about the
// migrations below it changed except that the checkpoint covering them is now
// recorded.
func TestMigrationStatusKeepsTheCheckpointBoundaryAfterTheBootstrap(t *testing.T) {
	c := qt.New(t)
	conn := sqliteConnection(c, "status-checkpoint-applied.sqlite")
	mig, err := migrator.NewFSMigrator(conn, checkpointBoundaryFS())
	c.Assert(err, qt.IsNil)
	c.Assert(mig.MigrateUp(c.Context()), qt.IsNil)

	status, err := mig.GetMigrationStatus(c.Context())

	c.Assert(err, qt.IsNil)
	c.Assert(status.PendingMigrations, qt.HasLen, 0)
	c.Assert(status.HasPendingChanges, qt.IsFalse)
	c.Assert(status.Migrations[0].State, qt.Equals, migrator.MigrationStateCheckpointCovered)
	c.Assert(status.Migrations[1].State, qt.Equals, migrator.MigrationStateApplied)
	c.Assert(status.Migrations[2].State, qt.Equals, migrator.MigrationStateApplied)
	// The field a reader uses to tell covered from missing answers for the
	// checkpoint that covers them, which is the one the database applied.
	c.Assert(status.CheckpointVersion, qt.Equals, int64(2))
}

// checkpointBoundaryFS is one migration below a checkpoint, the checkpoint, and
// one above it.
func checkpointBoundaryFS() fstest.MapFS {
	return fstest.MapFS{
		"0000000001_first.up.sql":   {Data: []byte("CREATE TABLE first (id INTEGER PRIMARY KEY);\n")},
		"0000000001_first.down.sql": {Data: []byte("DROP TABLE first;\n")},
		"0000000002_snapshot.checkpoint.up.sql": {
			Data: []byte("CREATE TABLE first (id INTEGER PRIMARY KEY);\nCREATE TABLE second (id INTEGER PRIMARY KEY);\n"),
		},
		"0000000002_snapshot.checkpoint.down.sql": {
			Data: []byte("DROP TABLE second;\nDROP TABLE first;\n"),
		},
		"0000000003_third.up.sql":   {Data: []byte("CREATE TABLE third (id INTEGER PRIMARY KEY);\n")},
		"0000000003_third.down.sql": {Data: []byte("DROP TABLE third;\n")},
	}
}

// TestMigrationStatusRecordsWhenEachRowWasWritten reads the time and duration
// from the revision row itself: the values written into the table are the
// values the document reports.
func TestMigrationStatusRecordsWhenEachRowWasWritten(t *testing.T) {
	c := qt.New(t)
	conn := sqliteConnection(c, "status-timing.sqlite")
	mig, err := migrator.NewFSMigrator(conn, twoMigrationDirectory())
	c.Assert(err, qt.IsNil)
	c.Assert(mig.MigrateUp(c.Context()), qt.IsNil)
	_, err = conn.ExecContext(c.Context(),
		"UPDATE schema_migrations SET applied_at = '2026-10-24 07:42:30', execution_time_ms = 1840 WHERE version = 1")
	c.Assert(err, qt.IsNil)

	status, err := mig.GetMigrationStatus(c.Context())

	c.Assert(err, qt.IsNil)
	c.Assert(status.Migrations[0].AppliedAt, qt.IsNotNil)
	c.Assert(status.Migrations[0].AppliedAt.Equal(time.Date(2026, 10, 24, 7, 42, 30, 0, time.UTC)), qt.IsTrue,
		qt.Commentf("applied_at %s", status.Migrations[0].AppliedAt))
	c.Assert(status.Migrations[0].AppliedAt.Location(), qt.Equals, time.UTC)
	c.Assert(status.Migrations[0].ExecutionMs, qt.DeepEquals, new(int64(1840)))
	c.Assert(status.Migrations[1].AppliedAt, qt.IsNotNil)
	c.Assert(status.Migrations[1].ExecutionMs, qt.IsNotNil)
}

// TestMigrationStatusLeavesTimingOutWhereNoRowExists keeps a pending
// migration's time and duration absent rather than zero, in the document as
// well as in the record.
func TestMigrationStatusLeavesTimingOutWhereNoRowExists(t *testing.T) {
	c := qt.New(t)
	conn := sqliteConnection(c, "status-timing-pending.sqlite")
	mig, err := migrator.NewFSMigrator(conn, twoMigrationDirectory())
	c.Assert(err, qt.IsNil)

	status, err := mig.GetMigrationStatus(c.Context())

	c.Assert(err, qt.IsNil)
	c.Assert(status.Migrations[0].State, qt.Equals, migrator.MigrationStatePending)
	c.Assert(status.Migrations[0].AppliedAt, qt.IsNil)
	c.Assert(status.Migrations[0].ExecutionMs, qt.IsNil)
	document, err := json.Marshal(status.Migrations[0])
	c.Assert(err, qt.IsNil)
	c.Assert(string(document), qt.Not(qt.Contains), `"applied_at"`)
	c.Assert(string(document), qt.Not(qt.Contains), `"execution_time_ms"`)
}

// TestMigrationStatusReportsNoDurationALegacyTableNeverRecorded reads a
// revision table from before Ptah recorded durations: the row's time is
// there, and a duration of zero would be invented.
func TestMigrationStatusReportsNoDurationALegacyTableNeverRecorded(t *testing.T) {
	c := qt.New(t)
	conn := sqliteConnection(c, "status-timing-legacy.sqlite")
	_, err := conn.ExecContext(c.Context(), `CREATE TABLE schema_migrations (
		version BIGINT PRIMARY KEY,
		description TEXT NOT NULL,
		applied_at TIMESTAMP NOT NULL
	)`)
	c.Assert(err, qt.IsNil)
	_, err = conn.ExecContext(c.Context(),
		"INSERT INTO schema_migrations (version, description, applied_at) VALUES (1, 'First', '2026-10-24 07:42:30')")
	c.Assert(err, qt.IsNil)
	conn.SchemaWriter().SetDryRun(true)
	mig, err := migrator.NewFSMigrator(conn, twoMigrationDirectory())
	c.Assert(err, qt.IsNil)

	status, err := mig.GetMigrationStatus(c.Context())

	c.Assert(err, qt.IsNil)
	c.Assert(status.Migrations[0].State, qt.Equals, migrator.MigrationStateApplied)
	c.Assert(status.Migrations[0].AppliedAt, qt.IsNotNil)
	c.Assert(status.Migrations[0].AppliedAt.Equal(time.Date(2026, 10, 24, 7, 42, 30, 0, time.UTC)), qt.IsTrue)
	c.Assert(status.Migrations[0].ExecutionMs, qt.IsNil)
}

// TestMigrationStatusTimesADirtyAndAMissingRow reports the rows that are not
// a clean applied migration with the time and duration they recorded.
func TestMigrationStatusTimesADirtyAndAMissingRow(t *testing.T) {
	c := qt.New(t)
	path := filepath.Join(c.TempDir(), "status-timing-dirty.sqlite")
	failing := twoMigrationDirectory()
	failing["0000000002_second.up.sql"] = &fstest.MapFile{
		Data: []byte("CREATE TABLE second (id INTEGER PRIMARY KEY);\nINSERT INTO absent VALUES (1);\n"),
	}
	mig, err := migrator.NewFSMigrator(sqliteConnectionAt(c, path), failing)
	c.Assert(err, qt.IsNil)
	c.Assert(mig.MigrateUp(c.Context()), qt.IsNotNil)

	dirty, err := mig.GetMigrationStatus(c.Context())

	c.Assert(err, qt.IsNil)
	c.Assert(dirty.Migrations[1].State, qt.Equals, migrator.MigrationStateDirty)
	c.Assert(dirty.Migrations[1].AppliedAt, qt.IsNotNil)
	c.Assert(dirty.Migrations[1].ExecutionMs, qt.IsNotNil)

	conn := sqliteConnection(c, "status-timing-missing.sqlite")
	applied, err := migrator.NewFSMigrator(conn, twoMigrationDirectory())
	c.Assert(err, qt.IsNil)
	c.Assert(applied.MigrateUp(c.Context()), qt.IsNil)
	shortened := twoMigrationDirectory()
	delete(shortened, "0000000002_second.up.sql")
	delete(shortened, "0000000002_second.down.sql")
	reread, err := migrator.NewFSMigrator(conn, shortened)
	c.Assert(err, qt.IsNil)

	missing, err := reread.GetMigrationStatus(c.Context())

	c.Assert(err, qt.IsNil)
	c.Assert(missing.MissingMigrations, qt.HasLen, 1)
	c.Assert(missing.MissingMigrations[0].AppliedAt, qt.IsNotNil)
	c.Assert(missing.MissingMigrations[0].ExecutionMs, qt.IsNotNil)
}

// TestMigrationStatusTimesAnAtlasHistory reads an Atlas revision table, which
// records its time as executed_at and its duration as execution_time.
func TestMigrationStatusTimesAnAtlasHistory(t *testing.T) {
	c := qt.New(t)
	dir := writeAtlasDirectory(c)
	conn := sqliteConnection(c, "status-timing-atlas.db")
	mig := atlasMigratorOver(c, dir, conn)
	c.Assert(mig.MigrateUp(c.Context()), qt.IsNil)
	_, err := conn.ExecContext(c.Context(),
		"UPDATE atlas_schema_revisions SET executed_at = '2026-10-24 07:42:30', execution_time = 1840000000 WHERE version = '20240101000000'")
	c.Assert(err, qt.IsNil)

	status, err := atlasMigratorOver(c, dir, conn).GetMigrationStatus(c.Context())

	c.Assert(err, qt.IsNil)
	c.Assert(status.Migrations, qt.HasLen, 2)
	c.Assert(status.Migrations[0].AppliedAt, qt.IsNotNil)
	c.Assert(status.Migrations[0].AppliedAt.Equal(time.Date(2026, 10, 24, 7, 42, 30, 0, time.UTC)), qt.IsTrue,
		qt.Commentf("applied_at %s", status.Migrations[0].AppliedAt))
	c.Assert(status.Migrations[0].ExecutionMs, qt.DeepEquals, new(int64(1840)))
}

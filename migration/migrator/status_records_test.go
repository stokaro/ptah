package migrator_test

import (
	"encoding/json"
	"path/filepath"
	"testing"
	"testing/fstest"

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

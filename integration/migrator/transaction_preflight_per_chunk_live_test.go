//go:build integration

package migrator_test

import (
	"context"
	"fmt"
	"testing"
	"testing/fstest"
	"time"

	qt "github.com/frankban/quicktest"

	"ptah.run/dbschema"
	"ptah.run/internal/dbtarget"
	"ptah.run/migration/migrator"
)

// A TimescaleDB per-chunk index build commits one transaction per chunk, so
// the server refuses it inside the transaction a migration file runs in
// (stokaro/ptah#3587). The preflight names the file, the statement and the
// marker before anything is sent, as it does for CONCURRENTLY.

const perChunkEvents = "CREATE TABLE pc_events (id bigint NOT NULL, happened_at timestamptz NOT NULL, kind text NOT NULL);\n" +
	"SELECT create_hypertable('pc_events', 'happened_at');\n"

const createTimescale = "CREATE EXTENSION IF NOT EXISTS timescaledb"

const perChunkIndex = "CREATE INDEX pc_events_kind_idx ON pc_events (kind) WITH (timescaledb.transaction_per_chunk);\n"

// perChunkConnection opens a database of this test's own on the server
// adminURL names, after running setup in it. Setup runs on a connection of its
// own, before the migrator's opens, because that connection reads
// pg_extension once, when it opens, to decide whether the server has
// hypertables.
func perChunkConnection(c *qt.C, adminURL, setup string) *dbschema.DatabaseConnection {
	c.Helper()
	admin, err := dbschema.ConnectToDatabase(c.Context(), adminURL)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(admin)

	database := fmt.Sprintf("ptah_pc_%d", time.Now().UnixNano())
	_, err = admin.ExecContext(c.Context(), `CREATE DATABASE "`+database+`"`)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() {
		cleanup, cleanupErr := dbschema.ConnectToDatabase(context.Background(), adminURL)
		c.Check(cleanupErr, qt.IsNil)
		defer dbschema.CloseAndWarn(cleanup)
		_, dropErr := cleanup.ExecContext(context.Background(), `DROP DATABASE IF EXISTS "`+database+`" WITH (FORCE)`)
		c.Check(dropErr, qt.IsNil)
	})

	databaseURL := preflightDatabaseURL(c, adminURL, database)
	seed, err := dbschema.ConnectToDatabase(c.Context(), databaseURL)
	c.Assert(err, qt.IsNil)
	_, err = seed.ExecContext(c.Context(), setup)
	c.Assert(err, qt.IsNil)
	dbschema.CloseAndWarn(seed)

	conn, err := dbschema.ConnectToDatabase(c.Context(), databaseURL)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { dbschema.CloseAndWarn(conn) })
	return conn
}

func migrateUpOnce(c *qt.C, conn *dbschema.DatabaseConnection, up string) error {
	c.Helper()
	mig, err := migrator.NewFSMigrator(conn, fstest.MapFS{
		"0000000001_a.up.sql":   &fstest.MapFile{Data: []byte(up)},
		"0000000001_a.down.sql": &fstest.MapFile{Data: []byte("SELECT 1;\n")},
	})
	c.Assert(err, qt.IsNil)
	c.Assert(mig.Initialize(c.Context()), qt.IsNil)
	return mig.MigrateUp(c.Context())
}

func TestTransactionPreflight_RefusesAPerChunkBuildBeforeReachingTimescaleDB(t *testing.T) {
	c := qt.New(t)
	conn := perChunkConnection(c, dbtarget.URL(c, dbtarget.TimescaleDB), createTimescale)

	err := migrateUpOnce(c, conn, perChunkEvents+perChunkIndex)

	c.Assert(err, qt.IsNotNil)
	c.Assert(err.Error(), qt.Contains, "cannot run inside a transaction: line 3 ")
	c.Assert(err.Error(), qt.Contains, "WITH (timescaledb.transaction_per_chunk) commits one transaction per chunk")
	c.Assert(err.Error(), qt.Contains, "no_transaction")
	// The server was never asked: its refusal carries a SQLSTATE, and the
	// table the file's first statement creates does not exist.
	c.Assert(err.Error(), qt.Not(qt.Contains), "SQLSTATE")
	c.Assert(preflightRelationExists(c, conn, "pc_events"), qt.IsFalse)
}

// TestTransactionPreflight_TimescaleDBRefusesAPerChunkBuildInATransaction pins
// that the preflight is the server's rule: the same statements inside one
// explicit transaction are refused by TimescaleDB itself.
func TestTransactionPreflight_TimescaleDBRefusesAPerChunkBuildInATransaction(t *testing.T) {
	c := qt.New(t)
	conn := perChunkConnection(c, dbtarget.URL(c, dbtarget.TimescaleDB), createTimescale)

	_, err := conn.ExecContext(c.Context(), "BEGIN; "+perChunkEvents+perChunkIndex+" COMMIT;")

	c.Assert(err, qt.ErrorMatches, `(?s).*cannot run inside a transaction block.*`)
}

// TestTransactionPreflight_AppliesAPerChunkBuildMarkedNoTransaction is the
// control for the remedy the refusal names: the marker takes the file out of
// the transaction, and the server builds the index.
func TestTransactionPreflight_AppliesAPerChunkBuildMarkedNoTransaction(t *testing.T) {
	c := qt.New(t)
	conn := perChunkConnection(c, dbtarget.URL(c, dbtarget.TimescaleDB), createTimescale)

	err := migrateUpOnce(c, conn, "-- +ptah no_transaction\n"+perChunkEvents+perChunkIndex)

	c.Assert(err, qt.IsNil)
	var valid bool
	c.Assert(conn.QueryRowContext(c.Context(),
		"SELECT indisvalid FROM pg_index WHERE indexrelid = 'pc_events_kind_idx'::regclass").Scan(&valid), qt.IsNil)
	c.Assert(valid, qt.IsTrue)
}

// TestTransactionPreflight_LeavesThePerChunkParameterToAServerWithoutTimescaleDB
// is the control for the capability the preflight is keyed on. PostgreSQL
// without the extension refuses the storage parameter itself, in a
// transaction or out of one, so telling the operator to take the file out of
// the transaction would send them to a second refusal. The preflight stays
// silent and the server's own answer is what the operator reads.
func TestTransactionPreflight_LeavesThePerChunkParameterToAServerWithoutTimescaleDB(t *testing.T) {
	c := qt.New(t)
	conn := perChunkConnection(c, dbtarget.URL(c, dbtarget.PostgreSQL), "SELECT 1")

	err := migrateUpOnce(c, conn,
		"CREATE TABLE pc_plain (kind text);\n"+
			"CREATE INDEX pc_plain_kind_idx ON pc_plain (kind) WITH (timescaledb.transaction_per_chunk);\n")

	c.Assert(err, qt.ErrorMatches, `(?s).*unrecognized parameter namespace "timescaledb".*`)
	c.Assert(err.Error(), qt.Not(qt.Contains), "cannot run inside a transaction")
}

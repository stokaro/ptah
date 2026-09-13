//go:build integration

package migrator_test

import (
	"context"
	"testing"
	"testing/fstest"

	qt "github.com/frankban/quicktest"

	"ptah.run/dbschema"
	"ptah.run/internal/dbtarget"
	"ptah.run/migration/migrator"
)

// The reach a false capability denied. `capability.SQLServer2022` carried
// `TransactionalDDL: false`, so `--tx-mode all` answered `this target commits
// schema changes as they run` -- a sentence about an engine that does no such
// thing. The tx-mode all batch is dialect-neutral, so what refused the target
// was the key rather than anything in the migrator or the server
// (stokaro/ptah#3192).
//
// Both directions are here, because either alone is satisfied by a build that
// does the wrong thing. A batch that commits says nothing about rollback, and a
// batch that rolls everything back is also what a target does when it never
// opened the transaction.

// TestMigrateUp_SQLServerCommitsATxModeAllBatch is the forward half: two
// migrations under one transaction, both tables present afterwards.
func TestMigrateUp_SQLServerCommitsATxModeAllBatch(t *testing.T) {
	dbURL := dbtarget.URL(t, dbtarget.SQLServer)
	c := qt.New(t)
	ctx := context.Background()

	conn, err := dbschema.ConnectToDatabase(ctx, dbURL)
	c.Assert(err, qt.IsNil)
	defer func() { _ = conn.Close() }()

	dropSQLServerTxModeAllFixture(c, conn)
	defer dropSQLServerTxModeAllFixture(c, conn)

	fsys := fstest.MapFS{
		"0000000001_first.up.sql": &fstest.MapFile{
			Data: []byte("CREATE TABLE ptah_mssql_txall_first (id INT PRIMARY KEY);"),
		},
		"0000000001_first.down.sql": &fstest.MapFile{
			Data: []byte("DROP TABLE ptah_mssql_txall_first;"),
		},
		"0000000002_second.up.sql": &fstest.MapFile{
			Data: []byte("CREATE TABLE ptah_mssql_txall_second (id INT PRIMARY KEY);"),
		},
		"0000000002_second.down.sql": &fstest.MapFile{
			Data: []byte("DROP TABLE ptah_mssql_txall_second;"),
		},
	}

	mig, err := migrator.NewFSMigrator(conn, fsys)
	c.Assert(err, qt.IsNil)
	c.Assert(mig.WithTransactionMode(migrator.MigrationTxModeAll).MigrateUp(ctx), qt.IsNil)

	c.Assert(sqlServerTableCount(c, conn, "ptah_mssql_txall_first"), qt.Equals, 1)
	c.Assert(sqlServerTableCount(c, conn, "ptah_mssql_txall_second"), qt.Equals, 1)
}

// TestMigrateUp_SQLServerRollsATxModeAllBatchBackAsAUnit is the half the
// capability actually promises: the second migration fails, and the first one's
// table is gone with it.
func TestMigrateUp_SQLServerRollsATxModeAllBatchBackAsAUnit(t *testing.T) {
	dbURL := dbtarget.URL(t, dbtarget.SQLServer)
	c := qt.New(t)
	ctx := context.Background()

	conn, err := dbschema.ConnectToDatabase(ctx, dbURL)
	c.Assert(err, qt.IsNil)
	defer func() { _ = conn.Close() }()

	dropSQLServerTxModeAllFixture(c, conn)
	defer dropSQLServerTxModeAllFixture(c, conn)

	// The second migration recreates the table the first one made, so the
	// server refuses it after the batch has already run real DDL.
	fsys := fstest.MapFS{
		"0000000001_first.up.sql": &fstest.MapFile{
			Data: []byte("CREATE TABLE ptah_mssql_txall_first (id INT PRIMARY KEY);"),
		},
		"0000000001_first.down.sql": &fstest.MapFile{
			Data: []byte("DROP TABLE ptah_mssql_txall_first;"),
		},
		"0000000002_second.up.sql": &fstest.MapFile{
			Data: []byte("CREATE TABLE ptah_mssql_txall_first (id INT PRIMARY KEY);"),
		},
		"0000000002_second.down.sql": &fstest.MapFile{
			Data: []byte("DROP TABLE ptah_mssql_txall_first;"),
		},
	}

	mig, err := migrator.NewFSMigrator(conn, fsys)
	c.Assert(err, qt.IsNil)

	err = mig.WithTransactionMode(migrator.MigrationTxModeAll).MigrateUp(ctx)
	c.Assert(err, qt.ErrorMatches, `(?s).*already an object named 'ptah_mssql_txall_first'.*`)

	c.Assert(sqlServerTableCount(c, conn, "ptah_mssql_txall_first"), qt.Equals, 0)
}

// sqlServerTableCount answers how many user tables carry the name, which is the
// only question either test asks of the catalog.
func sqlServerTableCount(c *qt.C, conn *dbschema.DatabaseConnection, name string) int {
	c.Helper()
	var tables int
	err := conn.QueryRowContext(context.Background(),
		"SELECT count(*) FROM sys.tables WHERE name = @p1", name).Scan(&tables)
	c.Assert(err, qt.IsNil)
	return tables
}

// dropSQLServerTxModeAllFixture removes what either test may have left, and is
// called before the run as well as after it: the failure-path test runs against
// a shared server, so an earlier abandoned run must not decide this one.
func dropSQLServerTxModeAllFixture(c *qt.C, conn *dbschema.DatabaseConnection) {
	c.Helper()
	ctx := context.Background()
	for _, statement := range []string{
		"DROP TABLE IF EXISTS ptah_mssql_txall_first",
		"DROP TABLE IF EXISTS ptah_mssql_txall_second",
		"DROP TABLE IF EXISTS schema_migrations",
	} {
		_, _ = conn.ExecContext(ctx, statement)
	}
}

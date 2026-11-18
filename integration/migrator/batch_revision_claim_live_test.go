//go:build integration

package migrator_test

import (
	"context"
	"path/filepath"
	"testing"
	"testing/fstest"
	"time"

	qt "github.com/frankban/quicktest"

	"ptah.run/dbschema"
	"ptah.run/internal/dbtarget"
	"ptah.run/migration/migrator"
)

// batchClaimEngine is an engine that runs --tx-mode all, which needs schema
// changes that roll back.
type batchClaimEngine struct {
	name string
	// target creates a database of the test's own and returns its address.
	target func(c *qt.C, t *testing.T) string
	// tables counts the tables named widgets.
	tables string
}

var batchClaimEngines = []batchClaimEngine{
	{
		name: "postgres",
		target: func(c *qt.C, t *testing.T) string {
			return repeatableScratchDatabaseURL(c, t, repeatablePostgres)
		},
		tables: "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = 'widgets'",
	},
	{
		name: "sqlserver",
		target: func(c *qt.C, _ *testing.T) string {
			return sqlServerScratchDatabaseURL(c, dbtarget.SQLServer, "ptah_rt_batch")
		},
		tables: "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = 'widgets'",
	},
	{
		name: "sqlite file",
		target: func(_ *qt.C, t *testing.T) string {
			return "sqlite://" + filepath.Join(t.TempDir(), "batch.db")
		},
		tables: "SELECT COUNT(*) FROM sqlite_master WHERE name = 'widgets'",
	},
	{
		// An in-memory database is one connection, which the transaction holds.
		name:   "sqlite memory",
		target: func(*qt.C, *testing.T) string { return "sqlite:///:memory:" },
		tables: "SELECT COUNT(*) FROM sqlite_master WHERE name = 'widgets'",
	},
}

// Under --tx-mode all the revision row is written inside the batch
// transaction after the body, so a row already there is met after every
// statement of the body ran. Here the body writes the row itself, which is
// what another run's committed row looks like to the INSERT. The run must not
// report ErrRevisionClaimLost, whose promise is that no statement of the body
// ran, and must not read the row back through a connection other than the
// transaction, which waits for that transaction on SQL Server and on an
// in-memory SQLite database. The rollback leaves neither the body's table nor
// a revision row.
func TestBatchRevisionClaimLive_FailurePath(t *testing.T) {
	for _, engine := range batchClaimEngines {
		t.Run(engine.name, func(t *testing.T) {
			c := qt.New(t)
			conn, err := dbschema.ConnectToDatabase(context.Background(), engine.target(c, t))
			c.Assert(err, qt.IsNil)
			c.Cleanup(func() { dbschema.CloseAndWarn(conn) })
			run, err := migrator.NewFSMigrator(conn, fstest.MapFS{
				"0000000001_widgets.up.sql": {Data: []byte("CREATE TABLE widgets (id INTEGER PRIMARY KEY);\n" +
					"INSERT INTO schema_migrations (version, description, applied_at) VALUES (1, 'written by the body', CURRENT_TIMESTAMP);\n")},
				"0000000001_widgets.down.sql": {Data: []byte("DROP TABLE widgets;\n")},
			})
			c.Assert(err, qt.IsNil)

			ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
			c.Cleanup(cancel)

			err = run.WithTransactionMode(migrator.MigrationTxModeAll).MigrateUp(ctx)

			c.Assert(err, qt.Not(qt.ErrorIs), migrator.ErrRevisionClaimLost)
			c.Assert(err, qt.ErrorMatches, `(?s).*failed to record migration 1 in tx-mode all transaction: `+
				`failed to record pending migration 1: a revision row for this migration appeared after this run read `+
				`the revision table, and the tx-mode all transaction rolls back: .*`)
			c.Assert(readOne(c, conn, engine.tables), qt.Equals, "0")
			c.Assert(readOne(c, conn, "SELECT COUNT(*) FROM schema_migrations"), qt.Equals, "0")
		})
	}
}

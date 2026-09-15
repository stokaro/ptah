//go:build integration

package migrator_test

import (
	"context"
	"testing"
	"testing/fstest"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform/capability"
	"ptah.run/dbschema"
	"ptah.run/internal/dbtarget"
	"ptah.run/migration/migrator"
)

// capability.TransactionalDDL is what `--tx-mode all` promises a user, and
// almost nothing asks a server whether the promise is true. The key is a
// hand-written literal per preset; the probe declares it undecidable, because
// no single accepted statement can answer it; and the only live measurement of
// it is the SQL Server pair in sqlserver_tx_mode_all_live_test.go
// (stokaro/ptah#3192). So a preset whose literal is wrong -- inherited from a
// family it does not belong to, or left behind when a server changed -- reads
// green everywhere. YugabyteDB, which takes the key from Postgres16 and has
// never been measured on it, is the standing example.
//
// The measurement is the key's own sentence: run one migration whose second
// statement fails, and look for the first statement's table afterwards. A
// target that rolls schema changes back as a unit has none; a target that
// commits them as they run keeps it. Every row runs the same migration through
// the same default per-file transaction mode, so the answer comes from the
// engine and from nothing in the test.

// txDDLFirstTable is created by the first statement of the fixture migration
// and is the only thing either test reads back. The second statement recreates
// it, which every one of these engines refuses after real DDL has already run.
const txDDLFirstTable = "ptah_g4_txddl_first"

// txDDLRevisionTable keeps this fixture off schema_migrations. The servers are
// shared, and dropping the default revision table would take another package's
// run with it.
const txDDLRevisionTable = "ptah_g4_txddl_revisions"

// txDDLRow is one engine, the capability key the connected server resolves to,
// and what that server actually left behind.
//
// Both facts are data because the test exists to prove they agree. Reading the
// key off the connection and asserting only the catalog would pass on a build
// where both had drifted the same way.
type txDDLRow struct {
	name string
	// engine names the address; dbtarget is the only place that mapping lives.
	engine dbtarget.Engine
	// declared is capability.TransactionalDDL as the live connection resolves
	// it, which for a version-laddered preset depends on the server's banner.
	// CockroachDB is the one row where that matters: it rolls DDL back through
	// v24 and stops at v25, so this row states the answer for the v25.4 and
	// v26.3 lines the fleet and the workflow run (stokaro/ptah#1849).
	declared bool
	// wantFirstTableSurvives is what the catalog holds after the migration
	// failed, which is the sentence the key makes about the engine.
	wantFirstTableSurvives bool
}

// TestMigrateUp_TransactionalDDLKeyMatchesTheLiveEngine measures the key
// against the server that decides it.
func TestMigrateUp_TransactionalDDLKeyMatchesTheLiveEngine(t *testing.T) {
	rows := []txDDLRow{{
		name:                   "postgresql rolls the whole migration back",
		engine:                 dbtarget.PostgreSQL,
		declared:               true,
		wantFirstTableSurvives: false,
	}, {
		name:                   "sqlserver rolls the whole migration back",
		engine:                 dbtarget.SQLServer,
		declared:               true,
		wantFirstTableSurvives: false,
	}, {
		name:                   "mysql commits the first statement as it runs",
		engine:                 dbtarget.MySQL,
		declared:               false,
		wantFirstTableSurvives: true,
	}, {
		name:                   "mariadb commits the first statement as it runs",
		engine:                 dbtarget.MariaDB,
		declared:               false,
		wantFirstTableSurvives: true,
	}, {
		name:                   "cockroachdb commits before the schema statement",
		engine:                 dbtarget.CockroachDB,
		declared:               false,
		wantFirstTableSurvives: true,
	}}

	for _, row := range rows {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)
			ctx := context.Background()
			conn := openTxDDLTarget(c, row.engine)

			info := conn.Info()
			c.Assert(info.Capabilities.Has(capability.TransactionalDDL), qt.Equals, row.declared,
				qt.Commentf("dialect=%q version=%q", info.Dialect, info.Version))

			mig, err := migrator.NewFSMigrator(conn, txDDLFailingMigration())
			c.Assert(err, qt.IsNil)

			err = mig.WithMigrationsTable("", txDDLRevisionTable).MigrateUp(ctx)
			c.Assert(err, qt.IsNotNil)

			c.Assert(txDDLFirstTableExists(c, conn), qt.Equals, row.wantFirstTableSurvives,
				qt.Commentf("dialect=%q version=%q apply error: %v", info.Dialect, info.Version, err))
		})
	}
}

// TestMigrateUp_TxModeAllIsRefusedWhereTheServerCommitsAsItRuns is the consumer
// half: the same key decides whether `--tx-mode all` is offered at all.
//
// It is live rather than offline because the value the migrator reads is
// resolved from the server's banner, not from the dialect name. CockroachDB is
// the row that shows the difference -- the same dialect accepts this mode on a
// v24 server and is refused on a v25 one -- and an offline test pinned to
// capability.ForDialect can never see which of the two a connection got.
func TestMigrateUp_TxModeAllIsRefusedWhereTheServerCommitsAsItRuns(t *testing.T) {
	rows := []struct {
		name    string
		engine  dbtarget.Engine
		wantErr string
	}{{
		name:    "mysql",
		engine:  dbtarget.MySQL,
		wantErr: `(?s).*tx-mode all is not supported for dialect "mysql": this target commits schema changes as they run, so a failed migration cannot be rolled back as a unit.*`,
	}, {
		name:    "mariadb",
		engine:  dbtarget.MariaDB,
		wantErr: `(?s).*tx-mode all is not supported for dialect "mariadb": this target commits schema changes as they run, so a failed migration cannot be rolled back as a unit.*`,
	}, {
		name:    "cockroachdb",
		engine:  dbtarget.CockroachDB,
		wantErr: `(?s).*tx-mode all is not supported for dialect "cockroachdb": this target commits schema changes as they run, so a failed migration cannot be rolled back as a unit.*`,
	}}

	for _, row := range rows {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)
			ctx := context.Background()
			conn := openTxDDLTarget(c, row.engine)

			mig, err := migrator.NewFSMigrator(conn, txDDLFailingMigration())
			c.Assert(err, qt.IsNil)

			err = mig.WithMigrationsTable("", txDDLRevisionTable).
				WithTransactionMode(migrator.MigrationTxModeAll).
				MigrateUp(ctx)
			c.Assert(err, qt.ErrorMatches, row.wantErr)

			// The refusal leaves the target as it found it. The message
			// alone would also be produced by a build that refused after
			// executing the first statement, and the sibling test above shows
			// this same helper answering true on this same engine, so the
			// assertion discriminates rather than restating the line before.
			c.Assert(txDDLFirstTableExists(c, conn), qt.IsFalse)
		})
	}
}

// txDDLFailingMigration is one migration of two statements whose second one
// every target here refuses, after the first has already created a table.
//
// Recreating the same table is the failure because it needs no engine-specific
// grammar: the statement that fails is the same text on all five, so a row that
// behaves differently differs about rollback and not about what it rejected.
func txDDLFailingMigration() fstest.MapFS {
	create := "CREATE TABLE " + txDDLFirstTable + " (id INT NOT NULL PRIMARY KEY);"
	return fstest.MapFS{
		"0000000001_first.up.sql": &fstest.MapFile{
			Data: []byte(create + "\n" + create + "\n"),
		},
		"0000000001_first.down.sql": &fstest.MapFile{
			Data: []byte("DROP TABLE " + txDDLFirstTable + ";"),
		},
	}
}

// openTxDDLTarget connects to a live engine and leaves the fixture removed on
// both sides of the test.
//
// Cleaning before the run as well as after it is deliberate: these servers are
// shared and long-lived, so a run abandoned halfway would otherwise decide the
// next one's answer.
func openTxDDLTarget(c *qt.C, engine dbtarget.Engine) *dbschema.DatabaseConnection {
	c.Helper()

	dbURL := dbtarget.URL(c, engine)
	conn, err := dbschema.ConnectToDatabase(context.Background(), dbURL)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { _ = conn.Close() })

	dropTxDDLFixture(c, conn)
	c.Cleanup(func() { dropTxDDLFixture(c, conn) })
	return conn
}

// dropTxDDLFixture removes everything either test creates.
func dropTxDDLFixture(c *qt.C, conn *dbschema.DatabaseConnection) {
	c.Helper()

	ctx := context.Background()
	for _, table := range []string{txDDLFirstTable, txDDLRevisionTable} {
		_, _ = conn.ExecContext(ctx, "DROP TABLE IF EXISTS "+table)
	}
}

// txDDLFirstTableQuery is the one catalog question either test asks.
//
// information_schema.tables is the spelling all five engines answer, and the
// name is unique enough to need no schema filter -- which matters on the MySQL
// family, where that view spans every database on the server.
const txDDLFirstTableQuery = "SELECT count(*) FROM information_schema.tables " +
	"WHERE table_name = '" + txDDLFirstTable + "'"

// txDDLFirstTableExists reports whether the first statement's table is there.
func txDDLFirstTableExists(c *qt.C, conn *dbschema.DatabaseConnection) bool {
	c.Helper()

	var found int
	err := conn.QueryRowContext(context.Background(), txDDLFirstTableQuery).Scan(&found)
	c.Assert(err, qt.IsNil)
	return found > 0
}

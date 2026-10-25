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
	"ptah.run/migration/migrator"
)

// These tests drive [migrator.Migrator.WithOwnTransactions] on PostgreSQL: a
// file written for a runner that wraps nothing, as golang-migrate's are, runs
// the way that runner runs it. A file with its own BEGIN and COMMIT runs
// outside any transaction of the migrator's, and a file whose one statement
// PostgreSQL refuses inside a transaction runs without one (stokaro/ptah#4962).
//
// What the server decides is read back from it: which tables exist, whether
// an index is valid, and what the revision row records.

// ownTransactionsRun is one migration applied over a fresh migrations table,
// with the tables the body names suffixed so runs do not collide.
type ownTransactionsRun struct {
	conn     *dbschema.DatabaseConnection
	table    string
	suffix   string
	provider migrator.MigrationProvider
}

// newOwnTransactionsRun connects to the PostgreSQL dbURL names and loads a
// directory whose first migration creates base and whose second holds body.
// Every %[1]s in body is the run's suffix.
func newOwnTransactionsRun(c *qt.C, dbURL, body string) *ownTransactionsRun {
	c.Helper()
	ctx := context.Background()
	conn, err := dbschema.ConnectToDatabase(ctx, dbURL)
	c.Assert(err, qt.IsNil)
	suffix := fmt.Sprintf("%d", time.Now().UnixNano())
	run := &ownTransactionsRun{conn: conn, table: "schema_migrations_owntx_" + suffix, suffix: suffix}
	c.Cleanup(func() {
		for _, table := range []string{"owntx_base_", "owntx_a_", "owntx_b_", "owntx_c_"} {
			_, _ = conn.ExecContext(context.WithoutCancel(ctx), "DROP TABLE IF EXISTS "+table+suffix+" CASCADE")
		}
		_, _ = conn.ExecContext(context.WithoutCancel(ctx), "DROP TABLE IF EXISTS "+run.table)
		dbschema.CloseAndWarn(conn)
	})
	provider, err := migrator.NewFSMigrationProvider(fstest.MapFS{
		"0000000001_base.up.sql":   {Data: fmt.Appendf(nil, "CREATE TABLE owntx_base_%s (id int);\n", suffix)},
		"0000000001_base.down.sql": {Data: fmt.Appendf(nil, "DROP TABLE owntx_base_%s;\n", suffix)},
		"0000000002_body.up.sql":   {Data: fmt.Appendf(nil, body, suffix)},
		"0000000002_body.down.sql": {Data: []byte("SELECT 1;\n")},
	})
	c.Assert(err, qt.IsNil)
	run.provider = provider
	return run
}

// migrator returns a migrator over the run's directory and table.
func (r *ownTransactionsRun) migrator(ownTransactions bool) *migrator.Migrator {
	return migrator.NewMigrator(r.conn, r.provider).
		WithMigrationsTable("", r.table).
		WithOwnTransactions(ownTransactions)
}

// tableExists reports whether the run's table with prefix exists.
func (r *ownTransactionsRun) tableExists(c *qt.C, prefix string) bool {
	c.Helper()
	var exists bool
	c.Assert(r.conn.QueryRowContext(context.Background(),
		"SELECT to_regclass($1) IS NOT NULL", prefix+r.suffix).Scan(&exists), qt.IsNil)
	return exists
}

// revision returns what the revision row of the second migration records:
// how many statements it applied, -1 when there is no row, and its error.
func (r *ownTransactionsRun) revision(c *qt.C) (int, string) {
	c.Helper()
	var applied int
	var failure string
	c.Assert(r.conn.QueryRowContext(context.Background(),
		"SELECT COALESCE(max(applied), -1), COALESCE(max(error), '') FROM "+r.table+" WHERE version = 2",
	).Scan(&applied, &failure), qt.IsNil)
	return applied, failure
}

func TestMigrateUpOwnTransactions_HappyPathLive(t *testing.T) {
	tests := []struct {
		name string
		body string
		// wantTables are the run's tables, by prefix, the body leaves.
		wantTables []string
		// wantApplied is the statement count the revision row records.
		wantApplied int
	}{
		{
			name:        "a file that opens and closes its own transaction",
			body:        "BEGIN;\nCREATE TABLE owntx_a_%[1]s (id int);\nINSERT INTO owntx_a_%[1]s VALUES (1);\nCOMMIT;\n",
			wantTables:  []string{"owntx_a_"},
			wantApplied: 4,
		},
		{
			name: "two transactions with a statement between them",
			body: "BEGIN;\nCREATE TABLE owntx_a_%[1]s (id int);\nCOMMIT;\nCREATE TABLE owntx_b_%[1]s (id int);\n" +
				"START TRANSACTION;\nINSERT INTO owntx_a_%[1]s VALUES (1);\nEND;\n",
			wantTables:  []string{"owntx_a_", "owntx_b_"},
			wantApplied: 7,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			run := newOwnTransactionsRun(c, postgresTestURL(t), test.body)

			err := run.migrator(true).MigrateUp(context.Background())

			c.Assert(err, qt.IsNil)
			for _, table := range test.wantTables {
				c.Assert(run.tableExists(c, table), qt.IsTrue, qt.Commentf("table %s", table))
			}
			applied, failure := run.revision(c)
			c.Assert(applied, qt.Equals, test.wantApplied)
			c.Assert(failure, qt.Equals, "")
		})
	}
}

// A file whose one statement PostgreSQL refuses inside a transaction runs
// without one, the way the runner sends it, and the index it builds is valid.
func TestMigrateUpOwnTransactions_LoneConcurrentIndexLive(t *testing.T) {
	c := qt.New(t)
	run := newOwnTransactionsRun(c, postgresTestURL(t), "CREATE INDEX CONCURRENTLY owntx_base_%[1]s_id_idx ON owntx_base_%[1]s (id);\n")

	err := run.migrator(true).MigrateUp(context.Background())

	c.Assert(err, qt.IsNil)
	var valid bool
	c.Assert(run.conn.QueryRowContext(context.Background(),
		"SELECT i.indisvalid FROM pg_index i JOIN pg_class x ON x.oid = i.indexrelid WHERE x.relname = $1",
		"owntx_base_"+run.suffix+"_id_idx").Scan(&valid), qt.IsNil)
	c.Assert(valid, qt.IsTrue)
	applied, failure := run.revision(c)
	c.Assert(applied, qt.Equals, 1)
	c.Assert(failure, qt.Equals, "")
}

// Without the policy the same file runs in the migrator's transaction, where
// PostgreSQL refuses the build, and the migrator says so before it runs.
func TestMigrateUpOwnTransactions_LoneConcurrentIndexWithoutThePolicyLive(t *testing.T) {
	c := qt.New(t)
	run := newOwnTransactionsRun(c, postgresTestURL(t), "CREATE INDEX CONCURRENTLY owntx_base_%[1]s_id_idx ON owntx_base_%[1]s (id);\n")

	err := run.migrator(false).MigrateUp(context.Background())

	c.Assert(err, qt.ErrorMatches, `(?s).*cannot run inside a transaction: line 1 CREATE or DROP INDEX CONCURRENTLY is refused inside a transaction block.*`)
}

// A run interrupted inside the file's own transaction leaves nothing of that
// transaction, and the revision row says so: nothing inside it is recorded as
// applied, because nothing inside it committed. A retry therefore runs the
// whole transaction again rather than resuming in its middle.
func TestMigrateUpOwnTransactions_InterruptedInsideTheFilesTransactionLive(t *testing.T) {
	c := qt.New(t)
	run := newOwnTransactionsRun(c, postgresTestURL(t),
		"BEGIN;\nCREATE TABLE owntx_a_%[1]s (id int);\nSELECT pg_sleep(4);\nINSERT INTO owntx_a_%[1]s VALUES (1);\nCOMMIT;\n")
	interrupted, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	c.Assert(run.migrator(true).MigrateUp(interrupted), qt.IsNotNil)

	c.Assert(run.tableExists(c, "owntx_a_"), qt.IsFalse)
	applied, _ := run.revision(c)
	c.Assert(applied, qt.Equals, 0)
	c.Assert(run.migrator(true).MigrateUpWithOptions(context.Background(), migrator.MigrateUpOptions{AllowDirty: true}), qt.IsNil)
	c.Assert(run.tableExists(c, "owntx_a_"), qt.IsTrue)
}

func TestMigrateUpOwnTransactions_FailurePathLive(t *testing.T) {
	tests := []struct {
		name string
		body string
		// txMode is the run's transaction mode.
		txMode migrator.MigrationTxMode
		// wantErr matches the run's error.
		wantErr string
		// wantTable is the run's table, by prefix, the body names, and
		// wantTableExists whether the failure left it.
		wantTable       string
		wantTableExists bool
		// wantApplied is what the revision row records, -1 for no row, and
		// wantFailure what its error says.
		wantApplied int
		wantFailure string
	}{
		{
			name:            "a failure after the file's transaction committed",
			txMode:          migrator.MigrationTxModeFile,
			body:            "BEGIN;\nCREATE TABLE owntx_a_%[1]s (id int);\nCOMMIT;\nINSERT INTO owntx_missing_%[1]s VALUES (1);\n",
			wantErr:         `(?s).*relation "owntx_missing_\d+" does not exist.*`,
			wantTable:       "owntx_a_",
			wantTableExists: true,
			wantApplied:     3,
			wantFailure:     "does not exist",
		},
		{
			name:            "a failure inside the file's transaction",
			txMode:          migrator.MigrationTxModeFile,
			body:            "BEGIN;\nCREATE TABLE owntx_a_%[1]s (id int);\nINSERT INTO owntx_missing_%[1]s VALUES (1);\nCOMMIT;\n",
			wantErr:         `(?s).*relation "owntx_missing_\d+" does not exist.*`,
			wantTable:       "owntx_a_",
			wantTableExists: false,
			wantApplied:     0,
			wantFailure:     "does not exist",
		},
		{
			name:            "a failure in the second of two transactions",
			txMode:          migrator.MigrationTxModeFile,
			body:            "BEGIN;\nCREATE TABLE owntx_a_%[1]s (id int);\nCOMMIT;\nBEGIN;\nCREATE TABLE owntx_b_%[1]s (id int);\nINSERT INTO owntx_missing_%[1]s VALUES (1);\nCOMMIT;\n",
			wantErr:         `(?s).*relation "owntx_missing_\d+" does not exist.*`,
			wantTable:       "owntx_b_",
			wantTableExists: false,
			wantApplied:     3,
			wantFailure:     "does not exist",
		},
		{
			name:            "tx-mode all",
			body:            "BEGIN;\nCREATE TABLE owntx_a_%[1]s (id int);\nCOMMIT;\n",
			txMode:          migrator.MigrationTxModeAll,
			wantErr:         `(?s).*migration 2 opens and closes its own transaction and cannot run with tx-mode all: .*`,
			wantTable:       "owntx_a_",
			wantTableExists: false,
			wantApplied:     -1,
		},
		{
			name:            "transaction control the migrator does not follow",
			txMode:          migrator.MigrationTxModeFile,
			body:            "BEGIN;\nCREATE TABLE owntx_a_%[1]s (id int);\nROLLBACK;\n",
			wantErr:         `(?s).*migration 2: the file controls its own transactions in a form Ptah does not run, at statement 3: ROLLBACK .*`,
			wantTable:       "owntx_a_",
			wantTableExists: false,
			wantApplied:     -1,
		},
		{
			name:            "a file that asks for a transaction and opens its own",
			txMode:          migrator.MigrationTxModeFile,
			body:            "-- atlas:txmode file\n\nBEGIN;\nCREATE TABLE owntx_a_%[1]s (id int);\nCOMMIT;\n",
			wantErr:         `(?s).*migration 2 asks for a transaction around the file and opens its own`,
			wantTable:       "owntx_a_",
			wantTableExists: false,
			wantApplied:     -1,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			run := newOwnTransactionsRun(c, postgresTestURL(t), test.body)

			err := run.migrator(true).WithTransactionMode(test.txMode).MigrateUp(context.Background())

			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(run.tableExists(c, test.wantTable), qt.Equals, test.wantTableExists)
			applied, failure := run.revision(c)
			c.Assert(applied, qt.Equals, test.wantApplied)
			c.Assert(failure, qt.Contains, test.wantFailure)
		})
	}
}

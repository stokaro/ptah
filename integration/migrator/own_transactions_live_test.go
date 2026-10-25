//go:build integration

package migrator_test

import (
	"context"
	"fmt"
	"net/url"
	"strings"
	"testing"
	"testing/fstest"
	"time"

	qt "github.com/frankban/quicktest"

	"ptah.run/dbschema"
	"ptah.run/migration/migrationfile"
	"ptah.run/migration/migrator"
)

// These tests drive [migrator.WithOwnTransactions] on PostgreSQL: a file
// written for a runner that wraps nothing, as golang-migrate's are, runs the
// way that runner runs it. A file with its own BEGIN and COMMIT runs outside
// any transaction of the migrator's, and a file whose one statement PostgreSQL
// refuses inside a transaction runs without one (stokaro/ptah#4962).
//
// What the server decides is read back from it: which tables exist, whether
// an index is valid, and what the revision row records.

// ownTransactionsRun is one migration applied over a fresh migrations table,
// with the tables the body names suffixed so runs do not collide.
type ownTransactionsRun struct {
	conn   *dbschema.DatabaseConnection
	table  string
	suffix string
	fsys   fstest.MapFS
}

// newOwnTransactionsRun connects to the PostgreSQL dbURL names and writes a
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
		for _, table := range []string{"owntx_base_", "owntx_a_", "owntx_b_", "owntx_c_", "owntx_d_", "owntx_missing_", "owntx_log_"} {
			_, _ = conn.ExecContext(context.WithoutCancel(ctx), "DROP TABLE IF EXISTS "+table+suffix+" CASCADE")
		}
		_, _ = conn.ExecContext(context.WithoutCancel(ctx), "DROP TABLE IF EXISTS "+run.table)
		dbschema.CloseAndWarn(conn)
	})
	run.fsys = fstest.MapFS{
		"0000000001_base.up.sql":   {Data: fmt.Appendf(nil, "CREATE TABLE owntx_base_%s (id int);\n", suffix)},
		"0000000001_base.down.sql": {Data: fmt.Appendf(nil, "DROP TABLE owntx_base_%s;\n", suffix)},
		"0000000002_body.up.sql":   {Data: fmt.Appendf(nil, body, suffix)},
		"0000000002_body.down.sql": {Data: []byte("SELECT 1;\n")},
	}
	return run
}

// migrator returns a migrator over the run's directory, loaded with options,
// and the run's table.
func (r *ownTransactionsRun) migrator(c *qt.C, options ...migrator.FSProviderOption) *migrator.Migrator {
	c.Helper()
	provider, err := migrator.NewFSMigrationProvider(r.fsys, options...)
	c.Assert(err, qt.IsNil)
	return migrator.NewMigrator(r.conn, provider).WithMigrationsTable("", r.table)
}

// exec runs query on the run's database.
func (r *ownTransactionsRun) exec(c *qt.C, query string) {
	c.Helper()
	_, err := r.conn.ExecContext(context.Background(), query)
	c.Assert(err, qt.IsNil)
}

// texts returns the one text column query selects, each value trimmed.
func (r *ownTransactionsRun) texts(c *qt.C, query string) []string {
	c.Helper()
	rows, err := r.conn.QueryContext(context.Background(), query)
	c.Assert(err, qt.IsNil)
	defer func() { c.Check(rows.Close(), qt.IsNil) }()
	var texts []string
	for rows.Next() {
		var text string
		c.Assert(rows.Scan(&text), qt.IsNil)
		texts = append(texts, strings.TrimSpace(text))
	}
	c.Assert(rows.Err(), qt.IsNil)
	return texts
}

// tables returns which of the run's tables a, b, c and d exist, by letter.
func (r *ownTransactionsRun) tables(c *qt.C) []string {
	c.Helper()
	return r.texts(c, "SELECT substr(relname, 7, 1) FROM pg_class WHERE relkind = 'r' AND relname IN ("+
		"'owntx_a_"+r.suffix+"', 'owntx_b_"+r.suffix+"', 'owntx_c_"+r.suffix+"', 'owntx_d_"+r.suffix+"') ORDER BY relname")
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

// errorText is err's message, or the empty string for no error.
func errorText(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}

// failOn is a statement observer that refuses the statements that start with
// prefix, after they ran.
func failOn(prefix string) migrator.FSProviderOption {
	return migrator.WithStatementObserver(migrator.StatementObserverFunc(
		func(_ context.Context, event migrator.StatementEvent) error {
			if strings.HasPrefix(strings.TrimSpace(event.Statement), prefix) {
				return fmt.Errorf("observer refuses %q", prefix)
			}
			return nil
		}))
}

// interruptAfter is a statement observer that cancels the run with interrupt
// once a statement that starts with prefix ran.
func interruptAfter(prefix string, interrupt context.CancelFunc) migrator.FSProviderOption {
	return migrator.WithStatementObserver(migrator.StatementObserverFunc(
		func(_ context.Context, event migrator.StatementEvent) error {
			if strings.HasPrefix(strings.TrimSpace(event.Statement), prefix) {
				interrupt()
			}
			return nil
		}))
}

// golangMigrateDefaultModeCases are files that open and close their own
// transactions, with statements on both sides of them, and the tables
// golang-migrate leaves when it applies each as the second migration of a
// directory in its default mode, which sends the file as one query. Measured
// with golang-migrate v4.20.1 through the postgres:// and pgx5:// drivers
// alike, on PostgreSQL 15.19 (stokaro/ptah#4979). Ptah sends the statements one
// at a time and has to leave the same tables; applied is what its revision row
// records, the statements of every transaction that committed.
var golangMigrateDefaultModeCases = []struct {
	name    string
	body    string
	wantErr string
	// wantTables are the tables golang-migrate leaves, by letter, and
	// wantAfterRetry the tables a retry leaves once the missing table exists.
	wantTables     []string
	wantApplied    int
	wantAfterRetry []string
}{
	{
		name: "a failure before the block",
		body: "CREATE TABLE owntx_a_%[1]s (id int);\nINSERT INTO owntx_missing_%[1]s VALUES (1);\nBEGIN;\n" +
			"CREATE TABLE owntx_b_%[1]s (id int);\nCOMMIT;\nCREATE TABLE owntx_c_%[1]s (id int);\n",
		wantErr:        `(?s).*relation "owntx_missing_\d+" does not exist.*`,
		wantTables:     nil,
		wantApplied:    0,
		wantAfterRetry: []string{"a", "b", "c"},
	},
	{
		name: "a failure inside the block",
		body: "CREATE TABLE owntx_a_%[1]s (id int);\nBEGIN;\nCREATE TABLE owntx_b_%[1]s (id int);\n" +
			"INSERT INTO owntx_missing_%[1]s VALUES (1);\nCOMMIT;\nCREATE TABLE owntx_c_%[1]s (id int);\n",
		wantErr:        `(?s).*relation "owntx_missing_\d+" does not exist.*`,
		wantTables:     nil,
		wantApplied:    0,
		wantAfterRetry: []string{"a", "b", "c"},
	},
	{
		name: "a failure after the block",
		body: "CREATE TABLE owntx_a_%[1]s (id int);\nBEGIN;\nCREATE TABLE owntx_b_%[1]s (id int);\nCOMMIT;\n" +
			"CREATE TABLE owntx_c_%[1]s (id int);\nINSERT INTO owntx_missing_%[1]s VALUES (1);\n",
		wantErr:        `(?s).*relation "owntx_missing_\d+" does not exist.*`,
		wantTables:     []string{"a", "b"},
		wantApplied:    4,
		wantAfterRetry: []string{"a", "b", "c"},
	},
	{
		name: "a failure in the second block",
		body: "CREATE TABLE owntx_a_%[1]s (id int);\nBEGIN;\nCREATE TABLE owntx_b_%[1]s (id int);\nCOMMIT;\n" +
			"CREATE TABLE owntx_c_%[1]s (id int);\nBEGIN;\nCREATE TABLE owntx_d_%[1]s (id int);\n" +
			"INSERT INTO owntx_missing_%[1]s VALUES (1);\nCOMMIT;\n",
		wantErr:        `(?s).*relation "owntx_missing_\d+" does not exist.*`,
		wantTables:     []string{"a", "b"},
		wantApplied:    4,
		wantAfterRetry: []string{"a", "b", "c", "d"},
	},
	{
		name: "statements on both sides of the block",
		body: "CREATE TABLE owntx_a_%[1]s (id int);\nBEGIN;\nCREATE TABLE owntx_b_%[1]s (id int);\nCOMMIT;\n" +
			"CREATE TABLE owntx_c_%[1]s (id int);\n",
		wantTables:  []string{"a", "b", "c"},
		wantApplied: 5,
	},
	{
		name: "an isolation level after a write",
		body: "CREATE TABLE owntx_a_%[1]s (id int);\nBEGIN ISOLATION LEVEL SERIALIZABLE;\n" +
			"CREATE TABLE owntx_b_%[1]s (id int);\nCOMMIT;\n",
		wantErr:     `(?s).*SET TRANSACTION ISOLATION LEVEL must be called before any query.*`,
		wantTables:  nil,
		wantApplied: 0,
	},
	{
		name: "an isolation level that opens the file",
		body: "BEGIN ISOLATION LEVEL SERIALIZABLE;\nCREATE TABLE owntx_b_%[1]s (id int);\nCOMMIT;\n" +
			"CREATE TABLE owntx_c_%[1]s (id int);\n",
		wantTables:  []string{"b", "c"},
		wantApplied: 4,
	},
	{
		name:        "an isolation level after a setting",
		body:        "SET lock_timeout = '5s';\nBEGIN ISOLATION LEVEL SERIALIZABLE;\nCREATE TABLE owntx_b_%[1]s (id int);\nCOMMIT;\n",
		wantTables:  []string{"b"},
		wantApplied: 4,
	},
	{
		name:        "read only after a write",
		body:        "CREATE TABLE owntx_a_%[1]s (id int);\nBEGIN READ ONLY;\nCREATE TABLE owntx_b_%[1]s (id int);\nCOMMIT;\n",
		wantErr:     `(?s).*cannot execute CREATE TABLE in a read-only transaction.*`,
		wantTables:  nil,
		wantApplied: 0,
	},
	{
		name: "an isolation level in the second block after a write",
		body: "BEGIN;\nCREATE TABLE owntx_a_%[1]s (id int);\nCOMMIT;\nCREATE TABLE owntx_b_%[1]s (id int);\n" +
			"BEGIN ISOLATION LEVEL SERIALIZABLE;\nCREATE TABLE owntx_c_%[1]s (id int);\nCOMMIT;\n",
		wantErr:     `(?s).*SET TRANSACTION ISOLATION LEVEL must be called before any query.*`,
		wantTables:  []string{"a"},
		wantApplied: 3,
	},
	{
		name: "a concurrent build after the block",
		body: "BEGIN;\nCREATE TABLE owntx_b_%[1]s (id int);\nCOMMIT;\n" +
			"CREATE INDEX CONCURRENTLY owntx_base_%[1]s_i ON owntx_base_%[1]s (id);\n",
		wantErr:     `(?s).*CREATE INDEX CONCURRENTLY cannot run inside a transaction block.*`,
		wantTables:  []string{"b"},
		wantApplied: 3,
	},
}

// The default tx-mode runs a file that opens its own transactions as
// golang-migrate's default mode does, and leaves the tables it leaves.
func TestMigrateUpOwnTransactions_MatchesGolangMigrateDefaultModeLive(t *testing.T) {
	for _, test := range golangMigrateDefaultModeCases {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			run := newOwnTransactionsRun(c, postgresTestURL(t), test.body)

			err := run.migrator(c, migrator.WithOwnTransactions()).MigrateUp(context.Background())

			c.Assert(errorText(err), qt.Matches, test.wantErr)
			c.Assert(run.tables(c), qt.DeepEquals, test.wantTables)
			applied, _ := run.revision(c)
			c.Assert(applied, qt.Equals, test.wantApplied)
		})
	}
}

// After a failure the default tx-mode recorded only committed transactions,
// so a retry once the missing table exists resumes after them and leaves
// every table, running no statement twice.
func TestMigrateUpOwnTransactions_RetryAfterTheDefaultModeFailureLive(t *testing.T) {
	for _, test := range golangMigrateDefaultModeCases[:4] {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			run := newOwnTransactionsRun(c, postgresTestURL(t), test.body)
			c.Assert(run.migrator(c, migrator.WithOwnTransactions()).MigrateUp(context.Background()), qt.ErrorMatches, test.wantErr)
			run.exec(c, "CREATE TABLE owntx_missing_"+run.suffix+" (id int)")

			err := run.migrator(c, migrator.WithOwnTransactions()).MigrateUpWithOptions(context.Background(),
				migrator.MigrateUpOptions{AllowDirty: true})

			c.Assert(err, qt.IsNil)
			c.Assert(run.tables(c), qt.DeepEquals, test.wantAfterRetry)
			applied, failure := run.revision(c)
			c.Assert(applied, qt.Equals, strings.Count(fmt.Sprintf(test.body, run.suffix), ";"))
			c.Assert(failure, qt.Equals, "")
			var rows int
			c.Assert(run.conn.QueryRowContext(context.Background(), "SELECT count(*) FROM owntx_missing_"+run.suffix).Scan(&rows), qt.IsNil)
			c.Assert(rows, qt.Equals, 1)
		})
	}
}

func TestMigrateUpOwnTransactions_HappyPathLive(t *testing.T) {
	tests := []struct {
		name string
		body string
		// txMode is the run's transaction mode.
		txMode migrator.MigrationTxMode
		// wantTables are the run's tables, by prefix, the body leaves.
		wantTables []string
		// wantApplied is the statement count the revision row records.
		wantApplied int
	}{
		{
			name:        "a file that opens and closes its own transaction",
			txMode:      migrator.MigrationTxModeFile,
			body:        "BEGIN;\nCREATE TABLE owntx_a_%[1]s (id int);\nINSERT INTO owntx_a_%[1]s VALUES (1);\nCOMMIT;\n",
			wantTables:  []string{"owntx_a_"},
			wantApplied: 4,
		},
		{
			name:   "two transactions with a statement between them",
			txMode: migrator.MigrationTxModeFile,
			body: "BEGIN;\nCREATE TABLE owntx_a_%[1]s (id int);\nCOMMIT;\nCREATE TABLE owntx_b_%[1]s (id int);\n" +
				"START TRANSACTION;\nINSERT INTO owntx_a_%[1]s VALUES (1);\nEND;\n",
			wantTables:  []string{"owntx_a_", "owntx_b_"},
			wantApplied: 7,
		},
		{
			name:        "a transaction closed by COMMIT AND NO CHAIN",
			txMode:      migrator.MigrationTxModeFile,
			body:        "BEGIN;\nCREATE TABLE owntx_a_%[1]s (id int);\nCOMMIT AND NO CHAIN;\n",
			wantTables:  []string{"owntx_a_"},
			wantApplied: 3,
		},
		{
			name:        "tx-mode none",
			txMode:      migrator.MigrationTxModeNone,
			body:        "BEGIN;\nCREATE TABLE owntx_a_%[1]s (id int);\nCOMMIT;\nCREATE TABLE owntx_b_%[1]s (id int);\n",
			wantTables:  []string{"owntx_a_", "owntx_b_"},
			wantApplied: 4,
		},
		{
			name:        "a file that asks for no transaction and opens its own",
			txMode:      migrator.MigrationTxModeFile,
			body:        "-- atlas:txmode none\n\nBEGIN;\nCREATE TABLE owntx_a_%[1]s (id int);\nCOMMIT;\n",
			wantTables:  []string{"owntx_a_"},
			wantApplied: 3,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			run := newOwnTransactionsRun(c, postgresTestURL(t), test.body)

			err := run.migrator(c, migrator.WithOwnTransactions()).WithTransactionMode(test.txMode).MigrateUp(context.Background())

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

	err := run.migrator(c, migrator.WithOwnTransactions()).MigrateUp(context.Background())

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

// The migrator keeps its transaction around a concurrent build that does not
// run alone: without the policy, beside a second statement, and in a file that
// names its transaction mode. PostgreSQL refuses the build there, and the
// migrator says so before anything runs.
func TestMigrateUpOwnTransactions_ConcurrentIndexKeepsTheTransactionLive(t *testing.T) {
	const build = "CREATE INDEX CONCURRENTLY owntx_base_%[1]s_id_idx ON owntx_base_%[1]s (id);\n"
	tests := []struct {
		name    string
		body    string
		options []migrator.FSProviderOption
	}{
		{name: "without the policy", body: build},
		{
			name:    "beside a second statement",
			body:    "CREATE TABLE owntx_a_%[1]s (id int);\n" + build,
			options: []migrator.FSProviderOption{migrator.WithOwnTransactions()},
		},
		{
			name:    "in a file that asks for a transaction",
			body:    "-- atlas:txmode file\n\n" + build,
			options: []migrator.FSProviderOption{migrator.WithOwnTransactions()},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			run := newOwnTransactionsRun(c, postgresTestURL(t), test.body)

			err := run.migrator(c, test.options...).MigrateUp(context.Background())

			c.Assert(err, qt.ErrorMatches,
				`(?s).*cannot run inside a transaction: line \d+ CREATE or DROP INDEX CONCURRENTLY is refused inside a transaction block.*`)
			c.Assert(run.tableExists(c, "owntx_a_"), qt.IsFalse)
		})
	}
}

// A run interrupted inside the file's own transaction leaves nothing of that
// transaction, and the revision row says so: nothing inside it is recorded as
// applied, because nothing inside it committed. A retry therefore runs the
// whole transaction again rather than resuming in its middle. The observer
// interrupts the run after the table is built, before the INSERT is sent.
func TestMigrateUpOwnTransactions_InterruptedInsideTheFilesTransactionLive(t *testing.T) {
	c := qt.New(t)
	run := newOwnTransactionsRun(c, postgresTestURL(t),
		"BEGIN;\nCREATE TABLE owntx_a_%[1]s (id int);\nINSERT INTO owntx_a_%[1]s VALUES (1);\nCOMMIT;\n")
	interrupted, interrupt := context.WithCancel(context.Background())
	c.Cleanup(interrupt)

	err := run.migrator(c, migrator.WithOwnTransactions(), interruptAfter("CREATE TABLE owntx_a_", interrupt)).
		MigrateUp(interrupted)

	// What the driver reports for the canceled INSERT varies, from the
	// context's error to a connection it closed; the run stops there either way.
	c.Assert(err, qt.ErrorMatches, `(?s).*SQL: INSERT INTO owntx_a_\d+ VALUES \(1\).*`)
	c.Assert(err.Error(), qt.Not(qt.Contains), "failed to roll back")
	c.Assert(run.tableExists(c, "owntx_a_"), qt.IsFalse)
	applied, _ := run.revision(c)
	c.Assert(applied, qt.Equals, 0)
	c.Assert(run.migrator(c, migrator.WithOwnTransactions()).MigrateUpWithOptions(context.Background(),
		migrator.MigrateUpOptions{AllowDirty: true}), qt.IsNil)
	var rows int
	c.Assert(run.conn.QueryRowContext(context.Background(), "SELECT count(*) FROM owntx_a_"+run.suffix).Scan(&rows), qt.IsNil)
	c.Assert(rows, qt.Equals, 1)
}

// A retry resumes after the statements the failed attempt left committed, a
// committed transaction of the file among them, on a fresh session. The
// prefix's session state is restored first: the plain SET inside the
// transaction outlived it and is replayed, while BEGIN, COMMIT, LOCK and
// SET LOCAL ended with it and are not. The INSERT reads the replayed setting.
func TestMigrateUpOwnTransactions_RetryAfterTheFilesTransactionCommittedLive(t *testing.T) {
	c := qt.New(t)
	run := newOwnTransactionsRun(c, postgresTestURL(t),
		"SET statement_timeout = '30s';\nBEGIN;\nCREATE TABLE owntx_a_%[1]s (id int);\n"+
			"LOCK TABLE owntx_a_%[1]s IN ACCESS EXCLUSIVE MODE;\nSET LOCAL lock_timeout = '1s';\n"+
			"SET application_name = 'owntx';\nCOMMIT;\nINSERT INTO owntx_b_%[1]s VALUES (current_setting('application_name'));\n")

	err := run.migrator(c, migrator.WithOwnTransactions()).MigrateUp(context.Background())

	c.Assert(err, qt.ErrorMatches, `(?s).*relation "owntx_b_\d+" does not exist.*`)
	applied, _ := run.revision(c)
	c.Assert(applied, qt.Equals, 7)
	run.exec(c, "CREATE TABLE owntx_b_"+run.suffix+" (setting text)")

	err = run.migrator(c, migrator.WithOwnTransactions()).MigrateUpWithOptions(context.Background(),
		migrator.MigrateUpOptions{AllowDirty: true})

	c.Assert(err, qt.IsNil)
	var setting string
	c.Assert(run.conn.QueryRowContext(context.Background(), "SELECT setting FROM owntx_b_"+run.suffix).Scan(&setting), qt.IsNil)
	c.Assert(setting, qt.Equals, "owntx")
	applied, failure := run.revision(c)
	c.Assert(applied, qt.Equals, 8)
	c.Assert(failure, qt.Equals, "")
}

// The mark written before COMMIT names the first statement of the
// transaction COMMIT ends and counts the statements before it, so an
// interrupted COMMIT is read as the whole transaction's. Under the default
// tx-mode that transaction starts at the file's first statement, which runs
// in the transaction its BEGIN goes on to name; under tx-mode none it starts
// at BEGIN, and the statement before it committed on its own. When the mark
// cannot be written, COMMIT is never sent: the migrator rolls the transaction
// back and records the statements before it as applied. The commit at the end
// of a file whose last statements follow its COMMIT is marked the same way.
// The trigger refuses exactly that mark, so a mark that names COMMIT, or
// counts the transaction, lets the run succeed. When the failure is recorded,
// the trigger also counts the sessions still inside a transaction of the
// file's into owntx_log: the rollback comes first, so there are none.
func TestMigrateUpOwnTransactions_MarkBeforeCommitFailsLive(t *testing.T) {
	const beforeBlock = "CREATE TABLE owntx_a_%[1]s (id int);\nBEGIN;\nCREATE TABLE owntx_b_%[1]s (id int);\nCOMMIT;\n"
	tests := []struct {
		name   string
		body   string
		txMode migrator.MigrationTxMode
		// markStatement and markApplied are what the refused mark records.
		markStatement string
		markApplied   int
		// wantTables are the tables that stay committed, by letter.
		wantTables []string
	}{
		{
			name: "tx-mode file", body: beforeBlock, txMode: migrator.MigrationTxModeFile,
			markStatement: "CREATE TABLE owntx_a_%", markApplied: 0,
		},
		{
			name: "tx-mode none", body: beforeBlock, txMode: migrator.MigrationTxModeNone,
			markStatement: "BEGIN", markApplied: 1, wantTables: []string{"a"},
		},
		{
			name:   "the end of the file",
			body:   "BEGIN;\nCREATE TABLE owntx_b_%[1]s (id int);\nCOMMIT;\nCREATE TABLE owntx_a_%[1]s (id int);\n",
			txMode: migrator.MigrationTxModeFile, markStatement: "CREATE TABLE owntx_a_%", markApplied: 3,
			wantTables: []string{"b"},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			run := newOwnTransactionsRun(c, postgresTestURL(t), test.body)
			m := run.migrator(c, migrator.WithOwnTransactions()).WithTransactionMode(test.txMode)
			c.Assert(m.Initialize(context.Background()), qt.IsNil)
			c.Cleanup(func() {
				_, _ = run.conn.ExecContext(context.Background(), "DROP FUNCTION IF EXISTS owntx_refuse_"+run.suffix+"() CASCADE")
			})
			run.exec(c, "CREATE TABLE owntx_log_"+run.suffix+" (open_sessions bigint)")
			run.exec(c, fmt.Sprintf("CREATE FUNCTION owntx_refuse_"+run.suffix+"() RETURNS trigger LANGUAGE plpgsql AS $$\n"+
				"BEGIN\n"+
				"  IF NEW.error_stmt LIKE '%s' AND NEW.applied = %d AND NEW.error LIKE 'statement execution outcome is unknown%%' THEN\n"+
				"    RAISE EXCEPTION 'owntx: the mark is refused';\n"+
				"  ELSIF NEW.error LIKE '%%the mark is refused%%' THEN\n"+
				"    INSERT INTO owntx_log_"+run.suffix+" SELECT count(*) FROM pg_stat_activity\n"+
				"      WHERE state LIKE 'idle in transaction%%' AND query LIKE '%%owntx_%%_"+run.suffix+" %%';\n"+
				"  END IF;\n"+
				"  RETURN NEW;\n"+
				"END $$", test.markStatement, test.markApplied))
			run.exec(c, "CREATE TRIGGER owntx_refuse BEFORE UPDATE ON "+run.table+
				" FOR EACH ROW EXECUTE FUNCTION owntx_refuse_"+run.suffix+"()")

			err := m.MigrateUp(context.Background())

			c.Assert(err, qt.ErrorMatches, `(?s).*owntx: the mark is refused.*`)
			c.Assert(run.tables(c), qt.DeepEquals, test.wantTables)
			applied, failure := run.revision(c)
			c.Assert(applied, qt.Equals, test.markApplied)
			c.Assert(failure, qt.Contains, "the mark is refused")
			var checks, open int
			c.Assert(run.conn.QueryRowContext(context.Background(),
				"SELECT count(*), COALESCE(max(open_sessions), 0) FROM owntx_log_"+run.suffix).Scan(&checks, &open), qt.IsNil)
			c.Assert(checks > 0, qt.IsTrue, qt.Commentf("the failure was never recorded through the trigger"))
			c.Assert(open, qt.Equals, 0)
		})
	}
}

// The checkpoint after the commit at the end of the file records the whole
// file once that commit succeeded. When the checkpoint cannot be written, the
// failure still records every statement as applied, because they committed,
// and a retry has nothing left to run. The trigger refuses the first update
// that reaches the last statement with no error, which is that checkpoint.
func TestMigrateUpOwnTransactions_CheckpointAfterTheEndOfTheFileFailsLive(t *testing.T) {
	c := qt.New(t)
	run := newOwnTransactionsRun(c, postgresTestURL(t),
		"BEGIN;\nCREATE TABLE owntx_b_%[1]s (id int);\nCOMMIT;\nCREATE TABLE owntx_a_%[1]s (id int);\n")
	m := run.migrator(c, migrator.WithOwnTransactions())
	c.Assert(m.Initialize(context.Background()), qt.IsNil)
	c.Cleanup(func() {
		_, _ = run.conn.ExecContext(context.Background(), "DROP FUNCTION IF EXISTS owntx_refuse_"+run.suffix+"() CASCADE")
	})
	run.exec(c, "CREATE FUNCTION owntx_refuse_"+run.suffix+"() RETURNS trigger LANGUAGE plpgsql AS $$\n"+
		"BEGIN\n"+
		"  IF NEW.version = 2 AND NEW.applied = 4 AND OLD.applied <> 4 AND COALESCE(NEW.error, '') = '' THEN\n"+
		"    RAISE EXCEPTION 'owntx: the checkpoint is refused';\n"+
		"  END IF;\n"+
		"  RETURN NEW;\n"+
		"END $$")
	run.exec(c, "CREATE TRIGGER owntx_refuse BEFORE UPDATE ON "+run.table+
		" FOR EACH ROW EXECUTE FUNCTION owntx_refuse_"+run.suffix+"()")

	err := m.MigrateUp(context.Background())

	c.Assert(err, qt.ErrorMatches, `(?s).*owntx: the checkpoint is refused.*`)
	c.Assert(run.tables(c), qt.DeepEquals, []string{"a", "b"})
	applied, failure := run.revision(c)
	c.Assert(applied, qt.Equals, 4)
	c.Assert(failure, qt.Contains, "the checkpoint is refused")
}

// Under tx-mode none a statement after the file's transaction commits on its
// own, so its failure leaves no transaction open, and the migrator sends no
// ROLLBACK, which PostgreSQL would answer with a warning. When the failure is
// recorded, the trigger reads the last statement of every other session of
// the run, the run's connections being the ones named by its
// application_name: the body's session last sent the failed INSERT.
func TestMigrateUpOwnTransactions_FailureOutsideTheFilesTransactionSendsNoRollbackLive(t *testing.T) {
	c := qt.New(t)
	application := fmt.Sprintf("owntx_%d", time.Now().UnixNano())
	dbURL, err := url.Parse(postgresTestURL(t))
	c.Assert(err, qt.IsNil)
	query := dbURL.Query()
	query.Set("application_name", application)
	dbURL.RawQuery = query.Encode()
	run := newOwnTransactionsRun(c, dbURL.String(),
		"BEGIN;\nCREATE TABLE owntx_a_%[1]s (id int);\nCOMMIT;\nINSERT INTO owntx_missing_%[1]s VALUES (1);\n")
	m := run.migrator(c, migrator.WithOwnTransactions()).WithTransactionMode(migrator.MigrationTxModeNone)
	c.Assert(m.Initialize(context.Background()), qt.IsNil)
	c.Cleanup(func() {
		_, _ = run.conn.ExecContext(context.Background(), "DROP FUNCTION IF EXISTS owntx_last_"+run.suffix+"() CASCADE")
	})
	run.exec(c, "CREATE TABLE owntx_c_"+run.suffix+" (last_statement text)")
	run.exec(c, "CREATE FUNCTION owntx_last_"+run.suffix+"() RETURNS trigger LANGUAGE plpgsql AS $$\n"+
		"BEGIN\n"+
		"  IF NEW.error LIKE '%does not exist%' THEN\n"+
		"    INSERT INTO owntx_c_"+run.suffix+" SELECT query FROM pg_stat_activity\n"+
		"      WHERE application_name = '"+application+"' AND pid <> pg_backend_pid();\n"+
		"  END IF;\n"+
		"  RETURN NEW;\n"+
		"END $$")
	run.exec(c, "CREATE TRIGGER owntx_last BEFORE UPDATE ON "+run.table+
		" FOR EACH ROW EXECUTE FUNCTION owntx_last_"+run.suffix+"()")

	err = m.MigrateUp(context.Background())

	c.Assert(err, qt.ErrorMatches, `(?s).*relation "owntx_missing_\d+" does not exist.*`)
	statements := run.texts(c, "SELECT last_statement FROM owntx_c_"+run.suffix)
	c.Assert(statements, qt.Any(qt.Matches), `INSERT INTO owntx_missing_\d+ VALUES \(1\)`)
	c.Assert(statements, qt.Not(qt.Any(qt.Equals)), "ROLLBACK")
}

// A statement observer that refuses a statement after it ran stops the file.
// Inside the file's transaction the rollback takes the statement back with the
// transaction, and under the default tx-mode the statement before BEGIN with
// it; the statement that closes the transaction committed it.
func TestMigrateUpOwnTransactions_ObserverRefusesLive(t *testing.T) {
	tests := []struct {
		name   string
		txMode migrator.MigrationTxMode
		// prefix starts the statement the observer refuses.
		prefix string
		// wantAExists and wantBExists are whether the statement before BEGIN
		// and the file's transaction stay committed, and wantApplied what the
		// revision row records.
		wantAExists bool
		wantBExists bool
		wantApplied int
	}{
		{
			name: "a statement inside the file's transaction", txMode: migrator.MigrationTxModeFile,
			prefix: "CREATE TABLE owntx_b_", wantAExists: false, wantBExists: false, wantApplied: 0,
		},
		{
			name: "the statement that closes it", txMode: migrator.MigrationTxModeFile,
			prefix: "COMMIT", wantAExists: true, wantBExists: true, wantApplied: 5,
		},
		{
			name: "a statement inside the file's transaction under tx-mode none", txMode: migrator.MigrationTxModeNone,
			prefix: "CREATE TABLE owntx_b_", wantAExists: true, wantBExists: false, wantApplied: 1,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			run := newOwnTransactionsRun(c, postgresTestURL(t),
				"CREATE TABLE owntx_a_%[1]s (id int);\nBEGIN;\nCREATE TABLE owntx_b_%[1]s (id int);\n"+
					"INSERT INTO owntx_b_%[1]s VALUES (1);\nCOMMIT;\n")

			err := run.migrator(c, migrator.WithOwnTransactions(), failOn(test.prefix)).
				WithTransactionMode(test.txMode).MigrateUp(context.Background())

			c.Assert(err, qt.ErrorMatches, `(?s).*observer refuses.*`)
			c.Assert(run.tableExists(c, "owntx_a_"), qt.Equals, test.wantAExists)
			c.Assert(run.tableExists(c, "owntx_b_"), qt.Equals, test.wantBExists)
			applied, _ := run.revision(c)
			c.Assert(applied, qt.Equals, test.wantApplied)
		})
	}
}

// A failure inside the file's transaction leaves the transaction aborted on
// the session the migration pinned. The migrator ends it before it puts the
// session's timeouts back; on a session still inside the aborted transaction
// PostgreSQL refuses that restore.
func TestMigrateUpOwnTransactions_FailureEndsTheFilesTransactionLive(t *testing.T) {
	c := qt.New(t)
	run := newOwnTransactionsRun(c, postgresTestURL(t),
		"BEGIN;\nCREATE TABLE owntx_a_%[1]s (id int);\nINSERT INTO owntx_missing_%[1]s VALUES (1);\nCOMMIT;\n")

	err := run.migrator(c, migrator.WithOwnTransactions()).
		WithDefaultTimeouts(migrationfile.Timeouts{LockTimeout: 5 * time.Second, HasLockTimeout: true}).
		MigrateUp(context.Background())

	c.Assert(err, qt.ErrorMatches, `(?s).*relation "owntx_missing_\d+" does not exist.*`)
	c.Assert(err.Error(), qt.Not(qt.Contains), "restore timeouts")
	c.Assert(run.tableExists(c, "owntx_a_"), qt.IsFalse)
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
			name:   "a failure after a statement outside the file's transaction",
			txMode: migrator.MigrationTxModeFile,
			body: "BEGIN;\nCREATE TABLE owntx_a_%[1]s (id int);\nCOMMIT;\nCREATE TABLE owntx_b_%[1]s (id int);\n" +
				"INSERT INTO owntx_missing_%[1]s VALUES (1);\n",
			wantErr:         `(?s).*relation "owntx_missing_\d+" does not exist.*`,
			wantTable:       "owntx_b_",
			wantTableExists: false,
			wantApplied:     3,
			wantFailure:     "does not exist",
		},
		{
			name:   "a failure after a statement outside the file's transaction under tx-mode none",
			txMode: migrator.MigrationTxModeNone,
			body: "BEGIN;\nCREATE TABLE owntx_a_%[1]s (id int);\nCOMMIT;\nCREATE TABLE owntx_b_%[1]s (id int);\n" +
				"INSERT INTO owntx_missing_%[1]s VALUES (1);\n",
			wantErr:         `(?s).*relation "owntx_missing_\d+" does not exist.*`,
			wantTable:       "owntx_b_",
			wantTableExists: true,
			wantApplied:     4,
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
			name:   "a failure in the second of two transactions",
			txMode: migrator.MigrationTxModeFile,
			body: "BEGIN;\nCREATE TABLE owntx_a_%[1]s (id int);\nCOMMIT;\nBEGIN;\nCREATE TABLE owntx_b_%[1]s (id int);\n" +
				"INSERT INTO owntx_missing_%[1]s VALUES (1);\nCOMMIT;\n",
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
			wantErr:         `(?s).*migration 2 opens and closes its own transaction, which tx-mode all would nest .*; apply the directory with tx-mode file`,
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

			err := run.migrator(c, migrator.WithOwnTransactions()).WithTransactionMode(test.txMode).MigrateUp(context.Background())

			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(run.tableExists(c, test.wantTable), qt.Equals, test.wantTableExists)
			applied, failure := run.revision(c)
			c.Assert(applied, qt.Equals, test.wantApplied)
			c.Assert(failure, qt.Contains, test.wantFailure)
		})
	}
}

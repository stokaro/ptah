//go:build integration

package migrator_test

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"ptah.run/migration/migrationfile"
	"ptah.run/migration/migrator"
)

// golangMigrateUnwrappedCases are files whose transaction control a runner
// that sends a file as it is, as golang-migrate does, accepts beyond a block
// the file opens and closes, and the forms Ptah refuses before anything runs.
// The tables each leaves are what golang-migrate v4.20.1 leaves when it applies
// the file as the second migration of a directory, measured through the
// postgres:// and pgx5:// drivers alike on PostgreSQL 15.19: in its default
// mode for tx-mode file, and with x-multi-statement=true for tx-mode none.
// applied is what Ptah's revision row records, -1 for a refusal that leaves
// no row.
var golangMigrateUnwrappedCases = []struct {
	name   string
	body   string
	txMode migrator.MigrationTxMode
	// wantErr matches the run's error, the empty string for none.
	wantErr     string
	wantTables  []string
	wantApplied int
}{
	{
		name: "a COMMIT outside a block, after a new enum value",
		body: "CREATE TYPE owntx_color_%[1]s AS ENUM ('red');\nALTER TYPE owntx_color_%[1]s ADD VALUE 'green';\nCOMMIT;\n" +
			"CREATE TABLE owntx_a_%[1]s (c owntx_color_%[1]s DEFAULT 'green');\nINSERT INTO owntx_a_%[1]s VALUES ('green');\n",
		txMode:      migrator.MigrationTxModeFile,
		wantTables:  []string{"a"},
		wantApplied: 5,
	},
	{
		name: "a failure after a COMMIT outside a block",
		body: "CREATE TABLE owntx_a_%[1]s (id int);\nCOMMIT;\nCREATE TABLE owntx_b_%[1]s (id int);\n" +
			"INSERT INTO owntx_missing_%[1]s VALUES (1);\n",
		txMode:      migrator.MigrationTxModeFile,
		wantErr:     `(?s).*relation "owntx_missing_\d+" does not exist.*`,
		wantTables:  []string{"a"},
		wantApplied: 2,
	},
	{
		name: "a failure after a COMMIT outside a block under tx-mode none",
		body: "CREATE TABLE owntx_a_%[1]s (id int);\nCOMMIT;\nCREATE TABLE owntx_b_%[1]s (id int);\n" +
			"INSERT INTO owntx_missing_%[1]s VALUES (1);\n",
		txMode:      migrator.MigrationTxModeNone,
		wantErr:     `(?s).*relation "owntx_missing_\d+" does not exist.*`,
		wantTables:  []string{"a", "b"},
		wantApplied: 3,
	},
	{
		name: "a savepoint rolled back inside the block",
		body: "BEGIN;\nCREATE TABLE owntx_a_%[1]s (id int);\nSAVEPOINT s;\nCREATE TABLE owntx_b_%[1]s (id int);\n" +
			"ROLLBACK TO SAVEPOINT s;\nCREATE TABLE owntx_c_%[1]s (id int);\nRELEASE SAVEPOINT s;\nCOMMIT;\n",
		txMode:      migrator.MigrationTxModeFile,
		wantTables:  []string{"a", "c"},
		wantApplied: 8,
	},
	{
		name: "a savepoint in a block the statement before it joins",
		body: "CREATE TABLE owntx_a_%[1]s (id int);\nBEGIN;\nSAVEPOINT s;\nCREATE TABLE owntx_b_%[1]s (id int);\n" +
			"ROLLBACK TO SAVEPOINT s;\nCOMMIT;\nCREATE TABLE owntx_c_%[1]s (id int);\n",
		txMode:      migrator.MigrationTxModeFile,
		wantTables:  []string{"a", "c"},
		wantApplied: 7,
	},
	{
		name:        "a BEGIN inside a block",
		body:        "BEGIN;\nBEGIN;\nCREATE TABLE owntx_a_%[1]s (id int);\nCOMMIT;\n",
		txMode:      migrator.MigrationTxModeFile,
		wantTables:  []string{"a"},
		wantApplied: 4,
	},
	{
		name:        "a BEGIN inside a block under tx-mode none",
		body:        "BEGIN;\nBEGIN;\nCREATE TABLE owntx_a_%[1]s (id int);\nCOMMIT;\n",
		txMode:      migrator.MigrationTxModeNone,
		wantTables:  []string{"a"},
		wantApplied: 4,
	},
	{
		name: "an isolation level inside a block before its first query",
		body: "BEGIN;\nBEGIN ISOLATION LEVEL SERIALIZABLE;\n" +
			"CREATE TABLE owntx_a_%[1]s AS SELECT current_setting('transaction_isolation') AS iso;\nCOMMIT;\n",
		txMode:      migrator.MigrationTxModeFile,
		wantTables:  []string{"a"},
		wantApplied: 4,
	},
	{
		name: "an isolation level inside a block after a write",
		body: "BEGIN;\nCREATE TABLE owntx_a_%[1]s (id int);\nBEGIN ISOLATION LEVEL SERIALIZABLE;\n" +
			"CREATE TABLE owntx_b_%[1]s (id int);\nCOMMIT;\n",
		txMode:      migrator.MigrationTxModeFile,
		wantErr:     `(?s).*SET TRANSACTION ISOLATION LEVEL must be called before any query.*`,
		wantTables:  nil,
		wantApplied: 0,
	},
	{
		name:        "a lone DO block that commits inside",
		body:        "DO $$ BEGIN CREATE TABLE owntx_a_%[1]s (id int); COMMIT; CREATE TABLE owntx_b_%[1]s (id int); END $$;\n",
		txMode:      migrator.MigrationTxModeFile,
		wantTables:  []string{"a", "b"},
		wantApplied: 1,
	},
	{
		name:        "a lone VACUUM",
		body:        "VACUUM owntx_base_%[1]s;\n",
		txMode:      migrator.MigrationTxModeFile,
		wantTables:  nil,
		wantApplied: 1,
	},
	{
		name:        "a ROLLBACK",
		body:        "BEGIN;\nCREATE TABLE owntx_a_%[1]s (id int);\nROLLBACK;\nCREATE TABLE owntx_b_%[1]s (id int);\n",
		txMode:      migrator.MigrationTxModeFile,
		wantErr:     `(?s).*at statement 3: ROLLBACK takes the file's own work back.*`,
		wantTables:  nil,
		wantApplied: -1,
	},
	{
		name: "a savepoint outside a block",
		body: "CREATE TABLE owntx_a_%[1]s (id int);\nSAVEPOINT s;\nCREATE TABLE owntx_b_%[1]s (id int);\n" +
			"RELEASE SAVEPOINT s;\n",
		txMode:      migrator.MigrationTxModeFile,
		wantErr:     `(?s).*at statement 2: SAVEPOINT outside a transaction the file opened.*`,
		wantTables:  nil,
		wantApplied: -1,
	},
	{
		name:        "a block the file leaves open",
		body:        "CREATE TABLE owntx_a_%[1]s (id int);\nBEGIN;\nCREATE TABLE owntx_b_%[1]s (id int);\n",
		txMode:      migrator.MigrationTxModeFile,
		wantErr:     `(?s).*at statement 2: the transaction is never closed by the file.*`,
		wantTables:  nil,
		wantApplied: -1,
	},
}

// Ptah leaves the tables golang-migrate leaves for each file, and refuses the
// forms it does not run before any migration runs. golang-migrate applies a
// ROLLBACK, and its drivers settle a block left open in opposite ways; see
// the documentation of the refused forms.
func TestMigrateUpOwnTransactions_UnwrappedFormsMatchGolangMigrateLive(t *testing.T) {
	for _, test := range golangMigrateUnwrappedCases {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			run := newOwnTransactionsRun(c, postgresTestURL(t), test.body)
			c.Cleanup(func() {
				_, _ = run.conn.ExecContext(context.Background(), "DROP TYPE IF EXISTS owntx_color_"+run.suffix+" CASCADE")
			})

			err := run.migrator(c, migrator.WithOwnTransactions()).WithTransactionMode(test.txMode).MigrateUp(context.Background())

			c.Assert(errorText(err), qt.Matches, test.wantErr)
			c.Assert(run.tables(c), qt.DeepEquals, test.wantTables)
			applied, _ := run.revision(c)
			c.Assert(applied, qt.Equals, test.wantApplied)
		})
	}
}

// After a failure that follows a COMMIT outside a block, a retry resumes after
// that COMMIT and runs the statements of the transaction the failure took back.
func TestMigrateUpOwnTransactions_RetryAfterACommitOutsideABlockLive(t *testing.T) {
	c := qt.New(t)
	run := newOwnTransactionsRun(c, postgresTestURL(t),
		"CREATE TABLE owntx_a_%[1]s (id int);\nCOMMIT;\nCREATE TABLE owntx_b_%[1]s (id int);\nINSERT INTO owntx_missing_%[1]s VALUES (1);\n")
	c.Assert(run.migrator(c, migrator.WithOwnTransactions()).MigrateUp(context.Background()), qt.IsNotNil)
	run.exec(c, "CREATE TABLE owntx_missing_"+run.suffix+" (id int)")

	err := run.migrator(c, migrator.WithOwnTransactions()).MigrateUpWithOptions(context.Background(),
		migrator.MigrateUpOptions{AllowDirty: true})

	c.Assert(err, qt.IsNil)
	c.Assert(run.tables(c), qt.DeepEquals, []string{"a", "b"})
	applied, failure := run.revision(c)
	c.Assert(applied, qt.Equals, 4)
	c.Assert(failure, qt.Equals, "")
}

// A resumed attempt reads a DO, a SELECT and a WITH in a committed transaction
// as durable: their text sets no session state, so nothing has to be
// replayed for them. One whose text sets such state, here set_config with a
// false is_local, keeps the resume refused, because the state it left is gone
// with the session. golang-migrate leaves a and b after the first failure.
func TestMigrateUpOwnTransactions_ResumeOverStatementsWithoutSessionStateLive(t *testing.T) {
	const prefix = "CREATE TABLE owntx_a_%[1]s (id serial);\nDO $$ BEGIN CREATE TABLE owntx_b_%[1]s (id int); END $$;\n" +
		"SELECT setval('owntx_a_%[1]s_id_seq', 10);\nWITH v AS (SELECT 1 AS id) INSERT INTO owntx_a_%[1]s SELECT id FROM v;\n"
	tests := []struct {
		name    string
		body    string
		wantErr string
	}{
		{
			name: "a DO, a SELECT and a WITH",
			body: prefix + "COMMIT;\nINSERT INTO owntx_missing_%[1]s VALUES (1);\n",
		},
		{
			name: "a DO that sets a session setting",
			body: prefix + "DO $$ BEGIN PERFORM set_config('application_name', 'owntx', false); END $$;\nCOMMIT;\n" +
				"INSERT INTO owntx_missing_%[1]s VALUES (1);\n",
			wantErr: `(?s).*cannot reconstruct session state after committed statement 5 because "DO .*`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			run := newOwnTransactionsRun(c, postgresTestURL(t), test.body)
			c.Assert(run.migrator(c, migrator.WithOwnTransactions()).MigrateUp(context.Background()), qt.IsNotNil)
			c.Assert(run.tables(c), qt.DeepEquals, []string{"a", "b"})
			run.exec(c, "CREATE TABLE owntx_missing_"+run.suffix+" (id int)")

			err := run.migrator(c, migrator.WithOwnTransactions()).MigrateUpWithOptions(context.Background(),
				migrator.MigrateUpOptions{AllowDirty: true})

			c.Assert(errorText(err), qt.Matches, test.wantErr)
		})
	}
}

// A run killed while the commit of one of the file's transactions was in
// flight leaves the mark written before it: the first statement of the
// transaction, and the statements before it as applied. The refusal to resume
// names the statements the commit decides together, and the commit, rather
// than the first statement alone. The mark is written into the revision row
// of a failed attempt here, as the killed run would have left it.
func TestMigrateUpOwnTransactions_UnknownCommitOutcomeNamesTheTransactionLive(t *testing.T) {
	const body = "CREATE TABLE owntx_a_%[1]s (id int);\nBEGIN;\nCREATE TABLE owntx_b_%[1]s (id int);\nCOMMIT;\n" +
		"CREATE TABLE owntx_c_%[1]s (id int);\nINSERT INTO owntx_missing_%[1]s VALUES (1);\n"
	tests := []struct {
		name string
		// applied and statement are the mark.
		applied   int
		statement string
		wantErr   string
	}{
		{
			name: "the file's COMMIT", applied: 0, statement: "CREATE TABLE owntx_a_%s (id int)",
			wantErr: `(?s).*migration 2 cannot resume automatically: statements 1 to 4 commit together, and whether the COMMIT ` +
				`at statement 4 took effect is unknown after an interrupted run; .*`,
		},
		{
			name: "the commit at the end of the file", applied: 4, statement: "CREATE TABLE owntx_c_%s (id int)",
			wantErr: `(?s).*migration 2 cannot resume automatically: statements 5 to 6 commit together, and whether the commit ` +
				`at the end of the file took effect is unknown after an interrupted run; .*`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			run := newOwnTransactionsRun(c, postgresTestURL(t), body)
			c.Assert(run.migrator(c, migrator.WithOwnTransactions()).MigrateUp(context.Background()), qt.IsNotNil)
			run.exec(c, fmt.Sprintf("UPDATE %s SET applied = %d, error = 'statement execution outcome is unknown after process "+
				"interruption', error_stmt = '%s' WHERE version = 2", run.table, test.applied, fmt.Sprintf(test.statement, run.suffix)))

			err := run.migrator(c, migrator.WithOwnTransactions()).MigrateUpWithOptions(context.Background(),
				migrator.MigrateUpOptions{AllowDirty: true})

			c.Assert(err, qt.ErrorMatches, test.wantErr)
		})
	}
}

// terminateRecordedSessionAfter is a statement observer that, once a
// statement starting with prefix ran, terminates the session whose process id
// the body wrote into owntx_log.
func terminateRecordedSessionAfter(c *qt.C, run *ownTransactionsRun, prefix string) migrator.FSProviderOption {
	return migrator.WithStatementObserver(migrator.StatementObserverFunc(
		func(_ context.Context, event migrator.StatementEvent) error {
			if strings.HasPrefix(strings.TrimSpace(event.Statement), prefix) {
				run.exec(c, "SELECT pg_terminate_backend(pid, 5000) FROM owntx_log_"+run.suffix)
			}
			return nil
		}))
}

// A body that committed every statement before the session's timeouts could
// be put back records every statement as applied, so a retry has nothing left
// to run. The session dies after the body's last commit here, which is what
// makes putting the timeouts back fail.
func TestMigrateUpOwnTransactions_RestoreFailsAfterTheBodyCommittedLive(t *testing.T) {
	tests := []struct {
		name   string
		body   string
		txMode migrator.MigrationTxMode
		// after starts the statement after which the session dies.
		after       string
		wantApplied int
	}{
		{
			name:   "a body whose last statement is its COMMIT",
			body:   "BEGIN;\nCREATE TABLE owntx_a_%[1]s (id int);\nINSERT INTO owntx_log_%[1]s SELECT pg_backend_pid();\nCOMMIT;\n",
			txMode: migrator.MigrationTxModeFile, after: "COMMIT", wantApplied: 4,
		},
		{
			name: "a body run per statement under tx-mode none",
			body: "BEGIN;\nCREATE TABLE owntx_a_%[1]s (id int);\nCOMMIT;\n" +
				"INSERT INTO owntx_log_%[1]s SELECT pg_backend_pid();\n",
			txMode: migrator.MigrationTxModeNone, after: "INSERT INTO owntx_log_", wantApplied: 4,
		},
		{
			name:   "a body with no transaction of its own under tx-mode none",
			body:   "CREATE TABLE owntx_a_%[1]s (id int);\nINSERT INTO owntx_log_%[1]s SELECT pg_backend_pid();\n",
			txMode: migrator.MigrationTxModeNone, after: "INSERT INTO owntx_log_", wantApplied: 2,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			run := newOwnTransactionsRun(c, postgresTestURL(t), test.body)
			run.exec(c, "CREATE TABLE owntx_log_"+run.suffix+" (pid int)")
			timeouts := migrationfile.Timeouts{LockTimeout: 5 * time.Second, HasLockTimeout: true}

			err := run.migrator(c, migrator.WithOwnTransactions(), terminateRecordedSessionAfter(c, run, test.after)).
				WithTransactionMode(test.txMode).WithDefaultTimeouts(timeouts).MigrateUp(context.Background())

			c.Assert(err, qt.ErrorMatches, `(?s).*failed to restore timeouts.*`)
			c.Assert(err.Error(), qt.Not(qt.Contains), "discard pinned database session")
			applied, _ := run.revision(c)
			c.Assert(applied, qt.Equals, test.wantApplied)
			c.Assert(run.migrator(c, migrator.WithOwnTransactions()).WithTransactionMode(test.txMode).
				MigrateUpWithOptions(context.Background(), migrator.MigrateUpOptions{AllowDirty: true}), qt.IsNil)
		})
	}
}

// Under tx-mode none a LOCK outside the file's block fails where it runs, with
// PostgreSQL's answer and a revision row, rather than in the replay of
// session state that comes before the migration.
func TestMigrateUpOwnTransactions_LockOutsideTheBlockFailsAtTheStatementLive(t *testing.T) {
	c := qt.New(t)
	run := newOwnTransactionsRun(c, postgresTestURL(t),
		"LOCK TABLE owntx_base_%[1]s IN SHARE MODE;\nBEGIN;\nCREATE TABLE owntx_a_%[1]s (id int);\nCOMMIT;\n")

	err := run.migrator(c, migrator.WithOwnTransactions()).WithTransactionMode(migrator.MigrationTxModeNone).
		MigrateUp(context.Background())

	c.Assert(err, qt.ErrorMatches, `(?s).*LOCK TABLE can only be used in transaction blocks.*SQL: LOCK TABLE owntx_base_\d+.*`)
	c.Assert(err.Error(), qt.Not(qt.Contains), "index recovery preflight")
	applied, _ := run.revision(c)
	c.Assert(applied, qt.Equals, 0)
}

// A concurrent build beside a second statement is refused in the per-file
// transaction, and the advice is one the directory's runner can follow: a
// file of its own runs without a transaction there, and golang-migrate has no
// directive for it.
func TestMigrateUpOwnTransactions_RefusalAdviceFitsTheRunnerLive(t *testing.T) {
	c := qt.New(t)
	run := newOwnTransactionsRun(c, postgresTestURL(t),
		"CREATE TABLE owntx_a_%[1]s (id int);\nCREATE INDEX CONCURRENTLY owntx_a_%[1]s_id_idx ON owntx_a_%[1]s (id);\n")

	err := run.migrator(c, migrator.WithOwnTransactions()).MigrateUp(context.Background())

	c.Assert(err, qt.ErrorMatches, `(?s).*cannot run inside a transaction: .*; move the statement into a migration file of its own, `+
		`which runs without a transaction.*`)
	c.Assert(err.Error(), qt.Not(qt.Contains), "no_transaction")
}

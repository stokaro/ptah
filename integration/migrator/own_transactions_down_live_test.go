//go:build integration

package migrator_test

import (
	"context"
	"fmt"
	"testing"
	"testing/fstest"

	qt "github.com/frankban/quicktest"

	"ptah.run/migration/migrator"
)

// These tests drive the rollback half of [migrator.WithOwnTransactions] on
// PostgreSQL: a down file written for golang-migrate runs the way an up file
// of the same directory does, outside any transaction of the migrator's.
//
// Every run applies a second migration that creates tables a, b and c, then
// rolls that migration back with the down file the test writes. What the
// server decides is read back from it: which tables are left, and what the
// revision row of the second migration records, or that it is gone.

// ownTransactionsDownUp is the up file of the migration each run rolls back.
const ownTransactionsDownUp = "CREATE TABLE owntx_a_%[1]s (id int);\nCREATE TABLE owntx_b_%[1]s (id int);\n" +
	"CREATE TABLE owntx_c_%[1]s (id int);\n"

// newOwnTransactionsDownRun applies a directory whose second migration
// creates tables a, b and c and rolls back with down. Every %[1]s in down is
// the run's suffix.
func newOwnTransactionsDownRun(c *qt.C, dbURL, down string) *ownTransactionsRun {
	c.Helper()
	run := newOwnTransactionsRun(c, dbURL, ownTransactionsDownUp)
	run.fsys["0000000002_body.down.sql"] = &fstest.MapFile{Data: fmt.Appendf(nil, down, run.suffix)}
	c.Assert(run.migrator(c, migrator.WithOwnTransactions()).MigrateUp(context.Background()), qt.IsNil)
	c.Assert(run.tables(c), qt.DeepEquals, []string{"a", "b", "c"})
	return run
}

// revisionState is the state the revision row of the second migration
// records, or the empty string when there is no row.
func (r *ownTransactionsRun) revisionState(c *qt.C) string {
	c.Helper()
	var state string
	c.Assert(r.conn.QueryRowContext(context.Background(),
		"SELECT COALESCE(max(state), '') FROM "+r.table+" WHERE version = 2").Scan(&state), qt.IsNil)
	return state
}

// rollBack rolls the run's second migration back.
func (r *ownTransactionsRun) rollBack(c *qt.C, options ...migrator.FSProviderOption) error {
	c.Helper()
	return r.migrator(c, options...).MigrateDownTo(context.Background(), 1)
}

// A down file that opens and closes its own transactions rolls back outside
// any transaction of the migrator's, and the revision is removed once the
// file's last statement ran.
func TestMigrateDownOwnTransactions_HappyPathLive(t *testing.T) {
	tests := []struct {
		name string
		down string
	}{
		{
			name: "a file that opens and closes its own transaction",
			down: "BEGIN;\nDROP TABLE owntx_a_%[1]s;\nDROP TABLE owntx_b_%[1]s;\nCOMMIT;\nDROP TABLE owntx_c_%[1]s;\n",
		},
		{
			name: "two transactions",
			down: "BEGIN;\nDROP TABLE owntx_a_%[1]s;\nCOMMIT;\nBEGIN;\nDROP TABLE owntx_b_%[1]s;\n" +
				"DROP TABLE owntx_c_%[1]s;\nCOMMIT;\n",
		},
		{
			name: "a file that asks for no transaction and opens its own",
			down: "-- atlas:txmode none\n\nDROP TABLE owntx_a_%[1]s;\nBEGIN;\nDROP TABLE owntx_b_%[1]s;\nCOMMIT;\n" +
				"DROP TABLE owntx_c_%[1]s;\n",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			run := newOwnTransactionsDownRun(c, postgresTestURL(t), test.down)

			err := run.rollBack(c, migrator.WithOwnTransactions())

			c.Assert(err, qt.IsNil)
			c.Assert(run.tables(c), qt.IsNil)
			applied, _ := run.revision(c)
			c.Assert(applied, qt.Equals, -1)
		})
	}
}

// A down file whose one statement PostgreSQL refuses inside a transaction
// rolls back without one, the way the runner sends it.
func TestMigrateDownOwnTransactions_LoneConcurrentDropLive(t *testing.T) {
	c := qt.New(t)
	run := newOwnTransactionsDownRun(c, postgresTestURL(t), "DROP INDEX CONCURRENTLY owntx_a_%[1]s_id_idx;\n")
	run.exec(c, "CREATE INDEX owntx_a_"+run.suffix+"_id_idx ON owntx_a_"+run.suffix+" (id)")

	err := run.rollBack(c, migrator.WithOwnTransactions())

	c.Assert(err, qt.IsNil)
	var indexes int
	c.Assert(run.conn.QueryRowContext(context.Background(),
		"SELECT count(*) FROM pg_class WHERE relname = $1", "owntx_a_"+run.suffix+"_id_idx").Scan(&indexes), qt.IsNil)
	c.Assert(indexes, qt.Equals, 0)
	applied, _ := run.revision(c)
	c.Assert(applied, qt.Equals, -1)
}

// Without the option the migrator wraps the down file, so transaction control
// in it, and a concurrent drop alone in it, are refused before the rollback
// marks the revision, and nothing is dropped. The revision row still records
// the migration applied, with every statement of its up file.
func TestMigrateDownOwnTransactions_WithoutTheOptionLive(t *testing.T) {
	tests := []struct {
		name    string
		down    string
		wantErr string
	}{
		{
			name:    "a file that opens its own transaction",
			down:    "BEGIN;\nDROP TABLE owntx_a_%[1]s;\nCOMMIT;\n",
			wantErr: `(?s).*migration 2 cannot run tx-mode file down statement 1 because it controls transaction state.*`,
		},
		{
			name: "a lone concurrent drop",
			down: "DROP INDEX CONCURRENTLY owntx_a_%[1]s_id_idx;\n",
			wantErr: `(?s)0000000002_body\.down\.sql cannot run inside a transaction: line 1 CREATE or DROP INDEX ` +
				`CONCURRENTLY is refused inside a transaction block.*`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			run := newOwnTransactionsDownRun(c, postgresTestURL(t), test.down)

			err := run.rollBack(c)

			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(run.tables(c), qt.DeepEquals, []string{"a", "b", "c"})
			c.Assert(run.revisionState(c), qt.Equals, "applied")
			applied, failure := run.revision(c)
			c.Assert(applied, qt.Equals, 3)
			c.Assert(failure, qt.Equals, "")
		})
	}
}

// Under the default runner a lone DROP INDEX CONCURRENTLY is refused before
// any migration of the run is rolled back, as transaction control is: the
// third migration, whose rollback runs first, keeps its table d and its
// applied revision, and the index is still there.
func TestMigrateDownOwnTransactions_ConcurrentDropRefusedBeforeTheFirstRollbackLive(t *testing.T) {
	c := qt.New(t)
	run := newOwnTransactionsRun(c, postgresTestURL(t), ownTransactionsDownUp)
	run.fsys["0000000002_body.down.sql"] = &fstest.MapFile{
		Data: fmt.Appendf(nil, "DROP INDEX CONCURRENTLY owntx_a_%s_id_idx;\n", run.suffix),
	}
	run.fsys["0000000003_d.up.sql"] = &fstest.MapFile{Data: fmt.Appendf(nil, "CREATE TABLE owntx_d_%s (id int);\n", run.suffix)}
	run.fsys["0000000003_d.down.sql"] = &fstest.MapFile{Data: fmt.Appendf(nil, "DROP TABLE owntx_d_%s;\n", run.suffix)}
	c.Assert(run.migrator(c).MigrateUp(context.Background()), qt.IsNil)
	run.exec(c, "CREATE INDEX owntx_a_"+run.suffix+"_id_idx ON owntx_a_"+run.suffix+" (id)")

	err := run.migrator(c).MigrateDownTo(context.Background(), 0)

	c.Assert(err, qt.ErrorMatches, `(?s)0000000002_body\.down\.sql cannot run inside a transaction: .*`)
	c.Assert(run.tables(c), qt.DeepEquals, []string{"a", "b", "c", "d"})
	c.Assert(run.texts(c, "SELECT version || ':' || state FROM "+run.table+" ORDER BY version"), qt.DeepEquals,
		[]string{"1:applied", "2:applied", "3:applied"})
	var indexes int
	c.Assert(run.conn.QueryRowContext(context.Background(),
		"SELECT count(*) FROM pg_class WHERE relname = $1", "owntx_a_"+run.suffix+"_id_idx").Scan(&indexes), qt.IsNil)
	c.Assert(indexes, qt.Equals, 1)
}

// A down file whose transaction control the migrator does not run, and one
// that asks for a transaction around it and opens its own, are refused before
// any migration is rolled back: every table is still there and the revision
// row still records the migration applied.
func TestMigrateDownOwnTransactions_RefusedBeforeAnythingRunsLive(t *testing.T) {
	tests := []struct {
		name    string
		down    string
		wantErr string
	}{
		{
			name: "a block ended by ROLLBACK",
			down: "BEGIN;\nDROP TABLE owntx_a_%[1]s;\nROLLBACK;\n",
			wantErr: `(?s).*rollback of migration 2: the file controls its own transactions in a form Ptah does not run, ` +
				`at statement 3: ROLLBACK .*`,
		},
		{
			name:    "a block left open",
			down:    "BEGIN;\nDROP TABLE owntx_a_%[1]s;\n",
			wantErr: `(?s).*rollback of migration 2: the file controls its own transactions in a form Ptah does not run, .*`,
		},
		{
			name:    "a file that asks for a transaction and opens its own",
			down:    "-- atlas:txmode file\n\nBEGIN;\nDROP TABLE owntx_a_%[1]s;\nCOMMIT;\n",
			wantErr: `(?s).*migration 2 asks for a transaction around its down file and opens its own.*`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			run := newOwnTransactionsDownRun(c, postgresTestURL(t), test.down)

			err := run.rollBack(c, migrator.WithOwnTransactions())

			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(run.tables(c), qt.DeepEquals, []string{"a", "b", "c"})
			c.Assert(run.revisionState(c), qt.Equals, "applied")
			applied, failure := run.revision(c)
			c.Assert(applied, qt.Equals, 3)
			c.Assert(failure, qt.Equals, "")
		})
	}
}

// The refusal reads every file of the run before the first one is rolled
// back: a rollback to version 0 whose second file is refused leaves the first
// migration's rollback unrun too.
func TestMigrateDownOwnTransactions_RefusalComesBeforeTheFirstRollbackLive(t *testing.T) {
	c := qt.New(t)
	run := newOwnTransactionsDownRun(c, postgresTestURL(t), "BEGIN;\nDROP TABLE owntx_a_%[1]s;\nCOMMIT;\n")
	run.fsys["0000000001_base.down.sql"] = &fstest.MapFile{
		Data: fmt.Appendf(nil, "BEGIN;\nDROP TABLE owntx_base_%s;\nROLLBACK;\n", run.suffix),
	}

	err := run.migrator(c, migrator.WithOwnTransactions()).MigrateDownTo(context.Background(), 0)

	c.Assert(err, qt.ErrorMatches, `(?s).*rollback of migration 1: the file controls its own transactions in a form Ptah does not run.*`)
	c.Assert(run.tables(c), qt.DeepEquals, []string{"a", "b", "c"})
	c.Assert(run.revisionState(c), qt.Equals, "applied")
	c.Assert(run.tableExists(c, "owntx_base_"), qt.IsTrue)
}

// A down file that fails is rolled back on its session, and the revision row
// stays, recording the rollback failed and the statements that stayed
// committed: none when the failure comes before the end of the file's first
// transaction, which PostgreSQL runs the statements before its BEGIN in, and
// every statement of each transaction that committed otherwise.
func TestMigrateDownOwnTransactions_FailurePathLive(t *testing.T) {
	tests := []struct {
		name string
		down string
		// wantTables are the tables the failure leaves, by letter, and
		// wantApplied what the revision row records.
		wantTables  []string
		wantApplied int
	}{
		{
			name:        "a failure inside the file's transaction",
			down:        "BEGIN;\nDROP TABLE owntx_a_%[1]s;\nINSERT INTO owntx_missing_%[1]s VALUES (1);\nCOMMIT;\n",
			wantTables:  []string{"a", "b", "c"},
			wantApplied: 0,
		},
		{
			name: "a failure before the file's transaction",
			down: "DROP TABLE owntx_a_%[1]s;\nINSERT INTO owntx_missing_%[1]s VALUES (1);\nBEGIN;\n" +
				"DROP TABLE owntx_b_%[1]s;\nCOMMIT;\n",
			wantTables:  []string{"a", "b", "c"},
			wantApplied: 0,
		},
		{
			name:        "a failure after the file's transaction committed",
			down:        "BEGIN;\nDROP TABLE owntx_a_%[1]s;\nCOMMIT;\nINSERT INTO owntx_missing_%[1]s VALUES (1);\n",
			wantTables:  []string{"b", "c"},
			wantApplied: 3,
		},
		{
			name: "a failure in the second of two transactions",
			down: "BEGIN;\nDROP TABLE owntx_a_%[1]s;\nCOMMIT;\nBEGIN;\nDROP TABLE owntx_b_%[1]s;\n" +
				"INSERT INTO owntx_missing_%[1]s VALUES (1);\nCOMMIT;\n",
			wantTables:  []string{"b", "c"},
			wantApplied: 3,
		},
		{
			name: "a failure after a statement that commits on its own",
			down: "-- atlas:txmode none\n\nBEGIN;\nDROP TABLE owntx_a_%[1]s;\nCOMMIT;\nDROP TABLE owntx_b_%[1]s;\n" +
				"INSERT INTO owntx_missing_%[1]s VALUES (1);\n",
			wantTables:  []string{"c"},
			wantApplied: 4,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			run := newOwnTransactionsDownRun(c, postgresTestURL(t), test.down)

			err := run.rollBack(c, migrator.WithOwnTransactions())

			c.Assert(err, qt.ErrorMatches, `(?s).*relation "owntx_missing_\d+" does not exist.*`)
			c.Assert(err.Error(), qt.Not(qt.Contains), "failed to roll back")
			c.Assert(run.tables(c), qt.DeepEquals, test.wantTables)
			c.Assert(run.revisionState(c), qt.Equals, "failed:down")
			applied, failure := run.revision(c)
			c.Assert(applied, qt.Equals, test.wantApplied)
			c.Assert(failure, qt.Contains, "does not exist")
		})
	}
}

// A rollback does not resume a dirty revision: the next rollback refuses it
// and runs nothing. Where the failure left nothing committed, `migrations
// repair` records the migration applied again, which is what the database
// holds, and the next rollback runs the whole file.
func TestMigrateDownOwnTransactions_RetryAfterNothingCommittedLive(t *testing.T) {
	c := qt.New(t)
	run := newOwnTransactionsDownRun(c, postgresTestURL(t),
		"BEGIN;\nDROP TABLE owntx_a_%[1]s;\nINSERT INTO owntx_missing_%[1]s VALUES (1);\nDROP TABLE owntx_b_%[1]s;\n"+
			"COMMIT;\nDROP TABLE owntx_c_%[1]s;\n")
	c.Assert(run.rollBack(c, migrator.WithOwnTransactions()), qt.ErrorMatches, `(?s).*does not exist.*`)
	run.exec(c, "CREATE TABLE owntx_missing_"+run.suffix+" (id int)")

	err := run.rollBack(c, migrator.WithOwnTransactions())

	c.Assert(err, qt.ErrorMatches, `(?s).*dirty.*`)
	c.Assert(run.tables(c), qt.DeepEquals, []string{"a", "b", "c"})

	err = run.migrator(c, migrator.WithOwnTransactions()).RepairMigration(context.Background(),
		migrator.RepairMigrationOptions{Version: 2})

	c.Assert(err, qt.IsNil)
	c.Assert(run.revisionState(c), qt.Equals, "applied")

	err = run.rollBack(c, migrator.WithOwnTransactions())

	c.Assert(err, qt.IsNil)
	c.Assert(run.tables(c), qt.IsNil)
	applied, _ := run.revision(c)
	c.Assert(applied, qt.Equals, -1)
	var rows int
	c.Assert(run.conn.QueryRowContext(context.Background(), "SELECT count(*) FROM owntx_missing_"+run.suffix).Scan(&rows), qt.IsNil)
	c.Assert(rows, qt.Equals, 1)
}

// A run interrupted inside the down file's own transaction leaves nothing of
// that transaction, and the revision row records nothing applied, so the
// repair that records the migration applied is the right one. The observer
// interrupts the run after the first table is dropped.
func TestMigrateDownOwnTransactions_InterruptedInsideTheFilesTransactionLive(t *testing.T) {
	c := qt.New(t)
	run := newOwnTransactionsDownRun(c, postgresTestURL(t),
		"BEGIN;\nDROP TABLE owntx_a_%[1]s;\nDROP TABLE owntx_b_%[1]s;\nCOMMIT;\n")
	interrupted, interrupt := context.WithCancel(context.Background())
	c.Cleanup(interrupt)

	err := run.migrator(c, migrator.WithOwnTransactions(), interruptAfter("DROP TABLE owntx_a_", interrupt)).
		MigrateDownTo(interrupted, 1)

	c.Assert(err, qt.ErrorMatches, `(?s).*SQL: DROP TABLE owntx_b_\d+.*`)
	c.Assert(err.Error(), qt.Not(qt.Contains), "failed to roll back")
	c.Assert(run.tables(c), qt.DeepEquals, []string{"a", "b", "c"})
	applied, _ := run.revision(c)
	c.Assert(applied, qt.Equals, 0)
	c.Assert(run.migrator(c, migrator.WithOwnTransactions()).RepairMigration(context.Background(),
		migrator.RepairMigrationOptions{Version: 2}), qt.IsNil)
	c.Assert(run.revisionState(c), qt.Equals, "applied")
}

// committedProgressDown is a down file whose own transaction drops table a
// and commits before a statement that fails, so a failed rollback leaves three
// of its six statements committed.
const committedProgressDown = "BEGIN;\nDROP TABLE owntx_a_%[1]s;\nCOMMIT;\nINSERT INTO owntx_missing_%[1]s VALUES (1);\n" +
	"DROP TABLE owntx_b_%[1]s;\nDROP TABLE owntx_c_%[1]s;\n"

// newCommittedProgressRun rolls committedProgressDown back and watches the
// rollback fail after its transaction committed.
func newCommittedProgressRun(c *qt.C, dbURL string) *ownTransactionsRun {
	c.Helper()
	run := newOwnTransactionsDownRun(c, dbURL, committedProgressDown)
	c.Assert(run.rollBack(c, migrator.WithOwnTransactions()), qt.ErrorMatches, `(?s).*does not exist.*`)
	applied, _ := run.revision(c)
	c.Assert(applied, qt.Equals, 3)
	return run
}

// Once a transaction of the down file committed, the database holds neither
// the migration nor its rollback, and no repair guesses which one the
// operator wants. Recording the migration applied is refused, and so is
// resuming the file, because a resume runs the rest of the file without the
// transactions it opens. Each refusal names the two endings that work. Read
// one statement at a time, as a migrator without the option reads it, the
// file's BEGIN is refused, and the refusal names the runner that reads it.
func TestMigrateDownOwnTransactions_CommittedProgressIsNotGuessed_FailurePathLive(t *testing.T) {
	const endings = `finish the rollback by hand and run "ptah migrations set --version <previous>" ` +
		`\(version 0 where this is the oldest migration in the directory\), or restore what it reverted and repair ` +
		`with --force to record the migration applied`
	tests := []struct {
		name    string
		options []migrator.FSProviderOption
		repair  migrator.RepairMigrationOptions
		wantErr string
	}{
		{
			name:    "a plain repair",
			options: []migrator.FSProviderOption{migrator.WithOwnTransactions()},
			repair:  migrator.RepairMigrationOptions{Version: 2},
			wantErr: `migration 2 stopped while rolling back: 3 of 6 down statements committed, in transactions its ` +
				`down file opens itself, and a resume would run the rest outside them; ` + endings,
		},
		{
			name:    "a resume",
			options: []migrator.FSProviderOption{migrator.WithOwnTransactions()},
			repair:  migrator.RepairMigrationOptions{Version: 2, ResumeFrom: 4},
			wantErr: `migration 2 cannot be resumed: its down file opens and closes its own transactions, and a ` +
				`resume would run the rest of it one statement at a time, outside them; ` + endings,
		},
		{
			name:   "a forced repair without the option",
			repair: migrator.RepairMigrationOptions{Version: 2, Force: true},
			wantErr: `(?s)migration 2 cannot be repaired safely: .*"BEGIN" controls transaction state.*; a directory ` +
				`written for golang-migrate, whose files open their own transactions, is repaired with ` +
				`--runner golang-migrate`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			run := newCommittedProgressRun(c, postgresTestURL(t))

			err := run.migrator(c, test.options...).RepairMigration(context.Background(), test.repair)

			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(run.tables(c), qt.DeepEquals, []string{"b", "c"})
			c.Assert(run.revisionState(c), qt.Equals, "failed:down")
			applied, _ := run.revision(c)
			c.Assert(applied, qt.Equals, 3)
		})
	}
}

// The endings the refusals name each end the revision. The operator who
// finishes the rollback by hand sets the revision, which removes the row; the
// one who restores what it reverted repairs with --force, which records the
// migration applied, and the next rollback runs the whole file again.
func TestMigrateDownOwnTransactions_CommittedProgressIsNotGuessed_HappyPathLive(t *testing.T) {
	t.Run("the rollback finished by hand", func(t *testing.T) {
		c := qt.New(t)
		run := newCommittedProgressRun(c, postgresTestURL(t))
		run.exec(c, "DROP TABLE owntx_b_"+run.suffix)
		run.exec(c, "DROP TABLE owntx_c_"+run.suffix)

		_, err := run.migrator(c, migrator.WithOwnTransactions()).SetRevision(context.Background(), 1)

		c.Assert(err, qt.IsNil)
		applied, _ := run.revision(c)
		c.Assert(applied, qt.Equals, -1)
	})

	t.Run("the migration restored by hand", func(t *testing.T) {
		c := qt.New(t)
		run := newCommittedProgressRun(c, postgresTestURL(t))
		run.exec(c, "CREATE TABLE owntx_a_"+run.suffix+" (id int)")

		err := run.migrator(c, migrator.WithOwnTransactions()).RepairMigration(context.Background(),
			migrator.RepairMigrationOptions{Version: 2, Force: true})

		c.Assert(err, qt.IsNil)
		c.Assert(run.revisionState(c), qt.Equals, "applied")
		run.exec(c, "CREATE TABLE owntx_missing_"+run.suffix+" (id int)")
		c.Assert(run.rollBack(c, migrator.WithOwnTransactions()), qt.IsNil)
		c.Assert(run.tables(c), qt.IsNil)
		applied, _ := run.revision(c)
		c.Assert(applied, qt.Equals, -1)
	})
}

// The mark written before the COMMIT of the down file's transaction names the
// transaction's first statement, which under the default tx-mode is the
// file's first statement, counts nothing applied, and records the rollback
// pending, so a process that dies at the COMMIT leaves a row a repair reads as
// a rollback's. When the mark cannot be written, COMMIT is never sent: the
// migrator rolls the transaction back, so every table is still there. The
// trigger refuses exactly that mark, so a mark that names COMMIT, counts the
// transaction, or records an apply lets the rollback succeed.
func TestMigrateDownOwnTransactions_MarkBeforeCommitFailsLive(t *testing.T) {
	c := qt.New(t)
	run := newOwnTransactionsDownRun(c, postgresTestURL(t),
		"DROP TABLE owntx_a_%[1]s;\nBEGIN;\nDROP TABLE owntx_b_%[1]s;\nCOMMIT;\n")
	c.Cleanup(func() {
		_, _ = run.conn.ExecContext(context.Background(), "DROP FUNCTION IF EXISTS owntx_refuse_"+run.suffix+"() CASCADE")
	})
	run.exec(c, "CREATE FUNCTION owntx_refuse_"+run.suffix+"() RETURNS trigger LANGUAGE plpgsql AS $$\n"+
		"BEGIN\n"+
		"  IF NEW.version = 2 AND NEW.state = 'pending:down' AND NEW.error_stmt LIKE 'DROP TABLE owntx_a_%' AND\n"+
		"     NEW.applied = 0 AND NEW.error LIKE 'statement execution outcome is unknown%' THEN\n"+
		"    RAISE EXCEPTION 'owntx: the mark is refused';\n"+
		"  END IF;\n"+
		"  RETURN NEW;\n"+
		"END $$")
	run.exec(c, "CREATE TRIGGER owntx_refuse BEFORE UPDATE ON "+run.table+
		" FOR EACH ROW EXECUTE FUNCTION owntx_refuse_"+run.suffix+"()")

	err := run.rollBack(c, migrator.WithOwnTransactions())

	c.Assert(err, qt.ErrorMatches, `(?s).*owntx: the mark is refused.*`)
	c.Assert(run.tables(c), qt.DeepEquals, []string{"a", "b", "c"})
	c.Assert(run.revisionState(c), qt.Equals, "failed:down")
	applied, failure := run.revision(c)
	c.Assert(applied, qt.Equals, 0)
	c.Assert(failure, qt.Contains, "the mark is refused")
}

// The checkpoint after the commit at the end of the down file records the
// whole file, on a rollback still pending, once that commit succeeded. When it
// cannot be written, the
// failure still records every statement as applied, because they committed,
// and the revision stays: it is removed only by a rollback that recorded its
// end. The trigger refuses the first update that reaches the last statement
// with no error, which is that checkpoint.
func TestMigrateDownOwnTransactions_CheckpointAfterTheEndOfTheFileFailsLive(t *testing.T) {
	c := qt.New(t)
	run := newOwnTransactionsDownRun(c, postgresTestURL(t),
		"BEGIN;\nDROP TABLE owntx_a_%[1]s;\nCOMMIT;\nDROP TABLE owntx_b_%[1]s;\n")
	c.Cleanup(func() {
		_, _ = run.conn.ExecContext(context.Background(), "DROP FUNCTION IF EXISTS owntx_refuse_"+run.suffix+"() CASCADE")
	})
	run.exec(c, "CREATE FUNCTION owntx_refuse_"+run.suffix+"() RETURNS trigger LANGUAGE plpgsql AS $$\n"+
		"BEGIN\n"+
		"  IF NEW.version = 2 AND NEW.state = 'pending:down' AND NEW.applied = 4 AND OLD.applied <> 4 AND\n"+
		"     COALESCE(NEW.error, '') = '' THEN\n"+
		"    RAISE EXCEPTION 'owntx: the checkpoint is refused';\n"+
		"  END IF;\n"+
		"  RETURN NEW;\n"+
		"END $$")
	run.exec(c, "CREATE TRIGGER owntx_refuse BEFORE UPDATE ON "+run.table+
		" FOR EACH ROW EXECUTE FUNCTION owntx_refuse_"+run.suffix+"()")

	err := run.rollBack(c, migrator.WithOwnTransactions())

	c.Assert(err, qt.ErrorMatches, `(?s).*owntx: the checkpoint is refused.*`)
	c.Assert(run.tables(c), qt.DeepEquals, []string{"c"})
	c.Assert(run.revisionState(c), qt.Equals, "failed:down")
	applied, failure := run.revision(c)
	c.Assert(applied, qt.Equals, 4)
	c.Assert(failure, qt.Contains, "the checkpoint is refused")
}

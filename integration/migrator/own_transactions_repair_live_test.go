//go:build integration

package migrator_test

import (
	"context"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/migration/migrator"
)

// These tests repair, on PostgreSQL, a revision a run left after every
// statement of a file that opens its own transactions committed: the
// checkpoint that records the end of the file is refused by a trigger, so the
// row stays dirty with every statement counted. A repair under
// [migrator.WithOwnTransactions] records what such a run would have recorded,
// as it does for a file the migrator wraps: the rollback removes the row, and
// the apply records the migration applied.

// refuseFinalCheckpoint installs a trigger that refuses the update recording
// statement applied of the second migration with no error, which is the
// checkpoint after the last statement of a file of applied statements, and
// removes it once the test ends.
func refuseFinalCheckpoint(c *qt.C, run *ownTransactionsRun, applied string) {
	c.Helper()
	c.Cleanup(func() {
		_, _ = run.conn.ExecContext(context.Background(), "DROP FUNCTION IF EXISTS owntx_refuse_"+run.suffix+"() CASCADE")
	})
	run.exec(c, "CREATE FUNCTION owntx_refuse_"+run.suffix+"() RETURNS trigger LANGUAGE plpgsql AS $$\n"+
		"BEGIN\n"+
		"  IF NEW.version = 2 AND NEW.applied = "+applied+" AND OLD.applied <> "+applied+" AND\n"+
		"     COALESCE(NEW.error, '') = '' THEN\n"+
		"    RAISE EXCEPTION 'owntx: the checkpoint is refused';\n"+
		"  END IF;\n"+
		"  RETURN NEW;\n"+
		"END $$")
	run.exec(c, "CREATE TRIGGER owntx_refuse BEFORE UPDATE ON "+run.table+
		" FOR EACH ROW EXECUTE FUNCTION owntx_refuse_"+run.suffix+"()")
}

// newCompletedRollbackRun rolls back a down file of four statements, one
// transaction of its own and a drop after it, and watches the checkpoint that
// records its end fail.
func newCompletedRollbackRun(c *qt.C, dbURL string) *ownTransactionsRun {
	c.Helper()
	run := newOwnTransactionsDownRun(c, dbURL, "BEGIN;\nDROP TABLE owntx_a_%[1]s;\nCOMMIT;\nDROP TABLE owntx_b_%[1]s;\n")
	refuseFinalCheckpoint(c, run, "4")
	c.Assert(run.rollBack(c, migrator.WithOwnTransactions()), qt.ErrorMatches, `(?s).*owntx: the checkpoint is refused.*`)
	c.Assert(run.revisionState(c), qt.Equals, "failed:down")
	applied, _ := run.revision(c)
	c.Assert(applied, qt.Equals, 4)
	return run
}

// newCompletedApplyRun applies an up file of four statements, one transaction
// of its own and a create after it, and watches the checkpoint that records
// its end fail.
func newCompletedApplyRun(c *qt.C, dbURL string) *ownTransactionsRun {
	c.Helper()
	run := newOwnTransactionsRun(c, dbURL,
		"BEGIN;\nCREATE TABLE owntx_b_%[1]s (id int);\nCOMMIT;\nCREATE TABLE owntx_a_%[1]s (id int);\n")
	m := run.migrator(c, migrator.WithOwnTransactions())
	c.Assert(m.Initialize(context.Background()), qt.IsNil)
	refuseFinalCheckpoint(c, run, "4")
	c.Assert(m.MigrateUp(context.Background()), qt.ErrorMatches, `(?s).*owntx: the checkpoint is refused.*`)
	c.Assert(run.revisionState(c), qt.Equals, "failed")
	applied, _ := run.revision(c)
	c.Assert(applied, qt.Equals, 4)
	return run
}

// The repair that runs no SQL records the end the run reached: the rollback's
// row is removed and the apply's records the migration applied. What the file
// left is what the database keeps.
func TestRepairMigrationOwnTransactions_CompletedBody_HappyPathLive(t *testing.T) {
	t.Run("a rollback", func(t *testing.T) {
		c := qt.New(t)
		run := newCompletedRollbackRun(c, postgresTestURL(t))

		err := run.migrator(c, migrator.WithOwnTransactions()).RepairMigration(context.Background(),
			migrator.RepairMigrationOptions{Version: 2})

		c.Assert(err, qt.IsNil)
		applied, _ := run.revision(c)
		c.Assert(applied, qt.Equals, -1)
		c.Assert(run.tables(c), qt.DeepEquals, []string{"c"})
	})

	t.Run("an apply", func(t *testing.T) {
		c := qt.New(t)
		run := newCompletedApplyRun(c, postgresTestURL(t))

		err := run.migrator(c, migrator.WithOwnTransactions()).RepairMigration(context.Background(),
			migrator.RepairMigrationOptions{Version: 2})

		c.Assert(err, qt.IsNil)
		c.Assert(run.revisionState(c), qt.Equals, "applied")
		applied, failure := run.revision(c)
		c.Assert(applied, qt.Equals, 4)
		c.Assert(failure, qt.Equals, "")
		c.Assert(run.tables(c), qt.DeepEquals, []string{"a", "b"})
	})
}

// Read one statement at a time, as a migrator without the option reads it,
// the file's BEGIN is refused, and the refusal names the runner that reads it.
// A resume is refused under the option too: no statement is left to run, and
// the refusal names the repair that ends the revision.
func TestRepairMigrationOwnTransactions_CompletedBody_FailurePathLive(t *testing.T) {
	const withoutTheRunner = `(?s)migration 2 cannot be repaired safely: .*"BEGIN" controls transaction state.*; ` +
		`a directory written for golang-migrate, whose files open their own transactions, is repaired with ` +
		`--runner golang-migrate`
	completedRuns := map[migrator.MigrationDirection]func(c *qt.C, dbURL string) *ownTransactionsRun{
		migrator.MigrationDirectionDown: newCompletedRollbackRun,
		migrator.MigrationDirectionUp:   newCompletedApplyRun,
	}
	tests := []struct {
		name string
		// direction is the run that left the revision.
		direction migrator.MigrationDirection
		options   []migrator.FSProviderOption
		repair    migrator.RepairMigrationOptions
		wantErr   string
		wantState string
		wantTable []string
	}{
		{
			name:      "a rollback without the option",
			direction: migrator.MigrationDirectionDown,
			repair:    migrator.RepairMigrationOptions{Version: 2},
			wantErr:   withoutTheRunner,
			wantState: "failed:down",
			wantTable: []string{"c"},
		},
		{
			name:      "a rollback resumed",
			direction: migrator.MigrationDirectionDown,
			options:   []migrator.FSProviderOption{migrator.WithOwnTransactions()},
			repair:    migrator.RepairMigrationOptions{Version: 2, ResumeFrom: 5},
			wantErr: `migration 2 cannot be resumed: its down file opens and closes its own transactions, and a ` +
				`resume would run the rest of it one statement at a time, outside them; every down statement ` +
				`committed, so repair without --resume-from to remove the revision`,
			wantState: "failed:down",
			wantTable: []string{"c"},
		},
		{
			name:      "an apply without the option",
			direction: migrator.MigrationDirectionUp,
			repair:    migrator.RepairMigrationOptions{Version: 2},
			wantErr:   withoutTheRunner,
			wantState: "failed",
			wantTable: []string{"a", "b"},
		},
		{
			name:      "an apply resumed",
			direction: migrator.MigrationDirectionUp,
			options:   []migrator.FSProviderOption{migrator.WithOwnTransactions()},
			repair:    migrator.RepairMigrationOptions{Version: 2, ResumeFrom: 5},
			wantErr: `migration 2 cannot be resumed: its up file opens and closes its own transactions, and a ` +
				`resume would run the rest of it one statement at a time, outside them; every statement ` +
				`committed, so repair without --resume-from to record it applied`,
			wantState: "failed",
			wantTable: []string{"a", "b"},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			run := completedRuns[test.direction](c, postgresTestURL(t))

			err := run.migrator(c, test.options...).RepairMigration(context.Background(), test.repair)

			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(run.revisionState(c), qt.Equals, test.wantState)
			applied, _ := run.revision(c)
			c.Assert(applied, qt.Equals, 4)
			c.Assert(run.tables(c), qt.DeepEquals, test.wantTable)
		})
	}
}

// newPartialApplyRun applies an up file whose own transaction creates table a
// and commits before a statement that fails, so the revision records three of
// its four statements applied.
func newPartialApplyRun(c *qt.C, dbURL string) *ownTransactionsRun {
	c.Helper()
	run := newOwnTransactionsRun(c, dbURL,
		"BEGIN;\nCREATE TABLE owntx_a_%[1]s (id int);\nCOMMIT;\nINSERT INTO owntx_missing_%[1]s VALUES (1);\n")
	c.Assert(run.migrator(c, migrator.WithOwnTransactions()).MigrateUp(context.Background()),
		qt.ErrorMatches, `(?s).*does not exist.*`)
	applied, _ := run.revision(c)
	c.Assert(applied, qt.Equals, 3)
	return run
}

// An apply that stopped after its own transaction committed is finished by a
// retry, which resumes after that transaction. A plain repair would record the
// statement that did not run, and a resume would run it outside the file's
// transactions, so both are refused, and each names the retry.
func TestRepairMigrationOwnTransactions_PartialApply_FailurePathLive(t *testing.T) {
	const retry = `run "ptah migrations up --allow-dirty" with the golang-migrate runner, which resumes after the ` +
		`transactions that committed, or repair with --force once you have run the rest yourself`
	tests := []struct {
		name    string
		repair  migrator.RepairMigrationOptions
		wantErr string
	}{
		{
			name:   "a plain repair",
			repair: migrator.RepairMigrationOptions{Version: 2},
			wantErr: `migration 2 recorded 3 of 4 applied statements, in transactions its file opens itself; repair ` +
				`without --resume-from would mark the rest applied without running them: ` + retry,
		},
		{
			name:   "a resume",
			repair: migrator.RepairMigrationOptions{Version: 2, ResumeFrom: 4},
			wantErr: `migration 2 cannot be resumed: its up file opens and closes its own transactions, and a ` +
				`resume would run the rest of it one statement at a time, outside them; ` + retry,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			run := newPartialApplyRun(c, postgresTestURL(t))

			err := run.migrator(c, migrator.WithOwnTransactions()).RepairMigration(context.Background(), test.repair)

			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(run.revisionState(c), qt.Equals, "failed")
			applied, _ := run.revision(c)
			c.Assert(applied, qt.Equals, 3)
			c.Assert(run.tableExists(c, "owntx_missing_"), qt.IsFalse)
		})
	}
}

// The forced repair the refusals name records the migration applied, for the
// operator who ran the rest of the file by hand.
func TestRepairMigrationOwnTransactions_PartialApply_HappyPathLive(t *testing.T) {
	c := qt.New(t)
	run := newPartialApplyRun(c, postgresTestURL(t))

	err := run.migrator(c, migrator.WithOwnTransactions()).RepairMigration(context.Background(),
		migrator.RepairMigrationOptions{Version: 2, Force: true})

	c.Assert(err, qt.IsNil)
	c.Assert(run.revisionState(c), qt.Equals, "applied")
	c.Assert(run.tables(c), qt.DeepEquals, []string{"a"})
}

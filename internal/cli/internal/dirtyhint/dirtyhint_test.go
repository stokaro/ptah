package dirtyhint_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/cli/internal/dirtyhint"
	"ptah.run/migration/migrator"
)

// The hint has to read `applied` rather than print one remedy for every dirty
// row: a migration that recorded nothing wants a retry, and pointing it at
// repair records SQL that never ran (stokaro/ptah#3452). An interrupted
// statement leaves applied=0 as well and wants neither, because the row cannot
// say whether it committed. Every command a hint names is one the command it
// names accepts over that row: a plain repair refuses a body it cannot show
// ran, and a resume refuses a body that opens its own transactions.
func TestRecovery(t *testing.T) {
	tests := []struct {
		name            string
		dialect         string
		ownTransactions bool
		revision        migrator.MigrationRevision
		want            string
	}{
		{
			name:    "a statement was in flight",
			dialect: "postgres",
			revision: migrator.MigrationRevision{
				Version: 2,
				Applied: 0,
				Total:   3,
				Error:   "statement execution outcome is unknown after process interruption",
			},
			want: "The run was interrupted while statement 1 of 3 was executing, so whether it " +
				"committed was never recorded, and nothing after it ran. Inspect the database and " +
				"apply what is missing, then run 'ptah migrations repair --version 2 --force' to record " +
				"the migration applied -- rerunning it could repeat the statement that committed.",
		},
		{
			name:    "a rollback left statements in place that no count describes",
			dialect: "yugabytedb",
			revision: migrator.MigrationRevision{
				Version: 2,
				Applied: 0,
				Total:   3,
				Error:   "the rollback of the migration's transaction left some statements in place and undid others",
			},
			want: "The migration stopped, and the rollback of its transaction left some statements in place " +
				"and undid others. Inspect the database and apply what is missing, then run 'ptah migrations " +
				"repair --version 2 --force' to record the migration applied -- rerunning it could repeat a statement " +
				"that stayed.",
		},
		{
			name:    "a down migration's rollback left statements in place that no count describes",
			dialect: "yugabytedb",
			revision: migrator.MigrationRevision{
				Version:   2,
				Applied:   0,
				Total:     3,
				Direction: migrator.MigrationDirectionDown,
				Error:     "the rollback of the migration's transaction left some statements in place and undid others",
			},
			want: "The rollback stopped, and the rollback of its transaction left some statements in place and " +
				"undid others. Inspect the database, then run 'ptah migrations repair --version 2 --force' to " +
				"record the migration applied if you restored what the rollback reverted, or 'ptah migrations " +
				"set --version <previous>' (version 0 where this is the oldest migration in the directory) if " +
				"you finished the rollback by hand.",
		},
		{
			name:    "a rollback statement was in flight",
			dialect: "postgres",
			revision: migrator.MigrationRevision{
				Version:   2,
				Applied:   0,
				Total:     3,
				Direction: migrator.MigrationDirectionDown,
				Error:     "statement execution outcome is unknown after process interruption",
			},
			want: "The rollback was interrupted while down statement 1 of 3 was executing, so " +
				"whether it committed was never recorded, and nothing after it ran. Inspect the " +
				"database, then run 'ptah migrations repair --version 2 --force' to record the " +
				"migration applied if you restored what the rollback reverted, or 'ptah migrations " +
				"set --version <previous>' (version 0 where this is the oldest migration in the " +
				"directory) if you finished the rollback by hand.",
		},
		{
			name:    "a statement was in flight after two committed",
			dialect: "postgres",
			revision: migrator.MigrationRevision{
				Version: 2,
				Applied: 2,
				Total:   3,
				Error:   "statement execution outcome is unknown after process interruption",
			},
			want: "The run was interrupted while statement 3 of 3 was executing, so whether it " +
				"committed was never recorded, and nothing after it ran. Inspect the database and " +
				"apply what is missing, then run 'ptah migrations repair --version 2 --force' to record " +
				"the migration applied -- rerunning it could repeat the statement that committed.",
		},
		{
			name:    "no progress recorded where statements commit on their own",
			dialect: "clickhouse",
			revision: migrator.MigrationRevision{
				Version: 2,
				Applied: 0,
				Total:   3,
			},
			want: "On clickhouse a statement commits on its own, so nothing records how far " +
				"this run got: it may have run in full, in part, or not at all. Inspect the " +
				"database and apply whatever is missing, then run 'ptah migrations repair " +
				"--version 2 --force' to record it applied. 'ptah migrations up --allow-dirty' is safe " +
				"only once you have confirmed no statement of it ran.",
		},
		{
			name:    "no rollback progress recorded where statements commit on their own",
			dialect: "clickhouse",
			revision: migrator.MigrationRevision{
				Version:   2,
				Applied:   0,
				Total:     3,
				Direction: migrator.MigrationDirectionDown,
			},
			want: "On clickhouse a statement commits on its own, so nothing records how far " +
				"this rollback got: it may have run in full, in part, or not at all. Inspect the " +
				"database and finish the rollback by hand, then run 'ptah migrations set " +
				"--version <previous>' (version 0 where this is the oldest migration in the " +
				"directory) to move the boundary back -- or, if you restore what it reverted " +
				"instead, 'ptah migrations repair --version 2 --force' to record the " +
				"migration applied.",
		},
		{
			name:     "nothing ran on a dialect that records a witness",
			dialect:  "mysql",
			revision: migrator.MigrationRevision{Version: 2, Applied: 0, Total: 3},
			want: "No statement of this migration reached the database. " +
				"Run 'ptah migrations up --allow-dirty' to apply it.",
		},
		{
			name:     "a rollback with no statements",
			dialect:  "postgres",
			revision: migrator.MigrationRevision{Version: 2, Applied: 0, Total: 0, Direction: migrator.MigrationDirectionDown},
			want: "Every down statement of this rollback committed and the run stopped before " +
				"removing the revision. Run 'ptah migrations repair --version 2' to finish it.",
		},
		{
			name:     "nothing ran",
			dialect:  "postgres",
			revision: migrator.MigrationRevision{Version: 2, Applied: 0, Total: 3},
			want: "No statement of this migration reached the database. " +
				"Run 'ptah migrations up --allow-dirty' to apply it.",
		},
		{
			name:     "every statement committed",
			dialect:  "postgres",
			revision: migrator.MigrationRevision{Version: 2, Applied: 3, Total: 3},
			want: "Every statement of this migration committed and the run stopped before " +
				"recording that. Run 'ptah migrations repair --version 2' to record it applied.",
		},
		{
			name:    "every down statement committed",
			dialect: "postgres",
			revision: migrator.MigrationRevision{
				Version:   2,
				Applied:   3,
				Total:     3,
				Direction: migrator.MigrationDirectionDown,
			},
			want: "Every down statement of this rollback committed and the run stopped before " +
				"removing the revision. Run 'ptah migrations repair --version 2' to finish it.",
		},
		{
			name:     "some statements ran",
			dialect:  "postgres",
			revision: migrator.MigrationRevision{Version: 2, Applied: 1, Total: 3},
			want: "This migration stopped after 1 of 3 statements. Run 'ptah migrations repair " +
				"--version 2 --resume-from 2' to run the rest, or repair with --force once you " +
				"have run them yourself.",
		},
		{
			name:    "an interrupted rollback",
			dialect: "postgres",
			revision: migrator.MigrationRevision{
				Version:   2,
				Applied:   1,
				Total:     3,
				Direction: migrator.MigrationDirectionDown,
			},
			want: "This rollback stopped after 1 of 3 down statements. Run 'ptah migrations repair " +
				"--version 2 --resume-from 2' to run the remaining down statements and remove the revision.",
		},
		{
			name:    "a rollback that committed no down statement",
			dialect: "postgres",
			revision: migrator.MigrationRevision{
				Version:   2,
				Applied:   0,
				Total:     3,
				Direction: migrator.MigrationDirectionDown,
			},
			want: "No down statement of this rollback committed, so the migration is still applied. " +
				"Run 'ptah migrations repair --version 2' to record it applied, then roll it back again.",
		},
		{
			name:            "a rollback of a file that opens its own transactions committed nothing",
			dialect:         "postgres",
			ownTransactions: true,
			revision: migrator.MigrationRevision{
				Version:   2,
				Applied:   0,
				Total:     3,
				Direction: migrator.MigrationDirectionDown,
			},
			want: "No down statement of this rollback committed, so the migration is still applied. " +
				"Run 'ptah migrations repair --version 2 --runner golang-migrate' to record it applied, " +
				"then roll it back again.",
		},
		{
			name:            "a rollback of a file that opens its own transactions stopped partway",
			dialect:         "postgres",
			ownTransactions: true,
			revision: migrator.MigrationRevision{
				Version:   2,
				Applied:   1,
				Total:     3,
				Direction: migrator.MigrationDirectionDown,
			},
			want: "This rollback committed 1 of 3 down statements, in transactions its file opens itself, " +
				"and a resume would run the rest of the file outside them. Finish the rollback by hand and " +
				"run 'ptah migrations set --version <previous>' (version 0 where this is the oldest migration " +
				"in the directory), or restore what it reverted and run 'ptah migrations repair --version 2 " +
				"--force --runner golang-migrate' to record the migration applied.",
		},
		{
			name:            "a rollback of a file that opens its own transactions committed every statement",
			dialect:         "postgres",
			ownTransactions: true,
			revision: migrator.MigrationRevision{
				Version:   2,
				Applied:   3,
				Total:     3,
				Direction: migrator.MigrationDirectionDown,
			},
			want: "Every down statement of this rollback committed and the run stopped before removing the " +
				"revision. Run 'ptah migrations repair --version 2 --runner golang-migrate' to finish it.",
		},
		{
			name:            "a file that opens its own transactions committed every statement",
			dialect:         "postgres",
			ownTransactions: true,
			revision:        migrator.MigrationRevision{Version: 2, Applied: 3, Total: 3},
			want: "Every statement of this migration committed and the run stopped before recording that. " +
				"Run 'ptah migrations repair --version 2 --runner golang-migrate' to record it applied.",
		},
		{
			name:            "a file that opens its own transactions reached nothing",
			dialect:         "postgres",
			ownTransactions: true,
			revision:        migrator.MigrationRevision{Version: 2, Applied: 0, Total: 3},
			want: "No statement of this migration reached the database. " +
				"Run 'ptah migrations up --allow-dirty --runner golang-migrate' to apply it.",
		},
		{
			name:            "a file that opens its own transactions stopped partway",
			dialect:         "postgres",
			ownTransactions: true,
			revision:        migrator.MigrationRevision{Version: 2, Applied: 1, Total: 3},
			want: "This migration stopped after 1 of 3 statements, in transactions its file opens itself. " +
				"Run 'ptah migrations up --allow-dirty --runner golang-migrate' to resume after the " +
				"transactions that committed, or run 'ptah migrations repair --version 2 --force --runner " +
				"golang-migrate' once you have run the rest yourself.",
		},
		{
			name:            "a statement of a file that opens its own transactions was in flight",
			dialect:         "postgres",
			ownTransactions: true,
			revision: migrator.MigrationRevision{
				Version: 2,
				Applied: 1,
				Total:   3,
				Error:   "statement execution outcome is unknown after process interruption",
			},
			want: "The run was interrupted while statement 2 of 3 was executing, so whether it committed " +
				"was never recorded, and nothing after it ran. Inspect the database and apply what is " +
				"missing, then run 'ptah migrations repair --version 2 --force --runner golang-migrate' to " +
				"record the migration applied -- rerunning it could repeat the statement that committed.",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			got := dirtyhint.Recovery(dirtyhint.Revision{
				Revision:        &test.revision,
				Dialect:         test.dialect,
				OwnTransactions: test.ownTransactions,
			})

			c.Assert(got, qt.Equals, test.want)
		})
	}
}

// TestRecoveryReadsTheFileStatementCount names the forced repair for a run that
// stopped partway through a file whose statement count changed after it: a
// resume from the recorded statement would index into the changed file. The
// same count, and no file at all, keep the resume.
func TestRecoveryReadsTheFileStatementCount(t *testing.T) {
	resume := "This migration stopped after 1 of 2 statements. Run 'ptah migrations repair --version 1 " +
		"--resume-from 2' to run the rest, or repair with --force once you have run them yourself."
	tests := []struct {
		name  string
		count *int
		want  string
	}{
		{
			name:  "the file has another count",
			count: new(3),
			want: "This migration stopped after 1 of 2 statements, and its file now has 3, so the recorded count " +
				"does not index into it and a resume would run the wrong statements. Inspect the database and " +
				"apply what is missing, then run 'ptah migrations repair --version 1 --force' to record the " +
				"migration applied.",
		},
		{name: "the file has the recorded count", count: new(2), want: resume},
		{name: "no file", want: resume},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			revision := migrator.MigrationRevision{Version: 1, Applied: 1, Total: 2, Direction: migrator.MigrationDirectionUp}

			got := dirtyhint.Recovery(dirtyhint.Revision{
				Revision:       &revision,
				Dialect:        "postgres",
				FileStatements: test.count,
			})

			c.Assert(got, qt.Equals, test.want)
		})
	}
}

// TestRecoveryReadsTheDownFileStatementCount names the forced repair and the
// set for a rollback that stopped partway through a down file whose statement
// count changed after it, where the same count keeps the resume.
func TestRecoveryReadsTheDownFileStatementCount(t *testing.T) {
	tests := []struct {
		name  string
		count *int
		want  string
	}{
		{
			name:  "the down file has another count",
			count: new(3),
			want: "This rollback stopped after 1 of 2 down statements, and its down file now has 3, so the recorded " +
				"count does not index into it and a resume would run the wrong statements. Inspect the database, " +
				"then run 'ptah migrations repair --version 1 --force' if you restored what the rollback reverted, " +
				"or 'ptah migrations set --version <previous>' (version 0 where this is the oldest migration in the " +
				"directory) if you finished the rollback by hand.",
		},
		{
			name:  "the down file has the recorded count",
			count: new(2),
			want: "This rollback stopped after 1 of 2 down statements. Run 'ptah migrations repair --version 1 " +
				"--resume-from 2' to run the remaining down statements and remove the revision.",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			revision := migrator.MigrationRevision{
				Version: 1, Applied: 1, Total: 2, Direction: migrator.MigrationDirectionDown,
			}

			got := dirtyhint.Recovery(dirtyhint.Revision{
				Revision:       &revision,
				Dialect:        "postgres",
				FileStatements: test.count,
			})

			c.Assert(got, qt.Equals, test.want)
		})
	}
}

// TestRecoveryReadsAStatementThatRunsSeveral names neither a run from the
// failed statement nor a resume after it on its own when that statement runs
// several statements outside a transaction: some of them may have committed,
// so the operator finishes it first. A body that opens its own transactions
// keeps its own hint, because nothing resumes it from a statement.
func TestRecoveryReadsAStatementThatRunsSeveral(t *testing.T) {
	tests := []struct {
		name            string
		direction       migrator.MigrationDirection
		applied         int
		ownTransactions bool
		want            string
	}{
		{
			name: "a statement before the last", direction: migrator.MigrationDirectionUp, applied: 1,
			want: "This migration failed in statement 2 of 3, which runs several statements outside a transaction, " +
				"so some of them may have committed. Inspect the database and finish what statement 2 left undone, " +
				"then run 'ptah migrations repair --version 1 --resume-from 3' to run the rest, or " +
				"'ptah migrations repair --version 1 --force' once you have run them yourself.",
		},
		{
			name: "the first statement", direction: migrator.MigrationDirectionUp, applied: 0,
			want: "This migration failed in statement 1 of 3, which runs several statements outside a transaction, " +
				"so some of them may have committed. Inspect the database and finish what statement 1 left undone, " +
				"then run 'ptah migrations repair --version 1 --resume-from 2' to run the rest, or " +
				"'ptah migrations repair --version 1 --force' once you have run them yourself.",
		},
		{
			name: "the last statement", direction: migrator.MigrationDirectionUp, applied: 2,
			want: "This migration failed in statement 3 of 3, which runs several statements outside a transaction, " +
				"so some of them may have committed. Inspect the database and finish what statement 3 left undone, " +
				"then run 'ptah migrations repair --version 1 --force' to record the migration applied.",
		},
		{
			name: "a down statement before the last", direction: migrator.MigrationDirectionDown, applied: 1,
			want: "This rollback failed in down statement 2 of 3, which runs several statements outside a " +
				"transaction, so some of them may have committed. Inspect the database and finish what statement 2 " +
				"left undone, then run 'ptah migrations repair --version 1 --resume-from 3' to run the remaining " +
				"down statements and remove the revision.",
		},
		{
			name: "the last down statement", direction: migrator.MigrationDirectionDown, applied: 2,
			want: "This rollback failed in down statement 3 of 3, which runs several statements outside a " +
				"transaction, so some of them may have committed. Inspect the database and finish what statement 3 " +
				"left undone, then run 'ptah migrations set --version <previous>' (version 0 where this is the oldest " +
				"migration in the directory) to move the boundary back.",
		},
		{
			name: "a body that opens its own transactions", direction: migrator.MigrationDirectionUp, applied: 1,
			ownTransactions: true,
			want: "This migration stopped after 1 of 3 statements, in transactions its file opens itself. Run " +
				"'ptah migrations up --allow-dirty --runner golang-migrate' to resume after the transactions that " +
				"committed, or run 'ptah migrations repair --version 1 --force --runner golang-migrate' once you " +
				"have run the rest yourself.",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			revision := migrator.MigrationRevision{
				Version: 1, Applied: test.applied, Total: 3, Direction: test.direction, Error: "mssql: Invalid object name",
			}

			got := dirtyhint.Recovery(dirtyhint.Revision{
				Revision:                   &revision,
				Dialect:                    "sqlserver",
				OwnTransactions:            test.ownTransactions,
				FailedStatementRunsSeveral: true,
			})

			c.Assert(got, qt.Equals, test.want)
		})
	}
}

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

package migratestatus

// White-box testing required: dirtyRevisionRecoveryHint is the unexported
// branch that turns one dirty revision into the sentence an operator acts on,
// and reaching it through the command needs a database left in each of the
// three states rather than the row that distinguishes them.

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/migration/migrator"
)

// The hint has to read `applied` rather than print one remedy for every dirty
// row: a migration that recorded nothing wants a retry, and pointing it at
// repair records SQL that never ran (stokaro/ptah#3452). An interrupted
// statement leaves applied=0 as well and wants neither, because the row cannot
// say whether it committed.
func TestDirtyRevisionRecoveryHint(t *testing.T) {
	tests := []struct {
		name     string
		dialect  string
		revision migrator.MigrationRevision
		want     string
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
				"apply what is missing, then run 'ptah migrations repair --version 2' to record " +
				"the migration applied -- rerunning it could repeat the statement that committed.",
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
				"set --version <previous>' if you finished the rollback by hand.",
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
				"apply what is missing, then run 'ptah migrations repair --version 2' to record " +
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
				"--version 2' to record it applied. 'ptah migrations up --allow-dirty' is safe " +
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
				"--version <previous>' to move the boundary back -- or, if you restore what it " +
				"reverted instead, 'ptah migrations repair --version 2 --force' to record the " +
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
			want: "This rollback stopped partway. Run 'ptah migrations repair --version 2 " +
				"--resume-from 2' to run the remaining down statements and remove the revision.",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			c.Assert(dirtyRevisionRecoveryHint(&test.revision, test.dialect), qt.Equals, test.want)
		})
	}
}

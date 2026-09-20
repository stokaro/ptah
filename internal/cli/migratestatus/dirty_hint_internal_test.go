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
// repair records SQL that never ran (stokaro/ptah#3452).
func TestDirtyRevisionRecoveryHint(t *testing.T) {
	tests := []struct {
		name     string
		revision migrator.MigrationRevision
		want     string
	}{
		{
			name:     "nothing ran",
			revision: migrator.MigrationRevision{Version: 2, Applied: 0, Total: 3},
			want: "No statement of this migration reached the database. " +
				"Run 'ptah migrations up --allow-dirty' to apply it.",
		},
		{
			name:     "some statements ran",
			revision: migrator.MigrationRevision{Version: 2, Applied: 1, Total: 3},
			want: "This migration stopped after 1 of 3 statements. Run 'ptah migrations repair " +
				"--version 2 --resume-from 2' to run the rest, or repair with --force once you " +
				"have run them yourself.",
		},
		{
			name: "an interrupted rollback",
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

			c.Assert(dirtyRevisionRecoveryHint(&test.revision), qt.Equals, test.want)
		})
	}
}

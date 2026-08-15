package migrator_test

// Commit failures, kept as their own case (issue #999 acceptance item 4).
//
// A revision-completion failure and a commit failure are different faults with
// different diagnostics: the first fails the UPDATE that records the migration
// applied, the second fails the COMMIT that makes that UPDATE and the whole
// migration body durable. Ptah spells them differently -- "failed to record
// migration N" against "failed to commit transaction for migration N" -- and
// before this file the second string had no test anywhere in the repository.
//
// Only ddltx.Transactional can be driven into it. On ddltx.ImplicitCommit the
// server has already committed the DDL before the migrator's commit is
// reached, and on ddltx.NoTransaction the commit is a writer no-op that cannot
// fail; ddltx.HasCommitStep is that rule, and
// TestCommitFailure_OnlyTheTransactionalClassHasACommitStep pins it so the
// absence of a MySQL or ClickHouse case here is a stated decision rather than
// an omission.
//
// PostgreSQL is the target because a deferred constraint trigger is a fault
// that fires at COMMIT and nowhere else. A BEFORE UPDATE trigger -- what the
// revision-completion cases use -- fires during the statement instead, and
// would produce the other fault.

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/internal/ddltx"
)

// TestCommitFailure_OnlyTheTransactionalClassHasACommitStep states why this
// file has one target while the revision-completion matrix has five.
func TestCommitFailure_OnlyTheTransactionalClassHasACommitStep(t *testing.T) {
	c := qt.New(t)

	tests := []struct {
		name  string
		class ddltx.Class
		want  bool
	}{
		{name: "transactional", class: ddltx.Transactional, want: true},
		{name: "implicit commit", class: ddltx.ImplicitCommit, want: false},
		{name: "no transaction", class: ddltx.NoTransaction, want: false},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(ddltx.HasCommitStep(test.class), qt.Equals, test.want)
		})
	}
	c.Assert(ddltx.HasCommitStep(ddltx.ClassOf("postgres")), qt.IsTrue)
}

package migrateup

// White-box testing required: migrateUpOutcome and its err method are
// unexported, and the property is the ORDER in which that method classifies
// two error types one of which unwraps to the other. Nothing observable from
// outside the package can tell a run that classified them in the wrong order
// from one that classified them at all.

import (
	"errors"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/migration/migrator"
)

// A failed postcondition means the migration applied. The advice attached to a
// failed PRECONDITION -- rerun with --skip-checks -- is wrong for it twice
// over: the retry runs nothing, because the migration is already applied, and
// the sentence that said so is gone (stokaro/ptah#3405).
func TestMigrateUpOutcomeErr_KeepsTheAppliedOutcome(t *testing.T) {
	c := qt.New(t)
	checkErr := &migrator.CheckFailedError{Version: 1, Name: "rows_arrived", Phase: migrator.CheckPhaseAfter}
	outcome := migrateUpOutcome{runErr: &migrator.PostMigrationCheckFailedError{Version: 1, Err: checkErr}}

	err := outcome.err()

	c.Assert(err, qt.IsNotNil)
	c.Assert(err.Error(), qt.Contains, "applied")
	c.Assert(err.Error(), qt.Not(qt.Contains), "--skip-checks")
}

// The control: a failed precondition still gets the advice, so the ordering
// above did not take it away from the case it belongs to.
func TestMigrateUpOutcomeErr_KeepsThePreconditionAdvice(t *testing.T) {
	c := qt.New(t)
	outcome := migrateUpOutcome{
		runErr: &migrator.CheckFailedError{Version: 1, Name: "users_empty"},
	}

	err := outcome.err()

	c.Assert(err, qt.IsNotNil)
	c.Assert(err.Error(), qt.Contains, "--skip-checks")
}

// An ordinary failure is neither, and reads as the run failing.
func TestMigrateUpOutcomeErr_WrapsAnOrdinaryFailure(t *testing.T) {
	c := qt.New(t)
	outcome := migrateUpOutcome{runErr: errors.New("connection refused")}

	err := outcome.err()

	c.Assert(err, qt.ErrorMatches, `error running migrations: connection refused`)
}

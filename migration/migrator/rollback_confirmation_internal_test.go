package migrator

// White-box testing required: which of the two outcomes database/sql produces
// for a canceled transaction is decided by its own goroutine, so a test driving
// the public API gets sql.ErrTxDone or a clean rollback depending on which won.
// The pair this rule reads is therefore constructed here, and the run that
// produces it is integration/atlas_migrate_apply_interrupt_unix_e2e_test.go.

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"testing"

	qt "github.com/frankban/quicktest"
)

func TestMigrationFailureAfterRollback_ConfirmsWhatDatabaseSQLRolledBack(t *testing.T) {
	c := qt.New(t)
	// The shape an interrupted migration arrives in: the statement failed with
	// the cancellation, and the rollback found the transaction already closed
	// by database/sql.
	failure := fmt.Errorf("failed to execute migration SQL: %w", context.Canceled)

	err := migrationFailureAfterRollback(7, failure, sql.ErrTxDone)

	version, confirmed := migrationTransactionRollbackVersion(err)
	c.Assert(confirmed, qt.IsTrue)
	c.Assert(version, qt.Equals, int64(7))
	c.Assert(err, qt.ErrorIs, context.Canceled)
	c.Assert(err.Error(), qt.Not(qt.Contains), "additionally failed to roll back")
}

func TestMigrationFailureAfterRollback_LeavesAnUnexplainedOutcomeUnconfirmed(t *testing.T) {
	tests := []struct {
		name        string
		failure     error
		rollbackErr error
	}{
		{
			// Without the cancellation, sql.ErrTxDone says the transaction was
			// committed OR rolled back, and a commit is the outcome nobody may
			// guess at.
			name:        "the transaction was closed for a reason nobody named",
			failure:     errors.New("syntax error at or near NOT"),
			rollbackErr: sql.ErrTxDone,
		},
		{
			name:        "the rollback itself failed during a cancellation",
			failure:     fmt.Errorf("failed to execute migration SQL: %w", context.Canceled),
			rollbackErr: errors.New("write tcp 127.0.0.1:5432: broken pipe"),
		},
		{
			name:        "neither half is explained",
			failure:     errors.New("syntax error at or near NOT"),
			rollbackErr: errors.New("write tcp 127.0.0.1:5432: broken pipe"),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)

			err := migrationFailureAfterRollback(7, tt.failure, tt.rollbackErr)

			_, confirmed := migrationTransactionRollbackVersion(err)
			c.Assert(confirmed, qt.IsFalse)
			c.Assert(err.Error(), qt.Contains, "additionally failed to roll back migration transaction")
		})
	}
}

// TestMigrationFailureAfterRollback_ConfirmsACleanRollback is the control for
// the rule above: a rollback that answered nothing was always confirmed, and
// the new reading must not be what confirms it.
func TestMigrationFailureAfterRollback_ConfirmsACleanRollback(t *testing.T) {
	c := qt.New(t)

	err := migrationFailureAfterRollback(7, errors.New("syntax error at or near NOT"), nil)

	version, confirmed := migrationTransactionRollbackVersion(err)
	c.Assert(confirmed, qt.IsTrue)
	c.Assert(version, qt.Equals, int64(7))
}

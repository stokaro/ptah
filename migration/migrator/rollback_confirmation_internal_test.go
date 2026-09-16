package migrator

// White-box testing required: which answer a canceled transaction's rollback
// gives is decided by database/sql's own goroutine and by how far the driver
// got tearing the connection down, so a test driving the public API gets
// sql.ErrTxDone, a connection error or a clean rollback depending on the race.
// Each is constructed here, and the run that produces them is
// integration/atlas_migrate_apply_interrupt_unix_e2e_test.go.

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"testing"

	qt "github.com/frankban/quicktest"
)

func TestMigrationFailureAfterRollback_ConfirmsWhatTheCancellationEnded(t *testing.T) {
	tests := []struct {
		name        string
		rollbackErr error
	}{
		{
			// database/sql rolled the transaction back itself and closed it,
			// so the attempt that follows finds it done.
			name:        "the transaction was already closed",
			rollbackErr: sql.ErrTxDone,
		},
		{
			// The cancellation tore the connection down first, and the server
			// rolls back what that connection held. This is the answer the CI
			// runner gives for the same interrupt.
			name:        "the connection went with it",
			rollbackErr: errors.New("conn closed"),
		},
		{
			name:        "the connection is closed, in database/sql's own words",
			rollbackErr: sql.ErrConnDone,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			failure := fmt.Errorf("failed to execute migration SQL: %w", context.Canceled)

			err := migrationFailureAfterRollback(7, failure, tt.rollbackErr)

			version, confirmed := migrationTransactionRollbackVersion(err)
			c.Assert(confirmed, qt.IsTrue)
			c.Assert(version, qt.Equals, int64(7))
			c.Assert(err, qt.ErrorIs, context.Canceled)
			c.Assert(err.Error(), qt.Not(qt.Contains), "additionally failed to roll back")
		})
	}
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
			// The same connection error, on a failure that is not the
			// cancellation: what the server did with the transaction is not
			// something this code may decide.
			name:        "the connection closed on an ordinary failure",
			failure:     errors.New("syntax error at or near NOT"),
			rollbackErr: errors.New("conn closed"),
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

package migrator

// White-box testing required: which answer a canceled transaction's rollback
// gives is decided by database/sql's own goroutine and by how far the driver
// got tearing the connection down, so a test driving the public API gets
// sql.ErrTxDone, a connection error or a clean rollback depending on the race.
// Each is constructed here, and the run that produces them is
// integration/atlas_migrate_apply_interrupt_unix_e2e_test.go. A commit's own
// evidence is constructed the same way, so a row can pair it with an uncertain
// cleanup rollback; integration/providercli/provider_history_e2e_test.go runs a
// provider that answers a commit with ROLLED_BACK.

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/core/ptaherr"
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

			err := migrationFailureAfterRollback(7, failure, tt.rollbackErr, cancellationRollbackEvidence{confirmed: true})

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
		transaction catalog.SchemaTransaction
	}{
		{
			name:        "a canceled RPC does not end a detached transaction",
			failure:     context.Canceled,
			rollbackErr: errors.New("rollback refused because the transaction committed"),
		},
		{
			name:        "cancellation does not confirm an uncertain rollback receipt",
			transaction: cancellationRollbackEvidence{confirmed: true},
			failure:     context.Canceled,
			rollbackErr: fmt.Errorf("lost rollback reply: %w", ptaherr.ErrOutcomeUnknown),
		},
		{
			name:        "uncertain execution overrides cancellation inference",
			transaction: cancellationRollbackEvidence{confirmed: true},
			failure:     errors.Join(context.Canceled, ptaherr.ErrOutcomeUnknown),
			rollbackErr: sql.ErrTxDone,
		},
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

			err := migrationFailureAfterRollback(7, tt.failure, tt.rollbackErr, tt.transaction)

			_, confirmed := migrationTransactionRollbackVersion(err)
			c.Assert(confirmed, qt.IsFalse)
			c.Assert(err, qt.ErrorIs, tt.failure)
			c.Assert(err, qt.ErrorIs, tt.rollbackErr)
			c.Assert(err.Error(), qt.Contains, "additionally failed to roll back migration transaction")
		})
	}
}

// TestMigrationFailureAfterRollback_ConfirmsACleanRollback is the control for
// the rule above: a rollback that answered nothing was always confirmed, and
// the new reading must not be what confirms it.
func TestMigrationFailureAfterRollback_ConfirmsACleanRollback(t *testing.T) {
	c := qt.New(t)

	err := migrationFailureAfterRollback(7, errors.New("syntax error at or near NOT"), nil, nil)

	version, confirmed := migrationTransactionRollbackVersion(err)
	c.Assert(confirmed, qt.IsTrue)
	c.Assert(version, qt.Equals, int64(7))
}

// cancellationRollbackEvidence isolates the owner's evidence from the error
// returned by Rollback, whose value depends on database/sql scheduling.
type cancellationRollbackEvidence struct {
	catalog.SchemaTransaction
	confirmed bool
}

func (e cancellationRollbackEvidence) RolledBackByCancellation() bool { return e.confirmed }

// commitRollbackEvidence is the owner's answer to whether the commit rolled
// the transaction back.
type commitRollbackEvidence struct {
	catalog.SchemaTransaction
	confirmed bool
}

func (e commitRollbackEvidence) RolledBackAtCommit() bool { return e.confirmed }

func TestMigrationFailureAfterCommit_HappyPath(t *testing.T) {
	c := qt.New(t)
	failure := errors.New("provider transaction outcome is ROLLED_BACK")

	err := migrationFailureAfterCommit(7, failure, commitRollbackEvidence{confirmed: true})

	version, confirmed := migrationTransactionRollbackVersion(err)
	c.Assert(confirmed, qt.IsTrue)
	c.Assert(version, qt.Equals, int64(7))
	c.Assert(err, qt.ErrorIs, failure)
	c.Assert(err.Error(), qt.Equals, failure.Error())
}

func TestMigrationFailureAfterCommit_FailurePath(t *testing.T) {
	tests := []struct {
		name        string
		failure     error
		transaction catalog.SchemaTransaction
	}{
		{
			name:        "the owner does not confirm a rollback",
			failure:     errors.New("provider transaction outcome is ROLLED_BACK"),
			transaction: commitRollbackEvidence{confirmed: false},
		},
		{
			// A database/sql transaction gives no such evidence: a failed
			// COMMIT may or may not have ended the transaction.
			name:    "the transaction carries no evidence",
			failure: errors.New("pq: could not serialize access"),
		},
		{
			// The cleanup rollback joined to the commit's failure said its
			// own outcome is unknown, so nothing may be inferred from it.
			name:        "uncertainty overrides the owner's evidence",
			failure:     errors.Join(errors.New("provider transaction outcome is ROLLED_BACK"), ptaherr.ErrOutcomeUnknown),
			transaction: commitRollbackEvidence{confirmed: true},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)

			err := migrationFailureAfterCommit(7, tt.failure, tt.transaction)

			_, confirmed := migrationTransactionRollbackVersion(err)
			c.Assert(confirmed, qt.IsFalse)
			c.Assert(err, qt.Equals, tt.failure)
		})
	}
}

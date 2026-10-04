package seeder

// White-box testing required: ydbSeedTransaction is the retry every YDB seed
// commits through, and the abort it retries -- `Transaction locks
// invalidated` -- cannot be provoked on demand through Apply against a live
// server. The linked SQLite engine supplies real transactions, and a body that
// fails with an error the retry classifier reads as transient stands in for
// the abort.

import (
	"context"
	"database/sql"
	"errors"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/dbschema"
)

// transientError is classified as retryable the way a SQLite busy error is:
// atlasretry reads the low byte of its code.
type transientError struct{}

func (transientError) Error() string { return "the transaction was aborted" }
func (transientError) Code() int     { return 5 }

func sqliteConnection(c *qt.C) *dbschema.DatabaseConnection {
	conn, err := dbschema.ConnectToDatabase(context.Background(), "sqlite://:memory:")
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { c.Check(conn.Close(), qt.IsNil) })
	return conn
}

// failing returns a body that fails with err on its first failures attempts and
// succeeds after, counting every attempt.
func failing(failures int, err error, attempts *int) func(*sql.Tx) error {
	return func(*sql.Tx) error {
		*attempts++
		if *attempts <= failures {
			return err
		}
		return nil
	}
}

func TestYDBSeedTransaction_HappyPath(t *testing.T) {
	tests := []struct {
		name         string
		failures     int
		wantAttempts int
	}{
		{name: "a transaction that commits the first time runs once", failures: 0, wantAttempts: 1},
		{name: "an aborted transaction runs again", failures: 1, wantAttempts: 2},
		{name: "the last attempt may commit", failures: ydbSeedTransactionAttempts - 1,
			wantAttempts: ydbSeedTransactionAttempts},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			attempts := 0
			err := ydbSeedTransaction(c.Context(), sqliteConnection(c), failing(test.failures, transientError{}, &attempts))
			c.Assert(err, qt.IsNil)
			c.Assert(attempts, qt.Equals, test.wantAttempts)
		})
	}
}

func TestYDBSeedTransaction_FailurePath(t *testing.T) {
	permanent := errors.New("Conflict with existing key")
	tests := []struct {
		name         string
		failures     int
		err          error
		wantAttempts int
		wantErr      string
	}{
		{name: "a failure that is not an abort runs once", failures: 1, err: permanent, wantAttempts: 1,
			wantErr: "Conflict with existing key"},
		{name: "aborts past the bound stop the retry", failures: ydbSeedTransactionAttempts, err: transientError{},
			wantAttempts: ydbSeedTransactionAttempts, wantErr: "the transaction was aborted"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			attempts := 0
			err := ydbSeedTransaction(c.Context(), sqliteConnection(c), failing(test.failures, test.err, &attempts))
			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(attempts, qt.Equals, test.wantAttempts)
		})
	}
	t.Run("a canceled context stops the wait before the next attempt", func(t *testing.T) {
		c := qt.New(t)
		ctx, cancel := context.WithCancel(c.Context())
		conn := sqliteConnection(c)
		attempts := 0
		err := ydbSeedTransaction(ctx, conn, func(*sql.Tx) error {
			attempts++
			cancel()
			return transientError{}
		})
		c.Assert(err, qt.ErrorIs, context.Canceled)
		c.Assert(err, qt.ErrorMatches, "(?s)the transaction was aborted.*context canceled")
		c.Assert(attempts, qt.Equals, 1)
	})
}

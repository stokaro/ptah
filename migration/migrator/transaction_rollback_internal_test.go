package migrator

// White-box testing required: these tests verify the private rollback-outcome
// marker that gates deletion of failed revision metadata. Commit and rollback
// failure injection is not exposed by the public DatabaseConnection API.

import (
	"errors"
	"testing"

	qt "github.com/frankban/quicktest"
)

func TestMigrationFailureAfterRollback_Confirmed(t *testing.T) {
	c := qt.New(t)
	const version int64 = 42
	cause := errors.New("migration execution failed")

	err := migrationFailureAfterRollback(version, cause, nil)
	gotVersion, confirmed := migrationTransactionRollbackVersion(err)

	c.Assert(err, qt.ErrorIs, cause)
	c.Assert(err.Error(), qt.Equals, cause.Error())
	c.Assert(confirmed, qt.IsTrue)
	c.Assert(gotVersion, qt.Equals, version)
}

func TestMigrationFailureAfterRollback_Unconfirmed(t *testing.T) {
	c := qt.New(t)
	cause := errors.New("migration execution failed")
	rollbackErr := errors.New("rollback failed")

	err := migrationFailureAfterRollback(42, cause, rollbackErr)
	gotVersion, confirmed := migrationTransactionRollbackVersion(err)

	c.Assert(err, qt.ErrorIs, cause)
	c.Assert(err.Error(), qt.Contains, rollbackErr.Error())
	c.Assert(confirmed, qt.IsFalse)
	c.Assert(gotVersion, qt.Equals, int64(0))
}

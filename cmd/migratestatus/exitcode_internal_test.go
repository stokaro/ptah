package migratestatus

// White-box testing required: pendingMigrationsExitCode is the narrow
// unexported exit-code decision point behind the Cobra status command.

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"github.com/stokaro/ptah/cmd/internal/exitcode"
	"github.com/stokaro/ptah/migration/migrator"
)

func TestMigrateStatusExitCode_Clean(t *testing.T) {
	c := qt.New(t)

	err := pendingMigrationsExitCode(&migrator.MigrationStatus{})

	c.Assert(err, qt.IsNil)
}

func TestMigrateStatusExitCode_Pending(t *testing.T) {
	c := qt.New(t)

	err := pendingMigrationsExitCode(&migrator.MigrationStatus{HasPendingChanges: true})

	c.Assert(err, qt.IsNotNil)
	c.Assert(exitcode.Code(err, 0), qt.Equals, 1)
}

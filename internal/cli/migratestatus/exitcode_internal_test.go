package migratestatus

// White-box testing required: notUpToDateExitCode is the narrow unexported
// exit-code decision point behind the Cobra status command.

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/cli/internal/exitcode"
	"ptah.run/migration/migrator"
)

func TestMigrateStatusExitCode_Clean(t *testing.T) {
	c := qt.New(t)

	err := notUpToDateExitCode(&migrator.MigrationStatus{})

	c.Assert(err, qt.IsNil)
}

func TestMigrateStatusExitCode_Pending(t *testing.T) {
	c := qt.New(t)

	err := notUpToDateExitCode(&migrator.MigrationStatus{HasPendingChanges: true})

	c.Assert(err, qt.IsNotNil)
	c.Assert(exitcode.Code(err, 0), qt.Equals, 1)
}

// TestMigrateStatusExitCode_Modified covers the state no other field reports:
// an applied migration whose file no longer accounts for the checksum the
// database recorded is not a pending change, so a gate reading only
// HasPendingChanges is handed a zero over an edited history.
func TestMigrateStatusExitCode_Modified(t *testing.T) {
	c := qt.New(t)

	err := notUpToDateExitCode(&migrator.MigrationStatus{
		Migrations: []migrator.MigrationRecord{
			{Version: 1, State: migrator.MigrationStateApplied},
			{Version: 2, State: migrator.MigrationStateModified},
		},
	})

	c.Assert(err, qt.IsNotNil)
	c.Assert(exitcode.Code(err, 0), qt.Equals, 1)
	c.Assert(err, qt.ErrorMatches, "modified migrations detected")
}

// TestMigrateStatusExitCode_AppliedOnly is the control for the row above: the
// same shape with every record clean has to stay silent, or the selection above
// would be measuring the presence of records rather than their state.
func TestMigrateStatusExitCode_AppliedOnly(t *testing.T) {
	c := qt.New(t)

	err := notUpToDateExitCode(&migrator.MigrationStatus{
		Migrations: []migrator.MigrationRecord{
			{Version: 1, State: migrator.MigrationStateApplied},
			{Version: 2, State: migrator.MigrationStateApplied},
		},
	})

	c.Assert(err, qt.IsNil)
}

// TestMigrateStatusExitCode_Missing covers the state that has no entry in the
// per-directory list at all: the revision names a migration the directory does
// not hold, so nothing in Migrations can carry it.
func TestMigrateStatusExitCode_Missing(t *testing.T) {
	c := qt.New(t)

	err := notUpToDateExitCode(&migrator.MigrationStatus{
		Migrations:        []migrator.MigrationRecord{{Version: 1, State: migrator.MigrationStateApplied}},
		MissingMigrations: []migrator.MigrationRecord{{Version: 2, State: migrator.MigrationStateMissing}},
	})

	c.Assert(err, qt.IsNotNil)
	c.Assert(exitcode.Code(err, 0), qt.Equals, 1)
	c.Assert(err, qt.ErrorMatches, "applied migrations with no file detected")
}

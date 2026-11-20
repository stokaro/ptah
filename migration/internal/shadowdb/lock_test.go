package shadowdb_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/migration/internal/shadowdb"
	"ptah.run/migration/migrator"
)

// A replay into a claimed database takes no migration lock, and a migrator
// built the ordinary way does: the control shows the lock is on by default, so
// the first assertion measures ReplayMigrator rather than the constructor.
func TestReplayMigrator_TakesNoMigrationLock(t *testing.T) {
	c := qt.New(t)
	built := migrator.NewMigrator(nil, migrator.NewRegisteredMigrationProvider())

	c.Assert(shadowdb.ReplayMigrator(built).MigrationLockSkipped(), qt.IsTrue)
	c.Assert(built.MigrationLockSkipped(), qt.IsFalse)
}

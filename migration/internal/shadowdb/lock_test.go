package shadowdb_test

import (
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/dbschema"
	"ptah.run/internal/atlasurl"
	"ptah.run/migration/internal/shadowdb"
	"ptah.run/migration/migrator"
)

// claimedSQLite claims an empty SQLite database in a temporary directory, and
// returns its connection with the lease.
func claimedSQLite(c *qt.C) (*dbschema.DatabaseConnection, *shadowdb.Lease) {
	c.Helper()
	conn, err := dbschema.ConnectToDatabase(c.Context(), atlasurl.SQLiteURLFromPath(filepath.Join(c.TempDir(), "shadow.db")))
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { dbschema.CloseAndWarn(conn) })
	lease, err := shadowdb.Claim(c.Context(), conn)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { c.Check(lease.Release(c.Context()), qt.IsNil) })
	return conn, lease
}

// A replay into a claimed database takes no migration lock, and a migrator
// built the ordinary way does: the control shows the lock is on by default, so
// the first assertion measures ReplayMigrator rather than the constructor. A
// guarded migrator that is not a replay of a verification keeps the lock.
func TestReplayMigrator_TakesNoMigrationLock(t *testing.T) {
	c := qt.New(t)
	conn, lease := claimedSQLite(c)
	built := migrator.NewMigrator(conn, migrator.NewRegisteredMigrationProvider())

	c.Assert(shadowdb.ReplayMigrator(lease, built).MigrationLockSkipped(), qt.IsTrue)
	c.Assert(shadowdb.GuardedMigrator(lease, built).MigrationLockSkipped(), qt.IsFalse)
	c.Assert(built.MigrationLockSkipped(), qt.IsFalse)
}

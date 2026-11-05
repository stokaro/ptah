//go:build integration

package providercli_test

import (
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"
)

// A derived rollback runs on a target a provider serves. The rollback is
// derived from the schema the migrations up to the target version build on
// the shadow database, its statements reach the target through the provider,
// and the revision boundary moves with them, so the next migrate up applies
// the migration again. The fixture's SQLite is the engine, and the expected
// state comes from reading the database file back.
func TestProviderDerivedRollbackRunsThroughTheProvider_HappyPath(t *testing.T) {
	c := qt.New(t)
	f := newDevCLI(c)
	dir := migrationDirectory(c)
	runProviderMigration(c, f.providerCLI, "up", "--migrations-dir", dir)
	shadow := fixtureAddress("", filepath.Join(c.TempDir(), "shadow.db"))

	derived := runProviderMigration(c, f.providerCLI, "down", "--migrations-dir", dir, "--target", "0", "--plan",
		"--shadow-db", shadow, "--confirm")
	rolledBack := tables(c, f.databasePath)
	runProviderMigration(c, f.providerCLI, "up", "--migrations-dir", dir)

	c.Assert(derived, qt.Contains, "Derived rollback plan: 1 statements")
	c.Assert(derived, qt.Contains, `DROP TABLE "records"`)
	c.Assert(rolledBack, qt.Not(qt.Contains), "records")
	c.Assert(tables(c, f.databasePath), qt.Contains, "records")
}

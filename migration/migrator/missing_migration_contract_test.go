package migrator_test

import (
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/dbschema"
	"ptah.run/migration/migrationfile"
	"ptah.run/migration/migrator"
)

// VerifyAppliedChecksums is the adoption verifier as well as the read-only half
// of what an apply does, and the two want different answers about a revision
// with no file. After a takeover the history is native, so the verifier reports
// it whatever format the rows are in today; the apply over an Atlas history is
// what may proceed (stokaro/ptah#3442).

func TestVerifyAppliedChecksums_AnAtlasHistoryStillReportsAMissingMigration(t *testing.T) {
	c := qt.New(t)
	dir := writeAtlasDirectory(c)
	conn := sqliteConnection(c, "atlas-missing.db")
	c.Assert(atlasMigratorOver(c, dir, conn).MigrateUp(c.Context()), qt.IsNil)
	c.Assert(os.Remove(filepath.Join(dir, "20240102000000_orders.sql")), qt.IsNil)
	mig := atlasMigratorOver(c, dir, conn)

	reconcile, err := mig.VerifyAppliedChecksums(c.Context())

	c.Assert(err, qt.ErrorMatches,
		`migration 20240102000000 is recorded as applied and this directory has no file for it.*`)
	c.Assert(reconcile, qt.IsFalse)
	c.Assert(mig.AppliesOverMissingMigration(err), qt.IsTrue)
	c.Assert(mig.MigrateUp(c.Context()), qt.IsNil)
}

func TestVerifyAppliedChecksums_AnIntactAtlasHistoryIsSilent(t *testing.T) {
	c := qt.New(t)
	dir := writeAtlasDirectory(c)
	conn := sqliteConnection(c, "atlas-intact.db")
	c.Assert(atlasMigratorOver(c, dir, conn).MigrateUp(c.Context()), qt.IsNil)
	mig := atlasMigratorOver(c, dir, conn)

	reconcile, err := mig.VerifyAppliedChecksums(c.Context())

	c.Assert(err, qt.IsNil)
	c.Assert(reconcile, qt.IsFalse)
	c.Assert(mig.AppliesOverMissingMigration(err), qt.IsFalse)
}

// writeAtlasDirectory writes the single-file layout an Atlas directory uses.
func writeAtlasDirectory(c *qt.C) string {
	c.Helper()
	dir := c.TempDir()
	files := map[string]string{
		"20240101000000_users.sql":  "CREATE TABLE users (id INTEGER PRIMARY KEY);\n",
		"20240102000000_orders.sql": "CREATE TABLE orders (id INTEGER PRIMARY KEY);\n",
	}
	for name, content := range files {
		c.Assert(os.WriteFile(filepath.Join(dir, name), []byte(content), 0o600), qt.IsNil)
	}
	return dir
}

// atlasMigratorOver reads the directory in the Atlas layout and records in the
// Atlas revision table, which is the pair the format rule is about.
func atlasMigratorOver(c *qt.C, dir string, conn *dbschema.DatabaseConnection) *migrator.Migrator {
	c.Helper()
	mig, err := migrator.NewFSMigrator(conn, os.DirFS(dir),
		migrator.WithMigrationDirFormat(migrationfile.DirFormatAtlas))
	c.Assert(err, qt.IsNil)
	return mig.WithRevisionTableFormat(migrator.RevisionTableFormatAtlas)
}

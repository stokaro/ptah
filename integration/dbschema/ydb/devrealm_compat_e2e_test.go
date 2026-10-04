//go:build integration

package ydb_test

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/dbtarget"
	"ptah.run/internal/ydburl"
)

// TestYDBCompatBinary_DevDatabaseOnAnotherServer is
// TestYDBBinary_DevDatabaseOnAnotherServer on the compatibility surface: the
// dev URL names the other line's server, whose database is /local too. The
// apply rehearses its plan in a dev realm there, and migrate down verifies the
// rollback in one, where a comparison of the two URLs as written could not
// tell the servers apart.
func TestYDBCompatBinary_DevDatabaseOnAnotherServer(t *testing.T) {
	c := qt.New(t)
	binary := buildCompatBinary(c, c.Context())
	for i, line := range ydbLines {
		other := ydbLines[(i+1)%len(ydbLines)]
		t.Run(line.name, func(t *testing.T) {
			url := dbtarget.URL(t, line.engine)
			devURL := dbtarget.URL(t, other.engine)
			c := qt.New(t)
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
			defer cancel()
			conn := openYDB(c, line)
			dropCompatTables(c, conn)
			c.Cleanup(func() { dropCompatTables(c, conn) })
			work := c.TempDir()
			desired := writeCompatFile(c, work, "desired.hcl", compatDesired)
			migrations := filepath.Join(work, "migrations")
			c.Assert(os.Mkdir(migrations, 0o750), qt.IsNil)
			for name, body := range compatMigrations {
				writeCompatFile(c, migrations, name, body)
			}
			dir := "file://" + filepath.ToSlash(migrations)

			applied, _, applyErr := runCompat(ctx, binary, "schema", "apply", "--url", url,
				"--to", "file://"+desired, "--dev-url", devURL, "--auto-approve")
			hashed, _, hashErr := runCompat(ctx, binary, "migrate", "hash", "--dir", dir)
			migrated, _, migrateErr := runCompat(ctx, binary, "migrate", "apply", "--url", url, "--dir", dir,
				"--revisions-schema", compatDir)
			rolledBack, rollbackStderr, downErr := runCompat(ctx, binary, "migrate", "down", "--url", url,
				"--dir", dir, "--revisions-schema", compatDir, "--to-version", "20260101000000", "--dev-url", devURL)

			c.Assert(applyErr, qt.IsNil, qt.Commentf("schema apply:\n%s", applied))
			c.Assert(applied, qt.Contains, "Schema apply completed successfully.")
			c.Assert(hashErr, qt.IsNil, qt.Commentf("migrate hash:\n%s", hashed))
			c.Assert(migrateErr, qt.IsNil, qt.Commentf("migrate apply:\n%s", migrated))
			c.Assert(downErr, qt.IsNil, qt.Commentf("migrate down:\n%s\n%s", rolledBack, rollbackStderr))
			c.Assert(rolledBack, qt.Contains, "Rollback plan verified on shadow database")
			c.Assert(tableColumns(c, conn, compatDir, "orders"), qt.Not(qt.HasLen), 0)
			c.Assert(tableColumns(c, conn, compatDir, "items"), qt.DeepEquals, []string{"items.id"})
			c.Assert(directoryNames(c, ctx, other), qt.Not(qt.Contains), ydburl.RealmDirectory)
			c.Assert(tableNames(readScoped(c, openYDB(c, other), []string{compatDir})), qt.HasLen, 0)
		})
	}
}

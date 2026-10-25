//go:build integration

package atlas_test

import (
	"database/sql"
	"net/url"
	"path/filepath"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"
	// The test writes both fixture databases through a driver of its own, so
	// the starting state does not depend on the provider under test.
	_ "modernc.org/sqlite"

	"ptah.run/internal/testutils"
)

// compatRebuildFixture serves the engine "fixture" through the program that
// honors the planning policy, with a current database holding the table items
// and a desired one that adds a NOT NULL column to it, which SQLite can add
// only by rebuilding the table.
func compatRebuildFixture(c *qt.C) (f compatProviderFixture, desired string) {
	c.Helper()
	binaries := compatProviderBinaries{
		compat:      buildSchemaInspectBinary(c, "ptah-compat", "ptah.run/cmd/ptah-compat"),
		provider:    testutils.BuildFixtureProvider(c, "operation-models"),
		environment: []string{"FIXTURE_RENDER_MODE=policy"},
	}
	f = newCompatProviderFixture(c, binaries)
	createFixtureTable(c, filepath.Join(f.dir, "database.db"), `CREATE TABLE "items" ("id" INTEGER NOT NULL PRIMARY KEY)`)
	path := filepath.Join(f.dir, "desired.db")
	createFixtureTable(c, path, `CREATE TABLE "items" ("id" INTEGER NOT NULL PRIMARY KEY, "flag" INTEGER NOT NULL)`)
	return f, (&url.URL{Scheme: "fixture", Path: "/" + strings.TrimPrefix(filepath.ToSlash(path), "/")}).String()
}

func createFixtureTable(c *qt.C, path, statement string) {
	c.Helper()
	db, err := sql.Open("sqlite", path)
	c.Assert(err, qt.IsNil)
	_, err = db.ExecContext(c.Context(), statement)
	c.Assert(err, qt.IsNil)
	c.Assert(db.Close(), qt.IsNil)
}

// TestCompatProviderRebuildRequest_FailurePath plans, through the provider, a
// change only a rebuild of its table can make. The compatibility surface takes
// no flag the pinned binary lacks, so the provider's refusal names the
// variable that asks for the rebuild on this surface.
func TestCompatProviderRebuildRequest_FailurePath(t *testing.T) {
	c := qt.New(t)
	f, desired := compatRebuildFixture(c)

	out, stderr, code := f.run(f.withManifest(), "schema", "diff", "--from", f.address, "--to", desired)

	c.Assert(code, qt.Equals, 1)
	c.Assert(out, qt.Equals, "")
	c.Assert(stderr, qt.Contains, `adding NOT NULL column "flag" to table "items" needs a rebuild of the table; ask for one with PTAH_ALLOW_TABLE_REBUILD=1`)
	f.assertStopped(c)
}

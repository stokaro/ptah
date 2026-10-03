//go:build integration

package integration_test

import (
	"context"
	"database/sql"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/dbtarget"
)

// A dev database's default privileges are its environment, as its extensions
// are. An image such as Supabase's grants its API roles on every table
// created in public through `ALTER DEFAULT PRIVILEGES ... IN SCHEMA public`,
// and the pinned community binary v1.3.0 takes such a dev database and keeps
// them. A reset revoked them, so the claim refused the database rather than
// lose them, and the image could not be a dev database (stokaro/ptah#4034).
// Each test reads pg_default_acl back afterwards.

// keptDefaultEngines are the servers whose dev database keeps its default
// privileges: the two that answer pg_default_acl in a shape that can be read
// back. A CockroachDB dev database holding one is still refused; see
// TestGlobalDefaultPrivilegeE2E_CockroachDBRefusesADevDatabaseHoldingOne.
var keptDefaultEngines = []struct {
	name   string
	engine dbtarget.Engine
}{
	{name: "PostgreSQL", engine: dbtarget.PostgreSQL},
	{name: "YugabyteDB", engine: dbtarget.YugabyteDB},
}

// keptDefaults are what a Supabase-shaped image sets for the connecting role:
// SELECT on every table created in public, and USAGE on every sequence
// created anywhere, for the reader role.
func keptDefaults(f globalDefaultFixture) []string {
	reader := quoteE2EIdent(f.reader)
	return []string{
		"ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO " + reader + ";",
		"ALTER DEFAULT PRIVILEGES GRANT USAGE ON SEQUENCES TO " + reader + ";",
	}
}

// devDefaultACL lists every pg_default_acl row of the database at dbURL as
// schema, class and list, `<global>` for a global row.
func devDefaultACL(c *qt.C, ctx context.Context, dbURL string) []string {
	c.Helper()
	db, err := sql.Open("pgx", postgresFamilyDriverURL(c, dbURL))
	c.Assert(err, qt.IsNil)
	defer db.Close()
	rows, err := db.QueryContext(ctx, `
		SELECT coalesce(n.nspname, '<global>') || ' ' || d.defaclobjtype::text || ' ' || array_to_string(d.defaclacl, ' ')
		FROM pg_default_acl d
		LEFT JOIN pg_namespace n ON n.oid = d.defaclnamespace
		ORDER BY 1`)
	c.Assert(err, qt.IsNil)
	defer rows.Close()
	var acl []string
	for rows.Next() {
		var row string
		c.Assert(rows.Scan(&row), qt.IsNil)
		acl = append(acl, row)
	}
	c.Assert(rows.Err(), qt.IsNil)
	return acl
}

// migrationBodies joins every migration file written to dir.
func migrationBodies(c *qt.C, dir string) string {
	c.Helper()
	files, err := filepath.Glob(filepath.Join(dir, "*.sql"))
	c.Assert(err, qt.IsNil)
	var bodies strings.Builder
	for _, file := range files {
		body, err := os.ReadFile(file)
		c.Assert(err, qt.IsNil)
		bodies.Write(body)
	}
	return bodies.String()
}

// TestDevDefaultPrivilegesE2E_MigrateDiffKeepsThem runs `migrate diff` twice
// against a dev database pinned to public that holds the defaults. The first
// run writes the table and nothing about the defaults, though the replayed
// table and its sequence receive grants from them. The second run finds the
// directory in sync, and the dev database holds the defaults it held before.
func TestDevDefaultPrivilegesE2E_MigrateDiffKeepsThem(t *testing.T) {
	for _, test := range keptDefaultEngines {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			ctx, cancel := context.WithTimeout(c.Context(), 5*time.Minute)
			defer cancel()
			fixture := newGlobalDefaultFixture(c, ctx, test.engine, "dev")
			fixture.exec(c, ctx, fixture.urls["dev"], keptDefaults(fixture))
			before := devDefaultACL(c, ctx, fixture.urls["dev"])
			dev := pinnedDevURL(c, fixture.urls["dev"], "public")
			schema := fixture.schemaFile(c, "schema.sql", []string{`CREATE TABLE "replayed" ("id" bigserial PRIMARY KEY);`})
			dir := c.TempDir()
			diff := func(name string) []string {
				return []string{"migrate", "diff", name, "--dir", "file://" + filepath.ToSlash(dir),
					"--to", "file://" + filepath.ToSlash(schema), "--dev-url", dev}
			}

			first, firstErr, err := runCompat(ctx, diff("init")...)
			c.Assert(err, qt.IsNil, qt.Commentf("stdout:\n%s\nstderr:\n%s", first, firstErr))
			second, secondErr, err := runCompat(ctx, diff("second")...)
			c.Assert(err, qt.IsNil, qt.Commentf("stdout:\n%s\nstderr:\n%s", second, secondErr))

			c.Assert(second, qt.Contains, "The migration directory is synced with the desired state")
			c.Assert(migrationBodies(c, dir), qt.Contains, `CREATE TABLE "replayed"`)
			c.Assert(migrationBodies(c, dir), qt.Not(qt.Contains), "DEFAULT PRIVILEGES")
			c.Assert(before, qt.HasLen, 2)
			c.Assert(devDefaultACL(c, ctx, fixture.urls["dev"]), qt.DeepEquals, before)
		})
	}
}

// TestDevDefaultPrivilegesE2E_SchemaApplyRehearsesBesideThem applies a schema
// to a target with a dev database that holds the defaults. The rehearsal runs
// on the dev database and resets it before and after, and the defaults are
// still there; the target gets the table.
func TestDevDefaultPrivilegesE2E_SchemaApplyRehearsesBesideThem(t *testing.T) {
	for _, test := range keptDefaultEngines {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			ctx, cancel := context.WithTimeout(c.Context(), 5*time.Minute)
			defer cancel()
			fixture := newGlobalDefaultFixture(c, ctx, test.engine, "dev", "target")
			fixture.exec(c, ctx, fixture.urls["dev"], keptDefaults(fixture))
			before := devDefaultACL(c, ctx, fixture.urls["dev"])
			schema := fixture.schemaFile(c, "schema.sql", []string{`CREATE TABLE "applied" ("id" bigserial PRIMARY KEY);`})

			stdout, stderr, err := runCompat(ctx, "schema", "apply",
				"-u", pinnedDevURL(c, fixture.urls["target"], "public"),
				"--to", "file://"+filepath.ToSlash(schema),
				"--dev-url", pinnedDevURL(c, fixture.urls["dev"], "public"), "--auto-approve")

			c.Assert(err, qt.IsNil, qt.Commentf("stdout:\n%s\nstderr:\n%s", stdout, stderr))
			c.Assert(devDefaultACL(c, ctx, fixture.urls["dev"]), qt.DeepEquals, before)
			db, err := sql.Open("pgx", postgresFamilyDriverURL(c, fixture.urls["target"]))
			c.Assert(err, qt.IsNil)
			defer db.Close()
			var applied int
			c.Assert(db.QueryRowContext(ctx,
				"SELECT count(*) FROM pg_tables WHERE schemaname = 'public' AND tablename = 'applied'").Scan(&applied), qt.IsNil)
			c.Assert(applied, qt.Equals, 1)
		})
	}
}

// TestDevDefaultPrivilegesE2E_ReplayRemovesTheDefaultsARunSets replays a
// migration that changes the defaults the dev database held and sets new ones,
// in public and globally. The cleanup after the replay returns them to what
// the dev database held, on both binaries.
func TestDevDefaultPrivilegesE2E_ReplayRemovesTheDefaultsARunSets(t *testing.T) {
	binaries := []struct {
		name string
		// run is the binary the row drives, in process.
		run  func(args ...string) (string, error)
		args []string
	}{
		{name: "ptah-compat migrate validate", run: runCompatVerb,
			args: []string{"migrate", "validate", "--dir", "{dir}", "--dev-url", "{dev}"}},
		{name: "ptah migrations validate", run: runPtahNativeWithError,
			args: []string{"migrations", "validate", "--dir", "{rawdir}", "--dev-url", "{dev}"}},
	}

	for _, test := range keptDefaultEngines {
		for _, binary := range binaries {
			t.Run(test.name+"/"+binary.name, func(t *testing.T) {
				c := qt.New(t)
				ctx, cancel := context.WithTimeout(c.Context(), 5*time.Minute)
				defer cancel()
				fixture := newGlobalDefaultFixture(c, ctx, test.engine, "dev")
				fixture.exec(c, ctx, fixture.urls["dev"], keptDefaults(fixture))
				before := devDefaultACL(c, ctx, fixture.urls["dev"])
				reader := quoteE2EIdent(fixture.reader)
				dir := fixture.migrationDir(c, ctx, []string{
					`CREATE TABLE "replayed" ("id" bigint PRIMARY KEY);`,
					"ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT INSERT ON TABLES TO " + reader + ";",
					"ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT USAGE ON TYPES TO " + reader + ";",
					"ALTER DEFAULT PRIVILEGES REVOKE USAGE ON SEQUENCES FROM " + reader + ";",
					"ALTER DEFAULT PRIVILEGES GRANT EXECUTE ON FUNCTIONS TO " + reader + ";",
				})
				args := devNotCleanArgs(binary.args, pinnedDevURL(c, fixture.urls["dev"], "public"), "", "", dir, "")

				output, err := binary.run(args...)

				c.Assert(err, qt.IsNil, qt.Commentf("%s", output))
				c.Assert(devDefaultACL(c, ctx, fixture.urls["dev"]), qt.DeepEquals, before)
			})
		}
	}
}

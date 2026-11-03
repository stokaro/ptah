//go:build integration

package providercli_test

import (
	"bytes"
	"database/sql"
	"slices"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/atlascompatpolicy"
	"ptah.run/internal/cli/atlas"
	"ptah.run/internal/cli/root"
)

// A database the fixture provider serves is cleaned by the provider, through
// `db drop-all` and `ptah-compat schema clean` alike. The fixture lists every
// table, view and trigger, removes the tables, and refuses a view. Its engine
// is named "fixture", so no built-in descriptor describes it; the plan is the
// provider's list or nothing. The expected state comes from reading the
// database file back with a driver of the test's own.

// heldBeforeCleanup is what populateForCleanup leaves in the database: a table
// created by hand, the table a migration created and the revision table the
// migrator recorded it in.
var heldBeforeCleanup = []string{"accounts", "records", "schema_migrations"}

// plannedCleanup is what the fixture lists for heldBeforeCleanup, as a
// cleanup's output names each object.
var plannedCleanup = []string{`table "accounts"`, `table "records"`, `table "schema_migrations"`}

func populateForCleanup(c *qt.C, f devCLI) {
	c.Helper()
	runProviderMigration(c, f.providerCLI, "up", "--migrations-dir", migrationDirectory(c))
	createTable(c, f.databasePath, "accounts")
	c.Assert(tables(c, f.databasePath), qt.DeepEquals, heldBeforeCleanup)
}

func createView(c *qt.C, path, name string) {
	c.Helper()
	db, err := sql.Open("sqlite", path)
	c.Assert(err, qt.IsNil)
	defer func() { c.Assert(db.Close(), qt.IsNil) }()
	_, err = db.ExecContext(c.Context(), `CREATE VIEW "`+name+`" AS SELECT 1 AS "one"`)
	c.Assert(err, qt.IsNil)
}

// dropAll runs `ptah db drop-all` against the fixture database.
func dropAll(c *qt.C, f devCLI, args ...string) (stdout, stderr string, exitCode int) {
	c.Helper()
	stdout, stderr, exitCode = executeProviderCLI(c.Context(),
		append([]string{"db", "drop-all", "--provider-config", f.manifestPath, "--db-url", f.address}, args...))
	f.assertStopped(c)
	return stdout, stderr, exitCode
}

// schemaClean runs `ptah-compat schema clean` against the fixture database,
// with the manifest selected the only way that surface selects one.
func schemaClean(c *qt.C, f devCLI, args ...string) (stdout, stderr string, exitCode int) {
	c.Helper()
	c.Setenv("PTAH_PROVIDER_CONFIG", f.manifestPath)
	cmd := atlas.NewCompatCommandWithPolicy("ptah-compat", atlascompatpolicy.Full())
	var out, diagnostics bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&diagnostics)
	cmd.SetIn(strings.NewReader(""))
	exitCode = root.RunContext(c.Context(), cmd, append([]string{"schema", "clean", "--url", f.address}, args...)...)
	f.assertStopped(c)
	return out.String(), diagnostics.String(), exitCode
}

// planned is the objects a cleanup's output lists, one on each line that
// starts with "- ".
func planned(out string) []string {
	lines := slices.DeleteFunc(strings.Split(out, "\n"), func(line string) bool { return !strings.HasPrefix(line, "- ") })
	for i := range lines {
		lines[i] = strings.TrimPrefix(lines[i], "- ")
	}
	return lines
}

// The dry run lists exactly what the fixture's cleanup then removes, the
// revision table included, and removes nothing; the run removes it all.
func TestProviderDropAllRemovesWhatTheProviderLists(t *testing.T) {
	c := qt.New(t)
	f := newDevCLI(c)
	populateForCleanup(c, f)

	out, diagnostics, code := dropAll(c, f, "--dry-run")
	c.Assert(code, qt.Equals, 0, qt.Commentf("%s\n%s", out, diagnostics))
	c.Assert(out, qt.Contains, "Planned cleanup changes: 3\n")
	c.Assert(planned(out), qt.DeepEquals, plannedCleanup)
	c.Assert(tables(c, f.databasePath), qt.DeepEquals, heldBeforeCleanup)

	out, diagnostics, code = dropAll(c, f, "--auto-approve")
	c.Assert(code, qt.Equals, 0, qt.Commentf("%s\n%s", out, diagnostics))
	c.Assert(planned(out), qt.DeepEquals, plannedCleanup)
	c.Assert(tables(c, f.databasePath), qt.HasLen, 0)
}

// An object the provider cannot remove stops its cleanup before it removes
// anything, so the plan is refused, dry run included, rather than listing the
// tables a run would not remove. An engine that offers no cleanup is refused
// by name.
func TestProviderDropAll_FailurePath(t *testing.T) {
	t.Run("an object the provider cannot remove", func(t *testing.T) {
		c := qt.New(t)
		f := newDevCLI(c)
		populateForCleanup(c, f)
		createView(c, f.databasePath, "recent")
		const refusal = `provider "example.org/fixture" would remove nothing, because it cannot remove ` +
			`view "recent": the fixture has no statement that drops a view`

		_, diagnostics, code := dropAll(c, f, "--dry-run")
		c.Assert(code, qt.Not(qt.Equals), 0)
		c.Assert(diagnostics, qt.Contains, refusal)

		_, diagnostics, code = dropAll(c, f, "--auto-approve")
		c.Assert(code, qt.Not(qt.Equals), 0)
		c.Assert(diagnostics, qt.Contains, refusal)
		c.Assert(tables(c, f.databasePath), qt.DeepEquals, heldBeforeCleanup)
	})
	t.Run("an engine that offers no cleanup", func(t *testing.T) {
		c := qt.New(t)
		dir := c.TempDir()
		spec := devdbSpec(c, dir)
		spec.Environment = append(spec.Environment, "FIXTURE_DEV_ENGINE=bare")
		f := devCLI{providerCLI: newProviderCLI(c, spec), dir: dir}
		createTable(c, f.databasePath, "accounts")
		const refusal = `unsupported feature: provider "example.org/fixture" serves the database and offers no database cleanup`

		out, diagnostics, code := dropAll(c, f, "--dry-run")
		c.Assert(code, qt.Not(qt.Equals), 0)
		c.Assert(diagnostics, qt.Contains, refusal)
		c.Assert(out, qt.Not(qt.Contains), "Planned cleanup changes")

		_, diagnostics, code = dropAll(c, f, "--auto-approve")
		c.Assert(code, qt.Not(qt.Equals), 0)
		c.Assert(diagnostics, qt.Contains, refusal)
		c.Assert(diagnostics, qt.Not(qt.Contains), "cannot find tables by name")
		c.Assert(tables(c, f.databasePath), qt.DeepEquals, []string{"accounts"})
	})
}

// ptah-compat schema clean reads the same plan and hands the cleanup to the
// provider too. The provider removes everything at once, so a plan an
// exclusion narrowed is refused before anything is removed.
func TestProviderCompatSchemaCleanRemovesWhatTheProviderLists(t *testing.T) {
	t.Run("the whole database", func(t *testing.T) {
		c := qt.New(t)
		f := newDevCLI(c)
		populateForCleanup(c, f)

		out, diagnostics, code := schemaClean(c, f, "--dry-run")
		c.Assert(code, qt.Equals, 0, qt.Commentf("%s\n%s", out, diagnostics))
		c.Assert(planned(out), qt.DeepEquals, plannedCleanup)
		c.Assert(tables(c, f.databasePath), qt.DeepEquals, heldBeforeCleanup)

		out, diagnostics, code = schemaClean(c, f, "--auto-approve")
		c.Assert(code, qt.Equals, 0, qt.Commentf("%s\n%s", out, diagnostics))
		c.Assert(planned(out), qt.DeepEquals, plannedCleanup)
		c.Assert(tables(c, f.databasePath), qt.HasLen, 0)
	})
	t.Run("a plan an exclusion narrowed", func(t *testing.T) {
		c := qt.New(t)
		f := newDevCLI(c)
		populateForCleanup(c, f)

		out, diagnostics, code := schemaClean(c, f, "--exclude", "accounts", "--auto-approve")

		c.Assert(code, qt.Not(qt.Equals), 0, qt.Commentf("%s", out))
		c.Assert(diagnostics, qt.Contains, `provider "example.org/fixture" removes everything the database holds at once `+
			`and cannot run a plan that names part of it: the plan leaves out table "accounts"`)
		c.Assert(tables(c, f.databasePath), qt.DeepEquals, heldBeforeCleanup)
	})
}

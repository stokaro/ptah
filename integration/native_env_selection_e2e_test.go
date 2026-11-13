//go:build integration

package integration_test

import (
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/clirun"
)

// nativeEnvProjectFiles are the two project files the native binary reads, each
// declaring one env that names prod.db and its migration directory.
var nativeEnvProjectFiles = []struct {
	name       string
	file       string
	content    string
	wantTables []string
}{
	{
		name:       "ptah.yaml",
		file:       "ptah.yaml",
		content:    "env:\n  prod:\n    url: sqlite://prod.db\n    migration:\n      dir: ./migrations\n",
		wantTables: []string{"schema_migrations", "schema_migrations_log", "t1", "t2"},
	},
	{
		name:       "atlas.hcl",
		file:       "atlas.hcl",
		content:    "env \"prod\" {\n  url = \"sqlite://prod.db\"\n  migration {\n    dir = \"file://migrations\"\n  }\n}\n",
		wantTables: []string{"atlas_schema_revisions", "t1", "t2"},
	},
}

// nativeEnvProject writes two migrations, the project file, and prod.db as an
// empty SQLite database, so a run that reads the env has a database to migrate
// and the read-back can tell.
func nativeEnvProject(c *qt.C, file, content string) string {
	c.Helper()
	dir := c.TempDir()
	migrations := filepath.Join(dir, "migrations")
	c.Assert(os.Mkdir(migrations, 0o750), qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(migrations, "1.sql"), []byte("CREATE TABLE t1 (id int);\n"), 0o600), qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(migrations, "2.sql"), []byte("CREATE TABLE t2 (id int);\n"), 0o600), qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(dir, file), []byte(content), 0o600), qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(dir, "prod.db"), nil, 0o600), qt.IsNil)
	return dir
}

// TestNativeMigrationsUpWithoutEnvLeavesTheEnvDatabaseAloneE2E is
// stokaro/ptah#5999 on the native binary: `ptah migrations up` with no flag
// beside a project file whose only env names a database. An env applies only
// when --env names it, so the run has no database URL. It refuses as any other
// usage error does, names the env it did not select, and prod.db holds no
// table. A loader that takes the lone env for the project default migrates
// prod.db at exit 0.
func TestNativeMigrationsUpWithoutEnvLeavesTheEnvDatabaseAloneE2E(t *testing.T) {
	for _, project := range nativeEnvProjectFiles {
		t.Run(project.name, func(t *testing.T) {
			c := qt.New(t)
			dir := nativeEnvProject(c, project.file, project.content)

			got := clirun.Run(c, clirun.Ptah, clirun.Options{Dir: dir, Env: environmentWithoutPtahVariables()}, "migrations", "up")

			c.Assert(got.ExitCode, qt.Equals, 2, qt.Commentf("stdout:\n%s\nstderr:\n%s", got.Stdout, got.Stderr))
			c.Assert(got.Stdout, qt.Equals, "")
			c.Assert(got.Stderr, qt.Equals,
				"error: database URL is required: no --env given; "+project.file+" declares env \"prod\"\n")
			c.Assert(envSelectionTables(c, dir), qt.HasLen, 0)
		})
	}
}

// TestNativeMigrationsUpWithEnvMigratesTheEnvDatabaseE2E is the control for the
// test above: naming the env, by flag or by PTAH_ENV, still applies its url and
// migration directory, so the refusal there comes from the selection rule and
// not from a project file that is never read.
func TestNativeMigrationsUpWithEnvMigratesTheEnvDatabaseE2E(t *testing.T) {
	selections := []struct {
		name string
		args []string
		env  []string
	}{
		{name: "the flag", args: []string{"--env", "prod"}},
		{name: "PTAH_ENV", env: []string{"PTAH_ENV=prod"}},
	}
	for _, project := range nativeEnvProjectFiles {
		for _, selection := range selections {
			t.Run(project.name+"/"+selection.name, func(t *testing.T) {
				c := qt.New(t)
				dir := nativeEnvProject(c, project.file, project.content)

				got := clirun.Run(c, clirun.Ptah, clirun.Options{
					Dir: dir,
					Env: append(environmentWithoutPtahVariables(), selection.env...),
				}, append([]string{"migrations", "up"}, selection.args...)...)

				c.Assert(got.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", got.Stdout, got.Stderr))
				c.Assert(envSelectionTables(c, dir), qt.DeepEquals, project.wantTables)
			})
		}
	}
}

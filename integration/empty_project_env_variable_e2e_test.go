//go:build integration

package integration_test

import (
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/clirun"
	"ptah.run/internal/dbtarget"
)

// An empty PTAH_ENV beside a project file that declares envs is refused
// rather than read as unset. Read as unset, it selects no env, and the run
// reads what the file says outside every env: a history the env keeps in a
// table or schema of its own is not found, and every migration runs again.
// Each test applies a history under the env, runs again with the variable
// empty, and reads the database back.

// emptyProjectEnvSettings are the PTAH_ENV settings that name no env, with
// how the refusal describes each.
var emptyProjectEnvSettings = []struct {
	env, state string
}{
	{env: "PTAH_ENV=", state: "set but empty"},
	{env: "PTAH_ENV=  ", state: `set to "  ", which names nothing`},
}

// projectEnvHistoryTable is the revision table the env dev names.
const projectEnvHistoryTable = "dev_history"

// writeProjectEnvHistory writes a one-migration directory under dir, whose
// migration creates the table a.
func writeProjectEnvHistory(c *qt.C, dir string, files map[string]string) {
	c.Helper()
	c.Assert(os.MkdirAll(filepath.Join(dir, "migrations"), 0o755), qt.IsNil)
	for name, content := range files {
		c.Assert(os.WriteFile(filepath.Join(dir, name), []byte(content), 0o600), qt.IsNil)
	}
}

// nativeProjectEnvRun runs `ptah migrations up` on the SQLite database app.db in
// dir, with env added to an environment holding no PTAH_* variable.
func nativeProjectEnvRun(c *qt.C, dir string, env []string, args ...string) clirun.Result {
	c.Helper()
	return clirun.Run(c, clirun.Ptah, clirun.Options{Dir: dir, Env: append(environmentWithoutPtahVariables(), env...)},
		append([]string{"migrations", "up", "--db-url", "sqlite://app.db", "--migrations-dir", "migrations"}, args...)...)
}

// nativeProjectEnvDir writes a ptah.yaml whose env dev keeps its history in
// projectEnvHistoryTable, applies the history under dev, and returns the
// directory and the tables the database holds afterwards.
func nativeProjectEnvDir(c *qt.C) (string, []string) {
	c.Helper()
	dir := c.TempDir()
	writeProjectEnvHistory(c, dir, map[string]string{
		"ptah.yaml":                        "env:\n  dev:\n    migration:\n      revisions_table: " + projectEnvHistoryTable + "\n",
		"migrations/0000000001_a.up.sql":   "CREATE TABLE a (id INTEGER);\n",
		"migrations/0000000001_a.down.sql": "DROP TABLE a;\n",
	})
	applied := nativeProjectEnvRun(c, dir, nil, "--env", "dev")
	c.Assert(applied.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", applied.Stdout, applied.Stderr))
	return dir, sqliteTables(c, filepath.Join(dir, "app.db"))
}

// TestMigrationsUpRefusesAnEmptyProjectEnvVariableE2E_FailurePath runs `ptah
// migrations up` again with PTAH_ENV empty or blank. Read as unset, the
// variable would select no env, read the default revision table, find nothing
// applied and run migration 1 again over the table it created.
func TestMigrationsUpRefusesAnEmptyProjectEnvVariableE2E_FailurePath(t *testing.T) {
	for _, setting := range emptyProjectEnvSettings {
		t.Run(setting.env, func(t *testing.T) {
			c := qt.New(t)
			dir, before := nativeProjectEnvDir(c)

			got := nativeProjectEnvRun(c, dir, []string{setting.env})

			c.Assert(got.ExitCode, qt.Equals, 2, qt.Commentf("stdout:\n%s\nstderr:\n%s", got.Stdout, got.Stderr))
			c.Assert(got.Stderr, qt.Contains, `PTAH_ENV is `+setting.state+`, and ptah.yaml declares env "dev"`)
			c.Assert(sqliteTables(c, filepath.Join(dir, "app.db")), qt.DeepEquals, before)
		})
	}
}

// TestMigrationsUpReadsTheProjectEnvVariableE2E_HappyPath is the control for
// the refusal: a value in PTAH_ENV selects the env, which finds its history and
// has nothing to apply, and a typed --env decides beside an empty variable.
func TestMigrationsUpReadsTheProjectEnvVariableE2E_HappyPath(t *testing.T) {
	tests := []struct {
		name string
		env  []string
		args []string
	}{
		{name: "a value", env: []string{"PTAH_ENV=dev"}},
		{name: "a typed --env beside an empty variable", env: []string{"PTAH_ENV="}, args: []string{"--env", "dev"}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			dir, before := nativeProjectEnvDir(c)

			got := nativeProjectEnvRun(c, dir, test.env, test.args...)

			c.Assert(got.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", got.Stdout, got.Stderr))
			c.Assert(sqliteTables(c, filepath.Join(dir, "app.db")), qt.DeepEquals, before)
		})
	}
}

// compatProjectEnvHistorySchema is the revision schema the compat env dev
// names.
const compatProjectEnvHistorySchema = "dev_history"

// compatProjectEnvTarget is a scratch PostgreSQL database, a project directory
// whose atlas.hcl declares the env dev over it, and the schemas the database
// holds once the history is applied under dev.
type compatProjectEnvTarget struct {
	url    string
	dir    string
	before []string
}

// newCompatProjectEnvTarget applies a one-migration history under the env dev,
// which keeps its revision table in compatProjectEnvHistorySchema.
func newCompatProjectEnvTarget(c *qt.C) compatProjectEnvTarget {
	c.Helper()
	url := createdDevDialectDatabase(c, dbtarget.URL(c, dbtarget.PostgreSQL), "DROP DATABASE IF EXISTS %s WITH (FORCE)", renameInPath)
	dir := c.TempDir()
	writeProjectEnvHistory(c, dir, map[string]string{
		"atlas.hcl": "env \"dev\" {\n  url = \"" + url + "\"\n  migration {\n    dir = \"file://migrations\"\n" +
			"    revisions_schema = \"" + compatProjectEnvHistorySchema + "\"\n  }\n}\n",
		"migrations/20240101000001_a.sql": "CREATE TABLE a (id integer);\n",
	})
	hashed := clirun.Run(c, clirun.Compat, clirun.Options{Dir: dir, Env: environmentWithoutPtahVariables()},
		"migrate", "hash", "--dir", "file://migrations")
	c.Assert(hashed.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", hashed.Stdout, hashed.Stderr))
	applied := clirun.Run(c, clirun.Compat, clirun.Options{Dir: dir, Env: environmentWithoutPtahVariables()},
		"migrate", "apply", "--env", "dev")
	c.Assert(applied.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", applied.Stdout, applied.Stderr))
	target := compatProjectEnvTarget{url: url, dir: dir}
	target.before = target.tables(c)
	return target
}

// tables lists the schema-qualified tables of the database outside the
// catalogs, sorted.
func (t compatProjectEnvTarget) tables(c *qt.C) []string {
	c.Helper()
	conn := connectDevDialect(c, t.url)
	rows, err := conn.QueryContext(c.Context(), `SELECT table_schema || '.' || table_name FROM information_schema.tables
		WHERE table_schema NOT IN ('pg_catalog', 'information_schema') ORDER BY 1`)
	c.Assert(err, qt.IsNil)
	defer func() { c.Check(rows.Close(), qt.IsNil) }()
	names := make([]string, 0)
	for rows.Next() {
		var name string
		c.Assert(rows.Scan(&name), qt.IsNil)
		names = append(names, name)
	}
	c.Assert(rows.Err(), qt.IsNil)
	return names
}

// run runs `ptah-compat migrate apply` over the target with the URL and the
// directory typed, as a pipeline that takes the env from PTAH_ENV does, and env
// added to an environment holding no PTAH_* variable.
func (t compatProjectEnvTarget) run(c *qt.C, env []string, args ...string) clirun.Result {
	c.Helper()
	return clirun.Run(c, clirun.Compat, clirun.Options{Dir: t.dir, Env: append(environmentWithoutPtahVariables(), env...)},
		append([]string{"migrate", "apply", "--url", t.url, "--dir", "file://migrations", "--allow-dirty"}, args...)...)
}

// TestCompatMigrateApplyRefusesAnEmptyProjectEnvVariableE2E_FailurePath runs
// `ptah-compat migrate apply` again with PTAH_ENV empty or blank. Read as
// unset, the variable would select no env, so the run would read no project
// file, look for its history in the default revision schema and run migration
// 1 again.
func TestCompatMigrateApplyRefusesAnEmptyProjectEnvVariableE2E_FailurePath(t *testing.T) {
	for _, setting := range emptyProjectEnvSettings {
		t.Run(setting.env, func(t *testing.T) {
			c := qt.New(t)
			target := newCompatProjectEnvTarget(c)

			got := target.run(c, []string{setting.env})

			c.Assert(got.ExitCode, qt.Equals, 1, qt.Commentf("stdout:\n%s\nstderr:\n%s", got.Stdout, got.Stderr))
			c.Assert(got.Stderr, qt.Contains, `PTAH_ENV is `+setting.state+`, and atlas.hcl declares env "dev"`)
			c.Assert(target.tables(c), qt.DeepEquals, target.before)
		})
	}
}

// TestCompatMigrateApplyReadsTheProjectEnvVariableE2E_HappyPath is the control
// for the refusal: a value in PTAH_ENV, or a typed --env beside an empty
// variable, selects the env, which finds its history and has nothing to apply.
func TestCompatMigrateApplyReadsTheProjectEnvVariableE2E_HappyPath(t *testing.T) {
	tests := []struct {
		name string
		env  []string
		args []string
	}{
		{name: "a value", env: []string{"PTAH_ENV=dev"}},
		{name: "a typed --env beside an empty variable", env: []string{"PTAH_ENV="}, args: []string{"--env", "dev"}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			target := newCompatProjectEnvTarget(c)

			got := target.run(c, test.env, test.args...)

			c.Assert(got.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", got.Stdout, got.Stderr))
			c.Assert(target.tables(c), qt.DeepEquals, target.before)
		})
	}
}

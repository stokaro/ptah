//go:build integration

package integration_test

import (
	"os"
	"path/filepath"
	"strconv"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/clirun"
	"ptah.run/internal/dbtarget"
)

// PTAH_MIGRATIONS_ENGINE names the table engine of the revision table that
// `ptah migrations up` and `ptah-compat migrate apply` create on ClickHouse.
// An engine is fixed when the table is created, so a revision table created
// with the wrong one stays that way. An empty value is refused rather than
// read as unset, which would select the dialect's default engine and create
// the table with it. Each test runs the first apply against a database of its
// own and reads system.tables back.

// migrationsEngineTarget is a scratch ClickHouse database and a one-migration
// directory for it.
type migrationsEngineTarget struct {
	url string
	dir string
}

func newMigrationsEngineTarget(c *qt.C) migrationsEngineTarget {
	c.Helper()
	url := createdDevDialectDatabase(c, dbtarget.URL(c, dbtarget.ClickHouse), "DROP DATABASE IF EXISTS %s SYNC", renameInPath)
	dir := c.TempDir()
	c.Assert(os.WriteFile(filepath.Join(dir, "0000000001_a.up.sql"),
		[]byte("CREATE TABLE a (id UInt64) ENGINE = MergeTree ORDER BY id;\n"), 0o600), qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(dir, "0000000001_a.down.sql"), []byte("DROP TABLE a;\n"), 0o600), qt.IsNil)
	return migrationsEngineTarget{url: url, dir: dir}
}

// up runs `ptah migrations up` over the target, with env added to an
// environment holding no PTAH_* variable.
func (t migrationsEngineTarget) up(c *qt.C, env []string, args ...string) clirun.Result {
	c.Helper()
	return clirun.Run(c, clirun.Ptah, clirun.Options{Dir: c.TempDir(), Env: append(environmentWithoutPtahVariables(), env...)},
		append([]string{"migrations", "up", "--db-url", t.url, "--migrations-dir", t.dir}, args...)...)
}

// engines maps each table of the target to its engine.
func (t migrationsEngineTarget) engines(c *qt.C) map[string]string {
	c.Helper()
	conn := connectDevDialect(c, t.url)
	rows, err := conn.QueryContext(c.Context(), "SELECT name, engine FROM system.tables WHERE database = currentDatabase()")
	c.Assert(err, qt.IsNil)
	defer func() { c.Check(rows.Close(), qt.IsNil) }()
	engines := make(map[string]string)
	for rows.Next() {
		var name, engine string
		c.Assert(rows.Scan(&name, &engine), qt.IsNil)
		engines[name] = engine
	}
	c.Assert(rows.Err(), qt.IsNil)
	return engines
}

func TestMigrationsUpReadsTheMigrationsEngineVariableE2E_HappyPath(t *testing.T) {
	tests := []struct {
		name string
		env  []string
		args []string
		want string
	}{
		{name: "unset, the default engine", want: "MergeTree"},
		{name: "a value", env: []string{"PTAH_MIGRATIONS_ENGINE=ReplacingMergeTree"}, want: "ReplacingMergeTree"},
		{
			name: "a typed flag beside an empty variable",
			env:  []string{"PTAH_MIGRATIONS_ENGINE="},
			args: []string{"--migrations-engine", "ReplacingMergeTree"},
			want: "ReplacingMergeTree",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			target := newMigrationsEngineTarget(c)

			got := target.up(c, test.env, test.args...)

			c.Assert(got.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", got.Stdout, got.Stderr))
			engines := target.engines(c)
			c.Assert(engines["schema_migrations"], qt.Equals, test.want)
			c.Assert(engines["a"], qt.Equals, "MergeTree")
		})
	}
}

// TestMigrationsUpRefusesAnEmptyMigrationsEngineVariableE2E_FailurePath runs
// the first `up` with PTAH_MIGRATIONS_ENGINE set but empty. The run refuses
// before it connects, so the database holds no table. Read as unset, the
// variable would create the revision table with MergeTree.
func TestMigrationsUpRefusesAnEmptyMigrationsEngineVariableE2E_FailurePath(t *testing.T) {
	c := qt.New(t)
	target := newMigrationsEngineTarget(c)

	got := target.up(c, []string{"PTAH_MIGRATIONS_ENGINE="})

	c.Assert(got.ExitCode, qt.Equals, 2, qt.Commentf("stdout:\n%s\nstderr:\n%s", got.Stdout, got.Stderr))
	c.Assert(got.Stderr, qt.Contains, "PTAH_MIGRATIONS_ENGINE is set but empty")
	c.Assert(target.engines(c), qt.DeepEquals, make(map[string]string))
}

// compatMigrationsEngineDir writes and hashes a one-migration directory in the
// Atlas layout, whose migration creates the table a, and returns the project
// directory that holds it.
func compatMigrationsEngineDir(c *qt.C) string {
	c.Helper()
	dir := c.TempDir()
	c.Assert(os.Mkdir(filepath.Join(dir, "migrations"), 0o755), qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(dir, "migrations", "20240101000001_a.sql"),
		[]byte("CREATE TABLE a (id UInt64) ENGINE = MergeTree ORDER BY id;\n"), 0o600), qt.IsNil)
	hashed := clirun.Run(c, clirun.Compat, clirun.Options{Dir: dir, Env: environmentWithoutPtahVariables()},
		"migrate", "hash", "--dir", "file://migrations")
	c.Assert(hashed.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", hashed.Stdout, hashed.Stderr))
	return dir
}

// compatApply runs `ptah-compat migrate apply` over the target with the
// directory compatMigrationsEngineDir writes, and env added to an environment
// holding no PTAH_* variable.
func (t migrationsEngineTarget) compatApply(c *qt.C, env []string) clirun.Result {
	c.Helper()
	return clirun.Run(c, clirun.Compat,
		clirun.Options{Dir: compatMigrationsEngineDir(c), Env: append(environmentWithoutPtahVariables(), env...)},
		"migrate", "apply", "--url", t.url, "--dir", "file://migrations")
}

// TestCompatMigrateApplyReadsTheMigrationsEngineVariableE2E_HappyPath reads
// the variable the compatibility surface takes in place of a flag: its value
// is the engine of atlas_schema_revisions.
func TestCompatMigrateApplyReadsTheMigrationsEngineVariableE2E_HappyPath(t *testing.T) {
	tests := []struct {
		name string
		env  []string
		want string
	}{
		{name: "unset, the default engine", want: "MergeTree"},
		{name: "a value", env: []string{"PTAH_MIGRATIONS_ENGINE=ReplacingMergeTree"}, want: "ReplacingMergeTree"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			target := newMigrationsEngineTarget(c)

			got := target.compatApply(c, test.env)

			c.Assert(got.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", got.Stdout, got.Stderr))
			engines := target.engines(c)
			c.Assert(engines["atlas_schema_revisions"], qt.Equals, test.want)
			c.Assert(engines["a"], qt.Equals, "MergeTree")
		})
	}
}

// TestCompatMigrateApplyRefusesAnEmptyMigrationsEngineVariableE2E_FailurePath
// runs the first `migrate apply` with the variable empty and blank. The run
// refuses before it connects, so the database holds no table. Read as unset,
// the variable would create atlas_schema_revisions with MergeTree.
func TestCompatMigrateApplyRefusesAnEmptyMigrationsEngineVariableE2E_FailurePath(t *testing.T) {
	for _, value := range []string{"", " "} {
		t.Run(strconv.Quote(value), func(t *testing.T) {
			c := qt.New(t)
			target := newMigrationsEngineTarget(c)

			got := target.compatApply(c, []string{"PTAH_MIGRATIONS_ENGINE=" + value})

			c.Assert(got.ExitCode, qt.Equals, 1, qt.Commentf("stdout:\n%s\nstderr:\n%s", got.Stdout, got.Stderr))
			c.Assert(got.Stderr, qt.Contains, "PTAH_MIGRATIONS_ENGINE is set")
			c.Assert(got.Stderr, qt.Contains, "or unset it to use the dialect's default engine")
			c.Assert(target.engines(c), qt.DeepEquals, make(map[string]string))
		})
	}
}

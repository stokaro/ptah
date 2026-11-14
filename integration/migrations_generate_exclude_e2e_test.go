//go:build integration

package integration_test

import (
	"database/sql"
	"os"
	"path/filepath"
	"slices"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/dbschema"
	"ptah.run/internal/clirun"
	"ptah.run/internal/dbtarget"
)

// The SQLite fixture declares `widgets` with a new column and a
// `reporting_cache` the selectors keep out. The target holds `widgets` and an
// unmanaged `external_audit`. The migration directory created `widgets` and a
// `legacy_log` the declaration no longer names, so a replay of it holds
// `legacy_log`. Without an exclusion the written migration creates
// `reporting_cache` and drops the table the current state holds; with one it
// only adds the column.

const generateExcludeDesiredSQL = "CREATE TABLE widgets (id INTEGER PRIMARY KEY, name TEXT);\n" +
	"CREATE TABLE reporting_cache (id INTEGER PRIMARY KEY);\n"

// generateExcludeFixture holds the paths one run of `ptah migrations generate`
// reads and writes.
type generateExcludeFixture struct {
	dir           string
	targetURL     string
	devURL        string
	shadowURL     string
	migrationsDir string
}

// writeGenerateExcludeFixture writes the declaration and the migration
// directory, and creates the target with targetDDL.
func writeGenerateExcludeFixture(c *qt.C, targetDDL string) generateExcludeFixture {
	c.Helper()
	dir := c.TempDir()
	sqliteURL := func(name string) string { return "sqlite://" + filepath.ToSlash(filepath.Join(dir, name)) }
	fixture := generateExcludeFixture{
		dir:           dir,
		targetURL:     sqliteURL("target.db"),
		devURL:        sqliteURL("dev.db"),
		shadowURL:     sqliteURL("shadow.db"),
		migrationsDir: filepath.Join(dir, "migrations"),
	}
	c.Assert(os.WriteFile(filepath.Join(dir, "schema.sql"), []byte(generateExcludeDesiredSQL), 0o600), qt.IsNil)
	c.Assert(os.MkdirAll(fixture.migrationsDir, 0o755), qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(fixture.migrationsDir, "0000000001_init.up.sql"),
		[]byte("CREATE TABLE widgets (id INTEGER PRIMARY KEY);\nCREATE TABLE legacy_log (id INTEGER PRIMARY KEY);\n"), 0o600), qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(fixture.migrationsDir, "0000000001_init.down.sql"),
		[]byte("DROP TABLE legacy_log;\nDROP TABLE widgets;\n"), 0o600), qt.IsNil)
	target, err := dbschema.ConnectToDatabase(c.Context(), fixture.targetURL)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(target)
	_, err = target.ExecContext(c.Context(), targetDDL)
	c.Assert(err, qt.IsNil)
	return fixture
}

// targetArgs read the current state from the target database.
func (f generateExcludeFixture) targetArgs() []string {
	return []string{
		"migrations", "generate", "--db-url", f.targetURL, "--schema-file", "schema.sql",
		"--migrations-dir", f.migrationsDir, "--name", "second",
	}
}

// replayArgs derive the current state by replaying the migration directory
// into the dev database.
func (f generateExcludeFixture) replayArgs() []string {
	return []string{
		"migrations", "generate", "--replay", "--dev-url", f.devURL, "--schema-file", "schema.sql",
		"--migrations-dir", f.migrationsDir, "--name", "second",
	}
}

// shadowArgs read the target and verify the candidate by replaying the
// directory and the candidate into the shadow database.
func (f generateExcludeFixture) shadowArgs() []string {
	return append(f.targetArgs(), "--shadow-db", f.shadowURL)
}

// generatedMigration returns the up and down bodies of the one migration named
// "second" in dir.
func generatedMigration(c *qt.C, dir string) (up, down string) {
	c.Helper()
	read := func(suffix string) string {
		files, err := filepath.Glob(filepath.Join(dir, "*_second."+suffix))
		c.Assert(err, qt.IsNil)
		c.Assert(files, qt.HasLen, 1)
		contents, err := os.ReadFile(files[0])
		c.Assert(err, qt.IsNil)
		return string(contents)
	}
	return read("up.sql"), read("down.sql")
}

const (
	generateExcludeTargetDDL = "CREATE TABLE widgets (id INTEGER PRIMARY KEY); CREATE TABLE external_audit (id INTEGER PRIMARY KEY);"
	generateExcludeLegacyDDL = "CREATE TABLE widgets (id INTEGER PRIMARY KEY); CREATE TABLE legacy_log (id INTEGER PRIMARY KEY);"
)

// TestMigrationsGenerateExclude_HappyPathE2E runs the shipped binary and reads
// each written file back. Each unexcluded row is the control for the row after
// it: it shows the fixture creates and drops the tables the excluded row
// protects. The shadow rows run against a target that still holds the
// replayed `legacy_log`, so a verification that did not exclude it as the plan
// did would report it as an extra table and write nothing.
func TestMigrationsGenerateExclude_HappyPathE2E(t *testing.T) {
	tests := []struct {
		name      string
		targetDDL string
		args      func(generateExcludeFixture) []string
		exclude   []string
		env       []string
		has       []string
		lacks     []string
	}{
		{
			name:      "the target, nothing excluded",
			targetDDL: generateExcludeTargetDDL,
			args:      generateExcludeFixture.targetArgs,
			has:       []string{`ADD COLUMN "name" TEXT`, `CREATE TABLE "reporting_cache"`, `DROP TABLE "external_audit"`},
		},
		{
			name:      "the target",
			targetDDL: generateExcludeTargetDDL,
			args:      generateExcludeFixture.targetArgs,
			exclude:   []string{"--exclude", "reporting_cache", "--exclude", "external_audit"},
			has:       []string{`ADD COLUMN "name" TEXT`},
			lacks:     []string{"reporting_cache", "external_audit"},
		},
		{
			name:      "the target, selected through PTAH_EXCLUDE",
			targetDDL: generateExcludeTargetDDL,
			args:      generateExcludeFixture.targetArgs,
			env:       []string{"PTAH_EXCLUDE=reporting_cache,external_audit"},
			has:       []string{`ADD COLUMN "name" TEXT`},
			lacks:     []string{"reporting_cache", "external_audit"},
		},
		{
			name:      "a replay, nothing excluded",
			targetDDL: generateExcludeTargetDDL,
			args:      generateExcludeFixture.replayArgs,
			has:       []string{`ADD COLUMN "name" TEXT`, `CREATE TABLE "reporting_cache"`, `DROP TABLE "legacy_log"`},
		},
		{
			name:      "a replay",
			targetDDL: generateExcludeTargetDDL,
			args:      generateExcludeFixture.replayArgs,
			exclude:   []string{"--exclude", "legacy_log,reporting_cache"},
			has:       []string{`ADD COLUMN "name" TEXT`},
			lacks:     []string{"reporting_cache", "legacy_log"},
		},
		{
			name:      "a shadow verification, nothing excluded",
			targetDDL: generateExcludeLegacyDDL,
			args:      generateExcludeFixture.shadowArgs,
			has:       []string{`ADD COLUMN "name" TEXT`, `CREATE TABLE "reporting_cache"`, `DROP TABLE "legacy_log"`},
		},
		{
			name:      "a shadow verification",
			targetDDL: generateExcludeLegacyDDL,
			args:      generateExcludeFixture.shadowArgs,
			exclude:   []string{"--exclude", "legacy_log", "--exclude", "reporting_cache"},
			has:       []string{`ADD COLUMN "name" TEXT`},
			lacks:     []string{"reporting_cache", "legacy_log"},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			fixture := writeGenerateExcludeFixture(c, test.targetDDL)
			env := append(environmentWithoutPtahVariables(), test.env...)

			result := clirun.Run(c, clirun.Ptah, clirun.Options{Dir: fixture.dir, Env: env},
				append(test.args(fixture), test.exclude...)...)

			c.Assert(result.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", result.Stdout, result.Stderr))
			up, down := generatedMigration(c, fixture.migrationsDir)
			for _, fragment := range test.has {
				c.Assert(up, qt.Contains, fragment)
			}
			for _, fragment := range test.lacks {
				c.Assert(up, qt.Not(qt.Contains), fragment)
				c.Assert(down, qt.Not(qt.Contains), fragment)
			}
		})
	}
}

// TestMigrationsGenerateExclude_FailurePathE2E refuses a malformed selector as
// a usage error, before the replay resets the dev database, so neither the dev
// database nor a migration file is written.
func TestMigrationsGenerateExclude_FailurePathE2E(t *testing.T) {
	c := qt.New(t)
	fixture := writeGenerateExcludeFixture(c, generateExcludeTargetDDL)

	result := clirun.Run(c, clirun.Ptah, clirun.Options{Dir: fixture.dir, Env: environmentWithoutPtahVariables()},
		append(fixture.replayArgs(), "--exclude", "us[ers")...)

	c.Assert(result.ExitCode, qt.Equals, 2)
	c.Assert(result.Stdout, qt.Equals, "")
	c.Assert(result.Stderr, qt.Equals, "error: invalid --exclude: invalid Atlas exclude glob \"us[ers\": syntax error in pattern\n")
	_, err := os.Stat(filepath.Join(fixture.dir, "dev.db"))
	c.Assert(err, qt.ErrorIs, os.ErrNotExist)
	written, err := filepath.Glob(filepath.Join(fixture.migrationsDir, "*_second.*"))
	c.Assert(err, qt.IsNil)
	c.Assert(written, qt.HasLen, 0)
}

// The PostgreSQL fixture is a table with row security enabled and one policy,
// and a declaration that adds a column and declares neither. Excluding every
// policy also leaves the table's row-security switch alone: a migration that
// kept the policies and disabled row security would still reveal every row
// they hid. Excluding one named policy keeps the switch in the comparison.

const (
	generateExcludePolicyTargetDDL = "CREATE TABLE tasks (id bigint PRIMARY KEY);\n" +
		"ALTER TABLE tasks ENABLE ROW LEVEL SECURITY;\n" +
		"CREATE POLICY admin_read ON tasks FOR SELECT USING (true);\n"
	generateExcludePolicyDesiredSQL = "CREATE TABLE tasks (id bigint PRIMARY KEY, title text);\n"
)

// TestMigrationsGenerateExcludePolicies_HappyPathE2E reads both written files
// back. The unexcluded row is the control: the fixture drops the policy and
// disables row security when nothing is excluded.
func TestMigrationsGenerateExcludePolicies_HappyPathE2E(t *testing.T) {
	tests := []struct {
		name     string
		exclude  []string
		upHas    []string
		upLacks  []string
		downHas  []string
		downLack []string
	}{
		{
			name:    "nothing excluded",
			upHas:   []string{`ADD COLUMN "title" text`, `DROP POLICY "admin_read"`, "DISABLE ROW LEVEL SECURITY"},
			downHas: []string{"CREATE POLICY", "ENABLE ROW LEVEL SECURITY"},
		},
		{
			name:     "every policy",
			exclude:  []string{"--exclude", "*[type=policy]"},
			upHas:    []string{`ADD COLUMN "title" text`},
			upLacks:  []string{"POLICY", "ROW LEVEL SECURITY"},
			downLack: []string{"POLICY", "ROW LEVEL SECURITY"},
		},
		{
			name:     "one named policy",
			exclude:  []string{"--exclude", "admin_read[type=policy]"},
			upHas:    []string{`ADD COLUMN "title" text`, "DISABLE ROW LEVEL SECURITY"},
			upLacks:  []string{"POLICY"},
			downHas:  []string{"ENABLE ROW LEVEL SECURITY"},
			downLack: []string{"POLICY"},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			targetURL := postgresScratchDevURL(c, "ptah_generate_exclude")
			target, err := sql.Open("pgx", targetURL)
			c.Assert(err, qt.IsNil)
			_, err = target.ExecContext(c.Context(), generateExcludePolicyTargetDDL)
			c.Assert(err, qt.IsNil)
			c.Assert(target.Close(), qt.IsNil)
			dir := c.TempDir()
			c.Assert(os.WriteFile(filepath.Join(dir, "schema.sql"), []byte(generateExcludePolicyDesiredSQL), 0o600), qt.IsNil)
			migrationsDir := filepath.Join(dir, "migrations")
			args := append([]string{
				"migrations", "generate", "--db-url", targetURL, "--schema-file", "schema.sql",
				"--migrations-dir", migrationsDir, "--name", "second",
			}, test.exclude...)

			result := clirun.Run(c, clirun.Ptah, clirun.Options{Dir: dir, Env: environmentWithoutPtahVariables()}, args...)

			c.Assert(result.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", result.Stdout, result.Stderr))
			up, down := generatedMigration(c, migrationsDir)
			for _, fragment := range test.upHas {
				c.Assert(up, qt.Contains, fragment)
			}
			for _, fragment := range test.upLacks {
				c.Assert(up, qt.Not(qt.Contains), fragment)
			}
			for _, fragment := range test.downHas {
				c.Assert(down, qt.Contains, fragment)
			}
			for _, fragment := range test.downLack {
				c.Assert(down, qt.Not(qt.Contains), fragment)
			}
		})
	}
}

// On MySQL and MariaDB a schema is a database, so the shadow database has a
// name of its own and a read labels each object with the database it read.
// The fixture is the SQLite one on such a server: a target database holding
// `widgets` and `legacy_log`, a history that created both, a declaration that
// adds a column to `widgets` and declares `reporting_cache`, and an empty
// shadow database on the same server.

// mysqlFamilyShadowExcludeFixture is one run's databases and arguments.
type mysqlFamilyShadowExcludeFixture struct {
	dir           string
	target        string
	migrationsDir string
	args          []string
}

func newMySQLFamilyShadowExcludeFixture(c *qt.C, engine dbtarget.Engine, scheme string) mysqlFamilyShadowExcludeFixture {
	c.Helper()
	server := newMySQLFamilyServer(c, engine)
	target := server.database(c, "gen_exclude_target")
	shadow := server.database(c, "gen_exclude_shadow")
	for _, statement := range []string{
		"CREATE TABLE `" + target + "`.widgets (id INT PRIMARY KEY)",
		"CREATE TABLE `" + target + "`.legacy_log (id INT PRIMARY KEY)",
	} {
		_, err := server.admin.ExecContext(c.Context(), statement)
		c.Assert(err, qt.IsNil, qt.Commentf("%s", statement))
	}
	dir := c.TempDir()
	migrationsDir := filepath.Join(dir, "migrations")
	c.Assert(os.MkdirAll(migrationsDir, 0o755), qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(migrationsDir, "0000000001_init.up.sql"),
		[]byte("CREATE TABLE widgets (id INT PRIMARY KEY);\nCREATE TABLE legacy_log (id INT PRIMARY KEY);\n"), 0o600), qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(migrationsDir, "0000000001_init.down.sql"),
		[]byte("DROP TABLE legacy_log;\nDROP TABLE widgets;\n"), 0o600), qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(dir, "schema.sql"), []byte(
		"CREATE TABLE widgets (id INT PRIMARY KEY, name TEXT);\nCREATE TABLE reporting_cache (id INT PRIMARY KEY);\n",
	), 0o600), qt.IsNil)
	return mysqlFamilyShadowExcludeFixture{
		dir:           dir,
		target:        target,
		migrationsDir: migrationsDir,
		args: []string{
			"migrations", "generate", "--db-url", server.url(scheme, target), "--schema-file", "schema.sql",
			"--migrations-dir", migrationsDir, "--name", "second", "--shadow-db", server.url(scheme, shadow),
		},
	}
}

// mysqlFamilyShadowServers are the servers where the shadow database has a
// name of its own.
var mysqlFamilyShadowServers = []struct {
	name   string
	engine dbtarget.Engine
	scheme string
}{
	{name: "MySQL", engine: dbtarget.MySQLAdmin, scheme: "mysql"},
	{name: "MariaDB", engine: dbtarget.MariaDBAdmin, scheme: "mariadb"},
}

// TestMigrationsGenerateExcludeUnderAMySQLFamilyShadow_HappyPathE2E holds the
// shadow check to the plan's reading of a selector qualified with the target's
// database name. The plan leaves both tables alone, and the replay creates
// `legacy_log` in the shadow database, under the shadow's name: a check that
// judged the selector against that name would report the table as extra and
// write nothing. The unexcluded run is the control: the fixture drops
// `legacy_log` and creates `reporting_cache`.
func TestMigrationsGenerateExcludeUnderAMySQLFamilyShadow_HappyPathE2E(t *testing.T) {
	for _, server := range mysqlFamilyShadowServers {
		t.Run(server.name, func(t *testing.T) {
			t.Run("nothing excluded", func(t *testing.T) {
				c := qt.New(t)
				fixture := newMySQLFamilyShadowExcludeFixture(c, server.engine, server.scheme)

				result := clirun.Run(c, clirun.Ptah, clirun.Options{Dir: fixture.dir, Env: environmentWithoutPtahVariables()}, fixture.args...)

				c.Assert(result.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", result.Stdout, result.Stderr))
				up, _ := generatedMigration(c, fixture.migrationsDir)
				c.Assert(up, qt.Contains, "DROP TABLE `legacy_log`")
				c.Assert(up, qt.Contains, "CREATE TABLE `reporting_cache`")
			})
			t.Run("selectors qualified with the target database", func(t *testing.T) {
				c := qt.New(t)
				fixture := newMySQLFamilyShadowExcludeFixture(c, server.engine, server.scheme)
				args := slices.Concat(fixture.args,
					[]string{"--exclude", fixture.target + ".legacy_log", "--exclude", fixture.target + ".reporting_cache"})

				result := clirun.Run(c, clirun.Ptah, clirun.Options{Dir: fixture.dir, Env: environmentWithoutPtahVariables()}, args...)

				c.Assert(result.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", result.Stdout, result.Stderr))
				up, down := generatedMigration(c, fixture.migrationsDir)
				c.Assert(up, qt.Contains, "ADD COLUMN `name` TEXT")
				for _, table := range []string{"legacy_log", "reporting_cache"} {
					c.Assert(up, qt.Not(qt.Contains), table)
					c.Assert(down, qt.Not(qt.Contains), table)
				}
			})
		})
	}
}

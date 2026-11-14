package migrate_test

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"
	"github.com/spf13/cobra"

	"ptah.run/dbschema"
	"ptah.run/internal/cli/migrate"
)

// The exclusion fixture declares `widgets` with a new column and a
// `reporting_cache` the selectors keep out. The target holds `widgets` and an
// unmanaged `external_audit`; the migration directory a replay reads created
// `widgets` and a `legacy_log` the declaration no longer names. Without an
// exclusion each command creates `reporting_cache` and drops the table the
// current state holds; with one it only adds the column.

const excludeDesiredSQL = "CREATE TABLE widgets (id INTEGER PRIMARY KEY, name TEXT);\n" +
	"CREATE TABLE reporting_cache (id INTEGER PRIMARY KEY);\n"

// excludeCLIFixture are the paths one run reads and writes.
type excludeCLIFixture struct {
	schemaFile    string
	targetURL     string
	devPath       string
	migrationsDir string
}

func writeExcludeCLIFixture(c *qt.C) excludeCLIFixture {
	c.Helper()
	dir := c.TempDir()
	fixture := excludeCLIFixture{
		schemaFile:    filepath.Join(dir, "schema.sql"),
		targetURL:     "sqlite://" + filepath.ToSlash(filepath.Join(dir, "target.db")),
		devPath:       filepath.Join(dir, "dev.db"),
		migrationsDir: filepath.Join(dir, "migrations"),
	}
	c.Assert(os.WriteFile(fixture.schemaFile, []byte(excludeDesiredSQL), 0o600), qt.IsNil)
	c.Assert(os.MkdirAll(fixture.migrationsDir, 0o755), qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(fixture.migrationsDir, "0000000001_init.up.sql"),
		[]byte("CREATE TABLE widgets (id INTEGER PRIMARY KEY);\nCREATE TABLE legacy_log (id INTEGER PRIMARY KEY);\n"), 0o600), qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(fixture.migrationsDir, "0000000001_init.down.sql"),
		[]byte("DROP TABLE legacy_log;\nDROP TABLE widgets;\n"), 0o600), qt.IsNil)
	target, err := dbschema.ConnectToDatabase(c.Context(), fixture.targetURL)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(target)
	_, err = target.ExecContext(c.Context(),
		"CREATE TABLE widgets (id INTEGER PRIMARY KEY); CREATE TABLE external_audit (id INTEGER PRIMARY KEY);")
	c.Assert(err, qt.IsNil)
	return fixture
}

// targetArgs reads the current state from the target database.
func (f excludeCLIFixture) targetArgs() []string {
	return []string{"--db-url", f.targetURL, "--schema-file", f.schemaFile, "--migrations-dir", f.migrationsDir, "--name", "second"}
}

// replayArgs derives the current state by replaying the migration directory.
func (f excludeCLIFixture) replayArgs() []string {
	return []string{
		"--replay", "--dev-url", "sqlite://" + filepath.ToSlash(f.devPath),
		"--schema-file", f.schemaFile, "--migrations-dir", f.migrationsDir, "--name", "second",
	}
}

// secondUpBody is the up body of the migration named "second".
func secondUpBody(c *qt.C, dir string) string {
	c.Helper()
	ups, err := filepath.Glob(filepath.Join(dir, "*_second.up.sql"))
	c.Assert(err, qt.IsNil)
	c.Assert(ups, qt.HasLen, 1)
	return string(must.Must(os.ReadFile(ups[0])))
}

func TestMigrateGenerateExclude_HappyPath(t *testing.T) {
	tests := []struct {
		name    string
		args    func(excludeCLIFixture) []string
		exclude []string
		has     []string
		lacks   []string
	}{
		{
			name:  "the target, nothing excluded",
			args:  excludeCLIFixture.targetArgs,
			has:   []string{`ADD COLUMN "name" TEXT`, `CREATE TABLE "reporting_cache"`, `DROP TABLE "external_audit"`},
			lacks: []string{"legacy_log"},
		},
		{
			name:    "the target",
			args:    excludeCLIFixture.targetArgs,
			exclude: []string{"--exclude", "reporting_cache", "--exclude", "external_audit"},
			has:     []string{`ADD COLUMN "name" TEXT`},
			lacks:   []string{"reporting_cache", "external_audit"},
		},
		{
			name:  "a replay, nothing excluded",
			args:  excludeCLIFixture.replayArgs,
			has:   []string{`ADD COLUMN "name" TEXT`, `CREATE TABLE "reporting_cache"`, `DROP TABLE "legacy_log"`},
			lacks: []string{"external_audit"},
		},
		{
			name:    "a replay",
			args:    excludeCLIFixture.replayArgs,
			exclude: []string{"--exclude", "legacy_log,reporting_cache"},
			has:     []string{`ADD COLUMN "name" TEXT`},
			lacks:   []string{"reporting_cache", "legacy_log"},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			fixture := writeExcludeCLIFixture(c)

			out, err := runGenerate(append(test.args(fixture), test.exclude...)...)

			c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
			up := secondUpBody(c, fixture.migrationsDir)
			for _, fragment := range test.has {
				c.Assert(up, qt.Contains, fragment)
			}
			for _, fragment := range test.lacks {
				c.Assert(up, qt.Not(qt.Contains), fragment)
			}
		})
	}
}

// TestMigrateGenerateExclude_FailurePath covers both points a selector is
// refused at. A syntax error is refused before the replay resets the dev
// database, so that file is never created. A selector with more parts than the
// connection's scope allows can only be judged once the connection says which
// scope it reads; it is refused before a migration file is written.
func TestMigrateGenerateExclude_FailurePath(t *testing.T) {
	t.Run("a syntax error", func(t *testing.T) {
		c := qt.New(t)
		fixture := writeExcludeCLIFixture(c)

		out, err := runGenerate(append(fixture.replayArgs(), "--exclude", "us[ers")...)

		c.Assert(err, qt.ErrorMatches, `invalid --exclude: invalid Atlas exclude glob "us\[ers": syntax error in pattern`, qt.Commentf("%s", out))
		_, statErr := os.Stat(fixture.devPath)
		c.Assert(statErr, qt.ErrorIs, os.ErrNotExist)
		matches, globErr := filepath.Glob(filepath.Join(fixture.migrationsDir, "*_second.*"))
		c.Assert(globErr, qt.IsNil)
		c.Assert(matches, qt.HasLen, 0)
	})
	t.Run("more parts than the scope allows", func(t *testing.T) {
		c := qt.New(t)
		fixture := writeExcludeCLIFixture(c)

		out, err := runGenerate(append(fixture.targetArgs(), "--exclude", "main.widgets.name")...)

		c.Assert(err, qt.ErrorMatches, `apply exclude selectors to the desired schema: too many parts in pattern "main\.widgets\.name": `+
			`this connection is bound to schema "main", so a pattern names object or object\.child; write "widgets\.name"`, qt.Commentf("%s", out))
		matches, globErr := filepath.Glob(filepath.Join(fixture.migrationsDir, "*_second.*"))
		c.Assert(globErr, qt.IsNil)
		c.Assert(matches, qt.HasLen, 0)
	})
}

// runPlan runs `migrations plan` and returns its standard output and error
// stream apart.
func runPlan(args ...string) (stdout, stderr string, err error) {
	cmd := migrate.NewMigrateCommand()
	var out, diagnostics bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&diagnostics)
	cmd.SetArgs(args)
	err = cmd.Execute()
	return out.String(), diagnostics.String(), err
}

func TestMigratePlanExclude_HappyPath(t *testing.T) {
	tests := []struct {
		name     string
		exclude  []string
		has      []string
		lacks    []string
		warnings string
	}{
		{
			name: "nothing excluded",
			has:  []string{`ADD COLUMN "name" TEXT`, `CREATE TABLE "reporting_cache"`, `DROP TABLE "external_audit"`},
		},
		{
			name:    "a declared table and an unmanaged one",
			exclude: []string{"--exclude", "reporting_cache", "--exclude", "external_audit"},
			has:     []string{`ADD COLUMN "name" TEXT`},
			lacks:   []string{"reporting_cache", "external_audit"},
		},
		{
			name:     "a selector that names nothing",
			exclude:  []string{"--exclude", "reporting_cache", "--exclude", "nothing_here"},
			has:      []string{`ADD COLUMN "name" TEXT`, `DROP TABLE "external_audit"`},
			lacks:    []string{"reporting_cache"},
			warnings: "Warning: the --exclude selection matched no objects: \"nothing_here\".\n",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			fixture := writeExcludeCLIFixture(c)

			stdout, stderr, err := runPlan(append([]string{"--db-url", fixture.targetURL, "--schema-file", fixture.schemaFile}, test.exclude...)...)

			c.Assert(err, qt.IsNil, qt.Commentf("%s", stderr))
			c.Assert(stderr, qt.Equals, test.warnings)
			for _, fragment := range test.has {
				c.Assert(stdout, qt.Contains, fragment)
			}
			for _, fragment := range test.lacks {
				c.Assert(stdout, qt.Not(qt.Contains), fragment)
			}
		})
	}
}

// TestMigratePlanExclude_FailurePath refuses a syntax error before the target
// is connected, where SQLite would create the file, and a selector with more
// parts than the connection's scope allows once the target is open.
func TestMigratePlanExclude_FailurePath(t *testing.T) {
	t.Run("a syntax error", func(t *testing.T) {
		c := qt.New(t)
		dir := t.TempDir()
		target := filepath.Join(dir, "target.db")
		schemaFile := filepath.Join(dir, "schema.sql")
		c.Assert(os.WriteFile(schemaFile, []byte(excludeDesiredSQL), 0o600), qt.IsNil)

		_, stderr, err := runPlan("--db-url", "sqlite://"+filepath.ToSlash(target), "--schema-file", schemaFile, "--exclude", "us[ers")

		c.Assert(err, qt.ErrorMatches, `invalid --exclude: invalid Atlas exclude glob "us\[ers": syntax error in pattern`, qt.Commentf("%s", stderr))
		_, statErr := os.Stat(target)
		c.Assert(statErr, qt.ErrorIs, os.ErrNotExist)
	})
	t.Run("more parts than the scope allows", func(t *testing.T) {
		c := qt.New(t)
		fixture := writeExcludeCLIFixture(c)

		stdout, stderr, err := runPlan("--db-url", fixture.targetURL, "--schema-file", fixture.schemaFile, "--exclude", "main.widgets.name")

		c.Assert(err, qt.ErrorMatches, `apply exclude selectors to the desired schema: too many parts in pattern "main\.widgets\.name": .*`, qt.Commentf("%s", stderr))
		c.Assert(stdout, qt.Not(qt.Contains), "=== MIGRATION SQL ===")
	})
}

// TestMigrationsPlanningCommandsTakeExclude holds both verbs to the flag the
// schema verbs register: repeatable, and so bound to PTAH_EXCLUDE by the
// root's environment binding.
func TestMigrationsPlanningCommandsTakeExclude(t *testing.T) {
	tests := []struct {
		name    string
		command func() *cobra.Command
		usage   string
	}{
		{name: "plan", command: migrate.NewMigrateCommand, usage: "Schema objects to exclude from planning (Atlas-style selectors)"},
		{name: "generate", command: migrate.NewMigrateGenerateCommand, usage: "Schema objects to exclude from the generated migration (Atlas-style selectors)"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			flag := test.command().Flags().Lookup("exclude")

			c.Assert(flag, qt.IsNotNil)
			c.Assert(flag.Value.Type(), qt.Equals, "stringArray")
			c.Assert(flag.Usage, qt.Equals, test.usage)
		})
	}
}

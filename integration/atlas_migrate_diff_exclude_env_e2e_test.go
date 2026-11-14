//go:build integration

package integration_test

import (
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/clirun"
)

// `ptah-compat migrate diff` registers no --exclude, because the pinned
// community binary registers none there. The command takes its exclusion from
// the selected env's `exclude` list in both profiles, and from
// PTAH_ATLAS_MIGRATE_DIFF_EXCLUDE in full compatibility only: strict mode
// refuses that variable by name before any command runs.

// migrateDiffExcludeEnvVar is the variable the full profile reads.
const migrateDiffExcludeEnvVar = "PTAH_ATLAS_MIGRATE_DIFF_EXCLUDE"

// migrateDiffExcludeArgs are the arguments every row below runs with, so the
// flag row and the variable rows differ only in what they add.
var migrateDiffExcludeArgs = []string{
	"--dev-url", "sqlite://file?mode=memory",
	"--dir", "file://migrations",
	"--to", "file://schema.sql",
}

// migrateDiffCreatedTable reads a table name out of a generated SQLite
// statement.
var migrateDiffCreatedTable = regexp.MustCompile(`(?m)^CREATE TABLE "([^"]+)"`)

// migrateDiffExcludeRoot writes a desired schema of two tables, and an
// atlas.hcl whose env excludes one of them, into a new directory. The
// migration directory the commands write is not created.
func migrateDiffExcludeRoot(c *qt.C) string {
	c.Helper()
	root := c.TempDir()
	c.Assert(os.WriteFile(filepath.Join(root, "schema.sql"), []byte(
		"CREATE TABLE widgets (id INTEGER PRIMARY KEY);\nCREATE TABLE manual (id INTEGER PRIMARY KEY);\n",
	), 0o600), qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(root, "atlas.hcl"), []byte(`env "local" {
  src     = "file://schema.sql"
  dev     = "sqlite://file?mode=memory"
  exclude = ["manual"]
  migration {
    dir = "file://migrations"
  }
}
`), 0o600), qt.IsNil)
	return root
}

// migrateDiffWrittenTables returns the tables the migration files under root
// create, sorted.
func migrateDiffWrittenTables(c *qt.C, root string) []string {
	c.Helper()
	files, err := filepath.Glob(filepath.Join(root, "migrations", "*.sql"))
	c.Assert(err, qt.IsNil)
	c.Assert(files, qt.HasLen, 1)
	contents, err := os.ReadFile(files[0])
	c.Assert(err, qt.IsNil)
	var tables []string
	for _, match := range migrateDiffCreatedTable.FindAllStringSubmatch(string(contents), -1) {
		tables = append(tables, match[1])
	}
	slices.Sort(tables)
	return tables
}

// TestAtlasCompatMigrateDiffRefusesTheExcludeFlag_FailurePathE2E pins the
// answer of the pinned community binary v1.3.0, measured with
// ATLAS_NO_UPDATE_NOTIFIER=1 and ATLAS_NO_ANON_TELEMETRY=true on these
// arguments: exit 1, nothing on stdout, `Error: unknown flag: --exclude` on
// stderr, and no migration directory. Both profiles answer the same way.
func TestAtlasCompatMigrateDiffRefusesTheExcludeFlag_FailurePathE2E(t *testing.T) {
	for _, test := range []struct {
		name string
		env  []string
	}{
		{name: "full compatibility"},
		{name: "strict Community Edition", env: []string{"PTAH_ATLAS_STRICT_COMPAT=1"}},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			root := migrateDiffExcludeRoot(c)
			env := append(environmentWithoutPtahVariables(), test.env...)
			args := append([]string{"migrate", "diff", "--exclude", "x"}, migrateDiffExcludeArgs...)

			result := clirun.Run(c, clirun.Compat, clirun.Options{Dir: root, Env: env}, args...)

			c.Assert(result.ExitCode, qt.Equals, 1)
			c.Assert(result.Stdout, qt.Equals, "")
			c.Assert(result.Stderr, qt.Equals, "Error: unknown flag: --exclude\n")
			_, err := os.Stat(filepath.Join(root, "migrations"))
			c.Assert(err, qt.ErrorIs, os.ErrNotExist)
		})
	}
}

// TestAtlasCompatMigrateDiffReadsTheExcludeVariable_HappyPathE2E is the control
// for the strict refusal below: outside strict mode the variable reaches the
// comparison, and the table it names is missing from the file written. The
// unset row shows that the fixture writes both tables when nothing is
// excluded.
func TestAtlasCompatMigrateDiffReadsTheExcludeVariable_HappyPathE2E(t *testing.T) {
	for _, test := range []struct {
		name string
		env  []string
		want []string
	}{
		{name: "unset", want: []string{"manual", "widgets"}},
		{name: "one table", env: []string{migrateDiffExcludeEnvVar + "=manual"}, want: []string{"widgets"}},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			root := migrateDiffExcludeRoot(c)
			env := append(environmentWithoutPtahVariables(), test.env...)
			args := append([]string{"migrate", "diff", "add"}, migrateDiffExcludeArgs...)

			result := clirun.Run(c, clirun.Compat, clirun.Options{Dir: root, Env: env}, args...)

			c.Assert(result.ExitCode, qt.Equals, 0, qt.Commentf("stderr:\n%s", result.Stderr))
			c.Assert(migrateDiffWrittenTables(c, root), qt.DeepEquals, test.want)
		})
	}
}

// TestAtlasStrictCompatRefusesTheMigrateDiffExcludeVariable_FailurePathE2E
// refuses the variable on presence, so the empty value is refused too, and
// before the command creates its migration directory.
func TestAtlasStrictCompatRefusesTheMigrateDiffExcludeVariable_FailurePathE2E(t *testing.T) {
	for _, test := range []struct {
		name  string
		value string
	}{
		{name: "a selector", value: "manual"},
		{name: "empty", value: ""},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			root := migrateDiffExcludeRoot(c)
			env := append(environmentWithoutPtahVariables(),
				"PTAH_ATLAS_STRICT_COMPAT=1", migrateDiffExcludeEnvVar+"="+test.value)
			args := append([]string{"migrate", "diff", "add"}, migrateDiffExcludeArgs...)

			result := clirun.Run(c, clirun.Compat, clirun.Options{Dir: root, Env: env}, args...)

			c.Assert(result.ExitCode, qt.Equals, 1)
			c.Assert(result.Stdout, qt.Equals, "")
			c.Assert(result.Stderr, qt.Equals,
				"Error: PTAH_ATLAS_STRICT_COMPAT does not allow PTAH_ATLAS_MIGRATE_DIFF_EXCLUDE\n")
			_, err := os.Stat(filepath.Join(root, "migrations"))
			c.Assert(err, qt.ErrorIs, os.ErrNotExist)
		})
	}
}

// TestAtlasCompatMigrateDiffReadsTheEnvExclude_HappyPathE2E covers the Atlas
// spelling of the selection, which both profiles honor. The pinned community
// binary v1.3.0 reads the same project and writes both tables: it accepts the
// list on this command and leaves the table it names in the file. Strict mode
// keeps the list rather than drop what the author wrote.
func TestAtlasCompatMigrateDiffReadsTheEnvExclude_HappyPathE2E(t *testing.T) {
	for _, test := range []struct {
		name string
		env  []string
	}{
		{name: "full compatibility"},
		{name: "strict Community Edition", env: []string{"PTAH_ATLAS_STRICT_COMPAT=1"}},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			root := migrateDiffExcludeRoot(c)
			env := append(environmentWithoutPtahVariables(), test.env...)

			result := clirun.Run(c, clirun.Compat, clirun.Options{Dir: root, Env: env},
				"migrate", "diff", "add", "--env", "local")

			c.Assert(result.ExitCode, qt.Equals, 0, qt.Commentf("stderr:\n%s", result.Stderr))
			c.Assert(migrateDiffWrittenTables(c, root), qt.DeepEquals, []string{"widgets"})
		})
	}
}

//go:build integration

package integration_test

import (
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/clirun"
)

// PTAH_RUNNER selects the runner a directory is applied, rolled back and
// linted by. The community binary has no runner to select, so strict mode
// refuses the variable by name before any command runs, whatever the command.
// Of the compatibility commands only `migrate down` reads it, and the control
// below proves that it does.

// strictRunnerDirectory writes and hashes an Atlas directory of one file in a
// new project root, and names a SQLite database in that root that nothing has
// created yet.
func strictRunnerDirectory(c *qt.C) (root, database string) {
	c.Helper()
	root = c.TempDir()
	dir := filepath.Join(root, "m")
	c.Assert(os.MkdirAll(dir, 0o750), qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(dir, "1_init.sql"), []byte("CREATE TABLE t1 (id int);\n"), 0o600), qt.IsNil)
	hashed := clirun.Run(c, clirun.Compat, clirun.Options{Dir: root, Env: environmentWithoutPtahVariables()},
		"migrate", "hash", "--dir", "file://m")
	c.Assert(hashed.ExitCode, qt.Equals, 0, qt.Commentf("%s", hashed.Stderr))
	return root, filepath.Join(root, "app.db")
}

// The rollback row carries a value that names no runner: the refusal it gets
// is strict mode's, so the variable was refused before `migrate down` could
// read it.
func TestAtlasStrictCompatRefusesTheRunnerVariable_FailurePathE2E(t *testing.T) {
	for _, test := range []struct {
		name   string
		runner string
		args   []string
	}{
		{name: "apply", runner: "golang-migrate", args: []string{"migrate", "apply", "--dir", "file://m", "--url", "sqlite://app.db"}},
		{name: "lint", runner: "golang-migrate", args: []string{"migrate", "lint", "--dir", "file://m", "--dev-url", "sqlite://app.db", "--latest", "1"}},
		{name: "down", runner: "flyway", args: []string{"migrate", "down", "--dir", "file://m", "--url", "sqlite://app.db"}},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			root, database := strictRunnerDirectory(c)
			env := append(environmentWithoutPtahVariables(), "PTAH_ATLAS_STRICT_COMPAT=1", "PTAH_RUNNER="+test.runner)

			result := clirun.Run(c, clirun.Compat, clirun.Options{Dir: root, Env: env}, test.args...)

			c.Assert(result.ExitCode, qt.Equals, 1)
			c.Assert(result.Stdout, qt.Equals, "")
			c.Assert(result.Stderr, qt.Equals, "Error: PTAH_ATLAS_STRICT_COMPAT does not allow PTAH_RUNNER\n")
			_, err := os.Stat(database)
			c.Assert(err, qt.ErrorIs, os.ErrNotExist)
		})
	}
}

// TestAtlasCompatMigrateDownReadsTheRunnerVariable_FailurePathE2E is the
// control for the refusal above: outside strict mode the same rollback reads
// the variable and refuses the value as `migrations down --runner flyway`
// would, so the strict refusal is not a refusal of a variable nothing reads.
func TestAtlasCompatMigrateDownReadsTheRunnerVariable_FailurePathE2E(t *testing.T) {
	c := qt.New(t)
	root, database := strictRunnerDirectory(c)
	env := append(environmentWithoutPtahVariables(), "PTAH_RUNNER=flyway")

	result := clirun.Run(c, clirun.Compat, clirun.Options{Dir: root, Env: env},
		"migrate", "down", "--dir", "file://m", "--url", "sqlite://app.db")

	c.Assert(result.ExitCode, qt.Equals, 1)
	c.Assert(result.Stdout, qt.Equals, "")
	c.Assert(result.Stderr, qt.Equals, "Error: invalid runner \"flyway\": expected ptah or golang-migrate\n")
	_, err := os.Stat(database)
	c.Assert(err, qt.ErrorIs, os.ErrNotExist)
}

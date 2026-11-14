//go:build integration

package integration_test

import (
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/dbschema"
	"ptah.run/internal/clirun"
)

// PTAH_RUNNER selects the runner the native apply, lint and rollback judge a
// directory by, and `ptah-compat` forwards to those commands with the
// variable's binding in place. The community binary has no runner to select,
// so strict mode refuses the variable by name before a command runs.

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

func TestAtlasStrictCompatRefusesTheRunnerVariable_FailurePathE2E(t *testing.T) {
	for _, test := range []struct {
		name string
		args []string
	}{
		{name: "apply", args: []string{"migrate", "apply", "--dir", "file://m", "--url", "sqlite://app.db"}},
		{name: "lint", args: []string{"migrate", "lint", "--dir", "file://m", "--dev-url", "sqlite://app.db", "--latest", "1"}},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			root, database := strictRunnerDirectory(c)
			env := append(environmentWithoutPtahVariables(), "PTAH_ATLAS_STRICT_COMPAT=1", "PTAH_RUNNER=golang-migrate")

			result := clirun.Run(c, clirun.Compat, clirun.Options{Dir: root, Env: env}, test.args...)

			c.Assert(result.ExitCode, qt.Equals, 1)
			c.Assert(result.Stdout, qt.Equals, "")
			c.Assert(result.Stderr, qt.Equals, "Error: PTAH_ATLAS_STRICT_COMPAT does not allow PTAH_RUNNER\n")
			_, err := os.Stat(database)
			c.Assert(err, qt.ErrorIs, os.ErrNotExist)
		})
	}
}

// The control for the refusal above: outside strict mode the variable is a
// Ptah extension like any other, and the same apply runs and creates the table.
func TestAtlasStrictCompatRefusesTheRunnerVariable_HappyPathE2E(t *testing.T) {
	c := qt.New(t)
	root, database := strictRunnerDirectory(c)
	env := append(environmentWithoutPtahVariables(), "PTAH_RUNNER=golang-migrate")

	result := clirun.Run(c, clirun.Compat, clirun.Options{Dir: root, Env: env},
		"migrate", "apply", "--dir", "file://m", "--url", "sqlite://app.db")

	c.Assert(result.ExitCode, qt.Equals, 0, qt.Commentf("%s%s", result.Stdout, result.Stderr))
	conn, err := dbschema.ConnectToDatabase(c.Context(), "sqlite://"+filepath.ToSlash(database))
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)
	var tables int
	c.Assert(conn.QueryRowContext(c.Context(),
		"SELECT count(*) FROM sqlite_master WHERE type = 'table' AND name = 't1'").Scan(&tables), qt.IsNil)
	c.Assert(tables, qt.Equals, 1)
}

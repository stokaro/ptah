//go:build integration

package integration_test

import (
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/clirun"
)

// sqliteTempTableVersions is a migration that stages rows in a TEMP table.
// The community binary lints it and applies it.
var sqliteTempTableVersions = map[string]string{
	"1_users.sql": "CREATE TABLE users (id int);\n",
	"2_scratch.sql": "CREATE TEMP TABLE scratch (id int);\n" +
		"INSERT INTO scratch SELECT id FROM users;\n" +
		"INSERT INTO users SELECT id + 1 FROM scratch;\n",
}

// sqliteTempTableLintScopes lint the last version, and then both versions, so
// the linter reads the starting state of the TEMP-creating version and of the
// one before it.
var sqliteTempTableLintScopes = []struct {
	name string
	args []string
}{
	{name: "latest 1", args: []string{"--latest", "1"}},
	{name: "latest 2", args: []string{"--latest", "2"}},
}

func sqliteTempTableProject(c *qt.C) string {
	c.Helper()
	dir := c.TempDir()
	migrations := filepath.Join(dir, "migrations")
	c.Assert(os.Mkdir(migrations, 0o750), qt.IsNil)
	for name, body := range sqliteTempTableVersions {
		c.Assert(os.WriteFile(filepath.Join(migrations, name), []byte(body), 0o600), qt.IsNil)
	}
	return dir
}

// TestCompatMigrateLintSQLiteTempTableE2E runs the pinned community binary and
// ptah-compat on a directory whose second version creates a TEMP table, and
// holds the compatibility report to the community one apart from durations.
func TestCompatMigrateLintSQLiteTempTableE2E(t *testing.T) {
	reference := atlasReferenceBinary(t)
	for _, scope := range sqliteTempTableLintScopes {
		t.Run(scope.name, func(t *testing.T) {
			c := qt.New(t)
			dir := sqliteTempTableProject(c)
			hashed := clirun.Run(c, clirun.Compat, clirun.Options{Dir: dir, Env: environmentWithoutPtahVariables()},
				"migrate", "hash", "--dir", "file://migrations")
			c.Assert(hashed.ExitCode, qt.Equals, 0, qt.Commentf("stderr:\n%s", hashed.Stderr))
			args := append([]string{"migrate", "lint", "--dir", "file://migrations", "--dev-url", "sqlite://dev?mode=memory"}, scope.args...)

			communityOut, communityStderr, err := runCLIProcess(c.Context(), dir, reference, args...)
			compat := clirun.Run(c, clirun.Compat, clirun.Options{Dir: dir, Env: environmentWithoutPtahVariables()}, args...)

			c.Assert(exitStatusOf(c, err), qt.Equals, 0, qt.Commentf("community stdout:\n%s\nstderr:\n%s", communityOut, communityStderr))
			c.Assert(compat.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", compat.Stdout, compat.Stderr))
			c.Assert(redactLintE2EDurations(compat.Stdout), qt.Equals, redactLintE2EDurations(communityOut))
			c.Assert(compat.Stderr, qt.Equals, withoutCommunityBuildNotice(communityStderr))
		})
	}
}

// TestMigrationsLintSQLiteTempTableE2E runs `ptah migrations lint` on the same
// directory.
func TestMigrationsLintSQLiteTempTableE2E(t *testing.T) {
	for _, scope := range sqliteTempTableLintScopes {
		t.Run(scope.name, func(t *testing.T) {
			c := qt.New(t)
			dir := sqliteTempTableProject(c)
			args := append([]string{"migrations", "lint", "--dir", "migrations", "--dev-url", "sqlite://dev?mode=memory"}, scope.args...)

			got := clirun.Run(c, clirun.Ptah, clirun.Options{Dir: dir, Env: environmentWithoutPtahVariables()}, args...)

			c.Assert(got.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", got.Stdout, got.Stderr))
		})
	}
}

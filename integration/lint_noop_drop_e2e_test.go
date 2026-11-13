//go:build integration

package integration_test

import (
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/clirun"
)

// noopDropBase is the schema every change below starts from. The linter reads
// it from the in-memory dev database the run replays the directory on, which is
// the only place a drop can learn whether its table or column held anything.
const noopDropBase = "CREATE TABLE users (id INTEGER, name TEXT);\n"

// noopDropChanges are second versions whose drops destroy nothing, and
// controls whose drops destroy what the base holds. Each control moves one
// axis away from a no-op row: the table or the column is in the starting
// state. nativeExit and nativeStdout are what `ptah migrations lint` answers;
// the compatibility surface is held to the community binary instead.
var noopDropChanges = []struct {
	name         string
	change       string
	nativeExit   int
	nativeStdout []string
}{
	{
		name: "IF EXISTS drop of an absent table", change: "DROP TABLE IF EXISTS missing;\n",
		nativeExit: 0, nativeStdout: []string{"No lint findings."},
	},
	{
		name: "a column the file added", change: "ALTER TABLE users ADD COLUMN nick TEXT;\nALTER TABLE users DROP COLUMN nick;\n",
		nativeExit: 0, nativeStdout: []string{"No lint findings."},
	},
	{
		name: "a column of a table the file created", change: "CREATE TABLE t (id INTEGER, x INTEGER);\nALTER TABLE t DROP COLUMN x;\n",
		nativeExit: 0, nativeStdout: []string{"No lint findings."},
	},
	{
		name: "IF NOT EXISTS create of an absent table, then a drop", change: "CREATE TABLE IF NOT EXISTS staging (id INTEGER);\nDROP TABLE staging;\n",
		nativeExit: 0, nativeStdout: []string{"No lint findings."},
	},
	{
		name: "control: IF NOT EXISTS create of a present table, then a drop", change: "CREATE TABLE IF NOT EXISTS users (id INTEGER);\nDROP TABLE users;\n",
		nativeExit: 1, nativeStdout: []string{
			"migrations/2_change.sql:2 [warning] BC103: dropping table users ",
			"migrations/2_change.sql:2 [error] DS101: DROP TABLE permanently deletes table users ",
		},
	},
	{
		name: "control: IF EXISTS drop of a present table", change: "DROP TABLE IF EXISTS users;\n",
		nativeExit: 1, nativeStdout: []string{
			"migrations/2_change.sql:1 [warning] BC103: dropping table users ",
			"migrations/2_change.sql:1 [error] DS101: DROP TABLE permanently deletes table users ",
		},
	},
	{
		name: "control: a column the starting state holds", change: "ALTER TABLE users DROP COLUMN name;\n",
		nativeExit: 1, nativeStdout: []string{
			"migrations/2_change.sql:1 [warning] BC104: dropping a column ",
			"migrations/2_change.sql:1 [error] DS102: DROP COLUMN permanently deletes ",
		},
	},
}

// noopDropProject writes a project whose migrations directory holds the base
// and change as versions 1 and 2, and returns the project directory.
func noopDropProject(c *qt.C, change string) string {
	c.Helper()
	dir := c.TempDir()
	migrations := filepath.Join(dir, "migrations")
	c.Assert(os.Mkdir(migrations, 0o750), qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(migrations, "1_users.sql"), []byte(noopDropBase), 0o600), qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(migrations, "2_change.sql"), []byte(change), 0o600), qt.IsNil)
	return dir
}

// TestCompatMigrateLintDropOfWhatNeverHeldDataE2E runs the pinned community
// binary and ptah-compat on the same directory and holds the compatibility
// report to the community one, apart from elapsed durations. The community
// binary compares the schema a version starts from with the one it ends with,
// so a drop of something the version brought in, or of a table that was never
// there, is no change: no diagnostic, and no schema change in the summary. The
// controls keep the report of a drop that destroys data.
func TestCompatMigrateLintDropOfWhatNeverHeldDataE2E(t *testing.T) {
	reference := atlasReferenceBinary(t)
	for _, test := range noopDropChanges {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			dir := noopDropProject(c, test.change)
			hashed := clirun.Run(c, clirun.Compat, clirun.Options{Dir: dir, Env: environmentWithoutPtahVariables()},
				"migrate", "hash", "--dir", "file://migrations")
			c.Assert(hashed.ExitCode, qt.Equals, 0, qt.Commentf("stderr:\n%s", hashed.Stderr))
			args := []string{"migrate", "lint", "--dir", "file://migrations", "--dev-url", "sqlite://dev?mode=memory", "--latest", "1"}

			communityOut, communityStderr, err := runCLIProcess(c.Context(), dir, reference, args...)
			compat := clirun.Run(c, clirun.Compat, clirun.Options{Dir: dir, Env: environmentWithoutPtahVariables()}, args...)

			c.Assert(compat.ExitCode, qt.Equals, exitStatusOf(c, err), qt.Commentf("stdout:\n%s\nstderr:\n%s", compat.Stdout, compat.Stderr))
			c.Assert(redactLintE2EDurations(compat.Stdout), qt.Equals, redactLintE2EDurations(communityOut))
			c.Assert(compat.Stderr, qt.Equals, withoutCommunityBuildNotice(communityStderr))
		})
	}
}

// TestMigrationsLintDropOfWhatNeverHeldDataE2E runs `ptah migrations lint` on
// the same directories. A no-op drop reports nothing: no data rule, because
// nothing is deleted, and no rollout rule, because no deployed version of the
// application ever read the name. The controls report both.
func TestMigrationsLintDropOfWhatNeverHeldDataE2E(t *testing.T) {
	for _, test := range noopDropChanges {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			dir := noopDropProject(c, test.change)

			got := clirun.Run(c, clirun.Ptah, clirun.Options{Dir: dir, Env: environmentWithoutPtahVariables()},
				"migrations", "lint", "--dir", "migrations", "--dev-url", "sqlite://dev?mode=memory", "--latest", "1")

			c.Assert(got.ExitCode, qt.Equals, test.nativeExit, qt.Commentf("stdout:\n%s\nstderr:\n%s", got.Stdout, got.Stderr))
			for _, line := range test.nativeStdout {
				c.Assert(got.Stdout, qt.Contains, line)
			}
		})
	}
}

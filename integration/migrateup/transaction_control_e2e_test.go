//go:build integration

package migrateup_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/clirun"
	"ptah.run/internal/dbtarget"
)

func TestPostgresCommitInWrappedBodyIsRefusedBeforeExecutionE2E(t *testing.T) {
	c := qt.New(t)
	target := newRollbackTarget(c, dbtarget.PostgreSQL)
	dir := migrationsDir(c, "DROP TABLE committed;",
		"CREATE TABLE committed (id INT PRIMARY KEY); COMMIT; INSERT INTO absent (id) VALUES (1);")

	result := clirun.Run(c, clirun.Ptah, clirun.Options{Dir: c.TempDir()},
		"migrations", "up", "--db-url", target.url, "--migrations-dir", dir, "--json")
	c.Assert(result.ExitCode, qt.Equals, 2, qt.Commentf("stdout:\n%s\nstderr:\n%s", result.Stdout, result.Stderr))
	c.Assert(result.Stdout+result.Stderr, qt.Contains, "controls transaction state")
	c.Assert(target.rowsIn(c, "committed"), qt.Equals, -1)
	c.Assert(target.dirtyRow(c, dir), qt.IsNil)
}

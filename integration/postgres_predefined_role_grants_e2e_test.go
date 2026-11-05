//go:build integration

package integration_test

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"
)

// TestSchemaApplyConvergesAGrantToAPredefinedRoleE2E declares a grant to a role
// PostgreSQL predefines. The first run grants and the next has nothing to do. A
// read that leaves such a grant out never sees it held, and plans it on every
// run. A grant such a role holds and the source leaves out stays, since a source
// cannot declare the role and so does not manage it.
func TestSchemaApplyConvergesAGrantToAPredefinedRoleE2E(t *testing.T) {
	c := qt.New(t)
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Minute)
	t.Cleanup(cancel)
	binaries := buildDiffSkipBinaries(c, ctx, e2eRepoRoot(t))
	const (
		current = "CREATE TABLE t (id integer PRIMARY KEY);\nCREATE TABLE held (id integer PRIMARY KEY);\n" +
			"GRANT SELECT ON held TO pg_monitor;\n"
		heldGrant = "SELECT count(*) FROM information_schema.role_table_grants WHERE table_name = 'held' AND grantee = 'pg_monitor'"
	)
	dir := c.TempDir()
	desired := "CREATE TABLE t (id integer PRIMARY KEY);\nCREATE TABLE held (id integer PRIMARY KEY);\nGRANT SELECT ON t TO pg_monitor;\n"
	c.Assert(os.WriteFile(filepath.Join(dir, "schema.sql"), []byte(desired), 0o600), qt.IsNil)
	url := databaseBuiltFrom(c, current)
	args := []string{"schema", "apply", "--db-url", url, "--schema-file", "schema.sql", "--auto-approve"}

	stdout, stderr, err := runCLIProcess(ctx, dir, binaries.native, args...)

	c.Assert(err, qt.IsNil, qt.Commentf("stdout:\n%s\nstderr:\n%s", stdout, stderr))
	c.Assert(stdout, qt.Contains, "GRANT SELECT ON TABLE \"t\" TO \"pg_monitor\";\n")
	c.Assert(stdout, qt.Not(qt.Contains), "REVOKE")

	stdout, stderr, err = runCLIProcess(ctx, dir, binaries.native, args...)

	c.Assert(err, qt.IsNil, qt.Commentf("stdout:\n%s\nstderr:\n%s", stdout, stderr))
	c.Assert(stdout, qt.Equals, "Schema is synced, no changes to be made.\n")
	c.Assert(countIn(c, url, heldGrant), qt.Equals, 1)
}

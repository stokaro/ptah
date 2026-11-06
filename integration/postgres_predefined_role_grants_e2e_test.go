//go:build integration

package integration_test

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/clirun"
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

// TestSchemaInspectLeavesOutTheDatabaseOwnersOwnRightsE2E reads a stock database.
// PostgreSQL creates public owned by pg_database_owner, with that role's own rights
// in the schema's ACL, and no statement declared them. A read that keeps them makes
// the strict compatibility surface refuse every stock database, and writes a grant
// into every inspected document. The baseline PUBLIC holds stays, as it does for a
// full round trip.
func TestSchemaInspectLeavesOutTheDatabaseOwnersOwnRightsE2E(t *testing.T) {
	c := qt.New(t)
	target := newEmptyReadScopeDatabase(c, "database_owner")

	got := runPtahIn(c, "schema", "inspect", "--db-url", target)

	c.Assert(got.ExitCode, qt.Equals, 0, qt.Commentf("stderr:\n%s", got.Stderr))
	c.Assert(got.Stdout, qt.Not(qt.Contains), "pg_database_owner")
	c.Assert(got.Stdout, qt.Contains, `to = "PUBLIC"`)
}

// TestSchemaInspectReadsAGrantAnotherSchemaGivesTheDatabaseOwnerE2E is the control.
// A schema postgres owns gives the role a privilege, which a statement declared, so
// the read reports it.
func TestSchemaInspectReadsAGrantAnotherSchemaGivesTheDatabaseOwnerE2E(t *testing.T) {
	c := qt.New(t)
	target := newEmptyReadScopeDatabase(c, "database_owner")
	execPostgresFamilySQL(c, c.Context(), target, "CREATE SCHEMA app")
	execPostgresFamilySQL(c, c.Context(), target, "GRANT USAGE ON SCHEMA app TO pg_database_owner")

	got := runPtahIn(c, "schema", "inspect", "--schemas", "app", "--db-url", target)

	c.Assert(got.ExitCode, qt.Equals, 0, qt.Commentf("stderr:\n%s", got.Stderr))
	c.Assert(got.Stdout, qt.Contains, `to = "pg_database_owner"`)
}

// TestStrictCompatTakesAStockDatabaseE2E applies a document that declares what a
// stock database holds, in strict mode. Refused, no strict run on PostgreSQL gets
// past reading its target.
func TestStrictCompatTakesAStockDatabaseE2E(t *testing.T) {
	c := qt.New(t)
	target := newEmptyReadScopeDatabase(c, "database_owner")
	document := filepath.Join(c.TempDir(), "schema.hcl")
	c.Assert(os.WriteFile(document, []byte("schema \"public\" {\n}\n"), 0o600), qt.IsNil)

	got := clirun.Run(c, clirun.Compat, clirun.Options{Dir: c.TempDir(), Env: append(os.Environ(), "PTAH_ATLAS_STRICT_COMPAT=1")},
		"schema", "apply", "-u", target, "--to", "file://"+filepath.ToSlash(document), "--dry-run")

	c.Assert(got.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", got.Stdout, got.Stderr))
	c.Assert(got.Stdout, qt.Contains, "synced")
}

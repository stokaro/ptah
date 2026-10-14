//go:build integration

package integration_test

import (
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"
)

// TestSchemaApplyOfAnInspectedFreshDatabaseReadsNoSystemSchemaE2E inspects a
// fresh PostgreSQL database as HCL and plans the document against the same
// database. The document declares plpgsql, which every PostgreSQL server
// installs in pg_catalog. An installation schema is not where the extension
// lives, so it must not widen the read: a read of pg_catalog or
// information_schema reports the system catalog as objects the document leaves
// out, and the plan drops pg_catalog's indexes and constraints.
func TestSchemaApplyOfAnInspectedFreshDatabaseReadsNoSystemSchemaE2E(t *testing.T) {
	c := qt.New(t)
	target, _ := scratchReplayDatabase(c)
	inspected := inspectHCL(c, target)
	c.Assert(inspected, qt.Contains, `schema = "pg_catalog"`)
	path := filepath.Join(c.TempDir(), "inspected.hcl")
	c.Assert(os.WriteFile(path, []byte(inspected), 0o600), qt.IsNil)

	plan := runPtahNative(c, "schema", "apply", "--db-url", target, "--schema-file", path, "--dry-run")
	c.Assert(plan, qt.Not(qt.Contains), "pg_catalog")
	c.Assert(plan, qt.Not(qt.Contains), "information_schema")
	c.Assert(plan, qt.Contains, "Schema is synced")
}

//go:build integration

package integration_test

import (
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"
)

// TestSchemaApplyChangesAMariaDBColumnCheckE2E covers the server's column
// CHECK level, which cannot be removed with DROP CONSTRAINT. A second apply
// must see the result as synced.
func TestSchemaApplyChangesAMariaDBColumnCheckE2E(t *testing.T) {
	for _, test := range []struct {
		name, current, desired string
		wantChecks             int
	}{
		{name: "remove check", current: "CREATE TABLE t(c int CHECK(c > 0));", desired: "CREATE TABLE t(c int);"},
		{name: "change check", current: "CREATE TABLE t(c int CHECK(c > 0));", desired: "CREATE TABLE t(c int CHECK(c > 1));", wantChecks: 1},
		{name: "replace json with longtext", current: "CREATE TABLE t(c json);", desired: "CREATE TABLE t(c longtext);"},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			scratch := newCheckScratch(c, mariaDBCheckEngine)
			name, target := scratch.builtFrom(c, "column_check", test.current)
			schema := filepath.Join(c.TempDir(), "schema.sql")
			c.Assert(os.WriteFile(schema, []byte(test.desired), 0o600), qt.IsNil)

			plan := runPtahNative(c, "schema", "apply", "--db-url", target, "--schema-file", schema, "--dry-run")
			c.Assert(plan, qt.Contains, "MODIFY COLUMN `c`")
			c.Assert(plan, qt.Not(qt.Contains), "DROP CONSTRAINT")
			runPtahNative(c, "schema", "apply", "--db-url", target, "--schema-file", schema, "--auto-approve")
			again := runPtahNative(c, "schema", "apply", "--db-url", target, "--schema-file", schema, "--dry-run")
			c.Assert(again, qt.Contains, "Schema is synced")
			c.Assert(scratch.checks(c, name), qt.HasLen, test.wantChecks)
		})
	}
}

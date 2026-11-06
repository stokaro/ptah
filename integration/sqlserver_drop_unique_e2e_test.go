//go:build integration

package integration_test

import (
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform"
	"ptah.run/internal/dbtarget"
)

// TestCompatMigrateDiffE2E_DropsAUniqueOverOneColumnOnSQLServer writes a named UNIQUE over
// one column and a file that drops it against a dev database, and runs both directions on
// another. The server's read marks the column unique from the key, so without the key the
// column reads as changed, and that alone refused the plan: `unsupported changes: unique`.
func TestCompatMigrateDiffE2E_DropsAUniqueOverOneColumnOnSQLServer(t *testing.T) {
	c := qt.New(t)
	const (
		first  = "CREATE TABLE t (id int PRIMARY KEY, v int, CONSTRAINT x UNIQUE (v));\n"
		second = "CREATE TABLE t (id int PRIMARY KEY, v int);\n"
		keys   = "SELECT COALESCE(STRING_AGG(name, ','), '') FROM sys.key_constraints WHERE type = 'UQ'"
	)
	newTarget := func() string {
		return createdDevDialectDatabase(c, dbtarget.URL(c, dbtarget.SQLServer),
			"ALTER DATABASE %[1]s SET SINGLE_USER WITH ROLLBACK IMMEDIATE; DROP DATABASE %[1]s", replaceSQLServerDatabase)
	}
	dev := newTarget()
	schema := filepath.Join(c.TempDir(), "second.sql")
	c.Assert(os.WriteFile(schema, []byte(second), 0o600), qt.IsNil)

	pairs := readMigrationPairs(c, diffEngineSteps(c, "", dev, []string{first, second}))
	target := newTarget()

	c.Assert(pairs, qt.HasLen, 2)
	applyEngineFile(c, target, platform.SQLServer, pairs[0].Up)
	c.Assert(heldColumns(c, target, keys), qt.Equals, "x")
	applyEngineFile(c, target, platform.SQLServer, pairs[1].Up)
	c.Assert(heldColumns(c, target, keys), qt.Equals, "")
	next, err := runCompatVerb("schema", "diff", "--from", target, "--to", "file://"+filepath.ToSlash(schema), "--dev-url", dev)
	c.Assert(err, qt.IsNil, qt.Commentf("%s", next))
	c.Assert(next, qt.Contains, "synced")
	applyEngineFile(c, target, platform.SQLServer, pairs[1].Down)
	c.Assert(heldColumns(c, target, keys), qt.Equals, "x")
}

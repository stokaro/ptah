//go:build integration

package integration_test

import (
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"
)

// TestMigrationsLintReadsPastAGoLineFS lints SQL Server migrations that end
// their batches with GO, as sqlcmd scripts and SQL Server tooling write them.
// The lint scanner read GO as a word that opened the next statement, so the
// DROP TABLE after a GO line was not linted at all and the run reported
// nothing. Each row's DROP TABLE users is on line 3.
func TestMigrationsLintReadsPastAGoLineFS(t *testing.T) {
	for _, test := range []struct {
		name string
		sql  string
	}{
		{name: "after a table", sql: "CREATE TABLE a (id int);\nGO\nDROP TABLE users;\nGO\n"},
		{name: "after a statement-bodied trigger", sql: "CREATE TRIGGER trg ON t AFTER INSERT AS SET NOCOUNT ON;\nGO\nDROP TABLE users;\n"},
		{name: "after a separator with a count", sql: "SELECT 1;\nGO 2 -- twice\nDROP TABLE users;\n"},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			dir := c.TempDir()
			c.Assert(os.WriteFile(filepath.Join(dir, "0001_init.up.sql"), []byte(test.sql), 0o600), qt.IsNil)
			c.Assert(os.WriteFile(filepath.Join(dir, "0001_init.down.sql"), []byte("SELECT 1;\n"), 0o600), qt.IsNil)

			out, _ := runPtahNativeWithError("migrations", "lint", "--dir", dir, "--dialect", "sqlserver",
				"--format", "json", "--fail-on", "none")

			c.Assert(lintedFindings(c, out), qt.DeepEquals, []string{"BC103@3 users", "DS101@3 users"})
		})
	}
}

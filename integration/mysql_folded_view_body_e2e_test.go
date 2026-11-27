//go:build integration

package integration_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"
)

// TestSchemaApplyViewNamingItsDatabaseInAnotherCasePlansNothingE2E holds a
// view on a MySQL or MariaDB server started with --lower-case-table-names=1,
// where the server reads a database name in any case as the one it stores in
// lower case. A schema file whose view body names the view's own database in
// upper case is the same query as the body the server holds: applying it plans
// nothing, and a drift check of it finds nothing (stokaro/ptah#6756).
func TestSchemaApplyViewNamingItsDatabaseInAnotherCasePlansNothingE2E(t *testing.T) {
	for _, engine := range foldedServerEngines {
		t.Run(engine.name, func(t *testing.T) {
			c := qt.New(t)
			scratch := newMySQLFamilyScratch(c, engine.server)
			database, url := scratch.database(c, "vb_fold")
			for _, statement := range []string{
				"CREATE TABLE `" + database + "`.t (id int PRIMARY KEY)",
				"CREATE SQL SECURITY INVOKER VIEW `" + database + "`.v AS SELECT id FROM `" + database + "`.t",
			} {
				_, err := scratch.admin.ExecContext(c.Context(), statement)
				c.Assert(err, qt.IsNil, qt.Commentf("statement: %s", statement))
			}
			file := filepath.Join(c.TempDir(), "schema.sql")
			c.Assert(os.WriteFile(file, []byte("CREATE TABLE t (id int PRIMARY KEY);\n"+
				"CREATE SQL SECURITY INVOKER VIEW v AS SELECT id FROM "+strings.ToUpper(database)+".t;\n"), 0o600), qt.IsNil)

			applied := runPtahIn(c, "schema", "apply", "--db-url", url, "--schema-file", file, "--dry-run")
			drift := runPtahIn(c, "schema", "drift", "--db-url", url, "--schema-file", file)

			c.Assert(applied.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", applied.Stdout, applied.Stderr))
			c.Assert(applied.Stdout, qt.Contains, "Schema is synced, no changes to be made.")
			c.Assert(drift.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", drift.Stdout, drift.Stderr))
		})
	}
}

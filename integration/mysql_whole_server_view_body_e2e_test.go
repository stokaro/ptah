//go:build integration

package integration_test

import (
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/atlasurl"
)

// TestSchemaInspectOfTwoDatabasesRoundTripsAViewE2E inspects two databases of
// a whole MySQL or MariaDB server, where a view of one joins a table of the
// other. A read of a whole server keeps each view's database on the body the
// server stores, and so does the file the inspection writes. Applying that
// file back plans nothing, and a drift check of the same file finds nothing.
func TestSchemaInspectOfTwoDatabasesRoundTripsAViewE2E(t *testing.T) {
	for _, engine := range mysqlServerEngines {
		t.Run(engine.name, func(t *testing.T) {
			c := qt.New(t)
			scratch := newMySQLFamilyScratch(c, engine.admin)
			crm, _ := scratch.database(c, "vb_crm")
			app, _ := scratch.database(c, "vb_app")
			for _, statement := range []string{
				"CREATE TABLE `" + crm + "`.accounts (id int PRIMARY KEY, name varchar(20))",
				"CREATE TABLE `" + app + "`.t (id int PRIMARY KEY)",
				"CREATE SQL SECURITY INVOKER VIEW `" + app + "`.vmix AS " +
					"SELECT t.id, a.name FROM `" + app + "`.t t JOIN `" + crm + "`.accounts a ON a.id = t.id",
			} {
				_, err := scratch.admin.ExecContext(c.Context(), statement)
				c.Assert(err, qt.IsNil, qt.Commentf("statement: %s", statement))
			}
			server, err := atlasurl.WithDatabaseName(scratch.adminURL, "")
			c.Assert(err, qt.IsNil)
			schemas := app + "," + crm

			inspected := runPtahIn(c, "schema", "inspect", "--db-url", server, "--schemas", schemas)
			c.Assert(inspected.ExitCode, qt.Equals, 0, qt.Commentf("stderr:\n%s", inspected.Stderr))
			c.Assert(inspected.Stdout, qt.Contains, "`"+app+"`.`t`")
			file := filepath.Join(c.TempDir(), "schema.hcl")
			c.Assert(os.WriteFile(file, []byte(inspected.Stdout), 0o600), qt.IsNil)

			applied := runPtahIn(c, "schema", "apply", "--db-url", server, "--schemas", schemas, "--schema-file", file, "--dry-run")
			drift := runPtahIn(c, "schema", "drift", "--db-url", server, "--schemas", schemas, "--schema-file", file)

			c.Assert(applied.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", applied.Stdout, applied.Stderr))
			c.Assert(applied.Stdout, qt.Contains, "Schema is synced, no changes to be made.")
			c.Assert(drift.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", drift.Stdout, drift.Stderr))
			c.Assert(drift.Stdout, qt.Contains, "No schema drift detected.")
		})
	}
}

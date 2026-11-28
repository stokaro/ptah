//go:build integration

package excludedschema_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/atlasurl"
	"ptah.run/internal/clirun"
	"ptah.run/internal/dbtarget"
)

// A foreign key into a table the run excludes names a table the server holds
// and the plan leaves alone. The first apply adds the key, the second plans
// nothing, and a declaration that leaves the key out plans nothing either: the
// key and the index it stands on stay. The excluded table keeps its row
// throughout (stokaro/ptah#6773).
func TestMySQLFamilyKeyIntoAnExcludedTableConvergesE2E(t *testing.T) {
	tests := []struct{ name, reference string }{
		{name: "qualified", reference: "`<database>`.t"},
		{name: "unqualified", reference: "t"},
	}
	for _, engine := range mysqlServerEngines {
		for _, test := range tests {
			t.Run(engine.name+"/"+test.name, func(t *testing.T) {
				c := qt.New(t)
				admin, database, _ := emptyDatabase(c, engine.admin)
				for _, statement := range []string{
					"CREATE TABLE `" + database + "`.t (id INT NOT NULL PRIMARY KEY)",
					"INSERT INTO `" + database + "`.t VALUES (1)",
				} {
					_, err := admin.ExecContext(c.Context(), statement)
					c.Assert(err, qt.IsNil)
				}
				single, err := atlasurl.WithDatabaseName(dbtarget.URL(c, engine.admin), database)
				c.Assert(err, qt.IsNil)
				root := c.TempDir()
				declared := writeFile(c, root, "declared.sql", database,
					"CREATE TABLE w (id INT NOT NULL PRIMARY KEY, t_id INT, CONSTRAINT w_t_fk FOREIGN KEY (t_id) REFERENCES "+
						test.reference+" (id));\n")
				undeclared := writeFile(c, root, "undeclared.sql", database, "CREATE TABLE w (id INT NOT NULL PRIMARY KEY, t_id INT);\n")
				apply := func(schema, mode string) clirun.Result {
					return clirun.Run(c, clirun.Ptah, clirun.Options{Dir: root},
						"schema", "apply", "--db-url", single, "--schema-file", schema, "--exclude", "t", mode)
				}

				first := apply(declared, "--auto-approve")
				second := apply(declared, "--auto-approve")
				left := apply(undeclared, "--dry-run")

				c.Assert(first.ExitCode, qt.Equals, 0, qt.Commentf("%s\n%s", first.Stdout, first.Stderr))
				c.Assert(first.Stdout, qt.Contains, "`w_t_fk`")
				c.Assert(second.ExitCode, qt.Equals, 0, qt.Commentf("%s\n%s", second.Stdout, second.Stderr))
				c.Assert(second.Stdout, qt.Contains, "no changes")
				c.Assert(left.ExitCode, qt.Equals, 0, qt.Commentf("%s\n%s", left.Stdout, left.Stderr))
				c.Assert(left.Stdout, qt.Contains, "no changes")
				var keys, rows int
				c.Assert(admin.QueryRowContext(c.Context(),
					"SELECT COUNT(*) FROM information_schema.REFERENTIAL_CONSTRAINTS WHERE CONSTRAINT_SCHEMA = ? AND TABLE_NAME = 'w' "+
						"AND REFERENCED_TABLE_NAME = 't'", database,
				).Scan(&keys), qt.IsNil)
				c.Assert(admin.QueryRowContext(c.Context(), "SELECT COUNT(*) FROM `"+database+"`.t").Scan(&rows), qt.IsNil)
				c.Assert(keys, qt.Equals, 1)
				c.Assert(rows, qt.Equals, 1)
			})
		}
	}
}

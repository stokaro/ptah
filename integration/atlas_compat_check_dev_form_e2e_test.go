//go:build integration

package integration_test

import (
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/dbschema"
	"ptah.run/internal/dbtarget"
)

// The pinned community binary plans from the desired schema as the dev
// database holds it, so on MySQL and MariaDB a CHECK it adds reads as the
// server prints it: each column name quoted, and on MySQL each string
// introduced with its character set and the whole clause in parentheses the
// plan leaves out. ptah-compat creates the declared tables on the dev database
// and writes each CHECK it adds in that form. The expected text is what the
// server reports for a table built from the declaration over the Go driver,
// and the plan, run on the table it alters, has to leave the server holding
// what the declaration left it holding.

// checkDevFormFrom is the table before its CHECKs are added.
const checkDevFormFrom = `CREATE TABLE t (id int PRIMARY KEY, c varchar(20), qty int);`

// checkDevFormTo is the table with a CHECK of each shape the servers rewrite:
// a comparison, a string holding a quote, and two conditions joined by AND.
const checkDevFormTo = `CREATE TABLE t (id int PRIMARY KEY, c varchar(20), qty int,
	CONSTRAINT k_plain CHECK (qty > 0),
	CONSTRAINT k_quote CHECK (c <> 'it''s'),
	CONSTRAINT k_and CHECK (id between 1 and 10 and c is not null));`

// checkDevFormServers are the engines the rows run on, with how the plan
// writes a CHECK clause the reader yields: MySQL's clause carries the
// parentheses the plan's CHECK keyword supplies, and MariaDB's does not.
var checkDevFormServers = []struct {
	name   string
	engine dbtarget.Engine
	form   string
}{
	{name: "mysql", engine: dbtarget.MySQLAdmin, form: "CONSTRAINT `%s` CHECK %s"},
	{name: "mariadb", engine: dbtarget.MariaDBAdmin, form: "CONSTRAINT `%s` CHECK (%s)"},
}

// TestCompatSchemaDiffWritesACheckAsTheDevDatabaseReportsItE2E plans the
// CHECKs added between two schema files.
func TestCompatSchemaDiffWritesACheckAsTheDevDatabaseReportsItE2E(t *testing.T) {
	for _, server := range checkDevFormServers {
		t.Run(server.name, func(t *testing.T) {
			c := qt.New(t)
			scratch := newMySQLFamilyScratch(c, server.engine)
			devName, dev := scratch.database(c, "ck_dev")
			want := scratch.checkDevForms(c, server.form)
			root := c.TempDir()
			from := writeCheckDevFormFile(c, root, "from.sql", checkDevFormFrom)
			to := writeCheckDevFormFile(c, root, "to.sql", checkDevFormTo)

			plan, err := runCompatVerb("schema", "diff", "--from", "file://"+from, "--to", "file://"+to, "--dev-url", dev)

			c.Assert(err, qt.IsNil, qt.Commentf("%s", plan))
			for _, check := range want {
				c.Assert(plan, qt.Contains, check)
			}
			c.Assert(scratch.tableCount(c, devName), qt.Equals, 0)
			scratch.assertPlanHoldsTheDeclaredChecks(c, plan)
		})
	}
}

// TestCompatMigrateDiffWritesACheckAsTheDevDatabaseReportsItE2E plans the
// CHECKs a schema file adds to a migration directory.
func TestCompatMigrateDiffWritesACheckAsTheDevDatabaseReportsItE2E(t *testing.T) {
	for _, server := range checkDevFormServers {
		t.Run(server.name, func(t *testing.T) {
			c := qt.New(t)
			scratch := newMySQLFamilyScratch(c, server.engine)
			devName, dev := scratch.database(c, "ck_dev")
			want := scratch.checkDevForms(c, server.form)
			dir, schema := writeRewrittenMigrationProject(c, checkDevFormFrom, checkDevFormTo)

			plan, err := runCompatVerb("migrate", "diff", "--dir", "file://"+dir, "--to", "file://"+schema,
				"--dev-url", dev, "--dry-run")

			c.Assert(err, qt.IsNil, qt.Commentf("%s", plan))
			for _, check := range want {
				c.Assert(plan, qt.Contains, check)
			}
			c.Assert(scratch.tableCount(c, devName), qt.Equals, 0)
			scratch.assertPlanHoldsTheDeclaredChecks(c, plan)
		})
	}
}

// TestCompatMigrateDiffReportsAnOmittedChangeOnceE2E plans a directory whose
// plan writes a CHECK, so the directory is replayed once to find that out and
// once more to plan the files with the dev database's spelling. A change the
// diff policy leaves out is reported once all the same.
func TestCompatMigrateDiffReportsAnOmittedChangeOnceE2E(t *testing.T) {
	for _, server := range checkDevFormServers {
		t.Run(server.name, func(t *testing.T) {
			c := qt.New(t)
			scratch := newMySQLFamilyScratch(c, server.engine)
			_, dev := scratch.database(c, "ck_dev")
			want := scratch.checkDevForms(c, server.form)
			dir, schema := writeRewrittenMigrationProject(c,
				checkDevFormFrom+"\nCREATE TABLE old_t (id int PRIMARY KEY);", checkDevFormTo)
			config := writeCheckDevFormFile(c, filepath.Dir(dir), "atlas.hcl", fmt.Sprintf(`env "local" {
  dev = %q
  schema {
    src = %q
  }
  migration {
    dir = %q
  }
  diff {
    skip {
      drop_table = true
    }
  }
}`, dev, "file://"+filepath.ToSlash(schema), "file://"+filepath.ToSlash(dir)))

			plan, err := runCompatVerb("migrate", "diff", "--config", "file://"+filepath.ToSlash(config),
				"--env", "local", "--dry-run")

			c.Assert(err, qt.IsNil, qt.Commentf("%s", plan))
			c.Assert(strings.Count(plan, "SKIP: DROP TABLE of old_t omitted by diff policy (skip: drop_table)\n"),
				qt.Equals, 1, qt.Commentf("%s", plan))
			for _, check := range want {
				c.Assert(plan, qt.Contains, check)
			}
		})
	}
}

// TestCompatSchemaDiffReportsARefusedCheckByItsDeclaredNameE2E plans a CHECK
// the dev server refuses, as it refuses it to the pinned community binary,
// which creates the same constraint there. The error names the CHECK as the
// declaration does, with its table and the server's message, which names the
// declared constraint too, and the dev database is left empty.
func TestCompatSchemaDiffReportsARefusedCheckByItsDeclaredNameE2E(t *testing.T) {
	tests := []struct {
		name   string
		engine dbtarget.Engine
		want   string
	}{
		{
			name:   "mysql",
			engine: dbtarget.MySQLAdmin,
			want: `Error: read CHECK expressions on dev database: the dev database refuses CHECK "qty_ck" of table "t": ` +
				"Error 3814 (HY000): An expression of a check constraint 'qty_ck' contains disallowed function: `nosuchfn`.\n",
		},
		{
			name:   "mariadb",
			engine: dbtarget.MariaDBAdmin,
			want: `Error: read CHECK expressions on dev database: the dev database refuses CHECK "qty_ck" of table "t": ` +
				"Error 1901 (HY000): Function or expression '`nosuchfn`()' cannot be used in the CHECK clause of `qty_ck`\n",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			scratch := newMySQLFamilyScratch(c, test.engine)
			devName, dev := scratch.database(c, "ck_dev")
			root := c.TempDir()
			from := writeCheckDevFormFile(c, root, "from.sql", checkDevFormFrom)
			to := writeCheckDevFormFile(c, root, "to.sql",
				"CREATE TABLE t (id int PRIMARY KEY, c varchar(20), qty int, CONSTRAINT qty_ck CHECK (nosuchfn(qty) > 0));")

			out, err := runCompatVerb("schema", "diff", "--from", "file://"+from, "--to", "file://"+to, "--dev-url", dev)

			c.Assert(err, qt.IsNotNil, qt.Commentf("%s", out))
			c.Assert(out, qt.Equals, test.want)
			c.Assert(scratch.tableCount(c, devName), qt.Equals, 0)
		})
	}
}

// checkDevForms builds [checkDevFormTo] in a database of its own and answers
// each CHECK the server reports there, read through the reader every command
// uses, written in form with the constraint's name, in name order.
func (s mysqlScratch) checkDevForms(c *qt.C, form string) []string {
	c.Helper()
	_, url := s.builtFrom(c, checkDevFormTo)
	conn, err := dbschema.ConnectToDatabase(c.Context(), url)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)
	read, err := conn.Reader().ReadSchemaContext(c.Context())
	c.Assert(err, qt.IsNil)
	checks := slices.DeleteFunc(slices.Clone(read.Constraints), withoutCheckClause)
	forms := make([]string, 0, len(checks))
	for _, check := range checks {
		forms = append(forms, fmt.Sprintf(form, check.Name, *check.CheckClause))
	}
	slices.Sort(forms)
	c.Assert(forms, qt.HasLen, 3)
	return forms
}

// assertPlanHoldsTheDeclaredChecks runs plan on a database built from
// [checkDevFormFrom] and holds the server to the CHECK clauses it reports for
// a database built from [checkDevFormTo].
func (s mysqlScratch) assertPlanHoldsTheDeclaredChecks(c *qt.C, plan string) {
	c.Helper()
	declared, _ := s.builtFrom(c, checkDevFormTo)
	planned, _ := s.builtFrom(c, checkDevFormFrom)
	conn, err := sql.Open("mysql", mySQLDSNForDatabase(c, s.adminDSN, planned)+"?multiStatements=true")
	c.Assert(err, qt.IsNil)
	defer func() { c.Check(conn.Close(), qt.IsNil) }()

	_, err = conn.ExecContext(c.Context(), plan)

	c.Assert(err, qt.IsNil)
	c.Assert(s.expressionsOf(c, checkClausesRead, planned), qt.Equals, s.expressionsOf(c, checkClausesRead, declared))
}

// tableCount answers how many tables database holds.
func (s mysqlScratch) tableCount(c *qt.C, database string) int {
	c.Helper()
	var count int
	err := s.admin.QueryRowContext(c.Context(),
		"SELECT COUNT(*) FROM information_schema.TABLES WHERE TABLE_SCHEMA = ?", database,
	).Scan(&count)
	c.Assert(err, qt.IsNil)
	return count
}

// writeCheckDevFormFile writes a schema file under root.
func writeCheckDevFormFile(c *qt.C, root, name, schema string) string {
	c.Helper()
	path := filepath.Join(root, name)
	c.Assert(os.WriteFile(path, []byte(schema+"\n"), 0o600), qt.IsNil) // #nosec G703 -- the path is c.TempDir() plus a constant name
	return path
}

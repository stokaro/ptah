//go:build integration

package integration_test

import (
	"encoding/json"
	"fmt"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/clirun"
	"ptah.run/internal/dbtarget"
)

// collatedMySQLTables is a MySQL database whose default is utf8mb4_unicode_ci,
// holding a table that takes it, one that takes another collation of the same
// character set, and one that takes another character set.
const collatedMySQLTables = "ALTER DATABASE CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;\n" +
	"CREATE TABLE same_default (id int PRIMARY KEY, name varchar(10));\n" +
	"CREATE TABLE other_coll (id int PRIMARY KEY, name varchar(10)) COLLATE utf8mb4_bin;\n" +
	"CREATE TABLE other_charset (id int PRIMARY KEY, name varchar(10)) CHARACTER SET latin1;\n"

// mySQLFamilyEngines are the servers a MySQL read is measured on.
var mySQLFamilyEngines = []dbtarget.Engine{dbtarget.MySQLAdmin, dbtarget.MariaDBAdmin}

// mySQLFamilyDialects are the dialects ptah renders for each of
// mySQLFamilyEngines.
var mySQLFamilyDialects = map[dbtarget.Engine]string{dbtarget.MySQLAdmin: "mysql", dbtarget.MariaDBAdmin: "mariadb"}

// inspectCollatedMySQLTables builds collatedMySQLTables in a new database of
// scratch and runs the compat binary's schema inspect on it in format. It
// returns the result and the database's name.
func inspectCollatedMySQLTables(c *qt.C, scratch mysqlScratch, format string) (clirun.Result, string) {
	c.Helper()
	name, url := scratch.builtFrom(c, collatedMySQLTables)
	args := []string{"schema", "inspect", "--url", url}
	if format != "" {
		args = append(args, "--format", format)
	}
	got := runFormatTemplateVerb(c, clirun.Build(c, clirun.Compat), c.TempDir(), nil, args...)
	c.Assert(got.ExitCode, qt.Equals, 0, qt.Commentf("stderr:\n%s", got.Stderr))
	c.Assert(got.Stderr, qt.Equals, "")
	return got, name
}

// tableCollationsOf maps each table of database to its character set and
// collation, as `charset collation`, the way the server reports them.
func tableCollationsOf(c *qt.C, scratch mysqlScratch, database string) map[string]string {
	c.Helper()
	rows, err := scratch.admin.QueryContext(c.Context(), `SELECT t.TABLE_NAME, cc.CHARACTER_SET_NAME, t.TABLE_COLLATION
		FROM information_schema.TABLES t
		JOIN information_schema.COLLATION_CHARACTER_SET_APPLICABILITY cc ON cc.COLLATION_NAME = t.TABLE_COLLATION
		WHERE t.TABLE_SCHEMA = ?`, database)
	c.Assert(err, qt.IsNil)
	defer func() { c.Check(rows.Close(), qt.IsNil) }()
	collations := make(map[string]string)
	for rows.Next() {
		var table, charset, collation string
		c.Assert(rows.Scan(&table, &charset, &collation), qt.IsNil)
		collations[table] = charset + " " + collation
	}
	c.Assert(rows.Err(), qt.IsNil)
	return collations
}

// The HCL document writes a table's charset and collate where they differ
// from its schema block's and leaves them to the schema block where they do
// not, as the pinned community binary v1.3.0 writes them on MySQL 8.4.
func TestCompatSchemaInspectHCLWritesAMySQLTableCollationE2E(t *testing.T) {
	for _, engine := range mySQLFamilyEngines {
		t.Run(engine.String(), func(t *testing.T) {
			c := qt.New(t)

			got, name := inspectCollatedMySQLTables(c, newMySQLFamilyScratch(c, engine), "")

			c.Assert(got.Stdout, qt.Contains, fmt.Sprintf("table \"other_charset\" {\n  schema = schema.%s\n"+
				"  charset = \"latin1\"\n  collate = \"latin1_swedish_ci\"\n  column \"id\" {\n", name))
			c.Assert(got.Stdout, qt.Contains, fmt.Sprintf("table \"other_coll\" {\n  schema = schema.%s\n"+
				"  collate = \"utf8mb4_bin\"\n  column \"id\" {\n", name))
			c.Assert(got.Stdout, qt.Contains, fmt.Sprintf("table \"same_default\" {\n  schema = schema.%s\n"+
				"  column \"id\" {\n", name))
			c.Assert(got.Stdout, qt.Contains, fmt.Sprintf("schema %q {\n  charset = \"utf8mb4\"\n"+
				"  collate = \"utf8mb4_unicode_ci\"\n}\n", name))
		})
	}
}

// The JSON document lists every table's charset and collation as the server
// reports them, a table that takes its schema's included.
func TestCompatSchemaInspectJSONListsAMySQLTableCollationE2E(t *testing.T) {
	for _, engine := range mySQLFamilyEngines {
		t.Run(engine.String(), func(t *testing.T) {
			c := qt.New(t)
			scratch := newMySQLFamilyScratch(c, engine)

			got, name := inspectCollatedMySQLTables(c, scratch, "{{ json . }}")

			var document struct {
				Schemas []struct {
					Tables []struct {
						Name    string `json:"name"`
						Charset string `json:"charset"`
						Collate string `json:"collate"`
					} `json:"tables"`
				} `json:"schemas"`
			}
			c.Assert(json.Unmarshal([]byte(got.Stdout), &document), qt.IsNil)
			c.Assert(document.Schemas, qt.HasLen, 1)
			listed := make(map[string]string)
			for _, table := range document.Schemas[0].Tables {
				listed[table.Name] = table.Charset + " " + table.Collate
			}
			c.Assert(listed, qt.DeepEquals, tableCollationsOf(c, scratch, name))
		})
	}
}

// The SQL inspect writes, replayed on a new database of the same server,
// creates every table with the charset and collation the inspected one has,
// a table that took its database's default included: the new database takes
// the server's default, which is not utf8mb4_unicode_ci on either engine.
func TestCompatSchemaInspectSQLReplaysAMySQLTableCollationE2E(t *testing.T) {
	for _, engine := range mySQLFamilyEngines {
		t.Run(engine.String(), func(t *testing.T) {
			c := qt.New(t)
			scratch := newMySQLFamilyScratch(c, engine)
			inspected, source := inspectCollatedMySQLTables(c, scratch, "{{ sql . }}")

			replayed, _ := scratch.builtFrom(c, inspected.Stdout)

			c.Assert(tableCollationsOf(c, scratch, replayed), qt.DeepEquals, tableCollationsOf(c, scratch, source))
		})
	}
}

// The Go models introspect writes, rendered for the same engine and replayed
// on a new database of it, create every table with the charset and collation
// the introspected one has: the read binds a table's character set to both
// MySQL-family targets, which the export writes as a platform property of
// each, and the collation is the table directive's collate.
func TestIntrospectKeepsAMySQLTableCollationE2E(t *testing.T) {
	for _, engine := range mySQLFamilyEngines {
		t.Run(engine.String(), func(t *testing.T) {
			c := qt.New(t)
			scratch := newMySQLFamilyScratch(c, engine)
			source, url := scratch.builtFrom(c, collatedMySQLTables)
			models := filepath.Join(c.TempDir(), "models")
			runPtahNative(c, "introspect", "--db-url", url, "--out", models)
			rendered, notes, err := runPtahSplitStreams(c.Context(),
				[]string{"schema", "render", "--root-dir", models, "--dialect", mySQLFamilyDialects[engine]})
			c.Assert(err, qt.IsNil, qt.Commentf("%s", notes))

			replayed, _ := scratch.builtFrom(c, rendered)

			c.Assert(tableCollationsOf(c, scratch, replayed), qt.DeepEquals, tableCollationsOf(c, scratch, source))
		})
	}
}

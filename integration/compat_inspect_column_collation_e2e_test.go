//go:build integration

package integration_test

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/clirun"
	"ptah.run/internal/dbtarget"
)

// inheritedCollationMySQLTables are two MySQL tables whose character columns
// mostly take the table's collation: t's is not the database default, u's is
// another one, b names a collation of its own and e a character set.
const inheritedCollationMySQLTables = "CREATE TABLE t (a varchar(5), b varchar(5) COLLATE utf8mb4_unicode_ci, " +
	"c varchar(5) COLLATE utf8mb4_bin, d int, e varchar(5) CHARACTER SET latin1) " +
	"DEFAULT CHARSET utf8mb4 COLLATE utf8mb4_bin;\n" +
	"CREATE TABLE u (a varchar(5)) DEFAULT CHARSET utf8mb4 COLLATE utf8mb4_unicode_ci;\n"

// inspectInheritedCollationMySQL builds inheritedCollationMySQLTables in a
// new MySQL database and runs schema inspect on it with the compat binary and
// format. It returns the result and the database's name.
func inspectInheritedCollationMySQL(c *qt.C, format string) (clirun.Result, string) {
	c.Helper()
	scratch := newMySQLFamilyScratch(c, dbtarget.MySQLAdmin)
	name, url := scratch.builtFrom(c, inheritedCollationMySQLTables)
	return runFormatTemplateVerb(c, clirun.Build(c, clirun.Compat), c.TempDir(), nil,
		"schema", "inspect", "--format", format, "--url", url), name
}

// The SQL a MySQL read renders names, beside each table's own collation, the
// collation every character column compares with, inherited or not, so a
// file replayed on a server with another default builds columns that compare
// as the inspected ones do.
func TestCompatSchemaInspectSQLNamesAnInheritedMySQLCollationE2E(t *testing.T) {
	c := qt.New(t)

	got, _ := inspectInheritedCollationMySQL(c, "{{ sql . }}")

	c.Assert(got.ExitCode, qt.Equals, 0, qt.Commentf("stderr:\n%s", got.Stderr))
	c.Assert(got.Stdout, qt.Equals, "-- Create \"t\" table\n"+
		"CREATE TABLE `t` (`a` varchar(5) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NULL, "+
		"`b` varchar(5) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NULL, "+
		"`c` varchar(5) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NULL, `d` int NULL, "+
		"`e` varchar(5) CHARACTER SET latin1 COLLATE latin1_swedish_ci NULL) CHARSET=utf8mb4 COLLATE=utf8mb4_bin;\n"+
		"-- Create \"u\" table\n"+
		"CREATE TABLE `u` (`a` varchar(5) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NULL) "+
		"CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;\n")
}

// The JSON document names every character column's collation, as the pinned
// community binary's does.
func TestCompatSchemaInspectJSONNamesAnInheritedMySQLCollationE2E(t *testing.T) {
	c := qt.New(t)

	got, name := inspectInheritedCollationMySQL(c, "{{ json . }}")

	c.Assert(got.ExitCode, qt.Equals, 0, qt.Commentf("stderr:\n%s", got.Stderr))
	c.Assert(got.Stdout, qt.Equals, fmt.Sprintf(`{"schemas":[{"name":%q,"tables":[`+
		`{"name":"t","columns":[`+
		`{"name":"a","type":"varchar(5)","null":true,"charset":"utf8mb4","collate":"utf8mb4_bin"},`+
		`{"name":"b","type":"varchar(5)","null":true,"charset":"utf8mb4","collate":"utf8mb4_unicode_ci"},`+
		`{"name":"c","type":"varchar(5)","null":true,"charset":"utf8mb4","collate":"utf8mb4_bin"},`+
		`{"name":"d","type":"int","null":true},`+
		`{"name":"e","type":"varchar(5)","null":true,"charset":"latin1","collate":"latin1_swedish_ci"}],`+
		`"charset":"utf8mb4","collate":"utf8mb4_bin"},`+
		`{"name":"u","columns":[`+
		`{"name":"a","type":"varchar(5)","null":true,"charset":"utf8mb4","collate":"utf8mb4_unicode_ci"}],`+
		`"charset":"utf8mb4","collate":"utf8mb4_unicode_ci"}],`+
		`"charset":"utf8mb4","collate":"utf8mb4_0900_ai_ci"}]}`, name))
}

// The JSON document names the collation a PostgreSQL column compares with as
// information_schema.columns does: a domain's for a column that inherits it,
// C for name, and none for the database default, which a column of a C
// domain takes with COLLATE "default".
func TestCompatSchemaInspectJSONNamesAnInheritedPostgresCollationE2E(t *testing.T) {
	c := qt.New(t)
	target := postgresScratchDevURL(c, "ptah_inherited_collation")
	execPostgresFamilySQL(c, c.Context(), target, `CREATE DOMAIN dc AS text COLLATE "C";
CREATE TABLE t (plain text, declared text COLLATE "C", posix varchar(5) COLLATE "POSIX", dom dc,
	dom_default dc COLLATE "default", dom_posix dc COLLATE "POSIX", nm name, n int);`)

	got := runFormatTemplateVerb(c, clirun.Build(c, clirun.Compat), c.TempDir(), nil,
		"schema", "inspect", "--format", "{{ json . }}", "--url", pinnedDevURL(c, target, "public"))

	c.Assert(got.ExitCode, qt.Equals, 0, qt.Commentf("stderr:\n%s", got.Stderr))
	c.Assert(got.Stdout, qt.Equals, `{"schemas":[{"name":"public","tables":[{"name":"t","columns":[`+
		`{"name":"plain","type":"text","null":true},`+
		`{"name":"declared","type":"text","null":true,"collate":"C"},`+
		`{"name":"posix","type":"character varying","null":true,"collate":"POSIX"},`+
		`{"name":"dom","type":"dc","null":true,"collate":"C"},`+
		`{"name":"dom_default","type":"dc","null":true},`+
		`{"name":"dom_posix","type":"dc","null":true,"collate":"POSIX"},`+
		`{"name":"nm","type":"name","null":true,"collate":"C"},`+
		`{"name":"n","type":"integer","null":true}]}],"comment":"standard public schema"}]}`)
}

// columnCollationsOf maps each character column of database's tables, as
// `table.column`, to the collation the server reports for it.
func columnCollationsOf(c *qt.C, scratch mysqlScratch, database string) map[string]string {
	c.Helper()
	rows, err := scratch.admin.QueryContext(c.Context(), `SELECT TABLE_NAME, COLUMN_NAME, COLLATION_NAME
		FROM information_schema.COLUMNS WHERE TABLE_SCHEMA = ? AND COLLATION_NAME IS NOT NULL`, database)
	c.Assert(err, qt.IsNil)
	defer func() { c.Check(rows.Close(), qt.IsNil) }()
	collations := make(map[string]string)
	for rows.Next() {
		var table, column, collation string
		c.Assert(rows.Scan(&table, &column, &collation), qt.IsNil)
		collations[table+"."+column] = collation
	}
	c.Assert(rows.Err(), qt.IsNil)
	return collations
}

// The SQL inspect writes, replayed on a new database of the same server,
// builds columns that compare as the inspected ones do. The new database
// takes the server's default collation, which is neither table's.
func TestCompatSchemaInspectSQLReplaysTheInheritedMySQLCollationsE2E(t *testing.T) {
	c := qt.New(t)
	scratch := newMySQLFamilyScratch(c, dbtarget.MySQLAdmin)
	source, url := scratch.builtFrom(c, inheritedCollationMySQLTables)
	inspected := runFormatTemplateVerb(c, clirun.Build(c, clirun.Compat), c.TempDir(), nil,
		"schema", "inspect", "--format", "{{ sql . }}", "--url", url)
	c.Assert(inspected.ExitCode, qt.Equals, 0, qt.Commentf("stderr:\n%s", inspected.Stderr))

	replayed, _ := scratch.builtFrom(c, inspected.Stdout)

	c.Assert(columnCollationsOf(c, scratch, replayed), qt.DeepEquals, columnCollationsOf(c, scratch, source))
}

// A MySQL NCHAR is a CHAR of the national character set, utf8mb3. In a table
// whose collation is utf8mb3_general_ci the column inherits it, and the
// comparison reads the collation it inherits to know the CHAR the server
// reports is the NCHAR the document declares.
func TestNativeSchemaCompareFindsAnNCharInAUtf8mb3TableUnchangedE2E(t *testing.T) {
	const table = "CREATE TABLE t (id int PRIMARY KEY, c NCHAR(5)) DEFAULT CHARSET=utf8mb3 COLLATE=utf8mb3_general_ci;\n"
	for _, engine := range []dbtarget.Engine{dbtarget.MySQLAdmin, dbtarget.MariaDBAdmin} {
		t.Run(engine.String(), func(t *testing.T) {
			c := qt.New(t)
			scratch := newMySQLFamilyScratch(c, engine)
			_, url := scratch.builtFrom(c, table)
			dir := c.TempDir()
			c.Assert(os.WriteFile(filepath.Join(dir, "schema.sql"), []byte(table), 0o600), qt.IsNil)

			got := runFormatTemplateVerb(c, clirun.Build(c, clirun.Ptah), dir, nil,
				"schema", "compare", "--db-url", url, "--schema-file", "schema.sql", "--exit-code")

			c.Assert(got.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", got.Stdout, got.Stderr))
			c.Assert(got.Stdout, qt.Contains, "No schema differences detected.")
		})
	}
}

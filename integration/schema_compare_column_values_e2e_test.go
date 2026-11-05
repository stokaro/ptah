//go:build integration

package integration_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform"
	"ptah.run/internal/dbtarget"
)

// compareValuesCases are columns that change on each engine. The old value comes
// from the comparator's read of the server, so it is the server's spelling of it.
var compareValuesCases = []struct {
	name    string
	dialect string
	target  func(c *qt.C) string
	current string
	desired string
	want    string
}{
	{
		name: "postgres", dialect: platform.Postgres,
		target: func(c *qt.C) string {
			return pinnedDevURL(c, postgresScratchDevURL(c, "ptah_compare_values"), "public")
		},
		current: "CREATE TABLE t (id bigint PRIMARY KEY, k bigint, n text NOT NULL, d text DEFAULT 'a', gone int);\n",
		desired: "CREATE TABLE t (id bigint PRIMARY KEY, k int, n text, d text DEFAULT 'b', added int);\n",
		want:    "Differences detected (1 category):\n  tables_modified (1): t\n    t.d: default_expr: 'a'::text -> 'b'\n    t.k: type: int8 -> int\n    t.n: nullable: false -> true\n\n",
	},
	{
		name: "mysql", dialect: platform.MySQL,
		target: func(c *qt.C) string {
			_, url := newMySQLFamilyScratch(c, dbtarget.MySQLAdmin).database(c, "compare_values")
			return url
		},
		current: "CREATE TABLE t (id bigint PRIMARY KEY, k bigint, n varchar(20) NOT NULL, gone int);\n",
		desired: "CREATE TABLE t (id bigint PRIMARY KEY, k int, n varchar(20), added int);\n",
		want:    "Differences detected (1 category):\n  tables_modified (1): t\n    t.k: type: bigint -> int\n    t.n: nullable: false -> true\n\n",
	},
	{
		name: "mariadb", dialect: platform.MariaDB,
		target: func(c *qt.C) string {
			_, url := newMySQLFamilyScratch(c, dbtarget.MariaDBAdmin).database(c, "compare_values")
			return url
		},
		current: "CREATE TABLE t (id bigint PRIMARY KEY, k bigint, n varchar(20) NOT NULL, gone int);\n",
		desired: "CREATE TABLE t (id bigint PRIMARY KEY, k int, n varchar(20), added int);\n",
		want:    "Differences detected (1 category):\n  tables_modified (1): t\n    t.k: type: bigint(20) -> int\n    t.n: nullable: false -> true\n\n",
	},
	{
		name: "sqlite", dialect: platform.SQLite,
		target:  sqliteScratchURL,
		current: "CREATE TABLE t (id integer PRIMARY KEY, k integer, n text NOT NULL);\n",
		desired: "CREATE TABLE t (id integer PRIMARY KEY, k text, n text);\n",
		want:    "Differences detected (1 category):\n  tables_modified (1): t\n    t.k: type: INTEGER -> text\n    t.n: nullable: false -> true\n\n",
	},
	{
		name: "sqlserver", dialect: platform.SQLServer,
		target: func(c *qt.C) string {
			return createdDevDialectDatabase(c, dbtarget.URL(c, dbtarget.SQLServer),
				"ALTER DATABASE %[1]s SET SINGLE_USER WITH ROLLBACK IMMEDIATE; DROP DATABASE %[1]s", replaceSQLServerDatabase)
		},
		current: "CREATE TABLE t (id bigint PRIMARY KEY, k bigint, n nvarchar(20) NOT NULL);\n",
		desired: "CREATE TABLE t (id bigint PRIMARY KEY, k int, n nvarchar(20));\n",
		want:    "Differences detected (1 category):\n  tables_modified (1): t\n    t.k: type: BIGINT -> int\n    t.n: nullable: false -> true\n\n",
	},
	{
		name: "clickhouse", dialect: platform.ClickHouse,
		target:  clickHouseScratchURL,
		current: "CREATE TABLE t (id Int64, k Int32) ENGINE = MergeTree ORDER BY id;\n",
		desired: "CREATE TABLE t (id Int64, k Int64) ENGINE = MergeTree ORDER BY id;\n",
		want:    "Differences detected (1 category):\n  tables_modified (1): t\n    t.k: type: Int32 -> Int64\n\n",
	},
}

// TestSchemaCompareE2E_ReportsTheOldAndNewValueOfAColumn reads the categories of each
// report, and then the statements under them, which stay in the exact statement style:
// the values are the report's, so the statements need no comment beside them and no
// guard.
func TestSchemaCompareE2E_ReportsTheOldAndNewValueOfAColumn(t *testing.T) {
	for _, test := range compareValuesCases {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			target := test.target(c)
			applyEngineFile(c, target, test.dialect, test.current)
			schema := filepath.Join(t.TempDir(), "schema.sql")
			c.Assert(os.WriteFile(schema, []byte(test.desired), 0o600), qt.IsNil)

			output := runPtahNative(c, "schema", "compare", "--db-url", target, "--schema-file", schema)

			_, report, found := strings.Cut(output, "=== SCHEMA COMPARISON ===\n\n")
			c.Assert(found, qt.IsTrue, qt.Commentf("%s", output))
			categories, statements, found := strings.Cut(report, "Reconciling SQL:\n")
			c.Assert(found, qt.IsTrue, qt.Commentf("%s", output))
			c.Assert(categories, qt.Equals, test.want)
			c.Assert(statements, qt.Not(qt.Matches), `(?s).*(--|IF EXISTS|CASCADE).*`)
			c.Assert(strings.TrimSpace(statements), qt.Not(qt.Equals), "")
		})
	}
}

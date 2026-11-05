//go:build integration

package integration_test

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform"
	"ptah.run/internal/dbtarget"
)

// columnTypeEngine is a database to build a table in, and what reads the type of its
// column c back.
type columnTypeEngine struct {
	dialect string
	target  func(c *qt.C) string
	typeOf  string
}

var (
	columnTypePostgres = columnTypeEngine{
		dialect: platform.Postgres,
		target:  func(c *qt.C) string { return pinnedDevURL(c, postgresScratchDevURL(c, "ptah_column_type"), "public") },
		typeOf: "SELECT format_type(atttypid, atttypmod) FROM pg_attribute " +
			"WHERE attrelid = 'public.t'::regclass AND attname = 'c'",
	}
	columnTypeMySQL = columnTypeEngine{
		dialect: platform.MySQL,
		target: func(c *qt.C) string {
			_, url := newMySQLFamilyScratch(c, dbtarget.MySQLAdmin).database(c, "column_type")
			return url
		},
		typeOf: "SELECT column_type FROM information_schema.columns " +
			"WHERE table_schema = DATABASE() AND table_name = 't' AND column_name = 'c'",
	}
	columnTypeMariaDB = columnTypeEngine{
		dialect: platform.MariaDB,
		target: func(c *qt.C) string {
			_, url := newMySQLFamilyScratch(c, dbtarget.MariaDBAdmin).database(c, "column_type")
			return url
		},
		typeOf: columnTypeMySQL.typeOf,
	}
)

// columnTypeTable is the table a row of these tests changes.
func columnTypeTable(columnType string) string {
	return fmt.Sprintf("CREATE TABLE t (id int PRIMARY KEY, c %s);\n", columnType)
}

// TestCompatMigrateDiffColumnTypeE2E_TheTypeChangesBothWays applies the file of a type
// change and reads the type back, then applies its rollback and reads the type before
// it. A comparison that folds two types together writes no file for the change.
func TestCompatMigrateDiffColumnTypeE2E_TheTypeChangesBothWays(t *testing.T) {
	tests := []struct {
		name       string
		engine     columnTypeEngine
		before     string
		after      string
		wantBefore string
		wantAfter  string
	}{
		{
			name: "a time zone", engine: columnTypePostgres, before: "timestamp", after: "timestamptz",
			wantBefore: "timestamp without time zone", wantAfter: "timestamp with time zone",
		},
		{
			name: "no time zone", engine: columnTypePostgres, before: "timestamptz", after: "timestamp",
			wantBefore: "timestamp with time zone", wantAfter: "timestamp without time zone",
		},
		{
			name: "an array of a time zone", engine: columnTypePostgres, before: "timestamp[]", after: "timestamptz[]",
			wantBefore: "timestamp without time zone[]", wantAfter: "timestamp with time zone[]",
		},
		{
			name: "the element's width", engine: columnTypePostgres, before: "smallint[]", after: "int[]",
			wantBefore: "smallint[]", wantAfter: "integer[]",
		},
		{
			name: "the element's precision", engine: columnTypePostgres, before: "numeric(5,2)[]", after: "numeric(7,2)[]",
			wantBefore: "numeric(5,2)[]", wantAfter: "numeric(7,2)[]",
		},
		{
			name: "the element's length", engine: columnTypePostgres, before: "varchar(10)[]", after: "varchar(20)[]",
			wantBefore: "character varying(10)[]", wantAfter: "character varying(20)[]",
		},
		{
			name: "a serial's width", engine: columnTypePostgres, before: "serial", after: "bigserial",
			wantBefore: "integer", wantAfter: "bigint",
		},
		{
			name: "a longer text on MySQL", engine: columnTypeMySQL, before: "text", after: "longtext",
			wantBefore: "text", wantAfter: "longtext",
		},
		{
			name: "unsigned on MySQL", engine: columnTypeMySQL, before: "int", after: "int unsigned",
			wantBefore: "int", wantAfter: "int unsigned",
		},
		{
			name: "a longer text on MariaDB", engine: columnTypeMariaDB, before: "text", after: "longtext",
			wantBefore: "text", wantAfter: "longtext",
		},
		{
			name: "unsigned on MariaDB", engine: columnTypeMariaDB, before: "int", after: "int unsigned",
			wantBefore: "int(11)", wantAfter: "int(10) unsigned",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			pairs := readMigrationPairs(c, diffEngineSteps(c, "", test.engine.target(c), []string{
				columnTypeTable(test.before), columnTypeTable(test.after),
			}))
			target := test.engine.target(c)

			c.Assert(pairs, qt.HasLen, 2)
			applyEngineFile(c, target, test.engine.dialect, pairs[0].Up)
			c.Assert(heldColumns(c, target, test.engine.typeOf), qt.Equals, test.wantBefore)
			applyEngineFile(c, target, test.engine.dialect, pairs[1].Up)
			c.Assert(heldColumns(c, target, test.engine.typeOf), qt.Equals, test.wantAfter)
			applyEngineFile(c, target, test.engine.dialect, pairs[1].Down)
			c.Assert(heldColumns(c, target, test.engine.typeOf), qt.Equals, test.wantBefore)
		})
	}
}

// TestSchemaCompareE2E_ATypeANameOnlyResembledIsAChange reads the report of a column
// changing to a type that has no cast from the one it holds, so PostgreSQL would stop
// the file and the report is what there is to read. Each pair folded to one name.
func TestSchemaCompareE2E_ATypeANameOnlyResembledIsAChange(t *testing.T) {
	tests := []struct {
		name   string
		before string
		after  string
		want   string
	}{
		{name: "an integer array", before: "int", after: "int[]", want: "t.c: type: integer -> integer[]"},
		{name: "a text array", before: "text", after: "text[]", want: "t.c: type: text -> text[]"},
		{name: "a boolean array", before: "boolean", after: "boolean[]", want: "t.c: type: boolean -> boolean[]"},
		{name: "a timestamp array", before: "timestamp", after: "timestamp[]", want: "t.c: type: timestamp -> timestamp[]"},
		{name: "a decimal array", before: "numeric", after: "numeric[]", want: "t.c: type: decimal -> decimal[]"},
		{name: "back from an array", before: "int[]", after: "int", want: "t.c: type: integer[] -> integer"},
		{name: "an interval", before: "int", after: "interval", want: "t.c: type: integer -> interval"},
		{name: "a point", before: "int", after: "point", want: "t.c: type: integer -> point"},
		{name: "a range", before: "int", after: "int4range", want: "t.c: type: integer -> int4range"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			target := columnTypePostgres.target(c)
			applyEngineFile(c, target, columnTypePostgres.dialect, columnTypeTable(test.before))
			schema := filepath.Join(t.TempDir(), "schema.sql")
			c.Assert(os.WriteFile(schema, []byte(columnTypeTable(test.after)), 0o600), qt.IsNil)

			output := runPtahNative(c, "schema", "compare", "--db-url", target, "--schema-file", schema)

			c.Assert(output, qt.Contains, test.want)
		})
	}
}

// columnTypeSameCases are spellings of a type a server reads back in another spelling.
// Each compares equal to the column it built, so telling types apart reports no change
// where there is none.
var columnTypeSameCases = []struct {
	name   string
	target func(c *qt.C) string
	exec   func(c *qt.C, target, statement string)
	types  []string
}{
	{
		name:   "postgres",
		target: columnTypePostgres.target,
		exec: func(c *qt.C, target, statement string) {
			applyEngineFile(c, target, platform.Postgres, statement)
		},
		types: []string{
			"int[]", "int4[]", "integer[]", "int[][]", "bigint[]", "smallint[]", "text[]", "varchar[]", "varchar(10)[]",
			"character varying(10)[]", "char(3)[]", "numeric(5,2)[]", "numeric[]", "boolean[]", "bool[]", "timestamp[]",
			"timestamptz[]", "timestamp with time zone[]", "float8[]", "double precision[]", "real[]", "uuid[]", "jsonb[]",
			"date[]", "time[]", "timetz[]", "interval[]", "timestamp", "timestamptz", "timestamp with time zone",
			"timestamp without time zone", "interval", "interval(3)", "point", "int4range", "int8range", "money",
			"serial", "bigserial", "smallserial",
		},
	},
	{
		name:   "mysql",
		target: columnTypeMySQL.target,
		exec: func(c *qt.C, target, statement string) {
			applyEngineFile(c, target, platform.MySQL, statement)
		},
		types: []string{
			"text", "tinytext", "mediumtext", "longtext", "int", "int unsigned", "bigint unsigned", "tinyint", "bool",
			"smallint", "mediumint", "decimal(10,2)", "decimal(10,2) unsigned", "timestamp NULL", "datetime", "json",
		},
	},
	{
		name:   "mariadb",
		target: columnTypeMariaDB.target,
		exec: func(c *qt.C, target, statement string) {
			applyEngineFile(c, target, platform.MariaDB, statement)
		},
		types: []string{
			"text", "tinytext", "mediumtext", "longtext", "int", "int unsigned", "bigint unsigned", "tinyint", "bool",
			"smallint", "mediumint", "decimal(10,2)", "decimal(10,2) unsigned", "timestamp NULL", "datetime",
		},
	},
	{
		name:   "cockroachdb",
		target: cockroachColumnTypeDatabase,
		exec:   func(c *qt.C, target, statement string) { execPostgresFamilySQL(c, c.Context(), target, statement) },
		types: []string{
			"int[]", "integer[]", "int8[]", "int4[]", "int2[]", "bigint[]", "smallint[]", "text[]", "string[]", "varchar[]",
			"varchar(10)[]", "numeric(5,2)[]", "boolean[]", "timestamp[]", "timestamptz[]", "uuid[]", "date[]", "timestamp",
			"timestamptz", "timestamp with time zone", "interval",
		},
	},
}

// TestSchemaCompareE2E_ASpellingOfTheSameTypeIsNoChange builds a table of one column of
// each type and compares it with the statement that built it.
func TestSchemaCompareE2E_ASpellingOfTheSameTypeIsNoChange(t *testing.T) {
	for _, engine := range columnTypeSameCases {
		t.Run(engine.name, func(t *testing.T) {
			c := qt.New(t)
			target := engine.target(c)
			table := "CREATE TABLE t (id int PRIMARY KEY"
			for i, columnType := range engine.types {
				table += fmt.Sprintf(", c%d %s", i, columnType)
			}
			table += ");\n"
			engine.exec(c, target, table)
			schema := filepath.Join(t.TempDir(), "schema.sql")
			c.Assert(os.WriteFile(schema, []byte(table), 0o600), qt.IsNil)

			output := runPtahNative(c, "schema", "compare", "--db-url", target, "--schema-file", schema)

			c.Assert(output, qt.Contains, "No schema differences detected.", qt.Commentf("%s", output))
		})
	}
}

// cockroachColumnTypeDatabase is an empty database, dropped when the test ends.
func cockroachColumnTypeDatabase(c *qt.C) string {
	c.Helper()
	adminURL := dbtarget.URL(c, dbtarget.CockroachDB)
	admin, err := sql.Open("pgx", postgresFamilyDriverURL(c, adminURL))
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { c.Check(admin.Close(), qt.IsNil) })
	name := fmt.Sprintf("ptah_column_type_%d", time.Now().UnixNano())
	createE2EDatabase(c, c.Context(), admin, name)
	c.Cleanup(func() { dropE2EDatabase(c, context.Background(), admin, name) })
	return replaceDatabaseName(c, adminURL, name)
}

//go:build integration

package safety_test

import (
	"fmt"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/clirun"
	"ptah.run/internal/dbtarget"
)

// sqlServerTypeOf reads what the server holds for t.c: its name, and a length where the
// type has one. -1 is MAX.
const sqlServerTypeOf = `SELECT DATA_TYPE + ISNULL('(' + CAST(CHARACTER_MAXIMUM_LENGTH AS varchar(10)) + ')', '')
	FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = 't' AND COLUMN_NAME = 'c'`

// sqlServerText reads a column of any type as text.
var sqlServerText = temporalEngine{
	engine: dbtarget.SQLServer, read: "SELECT CONVERT(nvarchar(4000), c) FROM t", table: temporalTable,
}

// TestSchemaPlanE2E_AnAdoptedTableInPortableNamesPlansNothing builds a table in SQL Server's
// own types and declares it in the portable names the renderer writes as those types. A plan
// here is a statement that changes nothing, on every run, and each row of it rated
// destructive stops --check-destructive and an approval on a database nobody changed.
func TestSchemaPlanE2E_AnAdoptedTableInPortableNamesPlansNothing(t *testing.T) {
	c := qt.New(t)
	conn, address := temporalScratch(c, sqlServerTemporal)
	_, err := conn.ExecContext(c.Context(),
		"CREATE TABLE r1 (id INT NOT NULL PRIMARY KEY, a DATETIME2 NULL, b DATETIMEOFFSET NULL, c BIT NULL, d NVARCHAR(MAX) NULL, e VARBINARY(MAX) NULL)")
	c.Assert(err, qt.IsNil)
	schema := schemaFile(c, "CREATE TABLE r1 (id INT PRIMARY KEY, a timestamp NULL, b timestamptz NULL, c boolean NULL, d text NULL, e bytea NULL)")
	ptah := func(args ...string) clirun.Result {
		return clirun.Run(c, clirun.Ptah, clirun.Options{Dir: c.TempDir()}, args...)
	}

	first := planOf(c, address, schema)
	c.Assert(first.Outcome, qt.Equals, "no-changes", qt.Commentf("%+v", first.Plan.Statements))
	c.Assert(planOf(c, address, schema).Outcome, qt.Equals, "no-changes")
	gate := ptah("migrations", "plan", "--db-url", address, "--schema-file", schema, "--check-destructive")
	c.Assert(gate.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", gate.Stdout, gate.Stderr))
	drift := ptah("schema", "drift", "--db-url", address, "--schema-file", schema, "--format", "json", "--severity", "destructive")
	c.Assert(drift.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", drift.Stdout, drift.Stderr))
}

// TestSchemaPlanE2E_APortableNameBuiltByHandPlansNothing builds each column on the server
// and declares the name that writes it. The second group holds the columns the server
// builds from the word itself, which match the word as well.
func TestSchemaPlanE2E_APortableNameBuiltByHandPlansNothing(t *testing.T) {
	pairs := [][2]string{
		{"datetime2", "timestamp"}, {"datetime2(7)", "TIMESTAMP"}, {"datetime2", "timestamp without time zone"},
		{"datetimeoffset", "timestamptz"}, {"datetimeoffset(7)", "TIMESTAMPTZ"}, {"datetimeoffset", "timestamp with time zone"},
		{"bit", "boolean"}, {"bit", "bool"}, {"nvarchar(max)", "text"}, {"nvarchar(max)", "TEXT"}, {"nvarchar(max)", "citext"},
		{"varbinary(max)", "bytea"}, {"varbinary(max)", "blob"}, {"float", "double precision"}, {"int", "integer"}, {"int", "int4"},
		{"nchar(1)", "char"}, {"nchar(5)", "char(5)"}, {"nchar(5)", "character(5)"},
		{"nvarchar(max)", "varchar"}, {"nvarchar(max)", "VARCHAR"}, {"nvarchar(50)", "varchar(50)"}, {"nvarchar(max)", "varchar(max)"},
		{"nvarchar(50)", "character varying(50)"},
		{"int identity(1,1)", "serial"}, {"bigint identity(1,1)", "bigserial"}, {"smallint identity(1,1)", "smallserial"},
		{"text", "text"}, {"char(1)", "char"}, {"char(5)", "char(5)"}, {"char(5)", "character(5)"},
		{"varchar(1)", "varchar"}, {"varchar(50)", "varchar(50)"}, {"varchar(50)", "character varying(50)"}, {"varchar(max)", "varchar(max)"},
	}
	conn, address := temporalScratch(qt.New(t), sqlServerTemporal)
	for _, pair := range pairs {
		t.Run(pair[0]+" as "+pair[1], func(t *testing.T) {
			c := qt.New(t)
			_, err := conn.ExecContext(c.Context(), "DROP TABLE IF EXISTS t")
			c.Assert(err, qt.IsNil)
			_, err = conn.ExecContext(c.Context(), fmt.Sprintf(temporalTable, pair[0]))
			c.Assert(err, qt.IsNil)

			plan := planOf(c, address, schemaFile(c, fmt.Sprintf(temporalTable, pair[1])))

			c.Assert(plan.Outcome, qt.Equals, "no-changes", qt.Commentf("%+v", plan.Plan.Statements))
		})
	}
}

// TestSchemaPlanE2E_APortableNameMatchesTheColumnPtahBuilds applies each name to an empty
// database, reads what the server holds and plans the same declaration. wantHeld is the
// server's answer, so a row also fails if the renderer starts writing another type.
func TestSchemaPlanE2E_APortableNameMatchesTheColumnPtahBuilds(t *testing.T) {
	rows := []struct{ declared, wantHeld string }{
		{"timestamp", "datetime2"}, {"TIMESTAMP", "datetime2"}, {"timestamp without time zone", "datetime2"},
		{"timestamptz", "datetimeoffset"}, {"TIMESTAMPTZ", "datetimeoffset"}, {"timestamp with time zone", "datetimeoffset"},
		{"boolean", "bit"}, {"BOOLEAN", "bit"}, {"bool", "bit"},
		{"text", "nvarchar(-1)"}, {"TEXT", "nvarchar(-1)"}, {"citext", "nvarchar(-1)"},
		{"varchar", "nvarchar(-1)"}, {"VARCHAR", "nvarchar(-1)"}, {"varchar(50)", "nvarchar(50)"}, {"character varying(50)", "nvarchar(50)"},
		{"char", "nchar(1)"}, {"CHAR", "nchar(1)"}, {"char(5)", "nchar(5)"}, {"character(5)", "nchar(5)"},
		{"bytea", "varbinary(-1)"}, {"BYTEA", "varbinary(-1)"}, {"blob", "varbinary(-1)"},
		{"double precision", "float"}, {"integer", "int"}, {"int4", "int"},
		{"serial", "int"}, {"bigserial", "bigint"}, {"smallserial", "smallint"},
	}
	conn, address := temporalScratch(qt.New(t), sqlServerTemporal)
	for _, row := range rows {
		t.Run(row.declared, func(t *testing.T) {
			c := qt.New(t)
			_, err := conn.ExecContext(c.Context(), "DROP TABLE IF EXISTS t")
			c.Assert(err, qt.IsNil)
			schema := schemaFile(c, fmt.Sprintf(temporalTable, row.declared))

			applySchema(c, address, schema)

			var held string
			c.Assert(conn.QueryRowContext(c.Context(), sqlServerTypeOf).Scan(&held), qt.IsNil)
			c.Assert(held, qt.Equals, row.wantHeld)
			plan := planOf(c, address, schema)
			c.Assert(plan.Outcome, qt.Equals, "no-changes", qt.Commentf("%+v", plan.Plan.Statements))
		})
	}
}

// TestSchemaPlanE2E_AnotherTypeUnderAPortableNameStillPlans holds the guard the other way.
// Each column is neither what the renderer writes for the name nor what the server builds
// from it, so the plan has work to do, the rating says what it says for any type change, and
// the column matches afterwards. The values come from the server, before and after.
func TestSchemaPlanE2E_AnotherTypeUnderAPortableNameStillPlans(t *testing.T) {
	const fraction = "'2024-01-02 03:04:05.123'"
	rows := []sizeRow{
		{"datetime2(3)", "timestamp", fraction, "2024-01-02 03:04:05.123", "2024-01-02 03:04:05.1230000", true},
		{"datetime", "timestamp", fraction, "Jan  2 2024  3:04AM", "2024-01-02 03:04:05.1233333", true},
		{"datetimeoffset", "timestamp", "'2024-01-02 03:04:05.123 +02:00'", "2024-01-02 03:04:05.1230000 +02:00", "2024-01-02 03:04:05.1230000", true},
		{"nvarchar(50)", "text", "'abc'", "abc", "abc", false},
		{"varchar(max)", "text", "'abc'", "abc", "abc", false},
		{"varchar(max)", "citext", "'abc'", "abc", "abc", false},
		{"ntext", "text", "'abc'", "abc", "abc", false},
		{"int", "text", "7", "7", "7", false},
		{"int", "boolean", "1", "1", "1", false},
		{"tinyint", "boolean", "1", "1", "1", false},
		{"bigint", "integer", "7", "7", "7", true},
		{"smallint", "integer", "7", "7", "7", false},
		{"real", "double precision", "1.5", "1.5", "1.5", false},
		{"nchar(10)", "char(5)", "'abc'", "abc       ", "abc  ", true},
		{"nvarchar(50)", "varchar(20)", "'abc'", "abc", "abc", true},
		{"nvarchar(max)", "varchar(50)", "'abc'", "abc", "abc", true},
	}
	conn, address := temporalScratch(qt.New(t), sqlServerText)
	for _, row := range rows {
		t.Run(row.before+" as "+row.after, func(t *testing.T) {
			c := qt.New(t)
			builtColumn(c, conn, sqlServerText, row.before, row.value)
			c.Assert(heldValue(c, conn, sqlServerText), qt.Equals, row.wantHeld)
			schema := schemaFile(c, fmt.Sprintf(temporalTable, row.after))

			plan := planOf(c, address, schema)

			c.Assert(plan.Outcome, qt.Equals, "changes")
			c.Assert(plan.Plan.Destructive, qt.Equals, row.wantDestructive, qt.Commentf("%+v", plan.Plan.Statements))
			applySchema(c, address, schema)
			c.Assert(heldValue(c, conn, sqlServerText), qt.Equals, row.wantAfter)
			c.Assert(planOf(c, address, schema).Outcome, qt.Equals, "no-changes")
		})
	}
}

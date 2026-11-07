//go:build integration

package safety_test

import (
	"fmt"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/clirun"
	"ptah.run/internal/dbtarget"
)

var (
	// sqlServerNumber reads a float through a decimal with room for its digits, since
	// SQL Server's own text of a float keeps six.
	sqlServerNumber = temporalEngine{
		engine: dbtarget.SQLServer, read: "SELECT CONVERT(varchar(50), CAST(c AS decimal(30,10))) FROM t", table: temporalTable,
	}
	sqlServerBytes = temporalEngine{engine: dbtarget.SQLServer, read: "SELECT CONVERT(varchar(50), c, 1) FROM t", table: temporalTable}
)

// TestSchemaPlanE2E_ALowerCapacityIsRated runs each row through `schema plan` and `schema
// apply`, and reads the value back. A row rated destructive can lose a value, whether or
// not this one survives: the float rows show the loss without an error, and the rest show
// that the rating follows the type and not the row. A widening stays safe.
func TestSchemaPlanE2E_ALowerCapacityIsRated(t *testing.T) {
	const (
		double   = "1.23456789012"
		pastBits = "9007199254740993"
	)
	engines := []struct {
		name   string
		engine temporalEngine
		rows   []sizeRow
	}{
		{
			name: "postgres", engine: postgresTemporal,
			rows: []sizeRow{
				{"double precision", "real", double, double, "1.2345679", true},
				{"real", "double precision", "1.5", "1.5", "1.5", false},
				{"double precision[]", "real[]", "ARRAY[" + double + "]", "{" + double + "}", "{1.2345679}", true},
				{"numeric(10,2)", "integer", "1.50", "1.50", "2", true},
				{"integer", "numeric(12,0)", "1234567", "1234567", "1234567", false},
				{"bigint", "double precision", pastBits, pastBits, "9.007199254740992e+15", true},
				{"integer", "double precision", "2147483647", "2147483647", "2147483647", false},
				{"integer", "real", "16777217", "16777217", "1.6777216e+07", true},
				{"double precision", "numeric(10,2)", "1.239", "1.239", "1.24", true},
				{"varbit(8)", "varbit(4)", "B'1010'", "1010", "1010", true},
				{"varbit(4)", "varbit", "B'1010'", "1010", "1010", false},
			},
		},
		{
			name: "cockroachdb", engine: cockroachTemporal,
			rows: []sizeRow{
				{"string(100)", "string(5)", "'abc'", "abc", "abc", true},
				{"string(5)", "string(100)", "'abc'", "abc", "abc", false},
				{"string", "string(5)", "'abc'", "abc", "abc", true},
				{"string(5)", "string", "'abc'", "abc", "abc", false},
				// The server keeps eight bytes for a row it has already written.
				{"float8", "float4", double, double, double, true},
				{"float4", "float8", "1.5", "1.5", "1.5", false},
				{"decimal(10,2)", "int4", "1.50", "1.50", "2", true},
				{"int8", "float8", pastBits, pastBits, "9.007199254740992e+15", true},
			},
		},
		{
			name: "yugabytedb", engine: yugabyteTemporal,
			rows: []sizeRow{
				{"double precision", "real", double, double, "1.2345679", true},
				{"real", "double precision", "1.5", "1.5", "1.5", false},
				{"bigint", "double precision", pastBits, pastBits, "9.007199254740992e+15", true},
			},
		},
		{
			name: "mysql", engine: mysqlTemporal,
			rows: []sizeRow{
				{"longtext", "text", "'abc'", "abc", "abc", true},
				{"longtext", "mediumtext", "'abc'", "abc", "abc", true},
				{"mediumtext", "text", "'abc'", "abc", "abc", true},
				{"text", "tinytext", "'abc'", "abc", "abc", true},
				{"tinytext", "text", "'abc'", "abc", "abc", false},
				{"text", "longtext", "'abc'", "abc", "abc", false},
				{"text", "text(65535)", "'abc'", "abc", "abc", false},
				{"varchar(63)", "tinytext", "'abc'", "abc", "abc", false},
				{"varchar(64)", "tinytext", "'abc'", "abc", "abc", true},
				{"tinytext", "varchar(255)", "'abc'", "abc", "abc", false},
				{"longblob", "blob", "'abc'", "abc", "abc", true},
				{"blob", "tinyblob", "'abc'", "abc", "abc", true},
				{"tinyblob", "blob", "'abc'", "abc", "abc", false},
				{"varbinary(50)", "varbinary(5)", "'abc'", "abc", "abc", true},
				{"varbinary(5)", "varbinary(50)", "'abc'", "abc", "abc", false},
				{"double", "float", double, double, "1.23457", true},
				{"float", "double", "1.5", "1.5", "1.5", false},
				{"smallint", "tinyint", "1", "1", "1", true},
				{"tinyint", "smallint", "1", "1", "1", false},
				{"int", "int unsigned", "1", "1", "1", true},
				{"int unsigned", "int", "1", "1", "1", true},
				{"int unsigned", "bigint", "1", "1", "1", false},
				{"decimal(10,2)", "int", "1.50", "1.50", "2", true},
				{"bigint", "double", pastBits, pastBits, "9.007199254740992e15", true},
				{"int", "double", "1", "1", "1", false},
				{"enum('a','b')", "enum('a')", "'a'", "a", "a", true},
				{"enum('a')", "enum('a','b')", "'a'", "a", "a", false},
			},
		},
		{
			name: "mariadb", engine: mariadbTemporal,
			rows: []sizeRow{
				{"longtext", "text", "'abc'", "abc", "abc", true},
				{"tinytext", "text", "'abc'", "abc", "abc", false},
				{"longblob", "blob", "'abc'", "abc", "abc", true},
				{"tinyblob", "blob", "'abc'", "abc", "abc", false},
				{"double", "float", double, double, "1.23457", true},
				{"float", "double", "1.5", "1.5", "1.5", false},
				{"int", "int unsigned", "1", "1", "1", true},
				{"decimal(10,2)", "int", "1.50", "1.50", "2", true},
				{"enum('a','b')", "enum('a')", "'a'", "a", "a", true},
			},
		},
		{
			name: "sqlserver", engine: sqlServerNumber,
			rows: []sizeRow{
				{"float", "real", double, "1.2345678901", "1.2345678806", true},
				{"real", "float", "1.5", "1.5000000000", "1.5000000000", false},
				{"money", "smallmoney", "1.5", "1.5000000000", "1.5000000000", true},
				{"smallmoney", "money", "1.5", "1.5000000000", "1.5000000000", false},
				{"float", "int", "1.5", "1.5000000000", "1.0000000000", true},
				{"int", "float", "1", "1.0000000000", "1.0000000000", false},
			},
		},
		{
			name: "sqlserver bytes", engine: sqlServerBytes,
			rows: []sizeRow{
				{"varbinary(max)", "varbinary(50)", "0x0102", "0x0102", "0x0102", true},
				{"varbinary(50)", "varbinary(max)", "0x0102", "0x0102", "0x0102", false},
				{"varbinary(50)", "varbinary(5)", "0x0102", "0x0102", "0x0102", true},
				{"varbinary(5)", "varbinary(50)", "0x0102", "0x0102", "0x0102", false},
			},
		},
		{
			name: "clickhouse", engine: clickHouseTemporal,
			rows: []sizeRow{
				{"Float64", "Float32", double, double, "1.2345679", true},
				{"Float32", "Float64", "1.5", "1.5", "1.5", false},
				{"Nullable(Float64)", "Nullable(Float32)", double, double, "1.2345679", true},
				{"String", "FixedString(2)", "'ab'", "ab", "ab", true},
				{"FixedString(2)", "String", "'ab'", "ab", "ab", false},
				{"Decimal64(2)", "Decimal32(2)", "1.5", "1.5", "1.5", true},
				{"Decimal32(2)", "Decimal64(2)", "1.5", "1.5", "1.5", false},
				{"Int64", "Float64", pastBits, pastBits, "9007199254740992", true},
				{"Int32", "Float64", "1", "1", "1", false},
				{"Float64", "Int64", "1.5", "1.5", "1", true},
			},
		},
	}
	for _, engine := range engines {
		t.Run(engine.name, func(t *testing.T) {
			conn, address := temporalScratch(qt.New(t), engine.engine)
			for _, row := range engine.rows {
				t.Run(row.before+" to "+row.after, func(t *testing.T) {
					c := qt.New(t)
					builtColumn(c, conn, engine.engine, row.before, row.value)
					c.Assert(heldValue(c, conn, engine.engine), qt.Equals, row.wantHeld)
					schema := schemaFile(c, fmt.Sprintf(engine.engine.table, row.after))

					plan := planOf(c, address, schema)

					c.Assert(plan.Outcome, qt.Equals, "changes")
					c.Assert(plan.Plan.Destructive, qt.Equals, row.wantDestructive, qt.Commentf("%+v", plan.Plan.Statements))
					applySchema(c, address, schema)
					c.Assert(heldValue(c, conn, engine.engine), qt.Equals, row.wantAfter)
					c.Assert(planOf(c, address, schema).Outcome, qt.Equals, "no-changes")
				})
			}
		})
	}
}

// TestSchemaPlanE2E_AServerRefusesAValueTheCapacityCannotHold puts a row the declared type
// cannot hold under the plan. These servers refuse it and keep the value, so the rating
// warns of a plan that fails and not of a lost row. The rating does not change for that:
// a plan does not know the rows.
func TestSchemaPlanE2E_AServerRefusesAValueTheCapacityCannotHold(t *testing.T) {
	rows := []struct {
		name          string
		engine        temporalEngine
		before, after string
		value         string
		wantHeld      string
		wantRefusal   string
	}{
		{"PostgreSQL a bit length", postgresTemporal, "bit(8)", "bit(4)", "B'10101010'", "10101010", "22026"},
		{"CockroachDB a STRING length", cockroachTemporal, "string(100)", "string(2)", "'abc'", "abc", "validation of CHECK"},
		{"MySQL a tier", mysqlTemporal, "text", "tinytext", "REPEAT('a', 300)", strings.Repeat("a", 300), "Data too long"},
		{"MariaDB a tier", mariadbTemporal, "text", "tinytext", "REPEAT('a', 300)", strings.Repeat("a", 300), "Data too long"},
		{"MySQL a sign", mysqlTemporal, "int", "int unsigned", "-1", "-1", "Out of range"},
		{"MySQL a member", mysqlTemporal, "enum('a','b')", "enum('a')", "'b'", "b", "Data truncated"},
		{"SQL Server a length", sqlServerBytes, "varbinary(max)", "varbinary(1)", "0x0102", "0x0102", "truncated"},
	}
	for _, row := range rows {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)
			conn, address := temporalScratch(c, row.engine)
			builtColumn(c, conn, row.engine, row.before, row.value)
			schema := schemaFile(c, fmt.Sprintf(row.engine.table, row.after))

			plan := planOf(c, address, schema)
			c.Assert(plan.Plan.Destructive, qt.IsTrue, qt.Commentf("%+v", plan.Plan.Statements))

			run := clirun.Run(c, clirun.Ptah, clirun.Options{}, "schema", "apply", "--db-url", address, "--schema-file", schema, "--auto-approve")
			c.Assert(run.ExitCode, qt.Not(qt.Equals), 0)
			c.Assert(run.Stdout+run.Stderr, qt.Contains, row.wantRefusal)
			c.Assert(heldValue(c, conn, row.engine), qt.Equals, row.wantHeld)
		})
	}
}

// TestSchemaPlanE2E_ClickHouseLeavesAColumnItCannotConvertUnreadable puts a row too long
// for the declared FixedString under the plan. The server accepts the statement, fails the
// mutation and has already recorded the new type, so every read of the column fails. That
// is why the rating stands whatever a row holds, and the plan says so before it runs.
func TestSchemaPlanE2E_ClickHouseLeavesAColumnItCannotConvertUnreadable(t *testing.T) {
	c := qt.New(t)
	conn, address := temporalScratch(c, clickHouseTemporal)
	builtColumn(c, conn, clickHouseTemporal, "String", "'abc'")
	schema := schemaFile(c, fmt.Sprintf(clickHouseTemporal.table, "FixedString(2)"))

	plan := planOf(c, address, schema)
	c.Assert(plan.Plan.Destructive, qt.IsTrue, qt.Commentf("%+v", plan.Plan.Statements))

	run := clirun.Run(c, clirun.Ptah, clirun.Options{}, "schema", "apply", "--db-url", address, "--schema-file", schema, "--auto-approve")
	c.Assert(run.ExitCode, qt.Not(qt.Equals), 0)
	var value string
	c.Assert(conn.QueryRowContext(c.Context(), clickHouseTemporal.read).Scan(&value), qt.ErrorMatches, `(?s).*String too long for type FixedString\(2\).*`)
}

// TestSchemaPlanE2E_OracleChangesACapacityAsTheServerAllows runs Oracle's rows, where a
// change of type needs an empty column and a lower precision does too. The rating answers
// to the type in every case.
func TestSchemaPlanE2E_OracleChangesACapacityAsTheServerAllows(t *testing.T) {
	number := temporalEngine{engine: dbtarget.OracleAdmin, read: "SELECT TO_CHAR(c) FROM t", table: oracleTypeTable}
	raw := temporalEngine{engine: dbtarget.OracleAdmin, read: "SELECT RAWTOHEX(c) FROM t", table: oracleTypeTable}
	const byteValue = "HEXTORAW('01')"
	rows := []struct {
		name            string
		engine          temporalEngine
		before, after   string
		value           string
		wantHeld        string
		wantDestructive bool
		wantRefusal     string
	}{
		{"a double to a single", number, "BINARY_DOUBLE", "BINARY_FLOAT", "1.5", "1.5E+000", true, "ORA-01439"},
		{"a single to a double", number, "BINARY_FLOAT", "BINARY_DOUBLE", "1.5", "1.5E+000", false, "ORA-01439"},
		{"a float's bits down", number, "FLOAT(10)", "FLOAT(5)", "1.5", "1.5", true, "ORA-01440"},
	}
	for _, row := range rows {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)
			conn, address := oracleScratch(c)
			builtColumn(c, conn, row.engine, row.before, row.value)
			schema := schemaFile(c, fmt.Sprintf(row.engine.table, row.after))

			plan := planOf(c, address, schema)
			c.Assert(plan.Outcome, qt.Equals, "changes")
			c.Assert(plan.Plan.Destructive, qt.Equals, row.wantDestructive, qt.Commentf("%+v", plan.Plan.Statements))

			refused := clirun.Run(c, clirun.Ptah, clirun.Options{}, "schema", "apply", "--db-url", address, "--schema-file", schema, "--auto-approve")
			c.Assert(refused.ExitCode, qt.Not(qt.Equals), 0)
			c.Assert(refused.Stdout+refused.Stderr, qt.Contains, row.wantRefusal)

			_, err := conn.ExecContext(c.Context(), "DELETE FROM t")
			c.Assert(err, qt.IsNil)
			applySchema(c, address, schema)
			c.Assert(planOf(c, address, schema).Outcome, qt.Equals, "no-changes")
		})
	}
	// A LOB takes no change at all, with a row or without one.
	neverRunning := []struct {
		name            string
		before, after   string
		wantDestructive bool
		wantRefusal     string
	}{
		{"a blob to a length", "BLOB", "RAW(50)", true, "ORA-22859"},
		{"a length to a blob", "RAW(50)", "BLOB", false, "ORA-22858"},
	}
	for _, row := range neverRunning {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)
			conn, address := oracleScratch(c)
			_, err := conn.ExecContext(c.Context(), fmt.Sprintf(oracleTypeTable, row.before))
			c.Assert(err, qt.IsNil)
			schema := schemaFile(c, fmt.Sprintf(oracleTypeTable, row.after))

			plan := planOf(c, address, schema)
			c.Assert(plan.Plan.Destructive, qt.Equals, row.wantDestructive, qt.Commentf("%+v", plan.Plan.Statements))

			refused := clirun.Run(c, clirun.Ptah, clirun.Options{}, "schema", "apply", "--db-url", address, "--schema-file", schema, "--auto-approve")
			c.Assert(refused.ExitCode, qt.Not(qt.Equals), 0)
			c.Assert(refused.Stdout+refused.Stderr, qt.Contains, row.wantRefusal)
		})
	}
	running := []struct {
		name            string
		engine          temporalEngine
		before, after   string
		value           string
		wantHeld        string
		wantDestructive bool
	}{
		{"a RAW's bytes down", raw, "RAW(10)", "RAW(5)", byteValue, "01", true},
		{"a RAW's bytes up", raw, "RAW(5)", "RAW(10)", byteValue, "01", false},
		{"a float's bits up", number, "FLOAT(5)", "FLOAT(10)", "1.5", "1.5", false},
	}
	for _, row := range running {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)
			conn, address := oracleScratch(c)
			builtColumn(c, conn, row.engine, row.before, row.value)
			c.Assert(heldValue(c, conn, row.engine), qt.Equals, row.wantHeld)
			schema := schemaFile(c, fmt.Sprintf(row.engine.table, row.after))

			plan := planOf(c, address, schema)

			c.Assert(plan.Plan.Destructive, qt.Equals, row.wantDestructive, qt.Commentf("%+v", plan.Plan.Statements))
			applySchema(c, address, schema)
			c.Assert(heldValue(c, conn, row.engine), qt.Equals, row.wantHeld)
			c.Assert(planOf(c, address, schema).Outcome, qt.Equals, "no-changes")
		})
	}
}

// TestSchemaPlanE2E_AColumnOfOneTierInTwoSpellings builds a column under a length and
// declares the tier the server built, and the other way round. Each planned a statement
// that changes nothing on every run.
func TestSchemaPlanE2E_AColumnOfOneTierInTwoSpellings(t *testing.T) {
	engines := []struct {
		name   string
		engine temporalEngine
	}{
		{name: "mysql", engine: mysqlTemporal},
		{name: "mariadb", engine: mariadbTemporal},
	}
	pairs := [][2]string{
		{"text(60)", "tinytext"}, {"tinytext", "text(60)"},
		{"text(70000)", "mediumtext"}, {"mediumtext", "text(70000)"},
		{"blob(100)", "tinyblob"}, {"tinyblob", "blob(100)"},
		{"blob(65535)", "blob"}, {"blob", "blob(65535)"},
	}
	for _, engine := range engines {
		t.Run(engine.name, func(t *testing.T) {
			conn, address := temporalScratch(qt.New(t), engine.engine)
			for _, pair := range pairs {
				t.Run(pair[0]+" as "+pair[1], func(t *testing.T) {
					c := qt.New(t)
					_, err := conn.ExecContext(c.Context(), "DROP TABLE IF EXISTS t")
					c.Assert(err, qt.IsNil)
					_, err = conn.ExecContext(c.Context(), fmt.Sprintf(engine.engine.table, pair[0]))
					c.Assert(err, qt.IsNil)

					plan := planOf(c, address, schemaFile(c, fmt.Sprintf(engine.engine.table, pair[1])))

					c.Assert(plan.Outcome, qt.Equals, "no-changes", qt.Commentf("%+v", plan.Plan.Statements))
				})
			}
		})
	}
}

// TestRatingE2E_ALowerCapacityReachesEveryReader asks each surface that reads the rating
// about one narrowing, and about one widening as the control.
func TestRatingE2E_ALowerCapacityReachesEveryReader(t *testing.T) {
	engines := []struct {
		name                  string
		engine                temporalEngine
		before, narrow, widen string
	}{
		{name: "postgres", engine: postgresTemporal, before: "real", narrow: "smallint", widen: "double precision"},
		{name: "mysql", engine: mysqlTemporal, before: "text", narrow: "tinytext", widen: "longtext"},
		{name: "sqlserver", engine: sqlServerTemporal, before: "varbinary(50)", narrow: "varbinary(5)", widen: "varbinary(max)"},
		{name: "clickhouse", engine: clickHouseTemporal, before: "Float32", narrow: "Decimal(4,2)", widen: "Float64"},
		{name: "cockroachdb", engine: cockroachTemporal, before: "string(50)", narrow: "string(5)", widen: "string(100)"},
	}
	for _, engine := range engines {
		t.Run(engine.name, func(t *testing.T) {
			c := qt.New(t)
			conn, address := temporalScratch(c, engine.engine)
			_, err := conn.ExecContext(c.Context(), fmt.Sprintf(engine.engine.table, engine.before))
			c.Assert(err, qt.IsNil)
			narrowingReachesEveryReader(c, address, fmt.Sprintf(engine.engine.table, engine.narrow))

			widen := schemaFile(c, fmt.Sprintf(engine.engine.table, engine.widen))
			c.Assert(planOf(c, address, widen).Plan.Destructive, qt.IsFalse)
			report := clirun.Run(c, clirun.Ptah, clirun.Options{Dir: c.TempDir()},
				"migrations", "plan", "--db-url", address, "--schema-file", widen, "--report", "json", "--check-destructive")
			c.Assert(report.ExitCode, qt.Equals, 0, qt.Commentf("%s", report.Stderr))
			c.Assert(report.Stdout, qt.Contains, `"destructive": false`)
			drift := clirun.Run(c, clirun.Ptah, clirun.Options{Dir: c.TempDir()},
				"schema", "drift", "--db-url", address, "--schema-file", widen, "--format", "json", "--severity", "destructive")
			c.Assert(drift.ExitCode, qt.Equals, 0, qt.Commentf("%s", drift.Stdout))
			c.Assert(drift.Stdout, qt.Not(qt.Contains), "column_types_narrowed")
		})
	}
}

// TestSchemaPlanE2E_ASameWidthInAnotherNameIsNotANarrowing changes a column to another
// name of the same width. The word rule read the comparison's folded `varchar`, with no
// limit, and called it destructive.
func TestSchemaPlanE2E_ASameWidthInAnotherNameIsNotANarrowing(t *testing.T) {
	rows := []struct {
		name          string
		engine        temporalEngine
		before, after string
	}{
		{"PostgreSQL a varchar to a longer char", postgresTemporal, "varchar(5)", "char(10)"},
		{"MySQL a varchar to a longer char", mysqlTemporal, "varchar(5)", "char(10)"},
		{"SQL Server an nvarchar to an nchar of its length", sqlServerTemporal, "nvarchar(50)", "nchar(50)"},
	}
	for _, row := range rows {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)
			conn, address := temporalScratch(c, row.engine)
			_, err := conn.ExecContext(c.Context(), fmt.Sprintf(row.engine.table, row.before))
			c.Assert(err, qt.IsNil)

			plan := planOf(c, address, schemaFile(c, fmt.Sprintf(row.engine.table, row.after)))

			c.Assert(plan.Outcome, qt.Equals, "changes")
			c.Assert(plan.Plan.Destructive, qt.IsFalse, qt.Commentf("%+v", plan.Plan.Statements))
		})
	}
}

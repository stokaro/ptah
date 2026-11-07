//go:build integration

package safety_test

import (
	"context"
	"fmt"
	"net/url"
	"os"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"ptah.run/dbschema"
	"ptah.run/internal/clirun"
	"ptah.run/internal/dbtarget"
)

const (
	oracleTypeTable = "CREATE TABLE t (id NUMBER(10) PRIMARY KEY, c %s)"
	// oracleFractionRead keeps every digit a TIMESTAMP(9) can hold, whatever the
	// session's NLS format says.
	oracleFractionRead = "SELECT TO_CHAR(c, 'YYYY-MM-DD HH24:MI:SS.FF6') FROM t"
)

var (
	oracleNumber = temporalEngine{engine: dbtarget.OracleAdmin, read: "SELECT TO_CHAR(c, 'TM') FROM t", table: oracleTypeTable}
	oracleTime   = temporalEngine{engine: dbtarget.OracleAdmin, read: oracleFractionRead, table: oracleTypeTable}
)

// oracleScratch is an account of its own, dropped when the test ends. A schema is a user
// on Oracle, and a plan reads every table of the account it connects with.
func oracleScratch(c *qt.C) (*dbschema.DatabaseConnection, string) {
	c.Helper()
	address := dbtarget.URL(c, dbtarget.OracleAdmin)
	admin, err := dbschema.ConnectToDatabase(c.Context(), address)
	c.Assert(err, qt.IsNil)
	account := fmt.Sprintf("ptah_size_%d_%d", os.Getpid(), time.Now().UnixNano())
	for _, statement := range []string{
		fmt.Sprintf("CREATE USER %s IDENTIFIED BY ptah_password", account),
		fmt.Sprintf("ALTER USER %s QUOTA UNLIMITED ON USERS", account),
		fmt.Sprintf("GRANT CREATE SESSION, CREATE TABLE TO %s", account),
	} {
		_, err = admin.ExecContext(c.Context(), statement)
		c.Assert(err, qt.IsNil, qt.Commentf("statement: %s", statement))
	}
	parsed, err := url.Parse(address)
	c.Assert(err, qt.IsNil)
	parsed.User = url.UserPassword(account, "ptah_password")
	conn, err := dbschema.ConnectToDatabase(c.Context(), parsed.String())
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() {
		dbschema.CloseAndWarn(conn)
		ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
		defer cancel()
		_, dropErr := admin.ExecContext(ctx, "DROP USER "+account+" CASCADE")
		c.Check(dropErr, qt.IsNil)
		dbschema.CloseAndWarn(admin)
	})
	return conn, parsed.String()
}

// A sizeRow inserts a value under one size of a type and declares another. wantAfter is
// what the server kept, read back after the plan ran.
type sizeRow struct {
	before, after, value, wantHeld, wantAfter string
	wantDestructive                           bool
}

// TestSchemaPlanE2E_ASizeOnOneSideOfAChange runs each row through `schema plan` and
// `schema apply`, and plans again. Unless the size on the bare side is read, each row plans
// nothing: the schema says one thing and the database holds another.
func TestSchemaPlanE2E_ASizeOnOneSideOfAChange(t *testing.T) {
	engines := []struct {
		name   string
		engine temporalEngine
		rows   []sizeRow
	}{
		{
			name: "postgres", engine: postgresTemporal,
			rows: []sizeRow{
				{"numeric", "numeric(10,2)", "1.239", "1.239", "1.24", true},
				{"numeric(10,2)", "numeric", "1.24", "1.24", "1.24", false},
				{"numeric[]", "numeric(10,2)[]", "ARRAY[1.239]", "{1.239}", "{1.24}", true},
				{"numeric(10,2)[]", "numeric[]", "ARRAY[1.24]", "{1.24}", "{1.24}", false},
				{"bpchar", "char", "'a'", "a", "a", true},
			},
		},
		{
			name: "cockroachdb", engine: cockroachTemporal,
			rows: []sizeRow{
				{"decimal", "decimal(10,2)", "1.24", "1.24", "1.24", true},
				{"decimal(10,2)", "decimal", "1.24", "1.24", "1.24", false},
			},
		},
		{
			name: "yugabytedb", engine: yugabyteTemporal,
			rows: []sizeRow{
				{"numeric", "numeric(10,2)", "1.239", "1.239", "1.24", true},
				{"numeric(10,2)", "numeric", "1.24", "1.24", "1.24", false},
			},
		},
		{
			name: "mysql", engine: mysqlTemporal,
			rows: []sizeRow{
				{"decimal(10,2)", "decimal", "1.50", "1.50", "2", true},
				{"decimal(5,0)", "decimal", "2", "2", "2", false},
			},
		},
		{
			name: "mariadb", engine: mariadbTemporal,
			rows: []sizeRow{
				{"decimal(10,2)", "decimal", "1.50", "1.50", "2", true},
				{"decimal(5,0)", "decimal", "2", "2", "2", false},
			},
		},
		{
			name: "sqlserver", engine: temporalEngine{
				engine: dbtarget.SQLServer, read: "SELECT CONVERT(nvarchar(4000), c) FROM t", table: temporalTable,
			},
			rows: []sizeRow{
				{"nvarchar(max)", "nvarchar(50)", "'abc'", "abc", "abc", true},
				{"nvarchar(50)", "nvarchar(max)", "'abc'", "abc", "abc", false},
				{"decimal(10,2)", "decimal", "1.50", "1.50", "2", true},
				{"decimal(5,0)", "decimal", "2", "2", "2", false},
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

// TestSchemaPlanE2E_AServerRefusesAValueTheSizeCannotHold puts a row the declared size
// cannot hold under the plan. The server refuses the statement and keeps the value, where
// a plan that reads as synced would never have asked.
func TestSchemaPlanE2E_AServerRefusesAValueTheSizeCannotHold(t *testing.T) {
	const long = "0123456789012345678901234567890123456789012345678901234567890123456789"
	sqlServerText := temporalEngine{engine: dbtarget.SQLServer, read: "SELECT CONVERT(nvarchar(4000), c) FROM t", table: temporalTable}
	rows := []struct {
		name          string
		engine        temporalEngine
		before, after string
		value         string
		wantHeld      string
		wantRefusal   string
	}{
		{"SQL Server a length", sqlServerText, "nvarchar(max)", "nvarchar(50)", "'" + long + "'", long, "truncated"},
		{"PostgreSQL digits", postgresTemporal, "numeric", "numeric(2,1)", "12.5", "12.5", "22003"},
		{"CockroachDB a fraction", cockroachTemporal, "decimal", "decimal(10,2)", "1.239", "1.239", "validation of CHECK"},
		{"YugabyteDB digits", yugabyteTemporal, "numeric", "numeric(2,1)", "12.5", "12.5", "22003"},
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

// TestSchemaPlanE2E_OracleLowersASizeOnlyOnAColumnWithoutARow runs each row on a column
// with a row and on an empty one. The plan rates a lower size destructive either way.
// The server refuses it while a row exists, and the value stays: no row is lost, and the
// plan cannot run until the column is empty or the table is rebuilt by hand. A higher size
// runs with the row in place.
func TestSchemaPlanE2E_OracleLowersASizeOnlyOnAColumnWithoutARow(t *testing.T) {
	const fraction = "TIMESTAMP '2024-01-02 03:04:05.123456'"
	const heldFraction = "2024-01-02 03:04:05.123456"
	lowered := []struct {
		name        string
		engine      temporalEngine
		before      string
		after       string
		value       string
		wantHeld    string
		wantRefusal string
	}{
		{"a timestamp's digits", oracleTime, "TIMESTAMP(6)", "TIMESTAMP(0)", fraction, heldFraction, "ORA-30082"},
		{"digits behind a zone", oracleTime, "TIMESTAMP(6) WITH TIME ZONE", "TIMESTAMP(0) WITH TIME ZONE", fraction, heldFraction, "ORA-30082"},
		{"any number to a size", oracleNumber, "NUMBER", "NUMBER(10,2)", "1.5", "1.5", "ORA-01440"},
		{"any number to no fraction", oracleNumber, "NUMBER", "NUMBER(*,0)", "2", "2", "ORA-01440"},
		{"a size to a lower size", oracleNumber, "NUMBER(12,2)", "NUMBER(10,2)", "1.5", "1.5", "ORA-01440"},
	}
	for _, row := range lowered {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)
			conn, address := oracleScratch(c)
			builtColumn(c, conn, row.engine, row.before, row.value)
			c.Assert(heldValue(c, conn, row.engine), qt.Equals, row.wantHeld)
			schema := schemaFile(c, fmt.Sprintf(row.engine.table, row.after))

			plan := planOf(c, address, schema)
			c.Assert(plan.Plan.Destructive, qt.IsTrue, qt.Commentf("%+v", plan.Plan.Statements))
			c.Assert(fmt.Sprint(plan.Plan.Statements), qt.Contains, narrowsReasonPrefix)

			refused := clirun.Run(c, clirun.Ptah, clirun.Options{}, "schema", "apply", "--db-url", address, "--schema-file", schema, "--auto-approve")
			c.Assert(refused.ExitCode, qt.Not(qt.Equals), 0)
			c.Assert(refused.Stdout+refused.Stderr, qt.Contains, row.wantRefusal)
			c.Assert(heldValue(c, conn, row.engine), qt.Equals, row.wantHeld)

			_, err := conn.ExecContext(c.Context(), "DELETE FROM t")
			c.Assert(err, qt.IsNil)
			c.Assert(planOf(c, address, schema).Plan.Destructive, qt.IsTrue)
			applySchema(c, address, schema)
			c.Assert(planOf(c, address, schema).Outcome, qt.Equals, "no-changes")
		})
	}
	raised := []struct {
		name      string
		engine    temporalEngine
		before    string
		after     string
		value     string
		wantHeld  string
		wantAfter string
	}{
		{"a timestamp's digits", oracleTime, "TIMESTAMP(0)", "TIMESTAMP(6)", "TIMESTAMP '2024-01-02 03:04:05'", "2024-01-02 03:04:05.000000", "2024-01-02 03:04:05.000000"},
		{"fewer digits to bare", oracleTime, "TIMESTAMP(3)", "TIMESTAMP", "TIMESTAMP '2024-01-02 03:04:05.123'", "2024-01-02 03:04:05.123000", "2024-01-02 03:04:05.123000"},
		{"a size to any number", oracleNumber, "NUMBER(10,2)", "NUMBER", "1.5", "1.5", "1.5"},
		{"no fraction to any number", oracleNumber, "NUMBER(*,0)", "NUMBER", "2", "2", "2"},
	}
	for _, row := range raised {
		t.Run(row.name+" goes up", func(t *testing.T) {
			c := qt.New(t)
			conn, address := oracleScratch(c)
			builtColumn(c, conn, row.engine, row.before, row.value)
			c.Assert(heldValue(c, conn, row.engine), qt.Equals, row.wantHeld)
			schema := schemaFile(c, fmt.Sprintf(row.engine.table, row.after))

			plan := planOf(c, address, schema)

			c.Assert(plan.Outcome, qt.Equals, "changes")
			c.Assert(plan.Plan.Destructive, qt.IsFalse, qt.Commentf("%+v", plan.Plan.Statements))
			applySchema(c, address, schema)
			c.Assert(heldValue(c, conn, row.engine), qt.Equals, row.wantAfter)
			c.Assert(planOf(c, address, schema).Outcome, qt.Equals, "no-changes")
		})
	}
}

// TestSchemaPlanE2E_ATypeAndItsSizeAreOneType builds a column under one spelling and
// declares the other. The catalog answers with the size the engine gave the column. Read
// as words, each row plans a statement that changes nothing on every run, which a person
// who adopted the database has no way to quiet.
func TestSchemaPlanE2E_ATypeAndItsSizeAreOneType(t *testing.T) {
	engines := []struct {
		name   string
		engine temporalEngine
		pairs  [][2]string
	}{
		{
			name: "postgres", engine: postgresTemporal,
			pairs: [][2]string{
				{"numeric", "numeric"}, {"numeric", "decimal"}, {"numeric(10,0)", "numeric(10)"}, {"numeric(10,2)[]", "numeric(10,2)[]"},
				{"char", "char"}, {"char(1)", "char"}, {"char(1)", "character"}, {"bpchar", "bpchar"}, {"bit", "bit"}, {"bit(1)", "bit"},
				{"double precision", "float"}, {"double precision", "float(53)"}, {"double precision", "float(25)"},
				{"real", "float(24)"}, {"real", "float(10)"},
			},
		},
		{
			name: "cockroachdb", engine: cockroachTemporal,
			pairs: [][2]string{
				{"decimal", "decimal"}, {"numeric", "decimal"}, {"decimal(10)", "decimal(10,0)"},
				{"char", "char"}, {"char(1)", "char"}, {"bit", "bit"}, {"float8", "float"}, {"float8", "float(53)"}, {"float4", "float(10)"},
				{"string(5)[]", "string(5)[]"}, {"string[]", "string[]"}, {"varchar(5)[]", "varchar(5)[]"},
			},
		},
		{
			name: "yugabytedb", engine: yugabyteTemporal,
			pairs: [][2]string{{"numeric", "numeric"}, {"numeric(10,0)", "numeric(10)"}, {"char(1)", "char"}, {"double precision", "float"}},
		},
		{
			name: "mysql", engine: mysqlTemporal,
			pairs: [][2]string{
				{"decimal", "decimal"}, {"decimal(10,0)", "decimal"}, {"decimal(10,0)", "numeric"}, {"decimal(5)", "decimal(5,0)"},
				{"char", "char"}, {"char(1)", "char"}, {"binary", "binary"}, {"binary(1)", "binary"}, {"bit", "bit"}, {"bit(1)", "bit"},
				{"float", "float(24)"}, {"float", "float(10)"}, {"double", "float(25)"}, {"double", "float(53)"},
				{"double", "real"}, {"double", "double precision"},
			},
		},
		{
			name: "mariadb", engine: mariadbTemporal,
			pairs: [][2]string{
				{"decimal", "decimal"}, {"decimal(10,0)", "decimal"}, {"char(1)", "char"}, {"binary(1)", "binary"}, {"bit(1)", "bit"},
				{"float", "float(24)"}, {"double", "float(53)"}, {"double", "real"}, {"year", "year"},
			},
		},
		{
			name: "sqlserver", engine: sqlServerTemporal,
			pairs: [][2]string{
				{"decimal", "decimal"}, {"decimal(18,0)", "decimal"}, {"numeric(18,0)", "numeric"}, {"numeric", "numeric"}, {"decimal(10)", "decimal(10,0)"},
				{"nvarchar(max)", "nvarchar(max)"}, {"varchar(1)", "varchar"}, {"varchar", "varchar"}, {"nvarchar(1)", "nvarchar"},
				{"char(1)", "char"}, {"char", "char"}, {"nchar(1)", "nchar"}, {"binary(1)", "binary"}, {"varbinary(1)", "varbinary"},
				{"float", "float(53)"}, {"float", "float(25)"}, {"real", "float(24)"}, {"real", "float(10)"}, {"float(10)", "float(10)"},
			},
		},
		{
			name: "clickhouse", engine: clickHouseTemporal,
			pairs: [][2]string{{"Decimal", "Decimal(10,0)"}, {"Decimal(10,0)", "Decimal"}, {"Decimal32(2)", "Decimal(9,2)"}},
		},
	}
	for _, engine := range engines {
		t.Run(engine.name, func(t *testing.T) {
			conn, address := temporalScratch(qt.New(t), engine.engine)
			for _, pair := range engine.pairs {
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
	oraclePairs := [][2]string{
		{"NUMBER", "NUMBER"}, {"NUMBER", "NUMBER(*)"}, {"NUMBER(10)", "NUMBER(10,0)"}, {"NUMBER(*,0)", "NUMBER(*,0)"},
		{"RAW(16)", "RAW(16)"}, {"RAW(16)", "UUID"},
		{"INTEGER", "INTEGER"}, {"NUMBER(10)", "INTEGER"}, {"SMALLINT", "SMALLINT"}, {"NUMBER(5)", "SMALLINT"},
		{"DECIMAL", "DECIMAL"}, {"NUMERIC", "NUMERIC"}, {"NUMBER", "DECIMAL"},
		{"DOUBLE PRECISION", "DOUBLE PRECISION"}, {"BINARY_DOUBLE", "DOUBLE PRECISION"},
		{"REAL", "REAL"}, {"BINARY_FLOAT", "REAL"}, {"BOOLEAN", "BOOLEAN"}, {"NUMBER(1)", "BOOLEAN"},
		{"FLOAT", "FLOAT"}, {"FLOAT(126)", "FLOAT"}, {"CHAR", "CHAR"}, {"CHAR(1)", "CHAR"}, {"NCHAR(1)", "NCHAR"},
		{"TIMESTAMP(6)", "TIMESTAMP"}, {"TIMESTAMP", "TIMESTAMP(6)"}, {"TIMESTAMP WITH TIME ZONE", "TIMESTAMP(6) WITH TIME ZONE"},
		{"INTERVAL DAY TO SECOND", "INTERVAL DAY TO SECOND"}, {"INTERVAL DAY(2) TO SECOND(6)", "INTERVAL DAY TO SECOND"},
		{"INTERVAL YEAR TO MONTH", "INTERVAL YEAR TO MONTH"}, {"INTERVAL YEAR(2) TO MONTH", "INTERVAL YEAR TO MONTH"},
	}
	t.Run("oracle", func(t *testing.T) {
		conn, address := oracleScratch(qt.New(t))
		for _, pair := range oraclePairs {
			t.Run(pair[0]+" as "+pair[1], func(t *testing.T) {
				c := qt.New(t)
				_, err := conn.ExecContext(c.Context(), "DROP TABLE IF EXISTS t")
				c.Assert(err, qt.IsNil)
				_, err = conn.ExecContext(c.Context(), fmt.Sprintf(oracleTypeTable, pair[0]))
				c.Assert(err, qt.IsNil)

				plan := planOf(c, address, schemaFile(c, fmt.Sprintf(oracleTypeTable, pair[1])))

				c.Assert(plan.Outcome, qt.Equals, "no-changes", qt.Commentf("%+v", plan.Plan.Statements))
			})
		}
	})
}

// TestRatingE2E_ASizeOnOneSideReachesEveryReader asks each surface that reads the rating
// about a change that a comparison of words reads as synced. A file holds no previous type
// to compare with, so this test does not ask lint.
func TestRatingE2E_ASizeOnOneSideReachesEveryReader(t *testing.T) {
	engines := []struct {
		name            string
		engine          temporalEngine
		dialect, before string
		narrow          string
	}{
		{name: "postgres", engine: postgresTemporal, dialect: "postgres", before: "numeric", narrow: "numeric(10,2)"},
		{name: "mysql", engine: mysqlTemporal, dialect: "mysql", before: "decimal(10,2)", narrow: "decimal"},
		{name: "sqlserver", engine: sqlServerTemporal, dialect: "sqlserver", before: "nvarchar(max)", narrow: "nvarchar(50)"},
	}
	for _, engine := range engines {
		t.Run(engine.name, func(t *testing.T) {
			c := qt.New(t)
			conn, address := temporalScratch(c, engine.engine)
			_, err := conn.ExecContext(c.Context(), fmt.Sprintf(engine.engine.table, engine.before))
			c.Assert(err, qt.IsNil)
			narrowingReachesEveryReader(c, address, fmt.Sprintf(engine.engine.table, engine.narrow))
		})
	}
	t.Run("oracle", func(t *testing.T) {
		c := qt.New(t)
		conn, address := oracleScratch(c)
		_, err := conn.ExecContext(c.Context(), fmt.Sprintf(oracleTypeTable, "TIMESTAMP(6)"))
		c.Assert(err, qt.IsNil)
		narrowingReachesEveryReader(c, address, fmt.Sprintf(oracleTypeTable, "TIMESTAMP(0)"))
	})
}

// narrowingReachesEveryReader holds the safety report, the --check-destructive gate on
// plan and generate, and the drift report to one narrowing plan.
func narrowingReachesEveryReader(c *qt.C, address, declaration string) {
	c.Helper()
	narrow := schemaFile(c, declaration)
	ptah := func(args ...string) clirun.Result {
		return clirun.Run(c, clirun.Ptah, clirun.Options{Dir: c.TempDir()}, args...)
	}
	planFlags := func(more ...string) []string {
		return append([]string{"migrations", "plan", "--db-url", address, "--schema-file", narrow}, more...)
	}
	report := ptah(planFlags("--report", "json")...)
	c.Assert(report.ExitCode, qt.Equals, 0, qt.Commentf("%s", report.Stderr))
	c.Assert(report.Stdout, qt.Contains, `"destructive": true`)
	c.Assert(report.Stdout, qt.Contains, narrowsReasonPrefix)
	refused := ptah(planFlags("--check-destructive")...)
	c.Assert(refused.ExitCode, qt.Not(qt.Equals), 0)
	c.Assert(refused.Stderr, qt.Contains, "destructive migration statements require --allow-destructive")
	c.Assert(ptah(planFlags("--check-destructive", "--allow-destructive")...).ExitCode, qt.Equals, 0)
	generated := ptah("migrations", "generate", "--db-url", address, "--schema-file", narrow, "--migrations-dir", c.TempDir(), "--check-destructive")
	c.Assert(generated.ExitCode, qt.Not(qt.Equals), 0)
	drift := ptah("schema", "drift", "--db-url", address, "--schema-file", narrow, "--format", "json", "--severity", "destructive")
	c.Assert(drift.ExitCode, qt.Not(qt.Equals), 0)
	c.Assert(drift.Stdout, qt.Contains, "column_types_narrowed")
}

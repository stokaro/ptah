//go:build integration

package safety_test

import (
	"context"
	"encoding/json"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"ptah.run/dbschema"
	"ptah.run/internal/clirun"
	"ptah.run/internal/dbtarget"
)

// temporalEngine is a server to build a column on: the address that may create a
// database, how it reads column c back as text, and how it builds table t.
type temporalEngine struct {
	engine dbtarget.Engine
	read   string
	table  string
}

var (
	postgresTemporal  = temporalEngine{engine: dbtarget.PostgreSQL, read: "SELECT c::text FROM t", table: temporalTable}
	cockroachTemporal = temporalEngine{engine: dbtarget.CockroachDB, read: "SELECT c::text FROM t", table: temporalTable}
	yugabyteTemporal  = temporalEngine{engine: dbtarget.YugabyteDB, read: "SELECT c::text FROM t", table: temporalTable}
	mysqlTemporal     = temporalEngine{engine: dbtarget.MySQLAdmin, read: "SELECT CAST(c AS CHAR) FROM t", table: temporalTable}
	mariadbTemporal   = temporalEngine{engine: dbtarget.MariaDBAdmin, read: "SELECT CAST(c AS CHAR) FROM t", table: temporalTable}
	sqlServerTemporal = temporalEngine{
		engine: dbtarget.SQLServer, read: "SELECT CONVERT(varchar(50), c, 121) FROM t", table: temporalTable,
	}
	clickHouseTemporal = temporalEngine{
		engine: dbtarget.ClickHouse, read: "SELECT toString(c) FROM t",
		table: "CREATE TABLE t (id Int32, c %s) ENGINE = MergeTree ORDER BY id",
	}
)

const temporalTable = "CREATE TABLE t (id int PRIMARY KEY, c %s)"

// sqlServerDatabaseRe finds the database a SQL Server URL names.
var sqlServerDatabaseRe = regexp.MustCompile(`database=[^&]*`)

// addressOfDatabase is the address of the database name on the server of address, which
// each engine family spells in its own place.
func addressOfDatabase(c *qt.C, address, name string) string {
	c.Helper()
	if sqlServerDatabaseRe.MatchString(address) {
		return sqlServerDatabaseRe.ReplaceAllString(address, "database="+name)
	}
	if strings.Contains(address, "@tcp(") {
		return address[:strings.LastIndex(address, "/")+1] + name
	}
	parsed, err := url.Parse(address)
	c.Assert(err, qt.IsNil)
	parsed.Path = "/" + name
	return parsed.String()
}

// temporalScratch is a database of its own on the engine, dropped when the test ends. A
// plan reads every table of what it connects to, and the servers are shared.
func temporalScratch(c *qt.C, engine temporalEngine) (*dbschema.DatabaseConnection, string) {
	c.Helper()
	address := dbtarget.URL(c, engine.engine)
	admin, err := dbschema.ConnectToDatabase(c.Context(), address)
	c.Assert(err, qt.IsNil)
	name := fmt.Sprintf("ptah_narrowing_%d_%d", os.Getpid(), time.Now().UnixNano())
	_, err = admin.ExecContext(c.Context(), "CREATE DATABASE "+name)
	c.Assert(err, qt.IsNil)
	scratchAddress := addressOfDatabase(c, address, name)
	conn, err := dbschema.ConnectToDatabase(c.Context(), scratchAddress)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() {
		dbschema.CloseAndWarn(conn)
		_, dropErr := admin.ExecContext(context.Background(), "DROP DATABASE "+name)
		c.Check(dropErr, qt.IsNil)
		dbschema.CloseAndWarn(admin)
	})
	return conn, scratchAddress
}

// plannedStatement and planOutcome are what `schema plan --json` says about a plan.
type plannedStatement struct {
	SQL      string `json:"sql"`
	Severity string `json:"severity"`
	Reason   string `json:"reason"`
}

type planOutcome struct {
	Outcome string `json:"outcome"`
	Plan    struct {
		Destructive bool               `json:"destructive"`
		Statements  []plannedStatement `json:"statements"`
	} `json:"plan"`
}

// schemaFile writes the statement as a desired schema and returns its path.
func schemaFile(c *qt.C, statement string) string {
	c.Helper()
	path := filepath.Join(c.TempDir(), "schema.sql")
	c.Assert(os.WriteFile(path, []byte(statement+";\n"), 0o600), qt.IsNil)
	return path
}

// planOf runs `schema plan` through the shipped binary.
func planOf(c *qt.C, address, schema string) planOutcome {
	c.Helper()
	run := clirun.Run(c, clirun.Ptah, clirun.Options{}, "schema", "plan", "--db-url", address, "--schema-file", schema, "--dry-run", "--json")
	c.Assert(run.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", run.Stdout, run.Stderr))
	var outcome planOutcome
	c.Assert(json.Unmarshal([]byte(run.Stdout), &outcome), qt.IsNil)
	return outcome
}

// applySchema runs `schema apply` through the shipped binary.
func applySchema(c *qt.C, address, schema string) {
	c.Helper()
	run := clirun.Run(c, clirun.Ptah, clirun.Options{}, "schema", "apply", "--db-url", address, "--schema-file", schema, "--auto-approve")
	c.Assert(run.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", run.Stdout, run.Stderr))
}

// builtColumn replaces table t with one whose column c is of the type, holding one row.
func builtColumn(c *qt.C, conn *dbschema.DatabaseConnection, engine temporalEngine, columnType, value string) {
	c.Helper()
	_, err := conn.ExecContext(c.Context(), "DROP TABLE IF EXISTS t")
	c.Assert(err, qt.IsNil)
	_, err = conn.ExecContext(c.Context(), fmt.Sprintf(engine.table, columnType))
	c.Assert(err, qt.IsNil)
	_, err = conn.ExecContext(c.Context(), "INSERT INTO t (id, c) VALUES (1, "+value+")")
	c.Assert(err, qt.IsNil)
}

func heldValue(c *qt.C, conn *dbschema.DatabaseConnection, engine temporalEngine) string {
	c.Helper()
	var value string
	c.Assert(conn.QueryRowContext(c.Context(), engine.read).Scan(&value), qt.IsNil)
	return value
}

// A temporal row inserts a value, reads it back, plans the declared type, applies the
// plan and reads the value again. The two reads are what the rating answers to: a plan
// that loses part of the value has to say so before it runs, and one that keeps all of
// it must not, because a false destructive rating stops a plan as well.
type temporalRow struct {
	before, after, value, wantHeld, wantAfter string
	wantDestructive                           bool
}

const narrowsReasonPrefix = "column type narrows from "

// TestSchemaPlanE2E_TheRatingAnswersToWhatTheServerKeeps runs each engine's rows through
// `schema plan` and `schema apply`. The values under wantAfter are the server's: it was
// asked to change a type, and nothing here writes them.
func TestSchemaPlanE2E_TheRatingAnswersToWhatTheServerKeeps(t *testing.T) {
	const (
		micro = "'2024-01-02 03:04:05.123456'"
		milli = "'2024-01-02 03:04:05.123'"
		whole = "'2024-01-02 03:04:05'"
	)
	engines := []struct {
		name   string
		engine temporalEngine
		rows   []temporalRow
	}{
		{
			name: "postgres", engine: postgresTemporal,
			rows: []temporalRow{
				{"timestamp(6)", "timestamp(0)", micro, "2024-01-02 03:04:05.123456", "2024-01-02 03:04:05", true},
				{"timestamp", "timestamp(3)", micro, "2024-01-02 03:04:05.123456", "2024-01-02 03:04:05.123", true},
				{"timestamp(3)", "timestamp(6)", milli, "2024-01-02 03:04:05.123", "2024-01-02 03:04:05.123", false},
				{"timestamp(3)", "timestamp", milli, "2024-01-02 03:04:05.123", "2024-01-02 03:04:05.123", false},
				{"timestamptz(6)", "timestamptz(0)", "'2024-01-02 03:04:05.123456+00'", "2024-01-02 03:04:05.123456+00", "2024-01-02 03:04:05+00", true},
				{"timestamp with time zone", "timestamptz(3)", "'2024-01-02 03:04:05.123456+00'", "2024-01-02 03:04:05.123456+00", "2024-01-02 03:04:05.123+00", true},
				{"time(6)", "time(0)", "'03:04:05.123456'", "03:04:05.123456", "03:04:05", true},
				{"time", "time(3)", "'03:04:05.123456'", "03:04:05.123456", "03:04:05.123", true},
				{"time(3)", "time", "'03:04:05.123'", "03:04:05.123", "03:04:05.123", false},
				{"timetz(6)", "timetz(2)", "'03:04:05.123456+00'", "03:04:05.123456+00", "03:04:05.12+00", true},
				{"timetz", "time", "'03:04:05.123456+00'", "03:04:05.123456+00", "03:04:05.123456", true},
				{"interval(6)", "interval(0)", "'1 day 03:04:05.123456'", "1 day 03:04:05.123456", "1 day 03:04:05", true},
				{"interval(3)", "interval", "'1 day 03:04:05.123'", "1 day 03:04:05.123", "1 day 03:04:05.123", false},
				{"interval", "interval year to month", "'2 years 3 mons 4 days'", "2 years 3 mons 4 days", "2 years 3 mons", true},
				{"interval", "interval day to second(3)", "'1 day 03:04:05.123456'", "1 day 03:04:05.123456", "1 day 03:04:05.123", true},
				{"interval hour to minute", "interval", "'1 day 03:04:00'", "1 day 03:04:00", "1 day 03:04:00", false},
				{"timestamp(3)[]", "timestamp(0)[]", "ARRAY['2024-01-02 03:04:05.123']::timestamp(3)[]", `{"2024-01-02 03:04:05.123"}`, `{"2024-01-02 03:04:05"}`, true},
				{"timestamp(0)[]", "timestamp(3)[]", "ARRAY['2024-01-02 03:04:05']::timestamp(0)[]", `{"2024-01-02 03:04:05"}`, `{"2024-01-02 03:04:05"}`, false},
				{"timestamp", "date", micro, "2024-01-02 03:04:05.123456", "2024-01-02", true},
				{"date", "timestamp", "'2024-01-02'", "2024-01-02", "2024-01-02 00:00:00", false},
				{"timestamp(6)", "time(6)", micro, "2024-01-02 03:04:05.123456", "03:04:05.123456", true},
			},
		},
		{
			name: "cockroachdb", engine: cockroachTemporal,
			rows: []temporalRow{
				{"timestamp(6)", "timestamp(0)", micro, "2024-01-02 03:04:05.123456", "2024-01-02 03:04:05", true},
				{"timestamp", "timestamp(3)", micro, "2024-01-02 03:04:05.123456", "2024-01-02 03:04:05.123", true},
				{"timestamp(3)", "timestamp", milli, "2024-01-02 03:04:05.123", "2024-01-02 03:04:05.123", false},
				{"time(6)", "time(0)", "'03:04:05.123456'", "03:04:05.123456", "03:04:05", true},
				{"interval(3)", "interval", "'1 day 03:04:05.123'", "1 day 03:04:05.123", "1 day 03:04:05.123", false},
				{"interval", "interval year to month", "'2 years 3 mons 4 days'", "2 years 3 mons 4 days", "2 years 3 mons", true},
				{"interval hour to minute", "interval", "'1 day 03:04:00'", "1 day 03:04:00", "1 day 03:04:00", false},
			},
		},
		{
			name: "yugabytedb", engine: yugabyteTemporal,
			rows: []temporalRow{
				{"timestamp(6)", "timestamp(0)", micro, "2024-01-02 03:04:05.123456", "2024-01-02 03:04:05", true},
				{"timestamp(3)", "timestamp", milli, "2024-01-02 03:04:05.123", "2024-01-02 03:04:05.123", false},
			},
		},
		{
			name: "mysql", engine: mysqlTemporal,
			rows: []temporalRow{
				{"timestamp(6) NULL", "timestamp NULL", micro, "2024-01-02 03:04:05.123456", "2024-01-02 03:04:05", true},
				{"timestamp(6) NULL", "timestamp(3) NULL", micro, "2024-01-02 03:04:05.123456", "2024-01-02 03:04:05.123", true},
				{"timestamp NULL", "timestamp(3) NULL", whole, "2024-01-02 03:04:05", "2024-01-02 03:04:05.000", false},
				{"datetime(6)", "datetime", micro, "2024-01-02 03:04:05.123456", "2024-01-02 03:04:05", true},
				{"datetime(6)", "datetime(0)", micro, "2024-01-02 03:04:05.123456", "2024-01-02 03:04:05", true},
				{"datetime(3)", "datetime(6)", milli, "2024-01-02 03:04:05.123", "2024-01-02 03:04:05.123000", false},
				{"datetime", "datetime(3)", whole, "2024-01-02 03:04:05", "2024-01-02 03:04:05.000", false},
				{"time(6)", "time", "'03:04:05.123456'", "03:04:05.123456", "03:04:05", true},
				{"datetime(6)", "date", micro, "2024-01-02 03:04:05.123456", "2024-01-02", true},
				{"timestamp(6) NULL", "datetime(6)", micro, "2024-01-02 03:04:05.123456", "2024-01-02 03:04:05.123456", false},
				{"timestamp(6) NULL", "datetime(0)", micro, "2024-01-02 03:04:05.123456", "2024-01-02 03:04:05", true},
			},
		},
		{
			name: "mariadb", engine: mariadbTemporal,
			rows: []temporalRow{
				{"timestamp(6) NULL", "timestamp NULL", micro, "2024-01-02 03:04:05.123456", "2024-01-02 03:04:05", true},
				{"datetime(6)", "datetime", micro, "2024-01-02 03:04:05.123456", "2024-01-02 03:04:05", true},
				{"datetime(3)", "datetime(6)", milli, "2024-01-02 03:04:05.123", "2024-01-02 03:04:05.123000", false},
				{"time(6)", "time", "'03:04:05.123456'", "03:04:05.123456", "03:04:05", true},
			},
		},
		{
			name: "sqlserver", engine: sqlServerTemporal,
			rows: []temporalRow{
				{"datetime2(7)", "datetime2(0)", "'2024-01-02 03:04:05.1234567'", "2024-01-02 03:04:05.1234567", "2024-01-02 03:04:05", true},
				{"datetime2", "datetime2(3)", "'2024-01-02 03:04:05.1234567'", "2024-01-02 03:04:05.1234567", "2024-01-02 03:04:05.123", true},
				{"datetime2(3)", "datetime2(7)", milli, "2024-01-02 03:04:05.123", "2024-01-02 03:04:05.1230000", false},
				{"datetime2(3)", "datetime2", milli, "2024-01-02 03:04:05.123", "2024-01-02 03:04:05.1230000", false},
				{"time(7)", "time(0)", "'03:04:05.1234567'", "03:04:05.1234567", "03:04:05", true},
				{"datetimeoffset(7)", "datetimeoffset(0)", "'2024-01-02 03:04:05.1234567 +01:00'", "2024-01-02 03:04:05.1234567 +01:00", "2024-01-02 03:04:05 +01:00", true},
				{"datetime2(7)", "datetime", "'2024-01-02 03:04:05.1234567'", "2024-01-02 03:04:05.1234567", "2024-01-02 03:04:05.123", true},
				{"datetime", "datetime2(7)", milli, "2024-01-02 03:04:05.123", "2024-01-02 03:04:05.1233333", false},
				{"datetime", "smalldatetime", milli, "2024-01-02 03:04:05.123", "2024-01-02 03:04:00.000", true},
				{"datetime2(7)", "date", "'2024-01-02 03:04:05.1234567'", "2024-01-02 03:04:05.1234567", "2024-01-02", true},
				{"datetimeoffset(7)", "datetime2(7)", "'2024-01-02 03:04:05.1234567 +01:00'", "2024-01-02 03:04:05.1234567 +01:00", "2024-01-02 03:04:05.1234567", true},
			},
		},
		{
			name: "clickhouse", engine: clickHouseTemporal,
			rows: []temporalRow{
				{"DateTime64(6)", "DateTime64(3)", micro, "2024-01-02 03:04:05.123456", "2024-01-02 03:04:05.123", true},
				{"DateTime64(3)", "DateTime64(6)", milli, "2024-01-02 03:04:05.123", "2024-01-02 03:04:05.123000", false},
				{"DateTime64(3)", "DateTime", milli, "2024-01-02 03:04:05.123", "2024-01-02 03:04:05", true},
				{"DateTime", "Date", whole, "2024-01-02 03:04:05", "2024-01-02", true},
				{"Nullable(DateTime64(6))", "Nullable(DateTime64(0))", micro, "2024-01-02 03:04:05.123456", "2024-01-02 03:04:05", true},
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

					c.Assert(plan.Plan.Destructive, qt.Equals, row.wantDestructive, qt.Commentf("%+v", plan.Plan.Statements))
					c.Assert(strings.Contains(fmt.Sprint(plan.Plan.Statements), narrowsReasonPrefix), qt.Equals, row.wantDestructive)
					applySchema(c, address, schema)
					c.Assert(heldValue(c, conn, engine.engine), qt.Equals, row.wantAfter)
					c.Assert(planOf(c, address, schema).Outcome, qt.Equals, "no-changes")
				})
			}
		})
	}
}

// TestSchemaPlanE2E_OneTypeInTwoSpellingsPlansNothing builds a column under one spelling
// and declares the other. The comparison has no change to rate here, and a read of the
// catalog that lost a precision or an interval's fields would report one on every run,
// which is the false destructive rating that stops every plan on an adopted database.
func TestSchemaPlanE2E_OneTypeInTwoSpellingsPlansNothing(t *testing.T) {
	engines := []struct {
		name   string
		engine temporalEngine
		pairs  [][2]string
	}{
		{
			name: "postgres", engine: postgresTemporal,
			pairs: [][2]string{
				{"timestamp", "timestamp(6)"}, {"timestamp(6)", "timestamp"},
				{"timestamptz", "timestamp with time zone"}, {"timestamp(3) with time zone", "timestamptz(3)"},
				{"time", "time(6)"}, {"timetz", "time with time zone"},
				{"interval", "interval(6)"}, {"interval(6)", "interval"}, {"interval(3)", "interval(3)"},
				{"interval year to month", "interval year to month"},
				{"interval day to second(3)", "interval day to second(3)"},
				{"interval day to second(6)", "interval day to second"},
				{"timestamp(3)[]", "timestamp(3)[]"},
			},
		},
		{
			name: "cockroachdb", engine: cockroachTemporal,
			pairs: [][2]string{
				{"timestamp", "timestamp(6)"}, {"timestamp(6)", "timestamp"}, {"timestamptz", "timestamp with time zone"},
				{"interval", "interval(6)"}, {"interval year to month", "interval year to month"},
				{"interval day to second(3)", "interval day to second(3)"},
			},
		},
		{
			name: "yugabytedb", engine: yugabyteTemporal,
			pairs: [][2]string{
				{"timestamp", "timestamp(6)"}, {"timestamptz", "timestamp with time zone"},
				{"interval year to month", "interval year to month"},
			},
		},
		{
			name: "mysql", engine: mysqlTemporal,
			pairs: [][2]string{
				{"timestamp NULL", "timestamp(0) NULL"}, {"timestamp(0) NULL", "timestamp NULL"},
				{"datetime", "datetime(0)"}, {"datetime(0)", "datetime"}, {"datetime(3)", "datetime(3)"}, {"time", "time(0)"},
			},
		},
		{
			name: "mariadb", engine: mariadbTemporal,
			pairs: [][2]string{{"timestamp NULL", "timestamp(0) NULL"}, {"datetime", "datetime(0)"}, {"time", "time(0)"}},
		},
		{
			name: "sqlserver", engine: sqlServerTemporal,
			pairs: [][2]string{
				{"datetime2", "datetime2(7)"}, {"datetime2(7)", "datetime2"}, {"datetime2(3)", "datetime2(3)"}, {"datetime2(0)", "datetime2(0)"},
				{"time", "time(7)"}, {"time(3)", "time(3)"}, {"datetimeoffset", "datetimeoffset(7)"}, {"datetimeoffset(3)", "datetimeoffset(3)"},
				{"datetime", "datetime"}, {"smalldatetime", "smalldatetime"},
			},
		},
		{
			name: "clickhouse", engine: clickHouseTemporal,
			pairs: [][2]string{{"DateTime64", "DateTime64(3)"}, {"DateTime64(3)", "DateTime64"}},
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
}

// TestRatingE2E_ANarrowingReachesEveryReader asks each surface that reads the rating
// about the same plan, on an engine where the type a declaration leaves bare holds fewer
// digits and on one where it holds as many. Lint reports the type change by DS103
// whichever way it goes: a file holds no previous type to compare with. DS103 reads
// ALTER COLUMN ... TYPE, MODIFY and CHANGE, and SQL Server's ALTER COLUMN is none of them,
// so its row asks only that lint ran.
func TestRatingE2E_ANarrowingReachesEveryReader(t *testing.T) {
	engines := []struct {
		name            string
		engine          temporalEngine
		dialect, before string
		narrow, widen   string
		wantLint        string
	}{
		{name: "postgres", engine: postgresTemporal, dialect: "postgres", before: "timestamp(3)", narrow: "timestamp(0)", widen: "timestamp", wantLint: `"DS103"`},
		{name: "mysql", engine: mysqlTemporal, dialect: "mysql", before: "datetime(3)", narrow: "datetime", widen: "datetime(6)", wantLint: `"DS103"`},
		{name: "sqlserver", engine: sqlServerTemporal, dialect: "sqlserver", before: "datetime2(3)", narrow: "datetime2(0)", widen: "datetime2", wantLint: `"findings"`},
	}
	for _, engine := range engines {
		t.Run(engine.name, func(t *testing.T) {
			c := qt.New(t)
			conn, address := temporalScratch(c, engine.engine)
			_, err := conn.ExecContext(c.Context(), fmt.Sprintf(engine.engine.table, engine.before))
			c.Assert(err, qt.IsNil)
			narrow := schemaFile(c, fmt.Sprintf(engine.engine.table, engine.narrow))
			widen := schemaFile(c, fmt.Sprintf(engine.engine.table, engine.widen))
			ptah := func(args ...string) clirun.Result {
				return clirun.Run(c, clirun.Ptah, clirun.Options{Dir: c.TempDir()}, args...)
			}
			planFlags := func(schema string, more ...string) []string {
				return append([]string{"migrations", "plan", "--db-url", address, "--schema-file", schema}, more...)
			}

			// The safety report, and the --check-destructive gate that reads it.
			narrowReport := ptah(planFlags(narrow, "--report", "json")...)
			c.Assert(narrowReport.ExitCode, qt.Equals, 0, qt.Commentf("%s", narrowReport.Stderr))
			c.Assert(narrowReport.Stdout, qt.Contains, `"destructive": true`)
			c.Assert(narrowReport.Stdout, qt.Contains, narrowsReasonPrefix)
			refused := ptah(planFlags(narrow, "--check-destructive")...)
			c.Assert(refused.ExitCode, qt.Not(qt.Equals), 0)
			c.Assert(refused.Stderr, qt.Contains, "destructive migration statements require --allow-destructive")
			c.Assert(ptah(planFlags(narrow, "--check-destructive", "--allow-destructive")...).ExitCode, qt.Equals, 0)
			widenReport := ptah(planFlags(widen, "--report", "json", "--check-destructive")...)
			c.Assert(widenReport.ExitCode, qt.Equals, 0, qt.Commentf("%s", widenReport.Stderr))
			c.Assert(widenReport.Stdout, qt.Contains, `"destructive": false`)

			// Generation, which writes the file lint and `migrations up` read next.
			dir := c.TempDir()
			generateFlags := []string{"migrations", "generate", "--db-url", address, "--schema-file", narrow, "--migrations-dir", dir, "--check-destructive"}
			c.Assert(ptah(generateFlags...).ExitCode, qt.Not(qt.Equals), 0)
			generated := ptah(append(generateFlags, "--allow-destructive")...)
			c.Assert(generated.ExitCode, qt.Equals, 0, qt.Commentf("%s", generated.Stderr))
			lint := ptah("migrations", "lint", "--dir", dir, "--dialect", engine.dialect, "--format", "json")
			c.Assert(lint.Stdout, qt.Contains, engine.wantLint)

			// The drift report, whose threshold a columns_modified warning would pass.
			narrowDrift := ptah("schema", "drift", "--db-url", address, "--schema-file", narrow, "--format", "json", "--severity", "destructive")
			c.Assert(narrowDrift.ExitCode, qt.Not(qt.Equals), 0)
			c.Assert(narrowDrift.Stdout, qt.Contains, "column_types_narrowed")
			widenDrift := ptah("schema", "drift", "--db-url", address, "--schema-file", widen, "--format", "json", "--severity", "destructive")
			c.Assert(widenDrift.ExitCode, qt.Equals, 0, qt.Commentf("%s", widenDrift.Stdout))
			c.Assert(widenDrift.Stdout, qt.Not(qt.Contains), "column_types_narrowed")
		})
	}
}

// TestSchemaPlanE2E_TheRatingAnswersToWhatAnInsertDoesNext builds a column, plans the
// declaration, applies it, and tries an insert each way. No row of these is lost. The
// plan is rated by what a writer that worked before can no longer do, and DROP IDENTITY
// takes the sequence with it as DROP SEQUENCE does.
func TestSchemaPlanE2E_TheRatingAnswersToWhatAnInsertDoesNext(t *testing.T) {
	engines := []struct {
		name   string
		engine temporalEngine
	}{
		{name: "postgres", engine: postgresTemporal},
		{name: "cockroachdb", engine: cockroachTemporal},
	}
	rows := []struct {
		name, before, after  string
		wantDestructive      bool
		wantHighestStatement string
		wantOmitted          bool
		wantNamed            bool
	}{
		{
			name: "an always identity leaves", before: "bigint GENERATED ALWAYS AS IDENTITY", after: "bigint NOT NULL",
			wantDestructive: true, wantHighestStatement: "destructive", wantOmitted: false, wantNamed: true,
		},
		{
			name: "a by-default identity leaves", before: "bigint GENERATED BY DEFAULT AS IDENTITY", after: "bigint NOT NULL",
			wantDestructive: true, wantHighestStatement: "destructive", wantOmitted: false, wantNamed: true,
		},
		{
			name: "a literal default leaves", before: "bigint NOT NULL DEFAULT 7", after: "bigint NOT NULL",
			wantDestructive: false, wantHighestStatement: "warning", wantOmitted: false, wantNamed: true,
		},
		{
			name: "a by-default identity becomes always", before: "bigint GENERATED BY DEFAULT AS IDENTITY", after: "bigint GENERATED ALWAYS AS IDENTITY",
			wantDestructive: false, wantHighestStatement: "warning", wantOmitted: true, wantNamed: false,
		},
		{
			name: "an always identity becomes by default", before: "bigint GENERATED ALWAYS AS IDENTITY", after: "bigint GENERATED BY DEFAULT AS IDENTITY",
			wantDestructive: false, wantHighestStatement: "warning", wantOmitted: true, wantNamed: true,
		},
		{
			name: "a default is set", before: "bigint NOT NULL", after: "bigint NOT NULL DEFAULT 7",
			wantDestructive: false, wantHighestStatement: "safe", wantOmitted: true, wantNamed: true,
		},
	}
	for _, engine := range engines {
		t.Run(engine.name, func(t *testing.T) {
			conn, address := temporalScratch(qt.New(t), engine.engine)
			for _, row := range rows {
				t.Run(row.name, func(t *testing.T) {
					c := qt.New(t)
					_, err := conn.ExecContext(c.Context(), "DROP TABLE IF EXISTS t")
					c.Assert(err, qt.IsNil)
					_, err = conn.ExecContext(c.Context(), fmt.Sprintf(engine.engine.table, row.before))
					c.Assert(err, qt.IsNil)
					schema := schemaFile(c, fmt.Sprintf(engine.engine.table, row.after))

					plan := planOf(c, address, schema)

					c.Assert(plan.Plan.Destructive, qt.Equals, row.wantDestructive, qt.Commentf("%+v", plan.Plan.Statements))
					c.Assert(highestSeverity(plan.Plan.Statements), qt.Equals, row.wantHighestStatement, qt.Commentf("%+v", plan.Plan.Statements))
					applySchema(c, address, schema)
					_, omittedErr := conn.ExecContext(c.Context(), "INSERT INTO t (id) VALUES (1)")
					c.Assert(omittedErr == nil, qt.Equals, row.wantOmitted, qt.Commentf("%v", omittedErr))
					_, namedErr := conn.ExecContext(c.Context(), "INSERT INTO t (id, c) VALUES (2, 200)")
					c.Assert(namedErr == nil, qt.Equals, row.wantNamed, qt.Commentf("%v", namedErr))
				})
			}
		})
	}
}

// TestSchemaPlanE2E_ASerialColumnGivesUpItsSequence is PostgreSQL's serial, where the
// comparison drops the default and the sequence it owned. CockroachDB's serial owns no
// sequence and its row above is the default.
func TestSchemaPlanE2E_ASerialColumnGivesUpItsSequence(t *testing.T) {
	c := qt.New(t)
	conn, address := temporalScratch(c, postgresTemporal)
	_, err := conn.ExecContext(c.Context(), fmt.Sprintf(postgresTemporal.table, "bigserial"))
	c.Assert(err, qt.IsNil)
	_, err = conn.ExecContext(c.Context(), "INSERT INTO t (id) VALUES (1)")
	c.Assert(err, qt.IsNil)
	schema := schemaFile(c, fmt.Sprintf(postgresTemporal.table, "bigint NOT NULL"))

	plan := planOf(c, address, schema)

	c.Assert(plan.Plan.Destructive, qt.IsTrue)
	c.Assert(severities(plan.Plan.Statements), qt.DeepEquals, []string{"warning", "destructive"}, qt.Commentf("%+v", plan.Plan.Statements))
	applySchema(c, address, schema)
	var held int
	c.Assert(conn.QueryRowContext(c.Context(), "SELECT c FROM t WHERE id = 1").Scan(&held), qt.IsNil)
	c.Assert(held, qt.Equals, 1)
	_, omittedErr := conn.ExecContext(c.Context(), "INSERT INTO t (id) VALUES (2)")
	c.Assert(omittedErr, qt.ErrorMatches, `(?s).*23502.*`)
}

// TestSchemaPlanE2E_ARestatedColumnGivesItUp is MySQL's and MariaDB's spelling of the
// rows above. MODIFY COLUMN states the whole column, so its words read the same whatever
// it gives up, and only the node's verdict can say.
func TestSchemaPlanE2E_ARestatedColumnGivesItUp(t *testing.T) {
	engines := []struct {
		name   string
		engine temporalEngine
	}{
		{name: "mysql", engine: mysqlTemporal},
		{name: "mariadb", engine: mariadbTemporal},
	}
	rows := []struct {
		name, before, after string
		wantSeverities      []string
		wantDestructive     bool
		wantOmitted         bool
	}{
		{
			name: "a default leaves", before: "bigint NOT NULL DEFAULT 7", after: "bigint NOT NULL",
			wantSeverities: []string{"warning"}, wantDestructive: false, wantOmitted: false,
		},
		{
			name: "a default leaves a nullable column", before: "bigint DEFAULT 7", after: "bigint",
			wantSeverities: []string{"warning"}, wantDestructive: false, wantOmitted: true,
		},
		{
			name: "NOT NULL leaves", before: "bigint NOT NULL", after: "bigint",
			wantSeverities: []string{"destructive"}, wantDestructive: true, wantOmitted: true,
		},
	}
	for _, engine := range engines {
		t.Run(engine.name, func(t *testing.T) {
			conn, address := temporalScratch(qt.New(t), engine.engine)
			for _, row := range rows {
				t.Run(row.name, func(t *testing.T) {
					c := qt.New(t)
					_, err := conn.ExecContext(c.Context(), "DROP TABLE IF EXISTS t")
					c.Assert(err, qt.IsNil)
					_, err = conn.ExecContext(c.Context(), fmt.Sprintf(engine.engine.table, row.before))
					c.Assert(err, qt.IsNil)
					schema := schemaFile(c, fmt.Sprintf(engine.engine.table, row.after))

					plan := planOf(c, address, schema)

					c.Assert(plan.Plan.Destructive, qt.Equals, row.wantDestructive, qt.Commentf("%+v", plan.Plan.Statements))
					c.Assert(severities(plan.Plan.Statements), qt.DeepEquals, row.wantSeverities, qt.Commentf("%+v", plan.Plan.Statements))
					applySchema(c, address, schema)
					_, omittedErr := conn.ExecContext(c.Context(), "INSERT INTO t (id) VALUES (1)")
					c.Assert(omittedErr == nil, qt.Equals, row.wantOmitted, qt.Commentf("%v", omittedErr))
				})
			}
		})
	}
}

func severities(statements []plannedStatement) []string {
	got := make([]string, 0, len(statements))
	for _, statement := range statements {
		got = append(got, statement.Severity)
	}
	return got
}

// highestSeverity is the strongest rating among statements, "safe" among none.
func highestSeverity(statements []plannedStatement) string {
	found := severities(statements)
	switch {
	case slices.Contains(found, "destructive"):
		return "destructive"
	case slices.Contains(found, "warning"):
		return "warning"
	default:
		return "safe"
	}
}

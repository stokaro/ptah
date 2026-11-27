//go:build integration

package valuerounding_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/dbtarget"
)

// A change from a type that carries a time zone or an offset to one that does
// not is converted by keeping the time of day: '10:00:00+02' under time(0) is
// 10:00:00, and a datetimeoffset under datetime2 keeps its clock and drops its
// offset. That loss needs permission even when no fractional digit is lost.
// PostgreSQL and YugabyteDB use America/New_York here: 2024-11-03 05:30 UTC
// and 06:30 UTC both become the local time 01:30 when the zone is removed.

// zoneEngine is one server, with a table whose columns lose their zone and
// keep their digits under the desired schema.
type zoneEngine struct {
	name   string
	server server
	// table creates the table, and setup runs before the rows are inserted.
	table string
	setup []string
	// desired is the same table with the zone taken out of each column.
	desired string
	// whole inserts a row with no fraction of a second, and fraction one with
	// a fraction the new types have no room for.
	whole, fraction string
	// readBack reads every column of the row as one string: held after
	// fraction is inserted, kept after the change is applied over whole.
	readBack   string
	held, kept string
	// refused are what a refusal says of each column.
	refused []string
}

// newYork sets the database's TimeZone, on a server with PostgreSQL's DO.
const newYork = "DO $$ BEGIN EXECUTE format('ALTER DATABASE %I SET timezone TO %L', current_database(), 'America/New_York'); END $$"

var zoneEngines = []zoneEngine{
	{
		name:     "postgres",
		server:   postgresServer,
		table:    "CREATE TABLE z (id int PRIMARY KEY, clock timetz(6), stamp timestamptz(6))",
		setup:    []string{newYork},
		desired:  "CREATE TABLE z (id int PRIMARY KEY, clock time(0), stamp timestamp(0));\n",
		whole:    "INSERT INTO z VALUES (1, '10:00:00+02', '2024-11-03 05:30:00+00')",
		fraction: "INSERT INTO z VALUES (1, '10:00:00.5+02', '2024-11-03 05:30:00.5+00')",
		readBack: "SELECT concat_ws('|', clock::text, stamp::text) FROM z",
		held:     "10:00:00.5+02|2024-11-03 01:30:00.5-04",
		kept:     "10:00:00|2024-11-03 01:30:00",
		refused: []string{
			`column "clock" of table "public.z" from timetz to time(0), where 1 row holds a value`,
			`column "stamp" of table "public.z" from timestamptz to timestamp(0), where 1 row holds a value`,
		},
	},
	{
		name: "yugabytedb",
		server: server{engine: dbtarget.YugabyteDB, create: `CREATE DATABASE "%[1]s"`,
			drop: []string{`DROP DATABASE IF EXISTS "%[1]s" WITH (FORCE)`}},
		table:    "CREATE TABLE z (id int PRIMARY KEY, clock timetz(6), stamp timestamptz(6))",
		setup:    []string{newYork},
		desired:  "CREATE TABLE z (id int PRIMARY KEY, clock time(0), stamp timestamp(0));\n",
		whole:    "INSERT INTO z VALUES (1, '10:00:00+02', '2024-11-03 05:30:00+00')",
		fraction: "INSERT INTO z VALUES (1, '10:00:00.5+02', '2024-11-03 05:30:00.5+00')",
		readBack: "SELECT concat_ws('|', clock::text, stamp::text) FROM z",
		held:     "10:00:00.5+02|2024-11-03 01:30:00.5-04",
		kept:     "10:00:00|2024-11-03 01:30:00",
		refused: []string{
			`column "clock" of table "public.z" from timetz to time(0), where 1 row holds a value`,
			`column "stamp" of table "public.z" from timestamptz to timestamp(0), where 1 row holds a value`,
		},
	},
	{
		name: "cockroachdb",
		server: server{engine: dbtarget.CockroachDB, create: `CREATE DATABASE "%[1]s"`,
			drop: []string{`DROP DATABASE IF EXISTS "%[1]s" CASCADE`}},
		table:    "CREATE TABLE z (id INT PRIMARY KEY, clock TIMETZ(6), stamp TIMESTAMPTZ(6))",
		desired:  "CREATE TABLE z (id INT PRIMARY KEY, clock TIME(0), stamp TIMESTAMP(0));\n",
		whole:    "INSERT INTO z VALUES (1, '10:00:00+02', '2024-11-03 05:30:00+00')",
		fraction: "INSERT INTO z VALUES (1, '10:00:00.5+02', '2024-11-03 05:30:00.5+00')",
		readBack: "SELECT concat_ws('|', clock::STRING, stamp::STRING) FROM z",
		held:     "10:00:00.5+02|2024-11-03 05:30:00.5+00",
		kept:     "10:00:00|2024-11-03 05:30:00",
		refused: []string{
			`column "clock" of table "public.z" from timetz to TIME(0), where 1 row holds a value`,
			`column "stamp" of table "public.z" from timestamptz to TIMESTAMP(0), where 1 row holds a value`,
		},
	},
	{
		name:     "sqlserver",
		server:   sqlServerServer,
		table:    "CREATE TABLE z (id INT PRIMARY KEY, a DATETIMEOFFSET(7), b DATETIMEOFFSET(7), c DATETIMEOFFSET(7))",
		desired:  "CREATE TABLE z (id INT PRIMARY KEY, a DATETIME2(0), b DATETIME, c SMALLDATETIME);\n",
		whole:    "INSERT INTO z VALUES (1, '2024-01-01 10:00:00 +02:00', '2024-01-01 10:00:00 +02:00', '2024-01-01 10:01:00 +05:30')",
		fraction: "INSERT INTO z VALUES (1, '2024-01-01 10:00:00.5 +02:00', '2024-01-01 10:00:00.001 +02:00', '2024-01-01 10:01:31 +05:30')",
		readBack: "SELECT CONCAT_WS('|', CONVERT(varchar(40), a, 121), CONVERT(varchar(40), b, 121), CONVERT(varchar(40), c, 121)) FROM z",
		held:     "2024-01-01 10:00:00.5000000 +02:00|2024-01-01 10:00:00.0010000 +02:00|2024-01-01 10:01:31.0000000 +05:30",
		kept:     "2024-01-01 10:00:00|2024-01-01 10:00:00.000|2024-01-01 10:01:00.000",
		refused: []string{
			`column "a" of table "z" from DATETIMEOFFSET to DATETIME2(0), where 1 row holds a value`,
			`column "b" of table "z" from DATETIMEOFFSET to DATETIME, where 1 row holds a value`,
			`column "c" of table "z" from DATETIMEOFFSET to SMALLDATETIME, where 1 row holds a value`,
		},
	},
}

// TestSchemaApplyDropsTheTimeZoneOnRequestE2E explicitly permits the loss
// and reads back the local date or time the target type keeps.
func TestSchemaApplyDropsTheTimeZoneOnRequestE2E(t *testing.T) {
	for _, engine := range zoneEngines {
		t.Run(engine.name, func(t *testing.T) {
			c := qt.New(t)
			target := engine.server.scratch(c, append(append([]string{engine.table}, engine.setup...), engine.whole)...)

			applied := ptah(c, "schema", "apply", "--db-url", target, "--schema-file", schemaFile(c, engine.desired), "--auto-approve", "--allow-value-rounding")

			c.Assert(applied.ExitCode, qt.Equals, 0, outcome(applied))
			c.Assert(read(c, target, engine.readBack), qt.Equals, engine.kept)
		})
	}
}

// TestSchemaApplyRefusesAFractionAZoneChangeRoundsE2E applies the same change
// over a row with a fraction of a second the new types have no room for. The run
// is refused, names each column, and the row reads back as it was.
func TestSchemaApplyRefusesAFractionAZoneChangeRoundsE2E(t *testing.T) {
	for _, engine := range zoneEngines {
		t.Run(engine.name, func(t *testing.T) {
			c := qt.New(t)
			target := engine.server.scratch(c, append(append([]string{engine.table}, engine.setup...), engine.fraction)...)

			applied := ptah(c, "schema", "apply", "--db-url", target, "--schema-file", schemaFile(c, engine.desired), "--auto-approve")

			c.Assert(applied.ExitCode, qt.Equals, 2, outcome(applied))
			c.Assert(applied.Stderr, qt.Contains, zoneLossRefusal)
			for _, fragment := range engine.refused {
				c.Assert(applied.Stderr, qt.Contains, fragment)
			}
			c.Assert(read(c, target, engine.readBack), qt.Equals, engine.held)
		})
	}
}

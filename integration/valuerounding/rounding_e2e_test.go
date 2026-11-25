//go:build integration

package valuerounding_test

import (
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/clirun"
	"ptah.run/internal/dbtarget"
)

// A decimal given a lower scale or put under an integer, or a temporal column
// given a lower fractional-second precision, is converted without an error by
// every engine below: each stored value is rounded or cut to the new digits,
// under a strict sql_mode too. These runs drive the shipped binaries against
// each server and read the stored values back.
//
// Each table holds a row whose extra digits round up, one whose digits round
// down, one below zero, and one that fits. A count of the rows that would
// change, compared one way only, would miss the second or the third.
//
// Oracle is not among them: it refuses such a change itself while the column
// holds a row (ORA-01440 for NUMBER, ORA-30082 for TIMESTAMP and INTERVAL),
// whatever the value. SQLite enforces no declared size, and a change of it
// alone plans no statement.

// refusal is the start of what a refused run says.
const refusal = "refusing a type change that keeps fewer digits after the point while the table holds values it would change"

// roundingEngine is one server whose conversion rounds or cuts, with a table
// whose columns the desired schema gives fewer digits after the point.
type roundingEngine struct {
	name   string
	server server
	// table creates the table as the database holds it.
	table string
	// desired is the desired schema: the same table with fewer digits.
	desired string
	// digits inserts rows 1 to 3, with digits past the new types', and row 4,
	// whose digits past the new types' are zero.
	digits string
	// fits inserts row 4 alone.
	fits string
	// readBack reads every stored value, row by row, as one string.
	readBack string
	// held is what readBack answers after the rows of digits are inserted
	// and nothing changed them; fitted is what it answers after the change
	// is applied over fits; rounded after the change is applied over digits.
	held, fitted, rounded string
	// refused are what a refusal says of each column: its name, the type it
	// holds and the type it would take, and how many rows would change.
	refused []string
}

// postgresServer and sqlServerServer make databases on the servers more than one
// test below reads.
var (
	postgresServer = server{engine: dbtarget.PostgreSQL, create: `CREATE DATABASE "%[1]s"`,
		drop: []string{`DROP DATABASE IF EXISTS "%[1]s" WITH (FORCE)`}}
	sqlServerServer = server{engine: dbtarget.SQLServer, create: "CREATE DATABASE [%[1]s]", drop: []string{
		"ALTER DATABASE [%[1]s] SET SINGLE_USER WITH ROLLBACK IMMEDIATE", "DROP DATABASE [%[1]s]",
	}}
)

var roundingEngines = []roundingEngine{
	{
		name:   "postgres",
		server: postgresServer,
		table: "CREATE TABLE prices (id int PRIMARY KEY, amount numeric(8,3), whole numeric(8,3), at timestamp(6), " +
			"zoned timestamptz(6), span interval(6), hours interval(0), cash money)",
		desired: "CREATE TABLE prices (id int PRIMARY KEY, amount numeric(8,1), whole integer, at timestamp(0), " +
			"zoned timestamptz(3), span interval(0), hours interval hour, cash numeric(12,1));\n",
		digits: "INSERT INTO prices VALUES " +
			"(1, 1234.567, 1234.567, '2024-01-01 10:00:00.987654', '2024-01-01 10:00:00.5678+00', '1 day 0.75 seconds', '1 day 02:03:04', 12.36), " +
			"(2, 1234.543, 1234.443, '2024-01-01 10:00:00.123456', '2024-01-01 10:00:00.1234+00', '1 day 0.25 seconds', '02:30:00', 12.34), " +
			"(3, -1234.567, -1234.567, '1969-12-31 23:59:59.987654', '1969-12-31 23:59:59.9876+00', '-0.5 seconds', '-01:30:00', -3.45), " +
			"(4, 1.5, 7, 'infinity', '2024-01-01 10:00:00.123+00', '1 day', '1 day 02:00:00', 5)",
		fits: "INSERT INTO prices VALUES (4, 1.5, 7, 'infinity', '2024-01-01 10:00:00.123+00', '1 day', '1 day 02:00:00', 5)",
		readBack: "SELECT string_agg(concat_ws('|', amount::text, whole::text, at::text, (zoned AT TIME ZONE 'UTC')::text, " +
			"span::text, hours::text, cash::numeric::text), ',' ORDER BY id) FROM prices",
		held: "1234.567|1234.567|2024-01-01 10:00:00.987654|2024-01-01 10:00:00.5678|1 day 00:00:00.75|1 day 02:03:04|12.36," +
			"1234.543|1234.443|2024-01-01 10:00:00.123456|2024-01-01 10:00:00.1234|1 day 00:00:00.25|02:30:00|12.34," +
			"-1234.567|-1234.567|1969-12-31 23:59:59.987654|1969-12-31 23:59:59.9876|-00:00:00.5|-01:30:00|-3.45," +
			"1.500|7.000|infinity|2024-01-01 10:00:00.123|1 day|1 day 02:00:00|5.00",
		fitted: "1.5|7|infinity|2024-01-01 10:00:00.123|1 day|1 day 02:00:00|5.0",
		rounded: "1234.6|1235|2024-01-01 10:00:01|2024-01-01 10:00:00.568|1 day 00:00:01|1 day 02:00:00|12.4," +
			"1234.5|1234|2024-01-01 10:00:00|2024-01-01 10:00:00.123|1 day|02:00:00|12.3," +
			"-1234.6|-1235|1970-01-01 00:00:00|1969-12-31 23:59:59.988|-00:00:01|-01:00:00|-3.5," +
			"1.5|7|infinity|2024-01-01 10:00:00.123|1 day|1 day 02:00:00|5.0",
		refused: []string{
			`column "amount" of table "public.prices" from numeric(8,3) to numeric(8,1), where 3 rows hold values`,
			`column "whole" of table "public.prices" from numeric(8,3) to integer, where 3 rows hold values`,
			`column "at" of table "public.prices" from timestamp to timestamp(0), where 3 rows hold values`,
			`column "zoned" of table "public.prices" from timestamptz to timestamptz(3), where 3 rows hold values`,
			`column "span" of table "public.prices" from interval to interval(0), where 3 rows hold values`,
			`column "hours" of table "public.prices" from interval(0) to interval hour, where 3 rows hold values`,
			`column "cash" of table "public.prices" from money to numeric(12,1), where 3 rows hold values`,
		},
	},
	{
		name: "cockroachdb",
		server: server{engine: dbtarget.CockroachDB, create: `CREATE DATABASE "%[1]s"`,
			drop: []string{`DROP DATABASE IF EXISTS "%[1]s" CASCADE`}},
		table: "CREATE TABLE prices (id INT PRIMARY KEY, amount DECIMAL(8,3), whole DECIMAL(8,3), at TIMESTAMP(6), " +
			"span INTERVAL(6), hours INTERVAL)",
		desired: "CREATE TABLE prices (id INT PRIMARY KEY, amount DECIMAL(8,1), whole INT, at TIMESTAMP(0), span INTERVAL(0), " +
			"hours INTERVAL HOUR);\n",
		digits: "INSERT INTO prices VALUES " +
			"(1, 1234.567, 1234.567, '2024-01-01 10:00:00.987654', '1 day 0.75 seconds', '1 day 02:03:04'), " +
			"(2, 1234.543, 1234.443, '2024-01-01 10:00:00.123456', '1 day 0.25 seconds', '02:30:00'), " +
			"(3, -1234.567, -1234.567, '1969-12-31 23:59:59.987654', '-0.5 seconds', '-0.5 seconds'), " +
			"(4, 1.5, 7, 'infinity', '1 day', '1 day 02:00:00')",
		fits: "INSERT INTO prices VALUES (4, 1.5, 7, 'infinity', '1 day', '1 day 02:00:00')",
		readBack: "SELECT string_agg(concat_ws('|', amount::STRING, whole::STRING, at::STRING, span::STRING, hours::STRING), ',' " +
			"ORDER BY id) FROM prices",
		held: "1234.567|1234.567|2024-01-01 10:00:00.987654|1 day 00:00:00.75|1 day 02:03:04," +
			"1234.543|1234.443|2024-01-01 10:00:00.123456|1 day 00:00:00.25|02:30:00," +
			"-1234.567|-1234.567|1969-12-31 23:59:59.987654|-00:00:00.5|-00:00:00.5," +
			"1.500|7.000|infinity|1 day|1 day 02:00:00",
		fitted: "1.5|7|infinity|1 day|1 day 02:00:00",
		rounded: "1234.6|1235|2024-01-01 10:00:01|1 day 00:00:01|1 day 02:00:00," +
			"1234.5|1234|2024-01-01 10:00:00|1 day|02:00:00," +
			"-1234.6|-1235|1970-01-01 00:00:00|-00:00:01|00:00:00," +
			"1.5|7|infinity|1 day|1 day 02:00:00",
		refused: []string{
			`column "amount" of table "public.prices" from numeric(8,3) to DECIMAL(8,1), where 3 rows hold values`,
			`column "whole" of table "public.prices" from numeric(8,3) to int8, where 3 rows hold values`,
			`column "at" of table "public.prices" from timestamp to TIMESTAMP(0), where 3 rows hold values`,
			`column "span" of table "public.prices" from interval to INTERVAL(0), where 3 rows hold values`,
			`column "hours" of table "public.prices" from interval to INTERVAL HOUR, where 3 rows hold values`,
		},
	},
	{
		name: "yugabytedb",
		server: server{engine: dbtarget.YugabyteDB, create: `CREATE DATABASE "%[1]s"`,
			drop: []string{`DROP DATABASE IF EXISTS "%[1]s" WITH (FORCE)`}},
		table:   "CREATE TABLE prices (id int PRIMARY KEY, amount numeric(8,3), whole numeric(8,3), at timestamp(6))",
		desired: "CREATE TABLE prices (id int PRIMARY KEY, amount numeric(8,1), whole integer, at timestamp(0));\n",
		digits: "INSERT INTO prices VALUES (1, 1234.567, 1234.567, '2024-01-01 10:00:00.987654'), " +
			"(2, 1234.543, 1234.443, '2024-01-01 10:00:00.123456'), (3, -1234.567, -1234.567, '1969-12-31 23:59:59.987654'), " +
			"(4, 1.5, 7, '2024-01-01 10:00:00')",
		fits:     "INSERT INTO prices VALUES (4, 1.5, 7, '2024-01-01 10:00:00')",
		readBack: "SELECT string_agg(concat_ws('|', amount::text, whole::text, at::text), ',' ORDER BY id) FROM prices",
		held: "1234.567|1234.567|2024-01-01 10:00:00.987654," +
			"1234.543|1234.443|2024-01-01 10:00:00.123456," +
			"-1234.567|-1234.567|1969-12-31 23:59:59.987654," +
			"1.500|7.000|2024-01-01 10:00:00",
		fitted:  "1.5|7|2024-01-01 10:00:00",
		rounded: "1234.6|1235|2024-01-01 10:00:01,1234.5|1234|2024-01-01 10:00:00,-1234.6|-1235|1970-01-01 00:00:00,1.5|7|2024-01-01 10:00:00",
		refused: []string{
			`column "amount" of table "public.prices" from numeric(8,3) to numeric(8,1), where 3 rows hold values`,
			`column "whole" of table "public.prices" from numeric(8,3) to integer, where 3 rows hold values`,
			`column "at" of table "public.prices" from timestamp to timestamp(0), where 3 rows hold values`,
		},
	},
	{
		name:   "mysql",
		server: server{engine: dbtarget.MySQLAdmin, create: "CREATE DATABASE `%[1]s`", drop: []string{"DROP DATABASE IF EXISTS `%[1]s`"}},
		table: "CREATE TABLE prices (id INT PRIMARY KEY, amount DECIMAL(8,3), whole DECIMAL(8,3), at DATETIME(6), " +
			"stamp TIMESTAMP(6) NULL, clock TIME(6))",
		desired: "CREATE TABLE prices (id INT PRIMARY KEY, amount DECIMAL(8,1), whole INT, at DATETIME, " +
			"stamp TIMESTAMP(3) NULL, clock TIME(3));\n",
		digits: "INSERT INTO prices VALUES " +
			"(1, 1234.567, 1234.567, '2024-01-01 10:00:00.987654', '2024-01-01 10:00:00.98765', '10:00:00.9876'), " +
			"(2, 1234.543, 1234.443, '2024-01-01 10:00:00.123456', '2024-01-01 10:00:00.12345', '10:00:00.1234'), " +
			"(3, -1234.567, -1234.567, '1969-12-31 23:59:59.987654', NULL, '-10:00:00.9876'), " +
			"(4, 1.5, 7, '2024-01-01 10:00:00', '2024-01-01 10:00:00.123', '10:00:00.25')",
		fits:     "INSERT INTO prices VALUES (4, 1.5, 7, '2024-01-01 10:00:00', '2024-01-01 10:00:00.123', '10:00:00.25')",
		readBack: "SELECT GROUP_CONCAT(CONCAT_WS('|', amount, whole, at, IFNULL(stamp, 'NULL'), clock) ORDER BY id SEPARATOR ',') FROM prices",
		held: "1234.567|1234.567|2024-01-01 10:00:00.987654|2024-01-01 10:00:00.987650|10:00:00.987600," +
			"1234.543|1234.443|2024-01-01 10:00:00.123456|2024-01-01 10:00:00.123450|10:00:00.123400," +
			"-1234.567|-1234.567|1969-12-31 23:59:59.987654|NULL|-10:00:00.987600," +
			"1.500|7.000|2024-01-01 10:00:00.000000|2024-01-01 10:00:00.123000|10:00:00.250000",
		fitted: "1.5|7|2024-01-01 10:00:00|2024-01-01 10:00:00.123|10:00:00.250",
		rounded: "1234.6|1235|2024-01-01 10:00:01|2024-01-01 10:00:00.988|10:00:00.988," +
			"1234.5|1234|2024-01-01 10:00:00|2024-01-01 10:00:00.123|10:00:00.123," +
			"-1234.6|-1235|1970-01-01 00:00:00|NULL|-10:00:00.988," +
			"1.5|7|2024-01-01 10:00:00|2024-01-01 10:00:00.123|10:00:00.250",
		refused: []string{
			`column "amount" of table "prices" from decimal(8,3) to DECIMAL(8,1), where 3 rows hold values`,
			`column "whole" of table "prices" from decimal(8,3) to INT, where 3 rows hold values`,
			`column "at" of table "prices" from datetime(6) to DATETIME, where 3 rows hold values`,
			`column "stamp" of table "prices" from timestamp(6) to TIMESTAMP(3), where 2 rows hold values`,
			`column "clock" of table "prices" from time(6) to TIME(3), where 3 rows hold values`,
		},
	},
	{
		name:   "mariadb",
		server: server{engine: dbtarget.MariaDBAdmin, create: "CREATE DATABASE `%[1]s`", drop: []string{"DROP DATABASE IF EXISTS `%[1]s`"}},
		table: "CREATE TABLE prices (id INT PRIMARY KEY, amount DECIMAL(8,3), whole DECIMAL(8,3), at DATETIME(6), " +
			"stamp TIMESTAMP(6) NULL, clock TIME(6))",
		desired: "CREATE TABLE prices (id INT PRIMARY KEY, amount DECIMAL(8,1), whole INT, at DATETIME, " +
			"stamp TIMESTAMP(3) NULL, clock TIME(3));\n",
		digits: "INSERT INTO prices VALUES " +
			"(1, 1234.567, 1234.567, '2024-01-01 10:00:00.987654', '2024-01-01 10:00:00.98765', '10:00:00.9876'), " +
			"(2, 1234.543, 1234.443, '2024-01-01 10:00:00.123456', '2024-01-01 10:00:00.12345', '10:00:00.1234'), " +
			"(3, -1234.567, -1234.567, '1969-12-31 23:59:59.987654', NULL, '-10:00:00.9876'), " +
			"(4, 1.5, 7, '2024-01-01 10:00:00', '2024-01-01 10:00:00.123', '10:00:00.25')",
		fits:     "INSERT INTO prices VALUES (4, 1.5, 7, '2024-01-01 10:00:00', '2024-01-01 10:00:00.123', '10:00:00.25')",
		readBack: "SELECT GROUP_CONCAT(CONCAT_WS('|', amount, whole, at, IFNULL(stamp, 'NULL'), clock) ORDER BY id SEPARATOR ',') FROM prices",
		held: "1234.567|1234.567|2024-01-01 10:00:00.987654|2024-01-01 10:00:00.987650|10:00:00.987600," +
			"1234.543|1234.443|2024-01-01 10:00:00.123456|2024-01-01 10:00:00.123450|10:00:00.123400," +
			"-1234.567|-1234.567|1969-12-31 23:59:59.987654|NULL|-10:00:00.987600," +
			"1.500|7.000|2024-01-01 10:00:00.000000|2024-01-01 10:00:00.123000|10:00:00.250000",
		fitted: "1.5|7|2024-01-01 10:00:00|2024-01-01 10:00:00.123|10:00:00.250",
		rounded: "1234.6|1235|2024-01-01 10:00:00|2024-01-01 10:00:00.987|10:00:00.987," +
			"1234.5|1234|2024-01-01 10:00:00|2024-01-01 10:00:00.123|10:00:00.123," +
			"-1234.6|-1235|1969-12-31 23:59:59|NULL|-10:00:00.987," +
			"1.5|7|2024-01-01 10:00:00|2024-01-01 10:00:00.123|10:00:00.250",
		refused: []string{
			`column "amount" of table "prices" from decimal(8,3) to DECIMAL(8,1), where 3 rows hold values`,
			`column "whole" of table "prices" from decimal(8,3) to INT, where 3 rows hold values`,
			`column "at" of table "prices" from datetime(6) to DATETIME, where 3 rows hold values`,
			`column "stamp" of table "prices" from timestamp(6) to TIMESTAMP(3), where 2 rows hold values`,
			`column "clock" of table "prices" from time(6) to TIME(3), where 3 rows hold values`,
		},
	},
	{
		name:   "sqlserver",
		server: sqlServerServer,
		table: "CREATE TABLE prices (id INT PRIMARY KEY, amount DECIMAL(8,3), whole DECIMAL(8,3), at DATETIME2(7), " +
			"zoned DATETIMEOFFSET(7), minute DATETIME2(0), legacy DATETIME2(3), cash MONEY)",
		desired: "CREATE TABLE prices (id INT PRIMARY KEY, amount DECIMAL(8,1), whole INT, at DATETIME2(0), " +
			"zoned DATETIMEOFFSET(3), minute SMALLDATETIME, legacy DATETIME, cash DECIMAL(12,1));\n",
		digits: "INSERT INTO prices VALUES " +
			"(1, 1234.567, 1234.567, '2024-01-01 10:00:00.9876543', '2024-01-01 10:00:00.5678+02:00', '2024-01-01 10:00:31', " +
			"'2024-01-01 10:00:00.001', 12.3567), " +
			"(2, 1234.543, 1234.443, '2024-01-01 10:00:00.1234567', '2024-01-01 10:00:00.1234+02:00', '2024-01-01 10:00:29', " +
			"'2024-01-01 10:00:00.002', 12.3412), " +
			"(3, -1234.567, -1234.567, '1899-12-31 23:59:59.9876543', '1899-12-31 23:59:59.9876+02:00', '1900-01-01 00:00:59', " +
			"'1899-12-31 23:59:59.999', -3.4567), " +
			"(4, 1.5, 7, '2024-01-01 10:00:00', '2024-01-01 10:00:00.123+02:00', '2024-01-01 10:01:00', '2024-01-01 10:00:00.120', 5)",
		fits: "INSERT INTO prices VALUES (4, 1.5, 7, '2024-01-01 10:00:00', '2024-01-01 10:00:00.123+02:00', '2024-01-01 10:01:00', " +
			"'2024-01-01 10:00:00.120', 5)",
		readBack: "SELECT STRING_AGG(CONCAT_WS('|', CONVERT(varchar(40), amount), CONVERT(varchar(40), whole), CONVERT(varchar(40), at, 121), " +
			"CONVERT(varchar(40), zoned, 121), CONVERT(varchar(40), minute, 121), CONVERT(varchar(40), legacy, 121), " +
			"CONVERT(varchar(40), cash)), ',') WITHIN GROUP (ORDER BY id) FROM prices",
		held: "1234.567|1234.567|2024-01-01 10:00:00.9876543|2024-01-01 10:00:00.5678000 +02:00|2024-01-01 10:00:31|2024-01-01 10:00:00.001|12.36," +
			"1234.543|1234.443|2024-01-01 10:00:00.1234567|2024-01-01 10:00:00.1234000 +02:00|2024-01-01 10:00:29|2024-01-01 10:00:00.002|12.34," +
			"-1234.567|-1234.567|1899-12-31 23:59:59.9876543|1899-12-31 23:59:59.9876000 +02:00|1900-01-01 00:00:59|1899-12-31 23:59:59.999|-3.46," +
			"1.500|7.000|2024-01-01 10:00:00.0000000|2024-01-01 10:00:00.1230000 +02:00|2024-01-01 10:01:00|2024-01-01 10:00:00.120|5.00",
		fitted: "1.5|7|2024-01-01 10:00:00|2024-01-01 10:00:00.123 +02:00|2024-01-01 10:01:00.000|2024-01-01 10:00:00.120|5.0",
		rounded: "1234.6|1234|2024-01-01 10:00:01|2024-01-01 10:00:00.568 +02:00|2024-01-01 10:01:00.000|2024-01-01 10:00:00.000|12.4," +
			"1234.5|1234|2024-01-01 10:00:00|2024-01-01 10:00:00.123 +02:00|2024-01-01 10:00:00.000|2024-01-01 10:00:00.003|12.3," +
			"-1234.6|-1234|1900-01-01 00:00:00|1899-12-31 23:59:59.988 +02:00|1900-01-01 00:01:00.000|1900-01-01 00:00:00.000|-3.5," +
			"1.5|7|2024-01-01 10:00:00|2024-01-01 10:00:00.123 +02:00|2024-01-01 10:01:00.000|2024-01-01 10:00:00.120|5.0",
		refused: []string{
			`column "amount" of table "prices" from DECIMAL(8,3) to DECIMAL(8,1), where 3 rows hold values`,
			`column "whole" of table "prices" from DECIMAL(8,3) to INT, where 3 rows hold values`,
			`column "at" of table "prices" from DATETIME2 to DATETIME2(0), where 3 rows hold values`,
			`column "zoned" of table "prices" from DATETIMEOFFSET to DATETIMEOFFSET(3), where 3 rows hold values`,
			`column "minute" of table "prices" from DATETIME2(0) to SMALLDATETIME, where 3 rows hold values`,
			`column "legacy" of table "prices" from DATETIME2(3) to DATETIME, where 3 rows hold values`,
			`column "cash" of table "prices" from MONEY to DECIMAL(12,1), where 3 rows hold values`,
		},
	},
	{
		name:   "clickhouse",
		server: server{engine: dbtarget.ClickHouse, create: "CREATE DATABASE `%[1]s`", drop: []string{"DROP DATABASE IF EXISTS `%[1]s` SYNC"}},
		table: "CREATE TABLE prices (id Int32, amount Decimal(8, 3), whole Decimal(8, 3), at DateTime64(6), " +
			"maybe Nullable(DateTime64(3))) ENGINE = MergeTree ORDER BY id",
		desired: "CREATE TABLE prices (id Int32, amount Decimal(8, 1), whole Int32, at DateTime64(3), " +
			"maybe Nullable(DateTime64(0))) ENGINE = MergeTree ORDER BY id;\n",
		digits: "INSERT INTO prices VALUES (1, 1234.567, 1234.567, '2024-01-01 10:00:00.987654', '2024-01-01 10:00:00.5'), " +
			"(2, 1234.543, 1234.443, '2024-01-01 10:00:00.123456', '2024-01-01 10:00:00.123'), " +
			"(3, -1234.567, -1234.567, '1969-12-31 23:59:59.987654', NULL), (4, 1.5, 7, '2024-01-01 10:00:00', NULL)",
		fits: "INSERT INTO prices VALUES (4, 1.5, 7, '2024-01-01 10:00:00', NULL)",
		readBack: "SELECT arrayStringConcat(groupArray(concat(toString(amount), '|', toString(whole), '|', toString(at), '|', " +
			"ifNull(toString(maybe), 'NULL'))), ',') FROM (SELECT * FROM prices ORDER BY id)",
		held: "1234.567|1234.567|2024-01-01 10:00:00.987654|2024-01-01 10:00:00.500," +
			"1234.543|1234.443|2024-01-01 10:00:00.123456|2024-01-01 10:00:00.123," +
			"-1234.567|-1234.567|1969-12-31 23:59:59.987654|NULL," +
			"1.5|7|2024-01-01 10:00:00.000000|NULL",
		fitted: "1.5|7|2024-01-01 10:00:00.000|NULL",
		rounded: "1234.5|1234|2024-01-01 10:00:00.987|2024-01-01 10:00:00," +
			"1234.5|1234|2024-01-01 10:00:00.123|2024-01-01 10:00:00," +
			"-1234.5|-1234|1969-12-31 23:59:59.988|NULL," +
			"1.5|7|2024-01-01 10:00:00.000|NULL",
		refused: []string{
			`column "amount" of table "prices" from Decimal(8, 3) to Decimal(8, 1), where 3 rows hold values`,
			`column "whole" of table "prices" from Decimal(8, 3) to Int32, where 3 rows hold values`,
			`column "at" of table "prices" from DateTime64(6) to DateTime64(3), where 3 rows hold values`,
			`column "maybe" of table "prices" from DateTime64(3) to DateTime64(0), where 2 rows hold values`,
		},
	},
}

// engineNamed is the engine of roundingEngines with the name.
func engineNamed(c *qt.C, name string) roundingEngine {
	c.Helper()
	for _, engine := range roundingEngines {
		if engine.name == name {
			return engine
		}
	}
	c.Fatalf("no rounding engine %q", name)
	return roundingEngine{}
}

// TestSchemaApplyRefusesToRoundStoredValuesE2E applies the change over rows
// with digits the new types have no room for. The run names each such column
// with the types and the rows that would change, exits 2, and every value
// reads back as it was.
func TestSchemaApplyRefusesToRoundStoredValuesE2E(t *testing.T) {
	for _, engine := range roundingEngines {
		t.Run(engine.name, func(t *testing.T) {
			c := qt.New(t)
			target := engine.server.scratch(c, engine.table, engine.digits)

			applied := ptah(c, "schema", "apply", "--db-url", target, "--schema-file", schemaFile(c, engine.desired), "--auto-approve")

			c.Assert(applied.ExitCode, qt.Equals, 2, outcome(applied))
			c.Assert(applied.Stderr, qt.Contains, refusal)
			for _, column := range engine.refused {
				c.Assert(applied.Stderr, qt.Contains, column)
			}
			c.Assert(read(c, target, engine.readBack), qt.Equals, engine.held)
		})
	}
}

// TestSchemaApplyChangesValuesThatKeepTheirDigitsE2E is the control: over a
// row whose digits past the new types are zero, the same change applies, and
// the values read back unchanged under the new types.
func TestSchemaApplyChangesValuesThatKeepTheirDigitsE2E(t *testing.T) {
	for _, engine := range roundingEngines {
		t.Run(engine.name, func(t *testing.T) {
			c := qt.New(t)
			target := engine.server.scratch(c, engine.table, engine.fits)

			applied := ptah(c, "schema", "apply", "--db-url", target, "--schema-file", schemaFile(c, engine.desired), "--auto-approve")

			c.Assert(applied.ExitCode, qt.Equals, 0, outcome(applied))
			c.Assert(read(c, target, engine.readBack), qt.Equals, engine.fitted)
		})
	}
}

// TestSchemaApplyRoundsStoredValuesOnRequestE2E asks for the change with
// --allow-value-rounding. The run warns of each column, applies, and the
// values read back as the server rounded or cut them.
func TestSchemaApplyRoundsStoredValuesOnRequestE2E(t *testing.T) {
	for _, engine := range roundingEngines {
		t.Run(engine.name, func(t *testing.T) {
			c := qt.New(t)
			target := engine.server.scratch(c, engine.table, engine.digits)

			applied := ptah(c, "schema", "apply", "--db-url", target, "--schema-file", schemaFile(c, engine.desired),
				"--auto-approve", "--allow-value-rounding")

			c.Assert(applied.ExitCode, qt.Equals, 0, outcome(applied))
			c.Assert(applied.Stderr, qt.Contains, "which keeps fewer digits after the point, without a scan of the values the table holds")
			c.Assert(read(c, target, engine.readBack), qt.Equals, engine.rounded)
		})
	}
}

// TestSchemaApplyPlanReadsTheValuesAgainE2E saves a plan while no value would
// change, and inserts one with digits before applying it. The apply reads the
// rows again, refuses, and the value reads back as it was. A dry run refuses
// too: the rows are read again when the plan is checked against the target,
// before the plan is shown for approval.
func TestSchemaApplyPlanReadsTheValuesAgainE2E(t *testing.T) {
	c := qt.New(t)
	engine := engineNamed(c, "mysql")
	target := engine.server.scratch(c, engine.table, engine.fits)
	plan := filepath.Join(c.TempDir(), "rounding.plan.json")

	planned := ptah(c, "schema", "plan", "--db-url", target, "--schema-file", schemaFile(c, engine.desired),
		"--save", "--output", plan)
	c.Assert(planned.ExitCode, qt.Equals, 0, outcome(planned))
	run(c, target, "INSERT INTO prices VALUES (1, 1234.567, 1234.567, '2024-01-01 10:00:00.987654', '2024-01-01 10:00:00.98765', '10:00:00.9876')")
	shown := ptah(c, "schema", "apply", "--db-url", target, "--plan", plan, "--dry-run")
	applied := ptah(c, "schema", "apply", "--db-url", target, "--plan", plan, "--auto-approve")

	c.Assert(shown.ExitCode, qt.Equals, 2, outcome(shown))
	c.Assert(shown.Stderr, qt.Contains, "The plan found no such value when it read the table")

	c.Assert(applied.ExitCode, qt.Equals, 2, outcome(applied))
	c.Assert(applied.Stderr, qt.Contains, refusal)
	c.Assert(applied.Stderr, qt.Contains, `column "amount" of table "prices" from decimal(8,3) to DECIMAL(8,1), where 1 row holds a value`)
	c.Assert(applied.Stderr, qt.Contains, "The plan found no such value when it read the table")
	c.Assert(read(c, target, "SELECT CONCAT_WS('|', amount, whole) FROM prices WHERE id = 1"), qt.Equals, "1234.567|1234.567")
}

// TestCompatSchemaApplyRefusesToRoundStoredValuesE2E is the refusal through
// `ptah-compat schema apply`, which plans through the same path and exits 1 as
// the community binary exits on an error. PTAH_ALLOW_VALUE_ROUNDING=1 is how
// that surface asks for the change, and the run with it rounds the values.
func TestCompatSchemaApplyRefusesToRoundStoredValuesE2E(t *testing.T) {
	c := qt.New(t)
	engine := engineNamed(c, "postgres")
	target := engine.server.scratch(c, engine.table, engine.digits)
	dev := engine.server.scratch(c)
	args := []string{"schema", "apply", "-u", target, "--to", "file://" + filepath.ToSlash(schemaFile(c, engine.desired)),
		"--dev-url", dev, "--auto-approve"}

	refused := clirun.Run(c, clirun.Compat, clirun.Options{Dir: c.TempDir()}, args...)
	c.Assert(refused.ExitCode, qt.Equals, 1, outcome(refused))
	c.Assert(refused.Stderr, qt.Contains, refusal)
	c.Assert(refused.Stderr, qt.Contains, "PTAH_ALLOW_VALUE_ROUNDING=1")
	c.Assert(read(c, target, engine.readBack), qt.Equals, engine.held)
	rounded := clirun.Run(c, clirun.Compat, clirun.Options{
		Dir: c.TempDir(), Env: append(os.Environ(), "PTAH_ALLOW_VALUE_ROUNDING=1"),
	}, args...)

	c.Assert(rounded.ExitCode, qt.Equals, 0, outcome(rounded))
	c.Assert(read(c, target, engine.readBack), qt.Equals, engine.rounded)
}

// TestCompatSchemaApplyReadsAnAtlasPlanAgainE2E plans the change, over a row
// that fits, into the Atlas .plan.hcl format, which records no type change
// whose rows the plan read. The plan is written, since no row would change.
// Rows with digits are inserted before the apply, which reads the type changes
// from the desired state, refuses, and leaves the values; with the request, the
// same plan applies and rounds them.
func TestCompatSchemaApplyReadsAnAtlasPlanAgainE2E(t *testing.T) {
	c := qt.New(t)
	engine := engineNamed(c, "postgres")
	target := engine.server.scratch(c, engine.table, engine.fits)
	dev := engine.server.scratch(c)
	to := "file://" + filepath.ToSlash(schemaFile(c, engine.desired))
	plan := filepath.Join(c.TempDir(), "rounding.plan.hcl")

	planned := clirun.Run(c, clirun.Compat, clirun.Options{Dir: c.TempDir()},
		"schema", "plan", "--from", target, "--to", to, "--dev-url", dev, "--save", "--output", plan, "--auto-approve")
	c.Assert(planned.ExitCode, qt.Equals, 0, outcome(planned))
	run(c, target, "DELETE FROM prices")
	run(c, target, engine.digits)
	args := []string{"schema", "apply", "-u", target, "--plan", "file://" + filepath.ToSlash(plan), "--to", to,
		"--dev-url", dev, "--auto-approve"}
	refused := clirun.Run(c, clirun.Compat, clirun.Options{Dir: c.TempDir()}, args...)

	c.Assert(refused.ExitCode, qt.Equals, 1, outcome(refused))
	c.Assert(refused.Stderr, qt.Contains, refusal)
	c.Assert(refused.Stderr, qt.Contains, engine.refused[0])
	c.Assert(read(c, target, engine.readBack), qt.Equals, engine.held)
	rounded := clirun.Run(c, clirun.Compat, clirun.Options{
		Dir: c.TempDir(), Env: append(os.Environ(), "PTAH_ALLOW_VALUE_ROUNDING=1"),
	}, args...)
	c.Assert(rounded.ExitCode, qt.Equals, 0, outcome(rounded))
	c.Assert(read(c, target, engine.readBack), qt.Equals, engine.rounded)
}

// TestSchemaApplyRefusesAValueTheNewTypeCannotHoldE2E lowers a scale where a
// value rounds past the precision the new type keeps. The server's CAST fails
// as the ALTER would, and the run names the column with the server's error
// rather than ending the plan with it; the value stays.
func TestSchemaApplyRefusesAValueTheNewTypeCannotHoldE2E(t *testing.T) {
	tests := []struct {
		name    string
		server  server
		table   string
		desired string
		insert  string
		want    string
		read    string
	}{
		{
			name:    "postgres",
			server:  postgresServer,
			table:   "CREATE TABLE prices (id int PRIMARY KEY, amount numeric(5,3))",
			desired: "CREATE TABLE prices (id int PRIMARY KEY, amount numeric(3,1));\n",
			insert:  "INSERT INTO prices VALUES (1, 99.999)",
			want:    `column "amount" of table "public.prices" from numeric(5,3) to numeric(3,1), where the server cannot convert a stored value to the new type: `,
			read:    "SELECT amount::text FROM prices",
		},
		{
			name:    "sqlserver",
			server:  sqlServerServer,
			table:   "CREATE TABLE prices (id INT PRIMARY KEY, amount DECIMAL(5,3))",
			desired: "CREATE TABLE prices (id INT PRIMARY KEY, amount DECIMAL(3,1));\n",
			insert:  "INSERT INTO prices VALUES (1, 99.999)",
			want:    `column "amount" of table "prices" from DECIMAL(5,3) to DECIMAL(3,1), where the server cannot convert a stored value to the new type: `,
			read:    "SELECT CONVERT(varchar(40), amount) FROM prices",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			target := test.server.scratch(c, test.table, test.insert)

			applied := ptah(c, "schema", "apply", "--db-url", target, "--schema-file", schemaFile(c, test.desired), "--auto-approve")

			c.Assert(applied.ExitCode, qt.Equals, 2, outcome(applied))
			c.Assert(applied.Stderr, qt.Contains, test.want)
			c.Assert(read(c, target, test.read), qt.Equals, "99.999")
		})
	}
}

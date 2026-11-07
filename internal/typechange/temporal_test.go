package typechange_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/typechange"
)

// TestIsNarrowing_Temporal holds what is true on every engine: both sides declare a
// precision, or the two types differ in what their values hold. The rows that widen and
// those that respell one type stand beside the rows that narrow, because a false
// destructive rating stops a plan as surely as a missed one loses a fraction.
func TestIsNarrowing_Temporal(t *testing.T) {
	tests := []struct {
		name    string
		oldType string
		newType string
		want    bool
	}{
		{"timestamp down", "timestamp(6)", "timestamp(0)", true},
		{"timestamp up", "timestamp(0)", "timestamp(6)", false},
		{"timestamp same", "timestamp(3)", "timestamp(3)", false},
		{"timestamptz down", "timestamptz(6)", "timestamptz(0)", true},
		{"timestamptz up", "timestamptz(0)", "timestamptz(6)", false},
		{"time down", "time(6)", "time(0)", true},
		{"time up", "time(0)", "time(6)", false},
		{"timetz down", "timetz(6)", "timetz(2)", true},
		{"datetime down", "datetime(6)", "datetime(0)", true},
		{"datetime up", "datetime(3)", "datetime(6)", false},
		{"interval down", "interval(6)", "interval(0)", true},
		{"interval up", "interval(0)", "interval(6)", false},

		// The catalog's spelling against a declaration's.
		{"catalog timestamp down", "timestamp(6) without time zone", "timestamp(0)", true},
		{"catalog timestamptz down", "timestamp(6) with time zone", "timestamptz(0)", true},
		{"catalog time down", "time(6) with time zone", "timetz(3)", true},
		{"catalog spelling same", "timestamp(3) with time zone", "timestamptz(3)", false},
		{"upper case down", "TIMESTAMP(6)", "timestamp(3)", true},
		{"array down", "timestamp(3) without time zone[]", "timestamp(0)[]", true},
		{"array up", "timestamp(0)[]", "timestamp(3)[]", false},
		{"array against its element is not this question", "timestamp(3)[]", "timestamp(0)", false},

		// A precision on one side only is the engine's answer, and no engine is named.
		{"bare new", "timestamp(6)", "timestamp", false},
		{"bare old", "timestamp", "timestamp(3)", false},
		{"both bare", "timestamp", "timestamp", false},
		{"a zone qualifier alone", "timestamptz", "timestamp with time zone", false},

		// What a value is made of.
		{"timestamp to date", "timestamp", "date", true},
		{"datetime to date", "datetime(6)", "date", true},
		{"date to timestamp", "date", "timestamp", false},
		{"timestamp to time", "timestamp(6)", "time(6)", true},
		{"date to time", "date", "time", true},
		{"time to date", "time", "date", true},
		{"time to timestamp adds a date", "time", "timestamp", false},
		{"interval against a timestamp is not this question", "interval", "timestamp", false},

		// A stored offset, and the instant a timestamptz holds instead.
		{"timetz to time", "timetz", "time", true},
		{"time to timetz", "time", "timetz", false},
		{"datetimeoffset to datetime2", "datetimeoffset(7)", "datetime2(7)", true},
		{"datetime2 to datetimeoffset", "datetime2(7)", "datetimeoffset(7)", false},
		{"timestamptz to timestamp drops nothing stored", "timestamptz(6)", "timestamp(6)", false},
		{"timestamp to timestamptz drops nothing", "timestamp(6)", "timestamptz(6)", false},

		// MySQL's narrower range.
		{"MySQL datetime to timestamp", "datetime(6)", "timestamp(6)", true},
		{"MySQL timestamp to datetime", "timestamp(6)", "datetime(6)", false},
		{"MySQL timestamp to a coarser datetime", "timestamp(6)", "datetime(0)", true},

		// SQL Server's fixed types.
		{"datetime2 to datetime", "datetime2(7)", "datetime", true},
		{"datetime2 with few digits to datetime", "datetime2(0)", "datetime", true},
		{"datetime to smalldatetime", "datetime", "smalldatetime", true},
		{"datetime2 to smalldatetime", "datetime2(7)", "smalldatetime", true},
		{"smalldatetime to datetime", "smalldatetime", "datetime", false},
		{"smalldatetime to datetime2", "smalldatetime", "datetime2(0)", false},
		{"datetime to datetime2 holding its digits", "datetime", "datetime2(3)", false},
		{"datetime to datetime2 that cannot", "datetime", "datetime2(2)", true},
		{"datetime to datetimeoffset that cannot", "datetime", "datetimeoffset(0)", true},

		// ClickHouse's names, and what wraps them.
		{"DateTime64 down", "DateTime64(6)", "DateTime64(3)", true},
		{"DateTime64 up", "DateTime64(3)", "DateTime64(6)", false},
		{"DateTime64 to DateTime", "DateTime64(3)", "DateTime", true},
		{"DateTime64(0) to DateTime spans less", "DateTime64(0)", "DateTime", true},
		{"DateTime to DateTime64", "DateTime", "DateTime64(3)", false},
		{"DateTime to Date", "DateTime", "Date", true},
		{"Date to DateTime", "Date", "DateTime", false},
		{"Date32 to Date", "Date32", "Date", true},
		{"Date to Date32", "Date", "Date32", false},
		{"a zone argument", "DateTime64(3, 'UTC')", "DateTime64(0, 'UTC')", true},
		{"Nullable down", "Nullable(DateTime64(6))", "Nullable(DateTime64(0))", true},
		{"LowCardinality down", "LowCardinality(DateTime64(6))", "DateTime64(0)", true},
		{"Array down", "Array(DateTime64(6))", "Array(DateTime64(3))", true},
		{"a bare DateTime64 is three digits", "DateTime64(3)", "DateTime64", false},
		{"a bare DateTime64 against more", "DateTime64(6)", "DateTime64", true},

		// Interval fields: what a server clears from every value.
		{"interval to year to month", "interval", "interval year to month", true},
		{"interval to hour to minute", "interval", "interval hour to minute", true},
		{"day to second to hour to minute", "interval day to second", "interval hour to minute", true},
		{"year to month to interval", "interval year to month", "interval", false},
		{"fields behind a precision", "interval day to second(6)", "interval day to second(0)", true},
		{"same fields same precision", "interval day to second(3)", "interval day to second(3)", false},
		{"Oracle fraction down", "interval day(2) to second(6)", "interval day(2) to second(0)", true},
		{"Oracle leading field down", "interval day(4) to second(6)", "interval day(2) to second(6)", true},
		{"Oracle year down", "interval year(4) to month", "interval year(2) to month", true},
		{"Oracle up", "interval day(2) to second(0)", "interval day(4) to second(6)", false},
		{"Oracle timestamp down", "TIMESTAMP(6) WITH TIME ZONE", "TIMESTAMP(0) WITH TIME ZONE", true},
		{"Oracle local timestamp down", "TIMESTAMP(6) WITH LOCAL TIME ZONE", "TIMESTAMP(0) WITH LOCAL TIME ZONE", true},

		// A column of text may hold what no timestamp can stand for.
		{"a varchar to a timestamp", "varchar(6)", "timestamp", true},

		// Types that only sound temporal.
		{"timestamp to text", "timestamp", "text", false},
		{"an enum named after one", "timestamp_kind", "timestamp", false},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(typechange.IsNarrowing(test.oldType, test.newType), qt.Equals, test.want)
		})
	}
}

// TestEngineNarrows_ABareTemporalTypeIsTheEnginesAnswer holds a precision declared on one side
// to what each engine's server answered, read back from a column of each spelling:
// PostgreSQL 17.11, CockroachDB 26.3 and YugabyteDB 2026.1 hold six digits, MySQL 8.4 and
// MariaDB 12.3 none, SQL Server 2025 seven, Oracle 23.26 six.
func TestEngineNarrows_ABareTemporalTypeIsTheEnginesAnswer(t *testing.T) {
	const postgres, mysql, sqlServer, oracle = 6, 0, 7, 6
	tests := []struct {
		name    string
		bare    int
		oldType string
		newType string
		want    bool
	}{
		{"PostgreSQL bare to fewer", postgres, "timestamp", "timestamp(3)", true},
		{"PostgreSQL catalog bare to fewer", postgres, "timestamp without time zone", "timestamp(3)", true},
		{"PostgreSQL bare to six", postgres, "timestamp", "timestamp(6)", false},
		{"PostgreSQL six to bare", postgres, "timestamp(6)", "timestamp", false},
		{"PostgreSQL fewer to bare widens", postgres, "timestamp(3)", "timestamp", false},
		{"PostgreSQL timestamptz spellings", postgres, "timestamptz", "timestamp with time zone", false},
		{"PostgreSQL bare timestamptz to fewer", postgres, "timestamp with time zone", "timestamptz(3)", true},
		{"PostgreSQL time to fewer", postgres, "time", "time(3)", true},
		{"PostgreSQL interval to fewer", postgres, "interval", "interval(3)", true},
		{"PostgreSQL interval to fields with fewer digits", postgres, "interval", "interval day to second(3)", true},
		{"PostgreSQL interval to fields with its digits", postgres, "interval", "interval day to second", false},
		{"PostgreSQL fields without digits widen", postgres, "interval hour to minute", "interval", false},

		{"MySQL six to bare", mysql, "timestamp(6)", "timestamp", true},
		{"MySQL three to bare", mysql, "datetime(3)", "datetime", true},
		{"MySQL time to bare", mysql, "time(6)", "time", true},
		{"MySQL bare to zero", mysql, "datetime", "datetime(0)", false},
		{"MySQL zero to bare", mysql, "timestamp(0)", "timestamp", false},
		{"MySQL bare to more widens", mysql, "datetime", "datetime(3)", false},

		{"SQL Server bare to fewer", sqlServer, "DATETIME2", "datetime2(3)", true},
		{"SQL Server bare to none", sqlServer, "DATETIME2", "datetime2(0)", true},
		{"SQL Server bare to seven", sqlServer, "DATETIME2", "datetime2(7)", false},
		{"SQL Server seven to bare", sqlServer, "datetime2(7)", "datetime2", false},
		{"SQL Server fewer to bare widens", sqlServer, "DATETIME2(3)", "datetime2", false},
		{"SQL Server time to fewer", sqlServer, "TIME", "time(3)", true},
		{"SQL Server offset to fewer", sqlServer, "DATETIMEOFFSET", "datetimeoffset(3)", true},
		{"SQL Server datetime to bare datetime2", sqlServer, "DATETIME", "datetime2", false},
		{"SQL Server datetime2 to datetime", sqlServer, "DATETIME2", "datetime", true},

		{"Oracle nine to bare", oracle, "TIMESTAMP(9)", "TIMESTAMP", true},
		{"Oracle six to bare", oracle, "TIMESTAMP(6)", "TIMESTAMP", false},
		{"Oracle timestamp to date", oracle, "TIMESTAMP(6)", "DATE", true},

		// The rest of IsNarrowing answers as it does with no engine.
		{"a string", postgres, "varchar(100)", "varchar(5)", true},
		{"an integer", mysql, "bigint", "int", true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(typechange.Engine{TimeDigits: test.bare}.Narrows(test.oldType, test.newType), qt.Equals, test.want)
		})
	}
}

func TestIsWidening_Temporal(t *testing.T) {
	tests := []struct {
		name    string
		oldType string
		newType string
		want    bool
	}{
		{"timestamp up", "timestamp(0)", "timestamp(6)", true},
		{"timestamp down", "timestamp(6)", "timestamp(0)", false},
		{"timestamp same", "timestamp(3)", "timestamp(3)", false},
		{"a bare side is the engine's answer", "timestamp(3)", "timestamp", false},
		{"DateTime to DateTime64", "DateTime", "DateTime64(0)", true},
		{"Date to Date32", "Date", "Date32", true},
		{"date to timestamp changes the category", "date", "timestamp", false},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(typechange.IsWidening(test.oldType, test.newType), qt.Equals, test.want)
		})
	}
}

func TestIsTemporal(t *testing.T) {
	tests := []struct {
		typeName string
		want     bool
	}{
		{"timestamp", true},
		{"TIMESTAMP(6) WITH TIME ZONE", true},
		{"interval day to second(3)", true},
		{"Nullable(DateTime64(3))", true},
		{"date", true},
		{"timestamp_kind", false},
		{"varchar(6)", false},
		{"", false},
	}
	for _, test := range tests {
		t.Run(test.typeName, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(typechange.IsTemporal(test.typeName), qt.Equals, test.want)
		})
	}
}

package typechange_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform/descriptor"
	"ptah.run/internal/typechange"
)

// clickHouseEngine is what ClickHouse's descriptor says: a DateTime keeps no digits, and a
// type that declares no size is not stated.
var clickHouseEngine = typechange.Engine{}

// postgresMoneyEngine is PostgreSQL's descriptor with what money keeps after the point
// under the server's lc_monetary.
var postgresMoneyEngine = typechange.Engine{TimeDigits: 6, Bare: descriptor.BareTypes{
	Stated: true, FloatBits: 53, RealBits: 24, MoneyScale: 2,
}}

// TestEngineRounds_HappyPath holds each change that keeps fewer digits after the point to
// the kind of type it changes. A size one side leaves out is the engine's: a bare datetime
// keeps no digits on MySQL and a bare datetime2 seven on SQL Server.
func TestEngineRounds_HappyPath(t *testing.T) {
	tests := []struct {
		name     string
		engine   typechange.Engine
		oldType  string
		newType  string
		temporal bool
	}{
		{"a decimal's scale", mysqlEngine, "decimal(8,3)", "decimal(8,1)", false},
		{"a scale with more digits before the point", mysqlEngine, "decimal(8,3)", "decimal(12,2)", false},
		{"a scale to none", postgresEngine, "numeric(10,2)", "numeric(10)", false},
		{"any number to a size", postgresEngine, "numeric", "numeric(10,2)", false},
		{"a fraction to MySQL's bare decimal", mysqlEngine, "decimal(10,2)", "decimal", false},
		{"a decimal under an integer", mysqlEngine, "decimal(8,3)", "int", false},
		{"a decimal under an unsigned integer", mysqlEngine, "decimal(8,3)", "int unsigned", false},
		{"any number under an integer", postgresEngine, "numeric", "integer", false},
		{"SQL Server's decimal under bigint", sqlServerEngine, "decimal(18,2)", "bigint", false},
		{"Oracle's NUMBER", oracleEngine, "NUMBER(8,3)", "NUMBER(8,1)", false},
		{"SQL Server's money", sqlServerEngine, "money", "decimal(19,2)", false},
		{"PostgreSQL's money under one digit", postgresMoneyEngine, "money", "numeric(12,1)", false},
		{"ClickHouse's named decimal", clickHouseEngine, "Decimal64(4)", "Decimal64(2)", false},
		{"ClickHouse inside Nullable", clickHouseEngine, "Nullable(Decimal(8, 3))", "Nullable(Decimal(8, 1))", false},
		{"ClickHouse's decimal under an integer", clickHouseEngine, "Decimal(8, 3)", "Int32", false},

		{"MySQL datetime to a bare datetime", mysqlEngine, "datetime(6)", "datetime", true},
		{"MySQL time", mysqlEngine, "time(6)", "time(3)", true},
		{"MySQL datetime to a timestamp", mysqlEngine, "datetime(6)", "timestamp", true},
		{"PostgreSQL timestamp", postgresEngine, "timestamp(6) without time zone", "timestamp(0)", true},
		{"PostgreSQL bare timestamptz", postgresEngine, "timestamptz", "timestamptz(3)", true},
		{"PostgreSQL timetz", postgresEngine, "time(6) with time zone", "timetz(3)", true},
		{"PostgreSQL interval", postgresEngine, "interval", "interval(0)", true},
		{"PostgreSQL interval fields", postgresEngine, "interval day to second(6)", "interval day to second(2)", true},
		{"PostgreSQL interval to whole hours", postgresEngine, "interval", "interval hour", true},
		{"PostgreSQL interval of whole seconds to days", postgresEngine, "interval(0)", "interval day", true},
		{"PostgreSQL interval to whole seconds", postgresEngine, "interval minute to second(6)", "interval minute to second(0)", true},
		{"SQL Server bare datetime2", sqlServerEngine, "datetime2", "datetime2(3)", true},
		{"SQL Server datetime2 under datetime", sqlServerEngine, "datetime2(7)", "datetime", true},
		{"SQL Server milliseconds under datetime", sqlServerEngine, "datetime2(3)", "datetime", true},
		{"SQL Server datetimeoffset", sqlServerEngine, "datetimeoffset(7)", "datetimeoffset(0)", true},
		{"SQL Server time", sqlServerEngine, "time(7)", "time(0)", true},
		{"SQL Server datetime to datetime2(0)", sqlServerEngine, "datetime", "datetime2(0)", true},
		{"SQL Server smalldatetime keeps whole minutes", sqlServerEngine, "datetime2(0)", "smalldatetime", true},
		{"ClickHouse DateTime64", clickHouseEngine, "DateTime64(6)", "DateTime64(3)", true},
		{"ClickHouse DateTime64 to DateTime", clickHouseEngine, "DateTime64(3)", "DateTime", true},
		{"ClickHouse bare DateTime64", clickHouseEngine, "DateTime64", "DateTime64(1)", true},
		{"Oracle timestamp", oracleEngine, "TIMESTAMP(6)", "TIMESTAMP(0)", true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			got, rounds := test.engine.Rounds(test.oldType, test.newType)

			c.Assert(rounds, qt.IsTrue)
			c.Assert(got, qt.Equals, typechange.Rounding{Temporal: test.temporal})
		})
	}
}

// TestEngineRounds_FailurePath holds the changes that keep every digit after the point, and
// those that lose more than digits or that the types alone cannot answer.
func TestEngineRounds_FailurePath(t *testing.T) {
	tests := []struct {
		name    string
		engine  typechange.Engine
		oldType string
		newType string
	}{
		{"a higher scale", mysqlEngine, "decimal(8,1)", "decimal(8,3)"},
		{"the same scale with fewer digits before the point", mysqlEngine, "decimal(8,3)", "decimal(6,3)"},
		{"the same scale", mysqlEngine, "decimal(8,2)", "DECIMAL(10,2)"},
		{"a size to any number", postgresEngine, "numeric(10,2)", "numeric"},
		{"an engine that states nothing of a bare decimal", typechange.Engine{}, "decimal", "decimal(10,2)"},
		{"a float under a decimal", mysqlEngine, "double", "decimal(8,1)"},
		{"a decimal under a float", mysqlEngine, "decimal(8,3)", "double"},
		{"a whole decimal under an integer", mysqlEngine, "decimal(8,0)", "int"},
		{"MySQL's bare decimal under an integer", mysqlEngine, "decimal", "bigint"},
		{"PostgreSQL's money under its own digits", postgresMoneyEngine, "money", "numeric(12,3)"},
		{"an array of decimals", postgresEngine, "numeric(8,3)[]", "numeric(8,1)[]"},

		{"a higher precision", mysqlEngine, "datetime(0)", "datetime(6)"},
		{"the same precision respelled", postgresEngine, "timestamp(6) without time zone", "timestamp"},
		{"PostgreSQL bare timestamp to a precision of six", postgresEngine, "timestamp", "timestamp(6)"},
		{"a timestamp under a date loses more than digits", mysqlEngine, "datetime(6)", "date"},
		{"a timestamp under a time loses more than digits", postgresEngine, "timestamp(6)", "time(0)"},
		{"an array of timestamps", postgresEngine, "timestamp(6)[]", "timestamp(0)[]"},
		{"PostgreSQL interval to a finer field", postgresEngine, "interval hour", "interval"},
		{"SQL Server datetime to datetime2(3)", sqlServerEngine, "datetime", "datetime2(3)"},
		{"SQL Server hundredths under datetime", sqlServerEngine, "datetime2(2)", "datetime"},
		{"a string", mysqlEngine, "varchar(10)", "varchar(3)"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			got, rounds := test.engine.Rounds(test.oldType, test.newType)

			c.Assert(rounds, qt.IsFalse)
			c.Assert(got, qt.Equals, typechange.Rounding{})
		})
	}
}

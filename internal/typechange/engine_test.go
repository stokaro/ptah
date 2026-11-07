package typechange_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform/descriptor"
	"ptah.run/internal/typechange"
)

// What each engine's server built for a type that declares no size, read back from its
// catalog: PostgreSQL 18, CockroachDB 26.3 and YugabyteDB 2026.1 share the first, MySQL
// 26.10 and MariaDB 12.3 the second, then SQL Server 2025 and Oracle 23.26.
var (
	postgresEngine  = typechange.Engine{TimeDigits: 6, Bare: descriptor.BareTypes{Stated: true, FloatBits: 53, RealBits: 24}}
	mysqlEngine     = typechange.Engine{Bare: descriptor.BareTypes{Stated: true, DecimalPrecision: 10, FloatBits: 24, RealBits: 53}}
	sqlServerEngine = typechange.Engine{TimeDigits: 7, Bare: descriptor.BareTypes{
		Stated: true, DecimalPrecision: 18, VariableLength: 1, FloatBits: 53, RealBits: 24,
	}}
	oracleEngine = typechange.Engine{TimeDigits: 6, Bare: descriptor.BareTypes{
		Stated: true, FloatBits: 126, FloatKeepsPrecision: true, IntervalLeading: 2,
	}}
)

// TestEngineNarrows_ASizeOnOneSideIsTheEnginesAnswer holds each transition that declares a
// size on one side to what that side holds on the engine. Which way a row goes is the
// point: `decimal(10,2)` under `decimal` loses the fraction on MySQL and keeps every value
// on PostgreSQL.
func TestEngineNarrows_ASizeOnOneSideIsTheEnginesAnswer(t *testing.T) {
	tests := []struct {
		name        string
		engine      typechange.Engine
		oldType     string
		newType     string
		wantNarrows bool
		wantWidens  bool
	}{
		{"PostgreSQL any number to a size", postgresEngine, "numeric", "numeric(10,2)", true, false},
		{"PostgreSQL a size to any number", postgresEngine, "numeric(10,2)", "numeric", false, true},
		{"PostgreSQL arrays narrow as their element does", postgresEngine, "numeric[]", "numeric(10,2)[]", true, false},
		{"PostgreSQL arrays widen as their element does", postgresEngine, "numeric(10,2)[]", "numeric[]", false, true},
		{"PostgreSQL bare bpchar to a length", postgresEngine, "bpchar", "char(1)", true, false},
		{"PostgreSQL bare bpchar to bare char", postgresEngine, "bpchar", "char", true, false},
		{"PostgreSQL a length to bare char", postgresEngine, "char(5)", "char", true, false},
		{"PostgreSQL bare char to a length", postgresEngine, "char", "char(5)", false, true},
		{"PostgreSQL a bare varchar to a length", postgresEngine, "varchar", "varchar(50)", true, false},
		{"PostgreSQL a length to a bare varchar", postgresEngine, "varchar(50)", "varchar", false, true},

		{"MySQL a fraction to a bare decimal", mysqlEngine, "decimal(10,2)", "decimal", true, true},
		{"MySQL more digits to a bare decimal", mysqlEngine, "decimal(12,0)", "decimal", true, false},
		{"MySQL fewer digits to a bare decimal", mysqlEngine, "decimal(5,0)", "decimal", false, true},
		{"MySQL a bare decimal to a fraction", mysqlEngine, "decimal", "decimal(10,2)", true, true},

		{"SQL Server MAX to a length", sqlServerEngine, "nvarchar(max)", "nvarchar(50)", true, false},
		{"SQL Server a length to MAX", sqlServerEngine, "nvarchar(50)", "nvarchar(max)", false, true},
		{"SQL Server a length to a bare nvarchar", sqlServerEngine, "nvarchar(50)", "nvarchar", true, false},
		{"SQL Server MAX to a bare varchar", sqlServerEngine, "varchar(max)", "varchar", true, false},
		{"SQL Server a bare varchar to a length", sqlServerEngine, "varchar", "varchar(50)", false, true},
		{"SQL Server a fraction to a bare decimal", sqlServerEngine, "DECIMAL(10,2)", "decimal", true, true},
		{"SQL Server more digits to a bare decimal", sqlServerEngine, "DECIMAL(20,0)", "decimal", true, false},

		{"Oracle any number to a size", oracleEngine, "NUMBER", "NUMBER(10,2)", true, false},
		{"Oracle a size to any number", oracleEngine, "NUMBER(10,2)", "NUMBER", false, true},
		{"Oracle no fraction to any number", oracleEngine, "NUMBER(*,0)", "NUMBER", false, true},
		{"Oracle no fraction to fewer digits", oracleEngine, "NUMBER(*,0)", "NUMBER(10)", true, false},
		{"Oracle a star keeps its scale", oracleEngine, "NUMBER(*,4)", "NUMBER(*,2)", true, true},
		{"Oracle a timestamp to none", oracleEngine, "TIMESTAMP(6)", "TIMESTAMP(0)", true, false},
		{"Oracle fewer digits to bare", oracleEngine, "TIMESTAMP(3)", "TIMESTAMP", false, true},
		{"Oracle more digits to bare", oracleEngine, "TIMESTAMP(9)", "TIMESTAMP", true, false},
		{"Oracle an interval's leading digits to bare", oracleEngine, "INTERVAL DAY(4) TO SECOND(6)", "INTERVAL DAY TO SECOND", true, false},
		{"Oracle a length in characters", oracleEngine, "VARCHAR2(100 CHAR)", "VARCHAR2(50 CHAR)", true, false},
		{"Oracle a length in bytes to characters", oracleEngine, "VARCHAR2(50)", "VARCHAR2(100 CHAR)", false, true},

		{"an engine that states nothing says nothing of a bare number", typechange.Engine{}, "numeric", "numeric(10,2)", false, false},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(test.engine.Narrows(test.oldType, test.newType), qt.Equals, test.wantNarrows)
			c.Assert(test.engine.Widens(test.oldType, test.newType), qt.Equals, test.wantWidens)
		})
	}
}

// TestEngineSame_ABareTypeIsTheSizeTheEngineGivesIt holds two spellings of one type to
// one answer, in either order, because a plan between them is a statement that changes
// nothing on every run. A row that answers false differs in what it holds, or in a name
// this package has no size for, and the comparison goes on to ask about it.
func TestEngineSame_ABareTypeIsTheSizeTheEngineGivesIt(t *testing.T) {
	tests := []struct {
		name   string
		engine typechange.Engine
		left   string
		right  string
		want   bool
	}{
		{"PostgreSQL bare numeric against itself", postgresEngine, "numeric", "numeric", true},
		{"PostgreSQL bare decimal is bare numeric", postgresEngine, "decimal", "numeric", true},
		{"PostgreSQL a scale of zero", postgresEngine, "numeric(10)", "numeric(10,0)", true},
		{"PostgreSQL an array of numbers", postgresEngine, "numeric(10,2)[]", "decimal(10,2)[]", true},
		{"PostgreSQL an array is not its element", postgresEngine, "numeric[]", "numeric", false},
		{"PostgreSQL any number is no size", postgresEngine, "numeric", "numeric(1000,0)", false},
		{"PostgreSQL char is one character", postgresEngine, "char(1)", "char", true},
		{"PostgreSQL character is char", postgresEngine, "character(1)", "char", true},
		{"PostgreSQL bpchar has no limit", postgresEngine, "bpchar", "char", false},
		{"PostgreSQL the catalog's name for char", postgresEngine, "bpchar(1)", "char", true},
		{"PostgreSQL the catalog's name for a length", postgresEngine, "bpchar(5)", "character(5)", true},
		{"PostgreSQL bit is one bit", postgresEngine, "bit(1)", "bit", true},
		{"PostgreSQL float is eight bytes", postgresEngine, "double precision", "float", true},
		{"PostgreSQL float(53) is eight bytes", postgresEngine, "double precision", "float(53)", true},
		{"PostgreSQL float(25) is eight bytes", postgresEngine, "float8", "float(25)", true},
		{"PostgreSQL float(24) is four bytes", postgresEngine, "real", "float(24)", true},
		{"PostgreSQL float(10) is four bytes", postgresEngine, "float4", "float(10)", true},
		{"PostgreSQL four bytes are not eight", postgresEngine, "real", "float", false},
		{"PostgreSQL a bare varchar is no length", postgresEngine, "varchar", "varchar(50)", false},
		{"PostgreSQL a name with no size", postgresEngine, "text", "text", false},
		{"PostgreSQL an interval and its default digits", postgresEngine, "interval", "interval(6)", true},
		{"PostgreSQL an interval keeps other fields", postgresEngine, "interval day to hour", "interval day to minute", false},

		{"MySQL bare decimal", mysqlEngine, "decimal(10,0)", "decimal", true},
		{"MySQL bare numeric", mysqlEngine, "decimal(10,0)", "numeric", true},
		{"MySQL a fraction is not bare", mysqlEngine, "decimal(10,2)", "decimal", false},
		{"MySQL float is four bytes", mysqlEngine, "float", "float(24)", true},
		{"MySQL float(25) is eight bytes", mysqlEngine, "double", "float(25)", true},
		{"MySQL real is eight bytes", mysqlEngine, "double", "real", true},
		{"MySQL double precision", mysqlEngine, "double", "double precision", true},
		{"MySQL four bytes are not eight", mysqlEngine, "float", "double", false},
		{"MySQL digits are not bits", mysqlEngine, "float(10,2)", "float", false},
		{"MySQL a double's digits are not bits", mysqlEngine, "double(10,2)", "double", false},
		{"MySQL a bare binary", mysqlEngine, "binary(1)", "binary", true},
		{"MySQL a length is not bare", mysqlEngine, "binary(5)", "binary", false},
		{"MariaDB's read of a year", mysqlEngine, "year(4)", "year", true},

		{"SQL Server bare decimal", sqlServerEngine, "DECIMAL(18,0)", "decimal", true},
		{"SQL Server bare numeric", sqlServerEngine, "NUMERIC(18,0)", "numeric", true},
		{"SQL Server float", sqlServerEngine, "FLOAT", "float(53)", true},
		{"SQL Server real", sqlServerEngine, "REAL", "float(24)", true},
		{"SQL Server float(10) is real", sqlServerEngine, "REAL", "float(10)", true},
		{"SQL Server four bytes are not eight", sqlServerEngine, "REAL", "float", false},
		{"SQL Server bare varchar is one", sqlServerEngine, "VARCHAR(1)", "varchar", true},
		{"SQL Server bare nvarchar is one", sqlServerEngine, "NVARCHAR(1)", "nvarchar", true},
		{"SQL Server a portable varchar", sqlServerEngine, "NVARCHAR(1)", "varchar", true},
		{"SQL Server bare nchar is one", sqlServerEngine, "NCHAR(1)", "nchar", true},
		{"SQL Server bare varbinary is one", sqlServerEngine, "VARBINARY(1)", "varbinary", true},
		{"SQL Server MAX is itself", sqlServerEngine, "NVARCHAR(MAX)", "nvarchar(max)", true},
		{"SQL Server MAX is no length", sqlServerEngine, "NVARCHAR(MAX)", "nvarchar(50)", false},
		{"SQL Server MAX is not bare", sqlServerEngine, "VARBINARY(MAX)", "varbinary", false},

		{"Oracle bare NUMBER", oracleEngine, "NUMBER", "NUMBER", true},
		{"Oracle a star is bare", oracleEngine, "NUMBER", "NUMBER(*)", true},
		{"Oracle a star with a scale is not", oracleEngine, "NUMBER", "NUMBER(*,0)", false},
		{"Oracle a star with a scale", oracleEngine, "NUMBER(*,0)", "NUMBER(*,0)", true},
		{"Oracle FLOAT", oracleEngine, "FLOAT(126)", "FLOAT", true},
		{"Oracle FLOAT keeps its bits", oracleEngine, "FLOAT(53)", "FLOAT", false},
		{"Oracle CHAR", oracleEngine, "CHAR(1)", "CHAR", true},
		{"Oracle NCHAR", oracleEngine, "NCHAR(1)", "NCHAR", true},
		{"Oracle a day interval", oracleEngine, "INTERVAL DAY(2) TO SECOND(6)", "INTERVAL DAY TO SECOND", true},
		{"Oracle a year interval", oracleEngine, "INTERVAL YEAR(2) TO MONTH", "INTERVAL YEAR TO MONTH", true},
		{"Oracle fewer digits are not bare", oracleEngine, "INTERVAL DAY(2) TO SECOND(0)", "INTERVAL DAY TO SECOND", false},
		{"Oracle more leading digits are not bare", oracleEngine, "INTERVAL YEAR(4) TO MONTH", "INTERVAL YEAR TO MONTH", false},
		{"Oracle year is not day", oracleEngine, "INTERVAL YEAR(2) TO MONTH", "INTERVAL DAY(2) TO SECOND(6)", false},

		{"an engine that states nothing", typechange.Engine{}, "char(1)", "char", false},
		{"an engine that states nothing says nothing of a float", typechange.Engine{}, "float4", "float(10)", false},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(test.engine.Same(test.left, test.right), qt.Equals, test.want)
			c.Assert(test.engine.Same(test.right, test.left), qt.Equals, test.want)
		})
	}
}

// TestIsNarrowing_NoEngineLeavesABareSizeAlone holds the reader that names no engine: a
// size on one side has no answer there, since the answer differs by engine.
func TestIsNarrowing_NoEngineLeavesABareSizeAlone(t *testing.T) {
	tests := []struct {
		name    string
		oldType string
		newType string
		want    bool
	}{
		{"a bare decimal to a size", "numeric", "numeric(10,2)", false},
		{"a size to a bare decimal", "numeric(10,2)", "numeric", false},
		{"a bare char to a length", "char", "char(5)", false},
		{"a length to a bare char", "char(5)", "char", false},
		{"MAX is no limit on its own", "nvarchar(max)", "nvarchar(50)", true},
		{"arrays of a length", "varchar(10)[]", "varchar(5)[]", true},
		{"an array against its element", "varchar(10)[]", "varchar(5)", false},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(typechange.IsNarrowing(test.oldType, test.newType), qt.Equals, test.want)
		})
	}
}

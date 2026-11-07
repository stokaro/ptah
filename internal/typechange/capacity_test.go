package typechange_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform/descriptor"
	"ptah.run/internal/typechange"
)

// TestEngineNarrows_ALowerCapacityNarrows holds each family to whether the new type can
// keep every value of the old one, and each row has its widening beside it, since a false
// destructive rating stops a plan as surely as a missed one loses a value. The server's
// answers for these rows are in integration/safety/type_capacity_e2e_test.go.
func TestEngineNarrows_ALowerCapacityNarrows(t *testing.T) {
	tiers := descriptor.BareTypes{Stated: true, DecimalPrecision: 10, FloatBits: 24, RealBits: 53, LobTiers: true}
	mysql := typechange.Engine{Bare: tiers}
	tests := []struct {
		name    string
		engine  typechange.Engine
		oldType string
		newType string
		want    bool
	}{
		{"MySQL longtext to text", mysql, "longtext", "text", true},
		{"MySQL longtext to mediumtext", mysql, "longtext", "mediumtext", true},
		{"MySQL mediumtext to text", mysql, "mediumtext", "text", true},
		{"MySQL text to tinytext", mysql, "text", "tinytext", true},
		{"MySQL text to longtext", mysql, "text", "longtext", false},
		{"MySQL tinytext to text", mysql, "tinytext", "text", false},
		{"MySQL a length that fits tinytext", mysql, "varchar(63)", "tinytext", false},
		{"MySQL a length that may not fit tinytext", mysql, "varchar(64)", "tinytext", true},
		{"MySQL a length that fits text", mysql, "varchar(16383)", "text", false},
		{"MySQL a length that may not fit text", mysql, "varchar(16384)", "text", true},
		{"MySQL tinytext fits its bytes as characters", mysql, "tinytext", "varchar(255)", false},
		{"MySQL tinytext may not fit fewer", mysql, "tinytext", "varchar(254)", true},
		{"MySQL text may not fit a length", mysql, "text", "varchar(100)", true},
		{"MySQL a text length holds four bytes each", mysql, "text", "text(65535)", false},
		{"MySQL a text length a smaller tier holds", mysql, "mediumtext", "text(60)", true},
		{"MySQL a length and the tier it builds", mysql, "tinytext", "text(60)", false},

		{"MySQL longblob to blob", mysql, "longblob", "blob", true},
		{"MySQL mediumblob to blob", mysql, "mediumblob", "blob", true},
		{"MySQL blob to tinyblob", mysql, "blob", "tinyblob", true},
		{"MySQL tinyblob to blob", mysql, "tinyblob", "blob", false},
		{"MySQL blob to longblob", mysql, "blob", "longblob", false},
		{"MySQL blob to a length", mysql, "blob", "varbinary(50)", true},
		{"MySQL a length to blob", mysql, "varbinary(50)", "blob", false},
		{"MySQL a length that fits tinyblob", mysql, "varbinary(255)", "tinyblob", false},
		{"MySQL a length past tinyblob", mysql, "varbinary(256)", "tinyblob", true},
		{"MySQL a length past blob", mysql, "varbinary(65000)", "blob(100)", true},
		{"MySQL varbinary down", mysql, "varbinary(50)", "varbinary(5)", true},
		{"MySQL varbinary up", mysql, "varbinary(5)", "varbinary(50)", false},
		{"MySQL binary down", mysql, "binary(10)", "binary(5)", true},
		{"MySQL binary up", mysql, "binary(5)", "binary(10)", false},
		{"MySQL binary to bare", mysql, "binary(5)", "binary", true},
		{"MySQL bare binary up", mysql, "binary", "binary(5)", false},
		{"MySQL fixed to variable keeps the bytes", mysql, "binary(5)", "varbinary(5)", false},
		{"MySQL bit down", mysql, "bit(8)", "bit(4)", true},
		{"MySQL bit up", mysql, "bit(4)", "bit(8)", false},
		{"MySQL bit to bare", mysql, "bit(8)", "bit", true},

		{"MySQL double to float", mysql, "double", "float", true},
		{"MySQL float to double", mysql, "float", "double", false},
		{"MySQL real to float", mysql, "real", "float", true},
		{"MySQL float(25) to float(24)", mysql, "float(25)", "float(24)", true},
		{"MySQL digits down", mysql, "float(10,2)", "float(8,2)", true},
		{"MySQL digits up", mysql, "float(8,2)", "float(10,2)", false},
		{"MySQL double digits down", mysql, "double(10,2)", "double(8,2)", true},

		{"MySQL bigint to mediumint", mysql, "bigint", "mediumint", true},
		{"MySQL mediumint to smallint", mysql, "mediumint", "smallint", true},
		{"MySQL smallint to tinyint", mysql, "smallint", "tinyint", true},
		{"MySQL tinyint to smallint", mysql, "tinyint", "smallint", false},
		{"MySQL a sign lost", mysql, "int", "int unsigned", true},
		{"MySQL half the range lost", mysql, "int unsigned", "int", true},
		{"MySQL bigint half the range lost", mysql, "bigint unsigned", "bigint", true},
		{"MySQL unsigned to wider", mysql, "int unsigned", "bigint", false},
		{"MySQL unsigned smallint to int", mysql, "smallint unsigned", "int", false},
		{"MySQL display widths with a sign", mysql, "int(10) unsigned", "int(11)", true},
		{"MySQL one type with a sign", mysql, "int unsigned", "int unsigned", false},
		{"MySQL ZEROFILL is unsigned", mysql, "int zerofill", "int", true},

		{"MySQL a fraction to an integer", mysql, "decimal(10,2)", "int", true},
		{"MySQL more digits than an integer", mysql, "decimal(10,0)", "int", true},
		{"MySQL fewer digits than an integer", mysql, "decimal(9,0)", "int", false},
		{"MySQL a sign to an unsigned integer", mysql, "decimal(5,0)", "int unsigned", true},
		{"MySQL an integer to fewer digits", mysql, "int", "decimal(5,0)", true},
		{"MySQL an integer to its digits", mysql, "int", "decimal(10,0)", false},
		{"MySQL an integer to digits and a fraction", mysql, "int", "decimal(10,2)", true},
		{"MySQL a bigint to its digits", mysql, "bigint", "decimal(19,0)", false},
		{"MySQL an unsigned bigint past them", mysql, "bigint unsigned", "decimal(19,0)", true},
		{"MySQL a fraction with room for its digits", mysql, "decimal(5,2)", "int", true},
		{"MySQL a bare decimal to an integer", mysql, "decimal", "int", true},
		{"MySQL a double to an integer", mysql, "double", "int", true},
		{"MySQL an integer past a double", mysql, "bigint", "double", true},
		{"MySQL an integer a double holds", mysql, "int", "double", false},
		{"MySQL an integer past a float", mysql, "int", "float", true},
		{"MySQL an integer a float holds", mysql, "mediumint", "float", false},
		{"MySQL digits past a double", mysql, "decimal(20,2)", "double", true},
		{"MySQL digits a double holds", mysql, "decimal(15,2)", "double", false},
		{"MySQL digits a float holds", mysql, "decimal(6,2)", "float", false},
		{"MySQL digits past a float", mysql, "decimal(7,2)", "float", true},
		{"MySQL a double to digits", mysql, "double", "decimal(10,2)", true},

		{"MySQL text to a number", mysql, "text", "int", true},
		{"MySQL text to a date", mysql, "varchar(20)", "date", true},
		{"MySQL a number to text", mysql, "int", "text", false},
		{"MySQL a number to room for its digits", mysql, "int", "varchar(11)", false},
		{"MySQL a number past a length", mysql, "int", "varchar(5)", true},
		{"MySQL a sign past a length", mysql, "int", "varchar(10)", true},
		{"MySQL digits to room for them", mysql, "decimal(10,2)", "varchar(12)", false},
		{"MySQL digits past a length", mysql, "decimal(10,2)", "varchar(11)", true},
		{"MySQL a float past a length", mysql, "double", "varchar(10)", true},

		{"MySQL a member lost", mysql, "enum('a','b')", "enum('a')", true},
		{"MySQL a member added", mysql, "enum('a')", "enum('a','b')", false},
		{"MySQL a set member lost", mysql, "set('a','b')", "set('a')", true},
		{"MySQL text to an enum", mysql, "varchar(10)", "enum('a')", true},
		{"MySQL an enum to text", mysql, "enum('a')", "varchar(10)", false},

		{"PostgreSQL double to real", postgresEngine, "double precision", "real", true},
		{"PostgreSQL real to double", postgresEngine, "real", "double precision", false},
		{"PostgreSQL float to real", postgresEngine, "float", "real", true},
		{"PostgreSQL the catalog's float", postgresEngine, "float8", "float4", true},
		{"PostgreSQL one float in two names", postgresEngine, "float8", "double precision", false},
		{"PostgreSQL any number past a length", postgresEngine, "numeric", "varchar(20)", true},
		{"PostgreSQL an array of doubles", postgresEngine, "double precision[]", "real[]", true},
		{"PostgreSQL an array of integers", postgresEngine, "integer[]", "smallint[]", true},
		{"PostgreSQL an array against its element", postgresEngine, "double precision[]", "real", false},
		{"PostgreSQL any number to bytes of mantissa", postgresEngine, "numeric", "real", true},
		{"PostgreSQL a double to any number", postgresEngine, "double precision", "numeric", false},
		{"PostgreSQL an integer to any number", postgresEngine, "bigint", "numeric", false},
		{"PostgreSQL any number to an integer", postgresEngine, "numeric", "bigint", true},
		{"PostgreSQL bit down", postgresEngine, "bit(8)", "bit(4)", true},
		{"PostgreSQL bit up holds the bits", postgresEngine, "bit(4)", "bit(8)", false},
		{"PostgreSQL varying down", postgresEngine, "varbit(8)", "varbit(4)", true},
		{"PostgreSQL varying to a length", postgresEngine, "varbit", "varbit(8)", true},
		{"PostgreSQL a length to varying", postgresEngine, "varbit(4)", "varbit", false},
		{"PostgreSQL the catalog's varying", postgresEngine, "bit varying(8)", "varbit(4)", true},
		{"PostgreSQL text has no tier", postgresEngine, "text", "text", false},
		{"PostgreSQL bytes have no limit", postgresEngine, "bytea", "bytea", false},

		{"SQL Server MAX to a length", sqlServerEngine, "VARBINARY(MAX)", "varbinary(50)", true},
		{"SQL Server a length to MAX", sqlServerEngine, "VARBINARY(50)", "varbinary(max)", false},
		{"SQL Server a length to bare", sqlServerEngine, "VARBINARY(5)", "varbinary", true},
		{"SQL Server binary to bare", sqlServerEngine, "BINARY(5)", "binary", true},
		{"SQL Server image to a length", sqlServerEngine, "IMAGE", "varbinary(50)", true},
		{"SQL Server a length to image", sqlServerEngine, "VARBINARY(50)", "image", false},
		{"SQL Server ntext to a length", sqlServerEngine, "NTEXT", "nvarchar(50)", true},
		{"SQL Server a length to ntext", sqlServerEngine, "NVARCHAR(50)", "ntext", false},
		{"SQL Server money to smallmoney", sqlServerEngine, "MONEY", "smallmoney", true},
		{"SQL Server smallmoney to money", sqlServerEngine, "SMALLMONEY", "money", false},
		{"SQL Server money to its digits", sqlServerEngine, "MONEY", "decimal(19,4)", false},
		{"SQL Server money to fewer", sqlServerEngine, "MONEY", "decimal(10,2)", true},
		{"SQL Server float to real", sqlServerEngine, "FLOAT", "real", true},
		{"SQL Server real to float", sqlServerEngine, "REAL", "float", false},
		{"SQL Server float(53) to float(24)", sqlServerEngine, "FLOAT", "float(24)", true},
		{"SQL Server the same width in another name", sqlServerEngine, "NVARCHAR(50)", "nchar(50)", false},

		{"Oracle double to single", oracleEngine, "BINARY_DOUBLE", "BINARY_FLOAT", true},
		{"Oracle single to double", oracleEngine, "BINARY_FLOAT", "BINARY_DOUBLE", false},
		{"Oracle FLOAT keeps its bits", oracleEngine, "FLOAT(10)", "FLOAT(5)", true},
		{"Oracle FLOAT up", oracleEngine, "FLOAT(5)", "FLOAT(10)", false},
		{"Oracle bare FLOAT down", oracleEngine, "FLOAT", "FLOAT(5)", true},
		{"Oracle FLOAT past a double", oracleEngine, "FLOAT", "BINARY_DOUBLE", true},
		{"Oracle a double to FLOAT", oracleEngine, "BINARY_DOUBLE", "FLOAT", false},
		{"Oracle RAW down", oracleEngine, "RAW(10)", "RAW(5)", true},
		{"Oracle RAW up", oracleEngine, "RAW(5)", "RAW(10)", false},
		{"Oracle BLOB to RAW", oracleEngine, "BLOB", "RAW(50)", true},
		{"Oracle RAW to BLOB", oracleEngine, "RAW(50)", "BLOB", false},
		{"Oracle NCLOB to a length", oracleEngine, "NCLOB", "NVARCHAR2(50)", true},
		{"Oracle digits past a double", oracleEngine, "NUMBER(20,2)", "BINARY_DOUBLE", true},
		{"Oracle digits a double holds", oracleEngine, "NUMBER(10,2)", "BINARY_DOUBLE", false},
		{"Oracle a number past a length", oracleEngine, "NUMBER(10)", "VARCHAR2(5)", true},

		{"CockroachDB STRING down", postgresEngine, "string(100)", "string(5)", true},
		{"CockroachDB STRING up", postgresEngine, "string(5)", "string(100)", false},
		{"CockroachDB bare STRING to a length", postgresEngine, "string", "string(5)", true},
		{"CockroachDB a length to bare STRING", postgresEngine, "string(5)", "string", false},
		{"CockroachDB text to a length", postgresEngine, "text", "string(5)", true},
		{"CockroachDB VARCHAR to a shorter STRING", postgresEngine, "varchar(100)", "string(5)", true},

		{"an engine that states nothing says nothing of a bare binary", typechange.Engine{}, "binary(8)", "binary", false},
		{"an engine that states nothing says nothing of a bare bit", typechange.Engine{}, "bit(8)", "bit", false},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(test.engine.Narrows(test.oldType, test.newType), qt.Equals, test.want)
		})
	}
}

// TestIsNarrowing_ANameThatStatesItsCapacity holds the reader with no engine to the names
// no engine reads differently: ClickHouse's and MySQL's own, and the names of a float's
// width. `real` and `float` are not among them, since each is four bytes on one engine and
// eight on another.
func TestIsNarrowing_ANameThatStatesItsCapacity(t *testing.T) {
	tests := []struct {
		name    string
		oldType string
		newType string
		want    bool
	}{
		{"Float64 to Float32", "Float64", "Float32", true},
		{"Float32 to Float64", "Float32", "Float64", false},
		{"Nullable floats", "Nullable(Float64)", "Nullable(Float32)", true},
		{"LowCardinality floats", "LowCardinality(Float64)", "LowCardinality(Float32)", true},
		{"Array floats", "Array(Float64)", "Array(Float32)", true},
		{"An array against its element", "Array(Float64)", "Float32", false},
		{"String to FixedString", "String", "FixedString(2)", true},
		{"FixedString to String", "FixedString(2)", "String", false},
		{"FixedString down", "FixedString(10)", "FixedString(2)", true},
		{"FixedString up", "FixedString(2)", "FixedString(10)", false},
		{"LowCardinality strings", "LowCardinality(String)", "LowCardinality(FixedString(2))", true},
		{"Nullable strings", "Nullable(String)", "Nullable(FixedString(2))", true},
		{"Array strings", "Array(String)", "Array(FixedString(2))", true},
		{"LowCardinality adds no capacity", "String", "LowCardinality(String)", false},
		{"LowCardinality takes none", "LowCardinality(String)", "String", false},
		{"Array integers", "Array(Int64)", "Array(Int32)", true},
		{"An integer array against its element", "Array(Int64)", "Int32", false},
		{"LowCardinality integers", "LowCardinality(Int64)", "LowCardinality(Int32)", true},
		{"Decimal64 to Decimal32", "Decimal64(2)", "Decimal32(2)", true},
		{"Decimal32 to Decimal64", "Decimal32(2)", "Decimal64(2)", false},
		{"Decimal128 to Decimal64", "Decimal128(2)", "Decimal64(2)", true},
		{"Decimal256 to Decimal128", "Decimal256(2)", "Decimal128(2)", true},
		{"A scale down", "Decimal64(4)", "Decimal64(2)", true},
		{"A scale up takes whole digits", "Decimal64(2)", "Decimal64(4)", true},
		{"A named width to digits", "Decimal(38,2)", "Decimal64(2)", true},
		{"Digits to a named width", "Decimal(9,2)", "Decimal64(2)", false},
		{"Int64 to Float64", "Int64", "Float64", true},
		{"Int32 to Float64", "Int32", "Float64", false},
		{"Int32 to Float32", "Int32", "Float32", true},
		{"Int16 to Float32", "Int16", "Float32", false},
		{"UInt32 to Float32", "UInt32", "Float32", true},
		{"Float64 to Int64", "Float64", "Int64", true},
		{"Decimal to Int32", "Decimal(10,2)", "Int32", true},
		{"Decimal to Float64", "Decimal(10,2)", "Float64", false},
		{"More digits to Float64", "Decimal(20,2)", "Float64", true},
		{"Float64 to Decimal", "Float64", "Decimal(10,2)", true},
		{"String to Int32", "String", "Int32", true},
		{"Int32 to String", "Int32", "String", false},
		{"A member lost", "Enum8('a' = 1, 'b' = 2)", "Enum8('a' = 1)", true},
		{"A member added", "Enum8('a' = 1)", "Enum8('a' = 1, 'b' = 2)", false},
		{"A wider enum takes no member", "Enum16('a' = 1)", "Enum8('a' = 1)", false},

		{"longtext to text", "longtext", "text", true},
		{"text to tinytext", "text", "tinytext", true},
		{"longblob to blob", "longblob", "blob", true},
		{"blob to tinyblob", "blob", "tinyblob", true},
		{"text to longtext", "text", "longtext", false},
		{"float8 to float4", "float8", "float4", true},
		{"float4 to float8", "float4", "float8", false},
		{"double to Oracle's single", "BINARY_DOUBLE", "BINARY_FLOAT", true},
		{"a bit length down", "bit(8)", "bit(4)", true},
		{"a bare bit has no answer", "bit(8)", "bit", false},
		{"a bare binary has no answer", "binary(8)", "binary", false},

		{"real has no answer", "double precision", "real", false},
		{"float has no answer", "float", "real", false},
		{"text has no tier on its own", "text", "text", false},
		{"blob has no tier on its own", "blob", "varbinary(70000)", true},
		{"a length against blob has no tier", "varbinary(70000)", "blob", false},
		{"a bare varchar has no limit against a tier", "varchar", "tinytext", true},
		{"bytea has no limit", "bytea", "varbinary(5)", true},
		{"float(p) needs an engine", "float(30)", "float(10)", false},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(typechange.IsNarrowing(test.oldType, test.newType), qt.Equals, test.want)
		})
	}
}

// TestEngineSame_ATierAndTheLengthThatBuildsIt holds a length MySQL resolves to a tier to
// that tier, as the server reads it back: a plan here changes nothing on every run.
func TestEngineSame_ATierAndTheLengthThatBuildsIt(t *testing.T) {
	mysql := typechange.Engine{Bare: descriptor.BareTypes{Stated: true, DecimalPrecision: 10, FloatBits: 24, RealBits: 53, LobTiers: true}}
	tests := []struct {
		name  string
		left  string
		right string
		want  bool
	}{
		{"text and its largest length", "text", "text(16383)", true},
		{"tinytext and a short length", "tinytext", "text(60)", true},
		{"text and a length past tinytext", "text", "text(64)", true},
		{"mediumtext and a long length", "mediumtext", "text(70000)", true},
		{"mediumtext and a length past text", "mediumtext", "text(65535)", true},
		{"tinyblob and a short length", "tinyblob", "blob(100)", true},
		{"blob and its largest length", "blob", "blob(65535)", true},
		{"mediumblob and a length past blob", "mediumblob", "blob(65536)", true},
		{"text is not a blob", "text", "blob", false},
		{"tiers differ", "tinytext", "text", false},
		{"a length is not a tier", "varbinary(255)", "tinyblob", false},
		{"a varying bit is not a bit", "bit(8)", "varbit(8)", false},
		{"two names of a varying bit", "bit varying(8)", "varbit(8)", true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(mysql.Same(test.left, test.right), qt.Equals, test.want)
		})
	}
}

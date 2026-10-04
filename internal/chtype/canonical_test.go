package chtype_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/chtype"
)

// Each row is a spelling and the type the server stored for it, read back from
// system.columns on 24.10.4 and 26.9.8, which answered every row alike.
func TestCanonical_ASpellingAgreesWithTheTypeTheServerStores(t *testing.T) {
	tests := []struct {
		written string
		stored  string
	}{
		{written: "INT", stored: "Int32"},
		{written: "Integer", stored: "Int32"},
		{written: "mediumint", stored: "Int32"},
		{written: "INT SIGNED", stored: "Int32"},
		{written: "bigint", stored: "Int64"},
		{written: "BigInt", stored: "Int64"},
		{written: "BIGINT SIGNED", stored: "Int64"},
		{written: "SIGNED", stored: "Int64"},
		{written: "smallint", stored: "Int16"},
		{written: "tinyint", stored: "Int8"},
		{written: "INT1", stored: "Int8"},
		{written: "BYTE", stored: "Int8"},
		{written: "TINYINT UNSIGNED", stored: "UInt8"},
		{written: "SMALLINT UNSIGNED", stored: "UInt16"},
		{written: "YEAR", stored: "UInt16"},
		{written: "MEDIUMINT UNSIGNED", stored: "UInt32"},
		{written: "int unsigned", stored: "UInt32"},
		{written: "INT  UNSIGNED", stored: "UInt32"},
		{written: "INTEGER UNSIGNED", stored: "UInt32"},
		{written: "bigint unsigned", stored: "UInt64"},
		{written: "UNSIGNED", stored: "UInt64"},
		{written: "BIT", stored: "UInt64"},
		{written: "float", stored: "Float32"},
		{written: "REAL", stored: "Float32"},
		{written: "SINGLE", stored: "Float32"},
		{written: "double", stored: "Float64"},
		{written: "DOUBLE  PRECISION", stored: "Float64"},
		{written: "bool", stored: "Bool"},
		{written: "BOOLEAN", stored: "Bool"},
		{written: "decimal(10,2)", stored: "Decimal(10, 2)"},
		{written: "numeric(10,2)", stored: "Decimal(10, 2)"},
		{written: "DEC(10,2)", stored: "Decimal(10, 2)"},
		{written: "FIXED(10, 2)", stored: "Decimal(10, 2)"},
		{written: "Decimal(10)", stored: "Decimal(10, 0)"},
		{written: "Decimal(38)", stored: "Decimal(38, 0)"},
		{written: "DECIMAL", stored: "Decimal(10, 0)"},
		{written: "NUMERIC", stored: "Decimal(10, 0)"},
		{written: "Decimal32(2)", stored: "Decimal(9, 2)"},
		{written: "DECIMAL32(2)", stored: "Decimal(9, 2)"},
		{written: "Decimal64(4)", stored: "Decimal(18, 4)"},
		{written: "Decimal128(3)", stored: "Decimal(38, 3)"},
		{written: "Decimal256(5)", stored: "Decimal(76, 5)"},
		{written: "datetime", stored: "DateTime"},
		{written: "TIMESTAMP", stored: "DateTime"},
		{written: "DateTime32", stored: "DateTime"},
		{written: "DateTime32('UTC')", stored: "DateTime('UTC')"},
		{written: "DateTime( 'UTC' )", stored: "DateTime('UTC')"},
		{written: "datetime64", stored: "DateTime64(3)"},
		{written: "DateTime64", stored: "DateTime64(3)"},
		{written: "DATETIME64(3)", stored: "DateTime64(3)"},
		{written: "DateTime64(3,'UTC')", stored: "DateTime64(3, 'UTC')"},
		{written: "date", stored: "Date"},
		{written: "TEXT", stored: "String"},
		{written: "LONGTEXT", stored: "String"},
		{written: "char(3)", stored: "String"},
		{written: "VARCHAR(10)", stored: "String"},
		{written: "NVARCHAR(10)", stored: "String"},
		{written: "VARBINARY(4)", stored: "String"},
		{written: "BYTEA", stored: "String"},
		{written: "BINARY(4)", stored: "FixedString(4)"},
		{written: "FixedString( 4 )", stored: "FixedString(4)"},
		{written: "INET4", stored: "IPv4"},
		{written: "Enum('a'=1,'b'=2)", stored: "Enum8('a' = 1, 'b' = 2)"},
		{written: "Enum8('a'=1,'b'=2)", stored: "Enum8('a' = 1, 'b' = 2)"},
		{written: "Enum('a', 'b')", stored: "Enum8('a' = 1, 'b' = 2)"},
		{written: "Enum('a' = 1, 'b')", stored: "Enum8('a' = 1, 'b' = 2)"},
		{written: "Enum('a' = 5, 'b')", stored: "Enum8('a' = 5, 'b' = 6)"},
		{written: "Enum('a' = 200)", stored: "Enum16('a' = 200)"},
		{written: "Enum('a' = -129)", stored: "Enum16('a' = -129)"},
		{written: "Enum('a' = 127, 'b' = -128)", stored: "Enum8('b' = -128, 'a' = 127)"},
		{written: "Enum8('a', 'b')", stored: "Enum8('a' = 1, 'b' = 2)"},
		{written: "Enum16('a', 'b')", stored: "Enum16('a' = 1, 'b' = 2)"},
		{written: "ENUM('x' = 1)", stored: "Enum8('x' = 1)"},
		{written: "Enum8('it''s' = 1)", stored: `Enum8('it\'s' = 1)`},
		{written: "Map(String,UInt64)", stored: "Map(String, UInt64)"},
		{written: "Map(String, INT)", stored: "Map(String, Int32)"},
		{written: "Map(LowCardinality(String), Nullable(BIGINT))", stored: "Map(LowCardinality(String), Nullable(Int64))"},
		{written: "Array(INT)", stored: "Array(Int32)"},
		{written: "Array(Array(INT))", stored: "Array(Array(Int32))"},
		{written: "Array(Decimal32(2))", stored: "Array(Decimal(9, 2))"},
		{written: "Array(Boolean)", stored: "Array(Bool)"},
		{written: "Tuple(a INT, b BIGINT)", stored: "Tuple(a Int32, b Int64)"},
		{written: "Tuple(INT, String)", stored: "Tuple(Int32, String)"},
		{written: "Tuple(date Date, int INT)", stored: "Tuple(date Date, int Int32)"},
		{written: "Nullable(INT)", stored: "Nullable(Int32)"},
		{written: "Nullable(Boolean)", stored: "Nullable(Bool)"},
		{written: "Nullable(DateTime64)", stored: "Nullable(DateTime64(3))"},
		{written: "Nullable(Decimal32(2))", stored: "Nullable(Decimal(9, 2))"},
		{written: "LowCardinality(Nullable(TEXT))", stored: "LowCardinality(Nullable(String))"},
	}

	for _, tt := range tests {
		t.Run(tt.written, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(chtype.Canonical(tt.written), qt.Equals, chtype.Canonical(tt.stored))
		})
	}
}

// A width, a scale, a time zone, a label or a value is part of the type, so no
// pair here may compare equal.
func TestCanonical_DifferentTypesStayApart(t *testing.T) {
	tests := []struct {
		name string
		a    string
		b    string
	}{
		{name: "integer width", a: "Int32", b: "Int64"},
		{name: "the narrowest integer", a: "Int8", b: "Int16"},
		{name: "unsigned width", a: "UInt8", b: "UInt16"},
		{name: "signedness", a: "Int32", b: "UInt32"},
		{name: "an alias keeps its width", a: "INT", b: "BIGINT"},
		{name: "an alias keeps its signedness", a: "SMALLINT", b: "SMALLINT UNSIGNED"},
		{name: "float width", a: "Float32", b: "Float64"},
		{name: "decimal precision", a: "Decimal(9, 2)", b: "Decimal(18, 2)"},
		{name: "decimal scale", a: "Decimal(9, 2)", b: "Decimal(9, 4)"},
		{name: "decimal family width", a: "Decimal32(2)", b: "Decimal64(2)"},
		{name: "second against subsecond precision", a: "DateTime", b: "DateTime64(3)"},
		{name: "subsecond precision", a: "DateTime64(3)", b: "DateTime64(6)"},
		{name: "time zone", a: "DateTime('UTC')", b: "DateTime('Europe/Prague')"},
		{name: "enum width", a: "Enum8('a' = 1)", b: "Enum16('a' = 1)"},
		{name: "enum value", a: "Enum8('a' = 1)", b: "Enum8('a' = 2)"},
		{name: "enum label", a: "Enum8('a' = 1)", b: "Enum8('b' = 1)"},
		{name: "a space inside a label", a: "Enum8('a b' = 1)", b: "Enum8('ab' = 1)"},
		{name: "fixed against variable length", a: "String", b: "FixedString(4)"},
		{name: "fixed length", a: "FixedString(4)", b: "FixedString(8)"},
		{name: "low cardinality", a: "LowCardinality(String)", b: "String"},
		{name: "nullable", a: "Nullable(Int32)", b: "Int32"},
		{name: "nullable width", a: "Nullable(Int32)", b: "Nullable(Int64)"},
		{name: "array element width", a: "Array(Int32)", b: "Array(Int64)"},
		{name: "map value width", a: "Map(String, Int32)", b: "Map(String, Int64)"},
		{name: "tuple element name", a: "Tuple(a Int32)", b: "Tuple(b Int32)"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(chtype.Canonical(tt.a), qt.Not(qt.Equals), chtype.Canonical(tt.b))
		})
	}
}

// The key is pinned for a few spellings, so a change to its shape is seen
// here rather than only as a pair that stopped agreeing.
func TestCanonical_Spelling(t *testing.T) {
	tests := []struct {
		written string
		want    string
	}{
		{written: "Decimal32(2)", want: "Decimal(9,2)"},
		{written: "SMALLINT UNSIGNED", want: "UInt16"},
		{written: "Enum('b' = 2, 'a')", want: "Enum8('b'=2,'a'=3)"},
		{written: "Tuple(a INT, b String)", want: "Tuple(a Int32,b String)"},
		{written: "Enum8('x\\'y' = 1)", want: `Enum8('x\'y'=1)`},
		{written: "Enum()", want: "Enum()"},
		{written: "Enum8('a' = x)", want: "Enum8('a'=x)"},
		{written: "Decimal(9, 2)", want: "Decimal(9,2)"},
	}

	for _, tt := range tests {
		t.Run(tt.written, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(chtype.Canonical(tt.written), qt.Equals, tt.want)
		})
	}
}

func TestAdmitsNull(t *testing.T) {
	tests := []struct {
		columnType string
		want       bool
	}{
		{columnType: "Nullable(Int32)", want: true},
		{columnType: " Nullable( String ) ", want: true},
		{columnType: "LowCardinality(Nullable(String))", want: true},
		{columnType: "Int32", want: false},
		{columnType: "LowCardinality(String)", want: false},
		{columnType: "Array(Nullable(Int32))", want: false},
		{columnType: "Map(String, Nullable(Int32))", want: false},
		{columnType: "Tuple(a Nullable(Int32))", want: false},
		{columnType: "Enum8('Nullable(' = 1)", want: false},
		{columnType: "Nullable(Int32) CODEC(ZSTD)", want: false},
	}

	for _, tt := range tests {
		t.Run(tt.columnType, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(chtype.AdmitsNull(tt.columnType), qt.Equals, tt.want)
		})
	}
}

func TestStripNullable(t *testing.T) {
	tests := []struct {
		columnType string
		want       string
	}{
		{columnType: "Nullable(Int64)", want: "Int64"},
		{columnType: " Nullable( Decimal(9, 2) ) ", want: "Decimal(9, 2)"},
		{columnType: "Int64", want: "Int64"},
		{columnType: "LowCardinality(Nullable(String))", want: "LowCardinality(Nullable(String))"},
		{columnType: "Array(Nullable(Int32))", want: "Array(Nullable(Int32))"},
	}

	for _, tt := range tests {
		t.Run(tt.columnType, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(chtype.StripNullable(tt.columnType), qt.Equals, tt.want)
		})
	}
}

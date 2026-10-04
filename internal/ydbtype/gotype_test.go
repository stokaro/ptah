package ydbtype_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/ydbtype"
)

// Each Go type is the one ydb-go-sdk's database/sql driver scanned a value of
// the YDB type into on local-ydb 26.2.1.14, except where the type's doc
// comment says why it differs.
func TestGoTypeOf_HappyPath(t *testing.T) {
	tests := []struct {
		name    string
		ydbType string
		want    ydbtype.GoType
	}{
		{name: "bool", ydbType: "Bool", want: ydbtype.GoType{Name: "bool"}},
		{name: "one byte integer", ydbType: "Int8", want: ydbtype.GoType{Name: "int8"}},
		{name: "two byte integer", ydbType: "Int16", want: ydbtype.GoType{Name: "int16"}},
		{name: "four byte integer", ydbType: "Int32", want: ydbtype.GoType{Name: "int32"}},
		{name: "eight byte integer", ydbType: "Int64", want: ydbtype.GoType{Name: "int64"}},
		{name: "one byte unsigned", ydbType: "Uint8", want: ydbtype.GoType{Name: "uint8"}},
		{name: "two byte unsigned", ydbType: "Uint16", want: ydbtype.GoType{Name: "uint16"}},
		{name: "four byte unsigned", ydbType: "Uint32", want: ydbtype.GoType{Name: "uint32"}},
		{name: "eight byte unsigned", ydbType: "Uint64", want: ydbtype.GoType{Name: "uint64"}},
		{name: "single precision", ydbType: "Float", want: ydbtype.GoType{Name: "float32"}},
		{name: "double precision", ydbType: "Double", want: ydbtype.GoType{Name: "float64"}},
		{name: "dynumber", ydbType: "DyNumber", want: ydbtype.GoType{Name: "string"}},
		{name: "bytes", ydbType: "String", want: ydbtype.GoType{Name: "[]byte"}},
		{name: "text", ydbType: "Utf8", want: ydbtype.GoType{Name: "string"}},
		{name: "json", ydbType: "Json", want: ydbtype.GoType{Name: "string"}},
		{name: "json document", ydbType: "JsonDocument", want: ydbtype.GoType{Name: "string"}},
		{name: "yson", ydbType: "Yson", want: ydbtype.GoType{Name: "[]byte"}},
		{name: "uuid", ydbType: "Uuid", want: ydbtype.GoType{Name: "string"}},
		{name: "date", ydbType: "Date", want: ydbtype.GoType{Name: "time.Time", Import: "time"}},
		{name: "datetime", ydbType: "Datetime", want: ydbtype.GoType{Name: "time.Time", Import: "time"}},
		{name: "timestamp", ydbType: "Timestamp", want: ydbtype.GoType{Name: "time.Time", Import: "time"}},
		{name: "wide date", ydbType: "Date32", want: ydbtype.GoType{Name: "time.Time", Import: "time"}},
		{name: "wide datetime", ydbType: "Datetime64", want: ydbtype.GoType{Name: "time.Time", Import: "time"}},
		{name: "wide timestamp", ydbType: "Timestamp64", want: ydbtype.GoType{Name: "time.Time", Import: "time"}},
		{name: "interval", ydbType: "Interval", want: ydbtype.GoType{Name: "time.Duration", Import: "time"}},
		{name: "wide interval", ydbType: "Interval64", want: ydbtype.GoType{Name: "time.Duration", Import: "time"}},
		{name: "small serial", ydbType: "SmallSerial", want: ydbtype.GoType{Name: "int16"}},
		{name: "serial", ydbType: "Serial", want: ydbtype.GoType{Name: "int32"}},
		{name: "big serial", ydbType: "BigSerial", want: ydbtype.GoType{Name: "int64"}},
		{name: "fixed decimal", ydbType: "Decimal(22,9)",
			want: ydbtype.GoType{Name: "types.Decimal", Import: "github.com/ydb-platform/ydb-go-sdk/v3/table/types"}},
		{name: "decimal with spaces", ydbType: " Decimal(35, 10) ",
			want: ydbtype.GoType{Name: "types.Decimal", Import: "github.com/ydb-platform/ydb-go-sdk/v3/table/types"}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			got, ok := ydbtype.GoTypeOf(test.ydbType)
			c.Assert(ok, qt.IsTrue)
			c.Assert(got, qt.Equals, test.want)
		})
	}
}

// A name that is not a YDB type has no answer. A SQL declaration is one of
// them: Map reads it first, and only its YDB answer has a Go type here.
func TestGoTypeOf_FailurePath(t *testing.T) {
	tests := []struct {
		name    string
		ydbType string
	}{
		{name: "sql declaration", ydbType: "VARCHAR(255)"},
		{name: "postgres eight byte integer", ydbType: "int8"},
		{name: "sql float", ydbType: "FLOAT"},
		{name: "varchar is bytes on YDB and never written", ydbType: "Varchar"},
		{name: "unclosed decimal", ydbType: "Decimal(22,9"},
		{name: "empty", ydbType: ""},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			got, ok := ydbtype.GoTypeOf(test.ydbType)
			c.Assert(ok, qt.IsFalse)
			c.Assert(got, qt.Equals, ydbtype.GoType{})
		})
	}
}

// Every YDB type the map writes for a declaration has a Go type, so a column
// the reader reports never falls through to a guess. Renderings covers both
// the wide and the narrow date and time types.
func TestGoTypeOf_CoversEveryRendering(t *testing.T) {
	declarations := []string{
		"BOOLEAN", "TINYINT", "SMALLINT", "INTEGER", "BIGINT", "TINYINT UNSIGNED", "SMALLINT UNSIGNED",
		"INT UNSIGNED", "BIGINT UNSIGNED", "REAL", "DOUBLE PRECISION", "DYNUMBER", "TEXT", "BYTEA",
		"JSON", "JSONB", "YSON", "UUID", "DATE", "TIMESTAMP", "INTERVAL", "DATETIME64", "DECIMAL(10,2)",
		"SERIAL", "BIGSERIAL", "SMALLSERIAL", "Datetime",
	}
	for _, declared := range declarations {
		t.Run(declared, func(t *testing.T) {
			c := qt.New(t)
			renderings := ydbtype.Renderings(declared)
			c.Assert(renderings, qt.Not(qt.HasLen), 0)
			for _, rendering := range renderings {
				_, ok := ydbtype.GoTypeOf(rendering)
				c.Assert(ok, qt.IsTrue, qt.Commentf("%s renders as %s", declared, rendering))
			}
		})
	}
}

package ydbtype_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform/capability"
	"ptah.run/internal/ydbtype"
)

// TestMap_HappyPath pins every declaration family against the newest line and
// the oldest one. Each YDB type on the right was created as a column on
// local-ydb 26.2.1.14 and read back under that name; the 25.1 column differs
// only where the line does.
func TestMap_HappyPath(t *testing.T) {
	tests := []struct {
		name     string
		declared string
		caps     capability.Capabilities
		want     ydbtype.Mapping
	}{
		{name: "varchar keeps no length", declared: "VARCHAR(255)", caps: capability.YDB262(),
			want: ydbtype.Mapping{Type: "Utf8", Dropped: "length 255"}},
		{name: "varchar without a length", declared: "varchar", caps: capability.YDB262(), want: ydbtype.Mapping{Type: "Utf8"}},
		{name: "nvarchar max", declared: "NVARCHAR(MAX)", caps: capability.YDB262(), want: ydbtype.Mapping{Type: "Utf8"}},
		{name: "char keeps no length", declared: "CHAR(2)", caps: capability.YDB262(),
			want: ydbtype.Mapping{Type: "Utf8", Dropped: "length 2"}},
		{name: "text", declared: "TEXT", caps: capability.YDB262(), want: ydbtype.Mapping{Type: "Utf8"}},
		{name: "clob", declared: "CLOB", caps: capability.YDB262(), want: ydbtype.Mapping{Type: "Utf8"}},
		{name: "bytea", declared: "BYTEA", caps: capability.YDB262(), want: ydbtype.Mapping{Type: "String"}},
		{name: "varbinary keeps no length", declared: "VARBINARY(16)", caps: capability.YDB262(),
			want: ydbtype.Mapping{Type: "String", Dropped: "length 16"}},
		{name: "boolean", declared: "BOOLEAN", caps: capability.YDB262(), want: ydbtype.Mapping{Type: "Bool"}},
		{name: "tinyint", declared: "TINYINT", caps: capability.YDB262(), want: ydbtype.Mapping{Type: "Int8"}},
		{name: "smallint", declared: "smallint", caps: capability.YDB262(), want: ydbtype.Mapping{Type: "Int16"}},
		{name: "integer", declared: "INTEGER", caps: capability.YDB262(), want: ydbtype.Mapping{Type: "Int32"}},
		{name: "mysql display width", declared: "INT(11)", caps: capability.YDB262(),
			want: ydbtype.Mapping{Type: "Int32", Dropped: "display width 11"}},
		{name: "bigint", declared: "BIGINT", caps: capability.YDB262(), want: ydbtype.Mapping{Type: "Int64"}},
		{name: "postgres int8 is eight bytes", declared: "int8", caps: capability.YDB262(), want: ydbtype.Mapping{Type: "Int64"}},
		{name: "ydb Int8 is one byte", declared: "Int8", caps: capability.YDB262(), want: ydbtype.Mapping{Type: "Int8"}},
		{name: "unsigned bigint", declared: "BIGINT UNSIGNED", caps: capability.YDB262(), want: ydbtype.Mapping{Type: "Uint64"}},
		{name: "unsigned with extra space", declared: "int   unsigned", caps: capability.YDB262(), want: ydbtype.Mapping{Type: "Uint32"}},
		{name: "real", declared: "REAL", caps: capability.YDB262(), want: ydbtype.Mapping{Type: "Float"}},
		{name: "sql float is double", declared: "FLOAT", caps: capability.YDB262(), want: ydbtype.Mapping{Type: "Double"}},
		{name: "ydb Float is single", declared: "Float", caps: capability.YDB262(), want: ydbtype.Mapping{Type: "Float"}},
		{name: "float with single precision", declared: "FLOAT(24)", caps: capability.YDB262(), want: ydbtype.Mapping{Type: "Float"}},
		{name: "double precision", declared: "DOUBLE PRECISION", caps: capability.YDB262(), want: ydbtype.Mapping{Type: "Double"}},
		{name: "decimal", declared: "DECIMAL(10,2)", caps: capability.YDB262(), want: ydbtype.Mapping{Type: "Decimal(10,2)"}},
		{name: "numeric without a scale", declared: "NUMERIC(12)", caps: capability.YDB262(), want: ydbtype.Mapping{Type: "Decimal(12,0)"}},
		{name: "widest decimal", declared: "Decimal(35, 35)", caps: capability.YDB262(), want: ydbtype.Mapping{Type: "Decimal(35,35)"}},
		{name: "the fixed decimal on 25.1", declared: "DECIMAL(22,9)", caps: capability.YDB251(), want: ydbtype.Mapping{Type: "Decimal(22,9)"}},
		{name: "json", declared: "JSON", caps: capability.YDB262(), want: ydbtype.Mapping{Type: "Json"}},
		{name: "jsonb", declared: "JSONB", caps: capability.YDB262(), want: ydbtype.Mapping{Type: "JsonDocument"}},
		{name: "uuid", declared: "UUID", caps: capability.YDB262(), want: ydbtype.Mapping{Type: "Uuid"}},
		{name: "timestamp is wide where it can be", declared: "TIMESTAMP", caps: capability.YDB262(), want: ydbtype.Mapping{Type: "Timestamp64"}},
		{name: "timestamp is narrow on 25.1", declared: "TIMESTAMP", caps: capability.YDB251(), want: ydbtype.Mapping{Type: "Timestamp"}},
		{name: "timestamptz", declared: "TIMESTAMPTZ", caps: capability.YDB252(), want: ydbtype.Mapping{Type: "Timestamp64"}},
		{name: "timestamp with time zone", declared: "timestamp with time zone", caps: capability.YDB262(), want: ydbtype.Mapping{Type: "Timestamp64"}},
		{name: "mysql datetime", declared: "DATETIME", caps: capability.YDB262(), want: ydbtype.Mapping{Type: "Timestamp64"}},
		{name: "microsecond precision drops nothing", declared: "TIMESTAMP(6)", caps: capability.YDB262(), want: ydbtype.Mapping{Type: "Timestamp64"}},
		{name: "millisecond precision is reported", declared: "TIMESTAMP(3)", caps: capability.YDB262(),
			want: ydbtype.Mapping{Type: "Timestamp64", Dropped: "precision 3 (YDB keeps microseconds)"}},
		{name: "date is wide where it can be", declared: "DATE", caps: capability.YDB262(), want: ydbtype.Mapping{Type: "Date32"}},
		{name: "date is narrow on 25.1", declared: "DATE", caps: capability.YDB251(), want: ydbtype.Mapping{Type: "Date"}},
		{name: "interval", declared: "INTERVAL", caps: capability.YDB262(), want: ydbtype.Mapping{Type: "Interval64"}},
		{name: "ydb narrow Timestamp", declared: "Timestamp", caps: capability.YDB262(), want: ydbtype.Mapping{Type: "Timestamp"}},
		{name: "ydb narrow Date", declared: "Date", caps: capability.YDB262(), want: ydbtype.Mapping{Type: "Date"}},
		{name: "ydb Datetime", declared: "Datetime", caps: capability.YDB262(), want: ydbtype.Mapping{Type: "Datetime"}},
		{name: "ydb Timestamp64 in any case", declared: "timestamp64", caps: capability.YDB252(), want: ydbtype.Mapping{Type: "Timestamp64"}},
		{name: "ydb Utf8 in any case", declared: "UTF8", caps: capability.YDB262(), want: ydbtype.Mapping{Type: "Utf8"}},
		{name: "ydb Uint64", declared: "Uint64", caps: capability.YDB262(), want: ydbtype.Mapping{Type: "Uint64"}},
		{name: "ydb DyNumber", declared: "DyNumber", caps: capability.YDB262(), want: ydbtype.Mapping{Type: "DyNumber"}},
		{name: "ydb Yson", declared: "Yson", caps: capability.YDB262(), want: ydbtype.Mapping{Type: "Yson"}},
		{name: "ydb String is bytes", declared: "String", caps: capability.YDB262(), want: ydbtype.Mapping{Type: "String"}},
		{name: "cockroachdb STRING is text", declared: "STRING", caps: capability.YDB262(), want: ydbtype.Mapping{Type: "Utf8"}},
		{name: "a lower-case string is text", declared: "string", caps: capability.YDB262(), want: ydbtype.Mapping{Type: "Utf8"}},
		{name: "serial", declared: "SERIAL", caps: capability.YDB262(), want: ydbtype.Mapping{Type: "Serial", Serial: true}},
		{name: "bigserial", declared: "BIGSERIAL", caps: capability.YDB251(), want: ydbtype.Mapping{Type: "BigSerial", Serial: true}},
		{name: "serial2", declared: "serial2", caps: capability.YDB262(), want: ydbtype.Mapping{Type: "SmallSerial", Serial: true}},
		{name: "surrounding space", declared: "  BIGINT  ", caps: capability.YDB262(), want: ydbtype.Mapping{Type: "Int64"}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			got, err := ydbtype.Map(test.declared, test.caps)
			c.Assert(err, qt.IsNil)
			c.Assert(got, qt.Equals, test.want)
		})
	}
}

// TestMap_FailurePath pins the refusals. A refusal that a capability would
// lift names the capability; one no YDB line has a type for names none.
func TestMap_FailurePath(t *testing.T) {
	tests := []struct {
		name     string
		declared string
		caps     capability.Capabilities
		wantKey  capability.Capability
		wantErr  string
	}{
		{name: "ydb Varchar is bytes", declared: "Varchar", caps: capability.YDB262(),
			wantErr: `Varchar has no YDB counterpart: YDB reads Varchar as String, a byte string; declare Utf8 \(or VARCHAR\) for text and String for bytes`},
		{name: "ydb Varchar with a length", declared: "Varchar(10)", caps: capability.YDB262(),
			wantErr: `Varchar\(10\) has no YDB counterpart: .*`},
		{name: "time of day", declared: "TIME", caps: capability.YDB262(), wantErr: `TIME has no YDB counterpart: YDB has no time-of-day type`},
		{name: "timetz", declared: "timetz", caps: capability.YDB262(), wantErr: `timetz has no YDB counterpart: YDB has no time-of-day type`},
		{name: "tz type", declared: "TzTimestamp", caps: capability.YDB262(), wantErr: `TzTimestamp has no YDB counterpart: .*not supported by storage.*`},
		{name: "postgres array", declared: "TEXT[]", caps: capability.YDB262(), wantErr: `TEXT\[\] has no YDB counterpart: YDB has no array column type`},
		{name: "yql list", declared: "List<Int32>", caps: capability.YDB262(), wantErr: `List<Int32> has no YDB counterpart: YDB stores only primitive types.*`},
		{name: "optional spelling", declared: "Int32?", caps: capability.YDB262(), wantErr: `Int32\? has no YDB counterpart: nullability is declared with NOT NULL.*`},
		{name: "mysql enum", declared: "ENUM('a','b')", caps: capability.YDB262(), wantErr: `ENUM\('a','b'\) has no YDB counterpart: YDB has no enum type`},
		{name: "xml", declared: "XML", caps: capability.YDB262(), wantErr: `XML has no YDB counterpart: YDB has no XML type`},
		{name: "range", declared: "tstzrange", caps: capability.YDB262(), wantErr: `tstzrange has no YDB counterpart: YDB has no range type`},
		{name: "money", declared: "MONEY", caps: capability.YDB262(), wantErr: `MONEY has no YDB counterpart: .*`},
		{name: "network", declared: "INET", caps: capability.YDB262(), wantErr: `INET has no YDB counterpart: YDB has no network address type`},
		{name: "geometric", declared: "POINT", caps: capability.YDB262(), wantErr: `POINT has no YDB counterpart: YDB has no geometric type`},
		{name: "citext", declared: "CITEXT", caps: capability.YDB262(), wantErr: `CITEXT has no YDB counterpart: .*`},
		{name: "vector", declared: "VECTOR(3)", caps: capability.YDB262(), wantErr: `VECTOR\(3\) has no YDB counterpart: .*`},
		{name: "postgres type in ydb", declared: "pgint4", caps: capability.YDB262(), wantErr: `pgint4 has no YDB counterpart: .*EnableTablePgTypes.*`},
		{name: "unknown name", declared: "geography_point", caps: capability.YDB262(), wantErr: `geography_point has no YDB counterpart: YDB has no type of that name`},
		{name: "mysql auto increment in the type", declared: "INT AUTO_INCREMENT", caps: capability.YDB262(), wantErr: `INT AUTO_INCREMENT has no YDB counterpart: .*`},
		{name: "decimal without a precision", declared: "DECIMAL", caps: capability.YDB262(), wantErr: `DECIMAL has no YDB counterpart: a YDB Decimal needs a precision and a scale.*`},
		{name: "decimal too wide", declared: "DECIMAL(36,2)", caps: capability.YDB262(), wantErr: `DECIMAL\(36,2\) has no YDB counterpart: the precision must be between 1 and 35`},
		{name: "decimal scale above precision", declared: "DECIMAL(5,6)", caps: capability.YDB262(), wantErr: `DECIMAL\(5,6\) has no YDB counterpart: the scale must be between 0 and the precision`},
		{name: "a length on a type without one", declared: "BOOLEAN(1)", caps: capability.YDB262(), wantErr: `BOOLEAN\(1\) has no YDB counterpart: Bool takes no arguments`},
		{name: "unclosed argument list", declared: "VARCHAR(10", caps: capability.YDB262(), wantErr: `VARCHAR\(10 has no YDB counterpart: the argument list does not close`},
		{name: "empty", declared: " ", caps: capability.YDB262(), wantErr: `an empty type has no YDB counterpart: a column needs a type`},

		{name: "decimal precision on 25.1", declared: "DECIMAL(10,2)", caps: capability.YDB251(),
			wantKey: capability.ParameterizedDecimal,
			wantErr: `DECIMAL\(10,2\) requires target capability parameterized_decimal: this line has Decimal\(22,9\) only`},
		{name: "wide native on 25.1", declared: "Date32", caps: capability.YDB251(),
			wantKey: capability.WideDateTimeTypes,
			wantErr: `Date32 requires target capability wide_date_time_types: .*`},
		{name: "serial without the key", declared: "SERIAL", caps: capability.YDB262().With(capability.SerialColumns, false),
			wantKey: capability.SerialColumns,
			wantErr: `SERIAL requires target capability serial_columns: this target has no Serial column type`},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			got, err := ydbtype.Map(test.declared, test.caps)
			c.Assert(err, qt.ErrorMatches, test.wantErr)
			var refusal *ydbtype.Refusal
			c.Assert(err, qt.ErrorAs, &refusal)
			c.Assert(refusal.Key, qt.Equals, test.wantKey)
			c.Assert(got, qt.Equals, ydbtype.Mapping{})
		})
	}
}

// TestRenderings lists what each declaration may have been rendered as, which
// is what lets a table built on 25.1 and read on 26.2 compare equal to the
// declaration that built it.
func TestRenderings(t *testing.T) {
	tests := []struct {
		name     string
		declared string
		want     []string
	}{
		{name: "an instant is either width", declared: "TIMESTAMP", want: []string{"Timestamp64", "Timestamp"}},
		{name: "a date is either width", declared: "DATE", want: []string{"Date32", "Date"}},
		{name: "an interval is either width", declared: "INTERVAL", want: []string{"Interval64", "Interval"}},
		{name: "a string has one", declared: "VARCHAR(100)", want: []string{"Utf8"}},
		{name: "a narrow native has one", declared: "Timestamp", want: []string{"Timestamp"}},
		{name: "a decimal keeps its arguments", declared: "DECIMAL(10,2)", want: []string{"Decimal(10,2)"}},
		{name: "no counterpart has none", declared: "TIME", want: nil},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(ydbtype.Renderings(test.declared), qt.DeepEquals, test.want)
		})
	}
}

// TestKeyComparable pins the types YDB refuses in a key or an index key,
// measured as `Column id has wrong key type Float` and its index twin.
func TestKeyComparable(t *testing.T) {
	tests := []struct {
		ydbType string
		want    bool
	}{
		{ydbType: "Float", want: false},
		{ydbType: "Double", want: false},
		{ydbType: "Json", want: false},
		{ydbType: "JsonDocument", want: false},
		{ydbType: "Yson", want: false},
		{ydbType: "Int64", want: true},
		{ydbType: "Utf8", want: true},
		{ydbType: "Uuid", want: true},
		{ydbType: "Decimal(22,9)", want: true},
		{ydbType: "DyNumber", want: true},
		{ydbType: "Interval", want: true},
		{ydbType: "Bool", want: true},
	}

	for _, test := range tests {
		t.Run(test.ydbType, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(ydbtype.KeyComparable(test.ydbType), qt.Equals, test.want)
		})
	}
}

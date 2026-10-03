package ydbtype_test

import (
	"math"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform/capability"
	"ptah.run/internal/ydbtype"
)

// TestLiteral_HappyPath pins the literal each column type takes as a default.
// Every spelling on the right was accepted in a CREATE TABLE on local-ydb
// 26.2.1.14 and read back from `scheme describe` as a `from_literal` of the
// column's type; 25.1.4.7 accepted the same spellings except where a key
// says otherwise.
func TestLiteral_HappyPath(t *testing.T) {
	tests := []struct {
		name    string
		ydbType string
		value   string
		want    string
	}{
		{name: "bool true", ydbType: "Bool", value: "true", want: "true"},
		{name: "a double with a leading point", ydbType: "Double", value: ".5", want: "Double('0.5')"},
		{name: "a double with a sign and an exponent", ydbType: "Double", value: "+1e3", want: "Double('1000')"},
		{name: "the first narrow date", ydbType: "Date", value: "1970-01-01", want: "Date('1970-01-01')"},
		{name: "the last narrow instant", ydbType: "Timestamp", value: "2105-12-31T23:59:59.999999Z", want: "Timestamp('2105-12-31T23:59:59.999999Z')"},
		{name: "a wide date before 1970", ydbType: "Date32", value: "1969-12-31", want: "Date32('1969-12-31')"},
		{name: "a wide instant before 1970", ydbType: "Timestamp64", value: "1960-01-01T00:00:00Z", want: "Timestamp64('1960-01-01T00:00:00Z')"},
		{name: "bool from a digit", ydbType: "Bool", value: "0", want: "false"},
		{name: "bool in capitals", ydbType: "Bool", value: "TRUE", want: "true"},
		{name: "int8", ydbType: "Int8", value: "-5", want: "-5t"},
		{name: "int16", ydbType: "Int16", value: "5", want: "5s"},
		{name: "int32 is unsuffixed", ydbType: "Int32", value: "5", want: "5"},
		{name: "int64", ydbType: "Int64", value: "-9223372036854775808", want: "-9223372036854775808l"},
		{name: "uint8", ydbType: "Uint8", value: "255", want: "255ut"},
		{name: "uint16", ydbType: "Uint16", value: "5", want: "5us"},
		{name: "uint32", ydbType: "Uint32", value: "5", want: "5u"},
		{name: "uint64 max", ydbType: "Uint64", value: "18446744073709551615", want: "18446744073709551615ul"},
		{name: "float", ydbType: "Float", value: "1.5", want: "Float('1.5')"},
		{name: "double from an integer", ydbType: "Double", value: "0", want: "Double('0')"},
		{name: "double in exponent form", ydbType: "Double", value: "1e3", want: "Double('1000')"},
		{name: "decimal", ydbType: "Decimal(10,2)", value: "12.5", want: "Decimal('12.5', 10, 2)"},
		{name: "negative decimal", ydbType: "Decimal(22,9)", value: "-1.5", want: "Decimal('-1.5', 22, 9)"},
		{name: "dynumber", ydbType: "DyNumber", value: "3.14", want: "DyNumber('3.14')"},
		{name: "utf8", ydbType: "Utf8", value: "active", want: "'active'u"},
		{name: "utf8 empty", ydbType: "Utf8", value: "", want: "''u"},
		{name: "utf8 quote and backslash", ydbType: "Utf8", value: `it's a\b`, want: `'it\'s a\\b'u`},
		{name: "utf8 control characters", ydbType: "Utf8", value: "l1\nl2\x01", want: `'l1\nl2\x01'u`},
		{name: "utf8 keeps non-ascii bytes", ydbType: "Utf8", value: "héllo ✓", want: "'héllo ✓'u"},
		{name: "string is unsuffixed", ydbType: "String", value: "bytes", want: "'bytes'"},
		{name: "json", ydbType: "Json", value: `{"a":1}`, want: `Json('{"a":1}')`},
		{name: "jsondocument", ydbType: "JsonDocument", value: `{"a":"it's"}`, want: `JsonDocument('{"a":"it\'s"}')`},
		{name: "yson", ydbType: "Yson", value: "[1;2]", want: "Yson('[1;2]')"},
		{name: "uuid in lower case", ydbType: "Uuid", value: "F9D5CC3F-F1DC-4D9C-B97E-766E57CA4CCB",
			want: "Uuid('f9d5cc3f-f1dc-4d9c-b97e-766e57ca4ccb')"},
		{name: "date", ydbType: "Date", value: "2026-01-02", want: "Date('2026-01-02')"},
		{name: "date32 before 1970", ydbType: "Date32", value: "1900-01-01", want: "Date32('1900-01-01')"},
		{name: "datetime from a sql spelling", ydbType: "Datetime", value: "2026-01-02 03:04:05", want: "Datetime('2026-01-02T03:04:05Z')"},
		{name: "timestamp in utc", ydbType: "Timestamp", value: "2026-01-02T03:04:05.123456Z",
			want: "Timestamp('2026-01-02T03:04:05.123456Z')"},
		{name: "timestamp moved to utc", ydbType: "Timestamp64", value: "2026-01-02T05:04:05+02:00",
			want: "Timestamp64('2026-01-02T03:04:05Z')"},
		{name: "timestamp from a date", ydbType: "Timestamp", value: "2026-01-02", want: "Timestamp('2026-01-02T00:00:00Z')"},
		{name: "interval", ydbType: "Interval", value: "P1DT2H", want: "Interval('P1DT2H')"},
		{name: "negative interval", ydbType: "Interval64", value: "-PT1S", want: "Interval64('-PT1S')"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			got, err := ydbtype.Literal(test.ydbType, test.value, capability.YDB262())
			c.Assert(err, qt.IsNil)
			c.Assert(got, qt.Equals, test.want)
		})
	}
}

// TestLiteral_WritesOneLiteralPerValue pins the canonical form: two spellings
// of one value write the same literal, which is what lets a comparison of a
// declared default with the one YDB stores compare the two literals.
func TestLiteral_WritesOneLiteralPerValue(t *testing.T) {
	tests := []struct {
		name     string
		ydbType  string
		spelling string
		want     string
	}{
		{name: "an integer with a sign", ydbType: "Int8", spelling: "+5", want: "5t"},
		{name: "an integer with a leading zero", ydbType: "Uint32", spelling: "007", want: "7u"},
		{name: "a float at its own width", ydbType: "Float", spelling: "0.10", want: "Float('0.1')"},
		{name: "a double in exponent form", ydbType: "Double", spelling: "2.5E0", want: "Double('2.5')"},
		{name: "a large double keeps its exponent", ydbType: "Double", spelling: "1e21", want: "Double('1e+21')"},
		{name: "a dynumber keeps its digits", ydbType: "DyNumber", spelling: "1e3", want: "DyNumber('1e3')"},
		{name: "a decimal with trailing zeros", ydbType: "Decimal(10,2)", spelling: "12.50", want: "Decimal('12.5', 10, 2)"},
		{name: "a decimal with leading zeros", ydbType: "Decimal(10,2)", spelling: "+0012", want: "Decimal('12', 10, 2)"},
		{name: "a negative zero decimal", ydbType: "Decimal(10,2)", spelling: "-0.00", want: "Decimal('0', 10, 2)"},
		{name: "a decimal below one", ydbType: "Decimal(10,2)", spelling: "0.50", want: "Decimal('0.5', 10, 2)"},
		{name: "hours past a day", ydbType: "Interval", spelling: "PT26H", want: "Interval('P1DT2H')"},
		{name: "a week", ydbType: "Interval64", spelling: "P1W", want: "Interval64('P7D')"},
		{name: "a zero interval", ydbType: "Interval", spelling: "P0D", want: "Interval('PT0S')"},
		{name: "fractional seconds", ydbType: "Interval", spelling: "PT1.500000S", want: "Interval('PT1.5S')"},
		{name: "seconds past a minute", ydbType: "Interval", spelling: "-PT90S", want: "Interval('-PT1M30S')"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			got, err := ydbtype.Literal(test.ydbType, test.spelling, capability.YDB262())
			c.Assert(err, qt.IsNil)
			c.Assert(got, qt.Equals, test.want)
		})
	}
}

// TestIntervalText writes a stored duration the way Literal writes one, which
// is how the schema reader spells the default YDB keeps in microseconds.
func TestIntervalText(t *testing.T) {
	tests := []struct {
		name   string
		micros int64
		want   string
	}{
		{name: "zero", micros: 0, want: "PT0S"},
		{name: "a day and two hours", micros: 93_600_000_000, want: "P1DT2H"},
		{name: "a whole day", micros: 86_400_000_000, want: "P1D"},
		{name: "minus a second", micros: -1_000_000, want: "-PT1S"},
		{name: "a microsecond", micros: 1, want: "PT0.000001S"},
		{name: "minutes and fractional seconds", micros: 61_250_000, want: "PT1M1.25S"},
		{name: "the most negative value", micros: math.MinInt64, want: "-P106751991DT4H54.775808S"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(ydbtype.IntervalText(test.micros), qt.Equals, test.want)
		})
	}
}

// TestDecimalText writes a decimal without the signs and zeros that do not
// change its value.
func TestDecimalText(t *testing.T) {
	tests := []struct {
		name  string
		value string
		want  string
	}{
		{name: "both parts", value: "12.34", want: "12.34"},
		{name: "zeros on both ends", value: "-0012.340", want: "-12.34"},
		{name: "no fraction left", value: "7.000", want: "7"},
		{name: "no fraction at all", value: "7", want: "7"},
		{name: "an empty fraction", value: "7.", want: "7"},
		{name: "no integer part left", value: "00.5", want: "0.5"},
		{name: "a plus sign", value: "+3.10", want: "3.1"},
		{name: "a negative zero", value: "-0.0", want: "0"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(ydbtype.DecimalText(test.value), qt.Equals, test.want)
		})
	}
}

// TestDeclaredValue reads both forms a declared default arrives in: a struct
// tag's bare value and the SQL parser's quoted literal.
func TestDeclaredValue(t *testing.T) {
	tests := []struct {
		name       string
		text       string
		wantValue  string
		wantIsNull bool
	}{
		{name: "a bare value", text: "active", wantValue: "active"},
		{name: "a quoted literal", text: "'active'", wantValue: "active"},
		{name: "a quoted literal with an escaped quote", text: "'it''s'", wantValue: "it's"},
		{name: "a quoted literal with a cast", text: "'{}'::jsonb", wantValue: "{}"},
		{name: "a bare NULL", text: " null ", wantValue: "null", wantIsNull: true},
		{name: "a quoted NULL is the text", text: "'NULL'", wantValue: "NULL"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			value, isNull := ydbtype.DeclaredValue(test.text)
			c.Assert(value, qt.Equals, test.wantValue)
			c.Assert(isNull, qt.Equals, test.wantIsNull)
		})
	}
}

// TestLiteral_FailurePath pins the defaults Ptah refuses to write. A value
// that would be rounded, truncated or reinterpreted is refused rather than
// changed, and a default the line cannot take names its key.
func TestLiteral_FailurePath(t *testing.T) {
	tests := []struct {
		name    string
		ydbType string
		value   string
		caps    capability.Capabilities
		wantKey capability.Capability
		wantErr string
	}{
		{name: "int16 on 25.1", ydbType: "Int16", value: "5", caps: capability.YDB251(),
			wantKey: capability.SmallIntegerDefaults,
			wantErr: `a default of type Int16 requires target capability small_integer_defaults: .*INTERNAL_ERROR.*`},
		{name: "uint16 on 25.1", ydbType: "Uint16", value: "5", caps: capability.YDB251(),
			wantKey: capability.SmallIntegerDefaults, wantErr: `a default of type Uint16 requires target capability small_integer_defaults: .*`},
		{name: "jsondocument on 25.2", ydbType: "JsonDocument", value: "{}", caps: capability.YDB252(),
			wantKey: capability.DocumentTypeDefaults,
			wantErr: "a default of type JsonDocument requires target capability document_type_defaults: this line answers `Unsupported type of literal: JsonDocument`"},
		{name: "dynumber on 25.1", ydbType: "DyNumber", value: "1", caps: capability.YDB251(),
			wantKey: capability.DocumentTypeDefaults, wantErr: `a default of type DyNumber requires target capability document_type_defaults: .*`},
		{name: "serial", ydbType: "Serial", value: "5", caps: capability.YDB262(),
			wantErr: `a default of type Serial has no YDB counterpart: a Serial column takes its value from its sequence.*`},
		{name: "int8 out of range", ydbType: "Int8", value: "300", caps: capability.YDB262(),
			wantErr: `default "300" has no YDB counterpart: it is not a Int8 value`},
		{name: "negative unsigned", ydbType: "Uint32", value: "-1", caps: capability.YDB262(),
			wantErr: `default "-1" has no YDB counterpart: it is not a Uint32 value`},
		{name: "a word in an integer", ydbType: "Int32", value: "five", caps: capability.YDB262(),
			wantErr: `default "five" has no YDB counterpart: it is not a Int32 value`},
		{name: "a word in a bool", ydbType: "Bool", value: "maybe", caps: capability.YDB262(),
			wantErr: `default "maybe" has no YDB counterpart: it is not a Bool value`},
		{name: "a word in a double", ydbType: "Double", value: "NaNa", caps: capability.YDB262(),
			wantErr: `default "NaNa" has no YDB counterpart: it is not a Double value`},
		// Go's ParseFloat reads each of these; YDB answers `Invalid value`.
		{name: "digit separators in a double", ydbType: "Double", value: "1_000", caps: capability.YDB262(),
			wantErr: `default "1_000" has no YDB counterpart: it is not a Double value`},
		{name: "a hex float", ydbType: "Double", value: "0x1p-2", caps: capability.YDB262(),
			wantErr: `default "0x1p-2" has no YDB counterpart: it is not a Double value`},
		{name: "not a number", ydbType: "Double", value: "NaN", caps: capability.YDB262(),
			wantErr: `default "NaN" has no YDB counterpart: it is not a Double value`},
		{name: "infinity in a dynumber", ydbType: "DyNumber", value: "Inf", caps: capability.YDB262(),
			wantErr: `default "Inf" has no YDB counterpart: it is not a DyNumber value`},
		{name: "a date before the narrow range", ydbType: "Date", value: "1969-12-31", caps: capability.YDB251(),
			wantErr: `default "1969-12-31" has no YDB counterpart: it is not a Date value`},
		{name: "a date past the narrow range", ydbType: "Date", value: "2106-01-01", caps: capability.YDB251(),
			wantErr: `default "2106-01-01" has no YDB counterpart: it is not a Date value`},
		{name: "an instant before the narrow range", ydbType: "Timestamp", value: "1960-01-01 00:00:00", caps: capability.YDB251(),
			wantErr: `default "1960-01-01 00:00:00" has no YDB counterpart: it is not a Timestamp value`},
		{name: "a datetime past the narrow range", ydbType: "Datetime", value: "2106-01-01T00:00:00Z", caps: capability.YDB251(),
			wantErr: `default "2106-01-01T00:00:00Z" has no YDB counterpart: it is not a Datetime value`},
		{name: "decimal that would round", ydbType: "Decimal(10,2)", value: "1.234", caps: capability.YDB262(),
			wantErr: `default "1.234" has no YDB counterpart: it is not a Decimal\(10,2\) value`},
		{name: "decimal that overflows", ydbType: "Decimal(4,2)", value: "123.4", caps: capability.YDB262(),
			wantErr: `default "123.4" has no YDB counterpart: it is not a Decimal\(4,2\) value`},
		{name: "malformed uuid", ydbType: "Uuid", value: "not-a-uuid", caps: capability.YDB262(),
			wantErr: `default "not-a-uuid" has no YDB counterpart: it is not a Uuid value`},
		{name: "malformed date", ydbType: "Date", value: "02/01/2026", caps: capability.YDB262(),
			wantErr: `default "02/01/2026" has no YDB counterpart: it is not a Date value`},
		{name: "datetime with a fraction", ydbType: "Datetime", value: "2026-01-02T03:04:05.5Z", caps: capability.YDB262(),
			wantErr: `default "2026-01-02T03:04:05.5Z" has no YDB counterpart: it is not a Datetime value`},
		{name: "timestamp with nanoseconds", ydbType: "Timestamp", value: "2026-01-02T03:04:05.123456789Z", caps: capability.YDB262(),
			wantErr: `default "2026-01-02T03:04:05.123456789Z" has no YDB counterpart: it is not a Timestamp value`},
		{name: "postgres interval words", ydbType: "Interval", value: "1 day", caps: capability.YDB262(),
			wantErr: `default "1 day" has no YDB counterpart: Interval takes an ISO 8601 duration such as P1D or PT30M`},
		{name: "a bare P", ydbType: "Interval", value: "P", caps: capability.YDB262(),
			wantErr: `default "P" has no YDB counterpart: Interval takes an ISO 8601 duration.*`},
		// 15250285 weeks is past the microseconds an int64 holds, so the
		// duration cannot be written in the unit YDB stores.
		{name: "weeks past int64 microseconds", ydbType: "Interval64", value: "P15250285W", caps: capability.YDB262(),
			wantErr: `default "P15250285W" has no YDB counterpart: Interval64 takes an ISO 8601 duration.*`},
		{name: "seconds past int64 microseconds", ydbType: "Interval64", value: "PT9223372036855S", caps: capability.YDB262(),
			wantErr: `default "PT9223372036855S" has no YDB counterpart: Interval64 takes an ISO 8601 duration.*`},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			got, err := ydbtype.Literal(test.ydbType, test.value, test.caps)
			c.Assert(err, qt.ErrorMatches, test.wantErr)
			var refusal *ydbtype.Refusal
			c.Assert(err, qt.ErrorAs, &refusal)
			c.Assert(refusal.Key, qt.Equals, test.wantKey)
			c.Assert(got, qt.Equals, "")
		})
	}
}

// TestLiteralValue_ReadsBackWhatLiteralWrote pins the inverse: each literal
// Literal writes reads back as a value, and that value writes the same literal
// again in the column's type.
func TestLiteralValue_ReadsBackWhatLiteralWrote(t *testing.T) {
	tests := []struct {
		name      string
		ydbType   string
		literal   string
		wantValue string
	}{
		{name: "bool", ydbType: "Bool", literal: "false", wantValue: "false"},
		{name: "int8", ydbType: "Int8", literal: "-5t", wantValue: "-5"},
		{name: "int32", ydbType: "Int32", literal: "5", wantValue: "5"},
		{name: "uint64", ydbType: "Uint64", literal: "18446744073709551615ul", wantValue: "18446744073709551615"},
		{name: "float", ydbType: "Float", literal: "Float('1.5')", wantValue: "1.5"},
		{name: "double with an exponent", ydbType: "Double", literal: "Double('1e+21')", wantValue: "1e+21"},
		{name: "string with escapes", ydbType: "String", literal: `'a\'b\\c\x00\n'`, wantValue: "a'b\\c\x00\n"},
		{name: "utf8", ydbType: "Utf8", literal: "'héllo'u", wantValue: "héllo"},
		{name: "json with a quote", ydbType: "Json", literal: `Json('{"a":"it\'s"}')`, wantValue: `{"a":"it's"}`},
		{name: "uuid", ydbType: "Uuid", literal: "Uuid('550e8400-e29b-41d4-a716-446655440000')",
			wantValue: "550e8400-e29b-41d4-a716-446655440000"},
		{name: "date32", ydbType: "Date32", literal: "Date32('1900-01-02')", wantValue: "1900-01-02"},
		{name: "timestamp", ydbType: "Timestamp", literal: "Timestamp('2026-01-02T03:04:05.123456Z')",
			wantValue: "2026-01-02T03:04:05.123456Z"},
		{name: "interval", ydbType: "Interval64", literal: "Interval64('-PT1S')", wantValue: "-PT1S"},
		{name: "decimal", ydbType: "Decimal(22,9)", literal: "Decimal('-1.5', 22, 9)", wantValue: "-1.5"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			value, ok := ydbtype.LiteralValue(test.literal)
			c.Assert(ok, qt.IsTrue)
			c.Assert(value, qt.Equals, test.wantValue)

			again, err := ydbtype.Literal(test.ydbType, value, capability.YDB262())
			c.Assert(err, qt.IsNil)
			c.Assert(again, qt.Equals, test.literal)
		})
	}
}

// TestLiteralValue_FailurePath pins the text that is not a literal Literal
// writes, which a caller then treats as an expression.
func TestLiteralValue_FailurePath(t *testing.T) {
	tests := []struct {
		name    string
		literal string
	}{
		{name: "a function call", literal: "CurrentUtcTimestamp()"},
		{name: "an unknown suffix", literal: "5x"},
		{name: "an unterminated string", literal: "'abc"},
		{name: "two strings", literal: "'a' || 'b'"},
		{name: "an unknown escape", literal: `'\q'`},
		{name: "a short hexadecimal escape", literal: `'\x1'`},
		{name: "a constructor of a type Literal does not write", literal: "TzDate('2026-01-02,UTC')"},
		{name: "a decimal without its precision", literal: "Decimal('1.5')"},
		{name: "a constructor around an expression", literal: "Float(1.5)"},
		{name: "an empty text", literal: ""},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			value, ok := ydbtype.LiteralValue(test.literal)

			c.Assert(ok, qt.IsFalse)
			c.Assert(value, qt.Equals, "")
		})
	}
}

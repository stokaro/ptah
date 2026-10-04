package ydbtype_test

import (
	"math"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/ydbtype"
)

// Each value is written in the column's own type, from the Go value a
// declaration or a read hands over.
func TestValueLiteral_HappyPath(t *testing.T) {
	moment := time.Date(2026, time.January, 2, 3, 4, 5, 123456000, time.FixedZone("CET", 3600))
	tests := []struct {
		name    string
		ydbType string
		value   any
		want    string
	}{
		{name: "nil", ydbType: ydbtype.Utf8, value: nil, want: "NULL"},
		{name: "int into Int32", ydbType: ydbtype.Int32, value: 5, want: "5"},
		{name: "int into Int64", ydbType: ydbtype.Int64, value: 5, want: "5l"},
		{name: "int64 into Int8 at its floor", ydbType: ydbtype.Int8, value: int64(-128), want: "-128t"},
		{name: "int into Int16", ydbType: ydbtype.Int16, value: 300, want: "300s"},
		{name: "uint64 into Uint64 at its ceiling", ydbType: ydbtype.Uint64, value: uint64(math.MaxUint64),
			want: "18446744073709551615ul"},
		{name: "uint8 into Uint8", ydbType: ydbtype.Uint8, value: uint8(200), want: "200ut"},
		{name: "int into Uint16", ydbType: ydbtype.Uint16, value: 7, want: "7us"},
		{name: "int into Uint32", ydbType: ydbtype.Uint32, value: 7, want: "7u"},
		{name: "a Serial column stores Int32", ydbType: ydbtype.Serial, value: 7, want: "7"},
		{name: "a BigSerial column stores Int64", ydbType: ydbtype.BigSerial, value: 7, want: "7l"},
		{name: "a SmallSerial column stores Int16", ydbType: ydbtype.SmallSerial, value: 7, want: "7s"},
		{name: "integer text into Int64", ydbType: ydbtype.Int64, value: "42", want: "42l"},
		{name: "bool", ydbType: ydbtype.Bool, value: true, want: "true"},
		{name: "bool text", ydbType: ydbtype.Bool, value: "false", want: "false"},
		{name: "float into Double", ydbType: ydbtype.Double, value: 2.25, want: "Double('2.25')"},
		{name: "float32 into Float", ydbType: ydbtype.Float, value: float32(1.1), want: "Float('1.1')"},
		{name: "int into Double", ydbType: ydbtype.Double, value: 3, want: "Double('3')"},
		{name: "float into Decimal", ydbType: "Decimal(10,2)", value: 12.5, want: "Decimal('12.5', 10, 2)"},
		{name: "text into Decimal", ydbType: "Decimal(22,9)", value: "12.50", want: "Decimal('12.5', 22, 9)"},
		{name: "int into Decimal", ydbType: "Decimal(10,2)", value: 12, want: "Decimal('12', 10, 2)"},
		{name: "text into Utf8", ydbType: ydbtype.Utf8, value: "it's", want: `'it\'s'u`},
		{name: "bytes into Utf8", ydbType: ydbtype.Utf8, value: []byte("tëxt"), want: "'tëxt'u"},
		{name: "bytes into String", ydbType: ydbtype.String, value: []byte{'b', 0, 0xff, '\\'},
			want: `'b\x00\xff\\'`},
		{name: "text into String escapes what is not printable ASCII", ydbType: ydbtype.String, value: "é",
			want: `'\xc3\xa9'`},
		{name: "bytes into Yson", ydbType: ydbtype.Yson, value: []byte("[1;2]"), want: "Yson('[1;2]')"},
		{name: "text into Json", ydbType: ydbtype.JSON, value: `{"a":1}`, want: `Json('{"a":1}')`},
		{name: "bytes into JsonDocument", ydbType: ydbtype.JSONDocument, value: []byte(`{"b": 2}`),
			want: `JsonDocument('{"b": 2}')`},
		{name: "text into Uuid", ydbType: ydbtype.UUID, value: "550E8400-E29B-41D4-A716-446655440000",
			want: "Uuid('550e8400-e29b-41d4-a716-446655440000')"},
		{name: "a moment into Timestamp, in UTC", ydbType: ydbtype.Timestamp, value: moment,
			want: "Timestamp('2026-01-02T02:04:05.123456Z')"},
		{name: "a moment into Timestamp64", ydbType: ydbtype.Timestamp64,
			value: time.Date(1900, time.January, 1, 0, 0, 0, 500000000, time.UTC),
			want:  "Timestamp64('1900-01-01T00:00:00.5Z')"},
		{name: "a whole second into Datetime", ydbType: ydbtype.Datetime,
			value: time.Date(2026, time.January, 2, 3, 4, 5, 0, time.UTC), want: "Datetime('2026-01-02T03:04:05Z')"},
		{name: "a midnight in another zone into Date", ydbType: ydbtype.Date,
			value: time.Date(2026, time.January, 2, 1, 0, 0, 0, time.FixedZone("CET", 3600)), want: "Date('2026-01-02')"},
		{name: "text into Date32", ydbType: ydbtype.Date32, value: "1900-01-01", want: "Date32('1900-01-01')"},
		{name: "text into Timestamp", ydbType: ydbtype.Timestamp, value: "2026-01-02 03:04:05",
			want: "Timestamp('2026-01-02T03:04:05Z')"},
		{name: "a duration into Interval", ydbType: ydbtype.Interval, value: 26 * time.Hour,
			want: "Interval('P1DT2H')"},
		{name: "a negative duration into Interval64", ydbType: ydbtype.Interval64, value: -time.Second,
			want: "Interval64('-PT1S')"},
		{name: "text into Interval", ydbType: ydbtype.Interval, value: "PT26H", want: "Interval('P1DT2H')"},
		{name: "the text a DyNumber reads back as", ydbType: ydbtype.DyNumber, value: ".125e2",
			want: "DyNumber('.125e2')"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			got, err := ydbtype.ValueLiteral(test.ydbType, test.value)
			c.Assert(err, qt.IsNil)
			c.Assert(got, qt.Equals, test.want)
		})
	}
}

// A value the column cannot hold is refused with the type and the value, never
// wrapped, rounded or cut.
func TestValueLiteral_FailurePath(t *testing.T) {
	tests := []struct {
		name    string
		ydbType string
		value   any
		wantErr string
	}{
		{name: "an int over Int32", ydbType: ydbtype.Int32, value: int64(5000000000),
			wantErr: "Int32 cannot hold 5000000000: it is outside the range -2147483648 to 2147483647"},
		{name: "a negative value for Uint32", ydbType: ydbtype.Uint32, value: -1,
			wantErr: "Uint32 cannot hold -1: it is outside the range 0 to 4294967295"},
		{name: "a uint64 over Int64", ydbType: ydbtype.Int64, value: uint64(math.MaxUint64),
			wantErr: "Int64 cannot hold 18446744073709551615: it is outside the range " +
				"-9223372036854775808 to 9223372036854775807"},
		{name: "a value over a Serial's Int32", ydbType: ydbtype.Serial, value: int64(math.MaxInt32) + 1,
			wantErr: "Int32 cannot hold 2147483648: it is outside the range -2147483648 to 2147483647"},
		{name: "integer text over Int8", ydbType: ydbtype.Int8, value: "200",
			wantErr: `Int8 cannot hold "200": it is not a Int8 value`},
		{name: "a float for an integer column", ydbType: ydbtype.Int64, value: 5.0,
			wantErr: "Int64 cannot hold 5: it is a Go float64, which is not a Int64 value"},
		{name: "an int for a Utf8 column", ydbType: ydbtype.Utf8, value: 7,
			wantErr: "Utf8 cannot hold 7: it is a Go int, which is not a Utf8 value"},
		{name: "a bool for an integer column", ydbType: ydbtype.Int32, value: true,
			wantErr: "Int32 cannot hold true: it is a Go bool, which is not a Int32 value"},
		{name: "bytes that are not UTF-8 for a Utf8 column", ydbType: ydbtype.Utf8, value: []byte{0xff},
			wantErr: `Utf8 cannot hold "\\xff": it is not valid UTF-8`},
		{name: "NaN", ydbType: ydbtype.Double, value: math.NaN(),
			wantErr: "Double cannot hold NaN: YDB reads no literal for it"},
		{name: "more digits than the Decimal scale", ydbType: "Decimal(10,2)", value: "1.234",
			wantErr: `Decimal\(10,2\) cannot hold "1.234": it is not a Decimal\(10,2\) value`},
		{name: "a moment with a time of day for a Date", ydbType: ydbtype.Date,
			value:   time.Date(2026, time.January, 2, 10, 0, 0, 0, time.UTC),
			wantErr: "Date cannot hold 2026-01-02T10:00:00Z: it has a time of day, and a Date holds a day"},
		{name: "a moment before 1970 for a Timestamp", ydbType: ydbtype.Timestamp,
			value:   time.Date(1969, time.December, 31, 0, 0, 0, 0, time.UTC),
			wantErr: `Timestamp cannot hold 1969-12-31T00:00:00Z: it is not a Timestamp value`},
		{name: "a nanosecond for a Timestamp", ydbType: ydbtype.Timestamp,
			value:   time.Date(2026, time.January, 2, 0, 0, 0, 1, time.UTC),
			wantErr: `Timestamp cannot hold 2026-01-02T00:00:00.000000001Z: it is not a Timestamp value`},
		{name: "a moment for a Utf8 column", ydbType: ydbtype.Utf8,
			value:   time.Date(2026, time.January, 2, 0, 0, 0, 0, time.UTC),
			wantErr: "Utf8 cannot hold 2026-01-02 00:00:00 \\+0000 UTC: it is a Go time.Time, which is not a Utf8 value"},
		{name: "a nanosecond duration", ydbType: ydbtype.Interval, value: time.Nanosecond,
			wantErr: "Interval cannot hold 1ns: it is finer than the microsecond Interval keeps"},
		{name: "a Go type with no literal", ydbType: ydbtype.Utf8, value: struct{}{},
			wantErr: `Utf8 cannot hold {}: Ptah writes no YQL literal for a Go struct {}`},
		{name: "text that is no UUID", ydbType: ydbtype.UUID, value: "nope",
			wantErr: `Uuid cannot hold "nope": it is not a Uuid value`},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			got, err := ydbtype.ValueLiteral(test.ydbType, test.value)
			c.Assert(err, qt.ErrorAs, new(*ydbtype.ValueError))
			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(got, qt.Equals, "")
		})
	}
}

// Two spellings of one value canonicalize to one Go value, which is what lets a
// declared row pair with the row a read hands back.
func TestCanonicalValue_HappyPath(t *testing.T) {
	local := time.FixedZone("CET", 3600)
	tests := []struct {
		name     string
		ydbType  string
		declared any
		read     any
		want     any
	}{
		{name: "an int and the Int32 a read returns", ydbType: ydbtype.Int32, declared: 5, read: int32(5),
			want: int64(5)},
		{name: "integer text and the Uint64 a read returns", ydbType: ydbtype.Uint64, declared: "7",
			read: uint64(7), want: uint64(7)},
		{name: "decimal text and the digits a read returns", ydbType: "Decimal(10,2)", declared: "12.50",
			read: "12.5", want: "12.5"},
		{name: "a float and the decimal digits", ydbType: "Decimal(22,9)", declared: 12.5, read: "12.5",
			want: "12.5"},
		{name: "a float and the Float a read returns", ydbType: ydbtype.Float, declared: 1.1, read: float32(1.1),
			want: 1.1},
		{name: "a declared day and the local midnight a read returns", ydbType: ydbtype.Date,
			declared: "2026-01-02", read: time.Date(2026, time.January, 2, 1, 0, 0, 0, local),
			want: time.Date(2026, time.January, 2, 0, 0, 0, 0, time.UTC)},
		{name: "a moment in UTC and the same moment in the local zone", ydbType: ydbtype.Timestamp,
			declared: time.Date(2026, time.January, 2, 3, 4, 5, 0, time.UTC),
			read:     time.Date(2026, time.January, 2, 4, 4, 5, 0, local),
			want:     time.Date(2026, time.January, 2, 3, 4, 5, 0, time.UTC)},
		{name: "a declared duration and the Duration a read returns", ydbType: ydbtype.Interval,
			declared: "PT26H", read: 26 * time.Hour, want: 26 * time.Hour},
		{name: "an upper-case UUID and the lower-case one YDB stores", ydbType: ydbtype.UUID,
			declared: "550E8400-E29B-41D4-A716-446655440000", read: "550e8400-e29b-41d4-a716-446655440000",
			want: "550e8400-e29b-41d4-a716-446655440000"},
		{name: "a JSON document and the form YDB returns", ydbType: ydbtype.JSONDocument,
			declared: `{"b": 2, "a": 1.50, "h": "<&>"}`, read: `{"a":1.5,"b":2,"h":"<&>"}`,
			want: `{"a":1.5,"b":2,"h":"<&>"}`},
		{name: "declared text and the bytes of a String", ydbType: ydbtype.String, declared: "ab",
			read: []byte("ab"), want: []byte("ab")},
		{name: "a Serial column", ydbType: ydbtype.Serial, declared: 3, read: int32(3), want: int64(3)},
		{name: "a bool", ydbType: ydbtype.Bool, declared: "true", read: true, want: true},
		{name: "Utf8 text", ydbType: ydbtype.Utf8, declared: "x", read: "x", want: "x"},
		{name: "NULL", ydbType: ydbtype.Utf8, declared: nil, read: nil, want: nil},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			declared, err := ydbtype.CanonicalValue(test.ydbType, test.declared)
			c.Assert(err, qt.IsNil)
			read, err := ydbtype.CanonicalValue(test.ydbType, test.read)
			c.Assert(err, qt.IsNil)
			c.Assert(declared, qt.DeepEquals, test.want)
			c.Assert(read, qt.DeepEquals, test.want)
		})
	}
}

// A value the column cannot hold is refused as ValueLiteral refuses it.
func TestCanonicalValue_FailurePath(t *testing.T) {
	tests := []struct {
		name    string
		ydbType string
		value   any
		wantErr string
	}{
		{name: "an int over Int16", ydbType: ydbtype.Int16, value: 40000,
			wantErr: "Int16 cannot hold 40000: it is outside the range -32768 to 32767"},
		{name: "a document that is not JSON", ydbType: ydbtype.JSONDocument, value: "{",
			wantErr: `JsonDocument cannot hold "{": it is not JSON`},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			got, err := ydbtype.CanonicalValue(test.ydbType, test.value)
			c.Assert(err, qt.ErrorAs, new(*ydbtype.ValueError))
			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(got, qt.IsNil)
		})
	}
}

// CanonicalRows writes each value of a typed column in its canonical form and
// leaves a column with no type as it is.
func TestCanonicalRows_HappyPath(t *testing.T) {
	c := qt.New(t)
	moment := time.Date(2026, 1, 2, 4, 4, 5, 0, time.FixedZone("CET", 3600))

	got, err := ydbtype.CanonicalRows(map[string]string{"id": "Int32", "weight": "Decimal(22,9)", "created": "Timestamp"},
		[]map[string]any{{"id": int32(1), "weight": "1.50", "created": moment, "note": "kept"}})

	c.Assert(err, qt.IsNil)
	c.Assert(got, qt.DeepEquals, []map[string]any{{
		"id": int64(1), "weight": "1.5", "created": time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC), "note": "kept",
	}})
}

// A value its column cannot hold names the row and the column, and nil rows
// stay nil.
func TestCanonicalRows_FailurePath(t *testing.T) {
	c := qt.New(t)

	got, err := ydbtype.CanonicalRows(map[string]string{"n": "Int32"},
		[]map[string]any{{"n": int64(1)}, {"n": int64(5000000000)}})
	none, noneErr := ydbtype.CanonicalRows(map[string]string{"n": "Int32"}, nil)

	c.Assert(err, qt.ErrorMatches, `row 2, column "n": .*`)
	c.Assert(got, qt.IsNil)
	c.Assert(noneErr, qt.IsNil)
	c.Assert(none, qt.IsNil)
}

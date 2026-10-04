package ydb_test

import (
	"encoding/binary"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/dbschema/ydb"
)

func TestHoldsBytes(t *testing.T) {
	tests := []struct {
		databaseType string
		want         bool
	}{
		{databaseType: "String", want: true},
		{databaseType: "Optional<String>", want: true},
		{databaseType: "Optional<Yson>", want: true},
		{databaseType: "Utf8", want: false},
		{databaseType: "Optional<Json>", want: false},
		{databaseType: "Optional<JsonDocument>", want: false},
		{databaseType: "Optional<Decimal(22,9)>", want: false},
		{databaseType: "Optional<Strin", want: false},
		{databaseType: "", want: false},
	}

	for _, test := range tests {
		t.Run(test.databaseType, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(ydb.HoldsBytes(test.databaseType), qt.Equals, test.want)
		})
	}
}

// decimalValue stands in for ydb-go-sdk's scanned Decimal: the 128-bit two's
// complement integer of the scaled value, big-endian.
type decimalValue struct {
	bytes            [16]byte
	precision, scale uint32
}

func (d decimalValue) Decimal() ([16]byte, uint32, uint32) { return d.bytes, d.precision, d.scale }

func scaledBytes(value int64) [16]byte {
	var out [16]byte
	binary.BigEndian.PutUint64(out[8:], uint64(value))
	// The high half is the sign extension: all zero bits, or all one bits for
	// a negative value, which an arithmetic shift by 63 produces.
	binary.BigEndian.PutUint64(out[:8], uint64(value>>63))
	return out
}

func TestRowValue_HappyPath(t *testing.T) {
	moment := time.Date(2026, time.January, 2, 4, 4, 5, 0, time.FixedZone("CET", 3600))
	tests := []struct {
		name  string
		value any
		want  any
	}{
		{name: "a Decimal(22,9) is its digits", value: decimalValue{bytes: scaledBytes(12500000000), precision: 22, scale: 9},
			want: "12.5"},
		{name: "a negative Decimal(10,2)", value: decimalValue{bytes: scaledBytes(-125), precision: 10, scale: 2},
			want: "-1.25"},
		{name: "a moment comes back in UTC", value: moment, want: time.Date(2026, time.January, 2, 3, 4, 5, 0, time.UTC)},
		{name: "an integer as scanned", value: int32(7), want: int32(7)},
		{name: "nil", value: nil, want: nil},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			got, err := ydb.RowValue(test.value)
			c.Assert(err, qt.IsNil)
			c.Assert(got, qt.DeepEquals, test.want)
		})
	}
}

// YDB stores infinity and NaN in a Decimal as values beyond its precision,
// which have no digits to return.
func TestRowValue_FailurePath(t *testing.T) {
	c := qt.New(t)
	var beyond [16]byte
	for i := range beyond {
		beyond[i] = 0x7f
	}
	got, err := ydb.RowValue(decimalValue{bytes: beyond, precision: 22, scale: 9})
	c.Assert(err, qt.ErrorMatches, `read a Decimal\(22,9\) value that is not a finite number`)
	c.Assert(got, qt.IsNil)
}

// The driver hands a moment over in the local zone, and RowValue returns it in
// UTC, the only zone YDB stores. qt.DeepEquals compares two times by instant,
// so the zone is asserted on its own.
func TestRowValue_ReturnsAMomentInUTC(t *testing.T) {
	c := qt.New(t)
	got, err := ydb.RowValue(time.Date(2026, time.January, 2, 4, 4, 5, 0, time.FixedZone("CET", 3600)))
	c.Assert(err, qt.IsNil)
	c.Assert(got.(time.Time).Location(), qt.Equals, time.UTC)
}

package ydbsequence_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform/capability"
	"ptah.run/internal/ydbsequence"
	"ptah.run/internal/ydbtype"
)

// TestPath_HappyPath pins the absolute path a Serial's sequence is addressed
// by: the database, the table's directory, the table, and the name YDB gives
// the sequence (measured on 25.1.4.7 and 26.2.1.14: `t/_serial_column_c`).
func TestPath_HappyPath(t *testing.T) {
	for _, test := range []struct {
		name, database, schema, table, column, want string
	}{
		{name: "a table at the root", database: "/local", table: "orders", column: "id",
			want: "/local/orders/_serial_column_id"},
		{name: "a table in a directory", database: "/local", schema: "app/sub", table: "orders", column: "id",
			want: "/local/app/sub/orders/_serial_column_id"},
		{name: "a database written without its leading slash", database: "Root/test", table: "t", column: "n",
			want: "/Root/test/t/_serial_column_n"},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(ydbsequence.Path(test.database, test.schema, test.table, test.column), qt.Equals, test.want)
		})
	}
}

// TestParse_HappyPath reads the settings a declaration gives, an omitted one
// being the 1 a new sequence has.
func TestParse_HappyPath(t *testing.T) {
	for _, test := range []struct {
		name             string
		start, increment string
		want             ydbsequence.Settings
		wantDefault      bool
	}{
		{name: "neither declared", want: ydbsequence.Settings{Start: 1, Increment: 1}, wantDefault: true},
		{name: "the defaults written out", start: "1", increment: " 1 ", want: ydbsequence.Settings{Start: 1, Increment: 1}, wantDefault: true},
		{name: "a start", start: "100", want: ydbsequence.Settings{Start: 100, Increment: 1}},
		{name: "an increment", increment: "5", want: ydbsequence.Settings{Start: 1, Increment: 5}},
		{name: "the largest start", start: "9223372036854775807",
			want: ydbsequence.Settings{Start: 9223372036854775807, Increment: 1}},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			got, err := ydbsequence.Parse(test.start, test.increment)
			c.Assert(err, qt.IsNil)
			c.Assert(got, qt.Equals, test.want)
			c.Assert(got.IsDefault(), qt.Equals, test.wantDefault)
		})
	}
}

// TestParse_FailurePath refuses what YDB's ALTER SEQUENCE refuses, measured
// on 25.1.4.7 and 26.2.1.14: a value that is not a whole number is a parse
// error, a start below 1 answers `Start value: 0 cannot be less than min
// value: 1`, a negative increment is a parse error, and 0 answers `Increment
// must not be zero`.
func TestParse_FailurePath(t *testing.T) {
	for _, test := range []struct {
		name             string
		start, increment string
		wantErr          string
	}{
		{name: "a start of 0", start: "0", wantErr: `identity_start 0: a YDB Serial's sequence takes a start of 1 or more .*`},
		{name: "a negative start", start: "-5", wantErr: `identity_start -5: .*`},
		{name: "an increment of 0", increment: "0",
			wantErr: `identity_increment 0: a YDB Serial's sequence takes an increment of 1 or more .*Increment must not be zero.*`},
		{name: "a negative increment", increment: "-1", wantErr: `identity_increment -1: .*`},
		{name: "a fraction", start: "1.5", wantErr: `identity_start "1.5" is not a whole number YDB's ALTER SEQUENCE takes`},
		{name: "an expression", increment: "1 + 1", wantErr: `identity_increment "1 \+ 1" is not a whole number .*`},
		{name: "past the Int64 maximum", start: "9223372036854775808", wantErr: `identity_start "9223372036854775808" .*`},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			got, err := ydbsequence.Parse(test.start, test.increment)
			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(got, qt.Equals, ydbsequence.Settings{})
		})
	}
}

// TestCanonical pins the form a comparison reads a setting in: two spellings
// of one value agree, and a value Parse refuses is kept, so it still differs.
func TestCanonical(t *testing.T) {
	for _, test := range []struct{ value, want string }{
		{value: "", want: "1"},
		{value: " 1 ", want: "1"},
		{value: "0100", want: "100"},
		{value: "+7", want: "7"},
		{value: "1.5", want: "1.5"},
	} {
		t.Run(test.value, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(ydbsequence.Canonical(test.value), qt.Equals, test.want)
		})
	}
}

// TestRangeRefusal_HappyPath passes the settings whose ALTER SEQUENCE cannot
// move a sequence past its column: a 64-bit Serial's, the defaults on any
// width, and any settings on a target that keeps the range.
func TestRangeRefusal_HappyPath(t *testing.T) {
	widened := capability.YDB262()
	kept := widened.With(capability.SerialSequenceKeepsRange, true)
	for _, test := range []struct {
		name       string
		serialType string
		settings   ydbsequence.Settings
		caps       capability.Capabilities
	}{
		{name: "a BigSerial", serialType: ydbtype.BigSerial, settings: ydbsequence.Settings{Start: 100, Increment: 5}, caps: widened},
		{name: "a Serial at the defaults", serialType: ydbtype.Serial, settings: ydbsequence.Default, caps: widened},
		{name: "a Serial on a target that keeps the range", serialType: ydbtype.Serial,
			settings: ydbsequence.Settings{Start: 100, Increment: 1}, caps: kept},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(ydbsequence.RangeRefusal(test.serialType, test.settings, test.caps), qt.Equals, "")
		})
	}
}

// TestRangeRefusal_FailurePath refuses settings other than the defaults on a
// 16-bit or 32-bit Serial where ALTER SEQUENCE widens the sequence: measured,
// the next value after the column's maximum is then stored as a negative
// number without an error.
func TestRangeRefusal_FailurePath(t *testing.T) {
	for _, test := range []struct {
		name       string
		serialType string
		want       string
	}{
		{name: "a Serial", serialType: ydbtype.Serial,
			want: `an ALTER SEQUENCE raises the maximum of a Serial's sequence from 2147483647 to the Int64 maximum, .*`},
		{name: "a SmallSerial", serialType: ydbtype.SmallSerial,
			want: `an ALTER SEQUENCE raises the maximum of a SmallSerial's sequence from 32767 to the Int64 maximum, .*`},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			got := ydbsequence.RangeRefusal(test.serialType, ydbsequence.Settings{Start: 1, Increment: 2}, capability.YDB262())
			c.Assert(got, qt.Matches, test.want)
		})
	}
}

package statementargs_test

import (
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"math"
	"strconv"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"
	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
	"google.golang.org/protobuf/testing/protocmp"

	"ptah.run/core/platform/descriptor"
	"ptah.run/core/ptaherr"
	"ptah.run/internal/providerhost/statementargs"
)

type (
	count      uint
	identifier int
	label      string
	small      int32
)

// textValuer is a string whose Value method decides what is bound.
type textValuer string

func (v textValuer) Value() (driver.Value, error) { return "text:" + string(v), nil }

// absentValuer binds null.
type absentValuer struct{}

func (absentValuer) Value() (driver.Value, error) { return nil, nil }

// receiverValuer implements driver.Valuer on its pointer alone.
type receiverValuer struct{ n int64 }

func (v *receiverValuer) Value() (driver.Value, error) { return v.n, nil }

// narrowValuer answers a value the contract cannot carry.
type narrowValuer struct{}

func (narrowValuer) Value() (driver.Value, error) { return int32(5), nil }

// pointerValuer answers a pointer.
type pointerValuer struct{}

func (pointerValuer) Value() (driver.Value, error) { return new(int64(5)), nil }

var errValue = errors.New("the value cannot be read")

type failingValuer struct{}

func (failingValuer) Value() (driver.Value, error) { return nil, errValue }

func signed(v int64) *protocol.DataValue {
	return &protocol.DataValue{Value: &protocol.DataValue_Int64Value{Int64Value: v}}
}

func unsigned(v uint64) *protocol.DataValue {
	return &protocol.DataValue{Value: &protocol.DataValue_Uint64Value{Uint64Value: v}}
}

func text(v string) *protocol.DataValue {
	return &protocol.DataValue{Value: &protocol.DataValue_StringValue{StringValue: v}}
}

func binary(v []byte) *protocol.DataValue {
	return &protocol.DataValue{Value: &protocol.DataValue_BytesValue{BytesValue: v}}
}

var null = &protocol.DataValue{Value: &protocol.DataValue_Null{Null: &protocol.Empty{}}}

// Each argument carries the case its Go type names: an unsigned integer stays
// unsigned at any value, a named type is its underlying kind, and a Valuer is
// what it answers.
func TestWire_HappyPath(t *testing.T) {
	const wide = 5000000000
	moment := time.Date(2026, 11, 3, 12, 30, 15, 250, time.FixedZone("CET", 3600))
	tests := []struct {
		name     string
		argument any
		want     *protocol.DataValue
	}{
		{name: "nil", argument: nil, want: null},
		{name: "a bool", argument: true, want: &protocol.DataValue{Value: &protocol.DataValue_BoolValue{BoolValue: true}}},
		{name: "an int", argument: int(wide), want: signed(wide)},
		{name: "an int64", argument: int64(-wide), want: signed(-wide)},
		{name: "a named int", argument: identifier(wide), want: signed(wide)},
		{name: "a uint", argument: uint(wide), want: unsigned(wide)},
		{name: "a uint64 above the int64 range", argument: uint64(math.MaxUint64), want: unsigned(math.MaxUint64)},
		{name: "a named uint", argument: count(wide), want: unsigned(wide)},
		{name: "a float64", argument: 1.5, want: &protocol.DataValue{Value: &protocol.DataValue_DoubleValue{DoubleValue: 1.5}}},
		{name: "a string", argument: "CZ", want: text("CZ")},
		{name: "a named string", argument: label("CZ"), want: text("CZ")},
		{name: "a byte slice", argument: []byte{1, 2}, want: binary([]byte{1, 2})},
		{name: "a named byte slice", argument: json.RawMessage(`{}`), want: binary([]byte(`{}`))},
		{name: "a time in another zone", argument: moment, want: &protocol.DataValue{Value: &protocol.DataValue_TimestampValue{
			TimestampValue: &protocol.TimestampValue{Seconds: moment.Unix(), Nanos: 250}}}},
		{name: "a duration", argument: 90*time.Second + 5, want: &protocol.DataValue{Value: &protocol.DataValue_IntervalValue{
			IntervalValue: &protocol.IntervalValue{Nanoseconds: 90000000005}}}},
		{name: "a valuer over a string", argument: textValuer("CZ"), want: text("text:CZ")},
		{name: "a valuer answering null", argument: absentValuer{}, want: null},
		{name: "a valuer on a pointer receiver", argument: &receiverValuer{n: 7}, want: signed(7)},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			got, err := statementargs.Wire(test.argument)
			c.Assert(err, qt.IsNil)
			c.Assert(got, qt.CmpEquals(protocmp.Transform()), test.want)
		})
	}
}

// The wire form holds its own bytes, so a caller reusing its buffer after the
// call changes nothing that is sent.
func TestWire_CopiesBytes(t *testing.T) {
	c := qt.New(t)
	buffer := []byte{1, 2}
	got, err := statementargs.Wire(buffer)
	buffer[0] = 9
	c.Assert(err, qt.IsNil)
	c.Assert(got.GetBytesValue(), qt.DeepEquals, []byte{1, 2})
}

// An argument the contract cannot carry exactly is refused rather than sent as
// another type: a narrower number would be bound at 64 bits, a type that
// admits null as a plain value, and a list not at all.
func TestWire_FailurePath(t *testing.T) {
	tests := []struct {
		name     string
		argument any
		wantErr  string
	}{
		{name: "an int8", argument: int8(5), wantErr: ` of type int8, since it carries an integer only as int64 or uint64`},
		{name: "an int16", argument: int16(5), wantErr: ` of type int16, since it carries an integer only as int64 or uint64`},
		{name: "an int32", argument: int32(5), wantErr: ` of type int32, since it carries an integer only as int64 or uint64`},
		{name: "a uint8", argument: uint8(5), wantErr: ` of type uint8, since it carries an integer only as int64 or uint64`},
		{name: "a uint16", argument: uint16(5), wantErr: ` of type uint16, since it carries an integer only as int64 or uint64`},
		{name: "a uint32", argument: uint32(5), wantErr: ` of type uint32, since it carries an integer only as int64 or uint64`},
		{name: "a named int32", argument: small(5), wantErr: ` of type statementargs_test.small, since it carries an integer only as int64 or uint64`},
		{name: "a float32", argument: float32(1.5), wantErr: ` of type float32, since it carries a floating-point number only as float64`},
		{name: "a pointer", argument: new(5), wantErr: ` of type \*int, since it cannot say that a type admits null`},
		{name: "a nil pointer", argument: (*uint)(nil), wantErr: ` of type \*uint, since it cannot say that a type admits null`},
		{name: "a valid generic null", argument: sql.Null[int]{V: 5, Valid: true},
			wantErr: ` of type sql.Null\[int\], since it cannot say that a type admits null`},
		{name: "an invalid generic null", argument: sql.Null[uint]{},
			wantErr: ` of type sql.Null\[uint\], since it cannot say that a type admits null`},
		{name: "a null string", argument: sql.NullString{String: "CZ", Valid: true},
			wantErr: ` of type sql.NullString, since it cannot say that a type admits null`},
		{name: "a null int32", argument: sql.NullInt32{Int32: 5, Valid: true},
			wantErr: ` of type sql.NullInt32, since it cannot say that a type admits null`},
		{name: "a pointer to a null value", argument: &sql.NullString{String: "CZ", Valid: true},
			wantErr: ` of type \*sql.NullString, since it cannot say that a type admits null`},
		{name: "a pointer to a valuer", argument: new(textValuer("CZ")),
			wantErr: ` of type \*statementargs_test.textValuer, since it cannot say that a type admits null`},
		{name: "a valuer answering an int32", argument: narrowValuer{}, wantErr: ` of type int32, since it carries an integer only as int64 or uint64`},
		{name: "a valuer answering a pointer", argument: pointerValuer{}, wantErr: ` of type \*int64, since it cannot say that a type admits null`},
		{name: "a slice of int", argument: []int{1}, wantErr: ` of type \[\]int, since it carries no list`},
		{name: "a slice of uint", argument: []uint{1}, wantErr: ` of type \[\]uint, since it carries no list`},
		{name: "a slice of string", argument: []string{"CZ"}, wantErr: ` of type \[\]string, since it carries no list`},
		{name: "an array of int", argument: [2]int{1, 2}, wantErr: ` of type \[2\]int, since it carries no list`},
		{name: "a byte array", argument: [2]byte{1, 2}, wantErr: ` of type \[2\]uint8`},
		{name: "a map", argument: map[string]int{"a": 1}, wantErr: ` of type map\[string\]int`},
		{name: "a struct", argument: struct{ A int }{A: 1}, wantErr: ` of type struct \{ A int \}`},
		{name: "a complex number", argument: complex(1, 2), wantErr: ` of type complex128`},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			got, err := statementargs.Wire(test.argument)
			c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
			c.Assert(err, qt.ErrorMatches, `unsupported feature: the experimental provider contract does not support a statement argument`+test.wantErr)
			c.Assert(got, qt.IsNil)
		})
	}
}

// A Valuer that cannot answer stops the argument with its own error.
func TestWire_ValuerFailure(t *testing.T) {
	c := qt.New(t)
	got, err := statementargs.Wire(failingValuer{})
	c.Assert(err, qt.ErrorIs, errValue)
	c.Assert(got, qt.IsNil)
}

// named is an argument database/sql passes for sql.Named(name, ...) at
// ordinal, holding the wire form of v.
func named(ordinal int, name string, v int64) driver.NamedValue {
	return driver.NamedValue{Ordinal: ordinal, Name: name, Value: signed(v)}
}

// positional is an argument without a name at ordinal, holding the wire form
// of v.
func positional(ordinal int, v int64) driver.NamedValue {
	return driver.NamedValue{Ordinal: ordinal, Value: signed(v)}
}

// Arguments without names keep the order of the call under every style, and
// a named argument under the dollar-named style takes the position its name
// gives, wherever the call put it.
func TestArguments_HappyPath(t *testing.T) {
	ten := make([]driver.NamedValue, 0, 10)
	tenWant := make([]*protocol.DataValue, 10)
	for i := range 10 {
		position := 10 - i
		ten = append(ten, named(i+1, "p"+strconv.Itoa(position), int64(position)))
		tenWant[position-1] = signed(int64(position))
	}
	tests := []struct {
		name      string
		style     descriptor.Placeholder
		arguments []driver.NamedValue
		want      []*protocol.DataValue
	}{
		{name: "no arguments", style: descriptor.PlaceholderDollarNamed, arguments: nil, want: make([]*protocol.DataValue, 0)},
		{name: "positional under no style", style: "", arguments: []driver.NamedValue{positional(1, 1), positional(2, 2)},
			want: []*protocol.DataValue{signed(1), signed(2)}},
		{name: "positional under question", style: descriptor.PlaceholderQuestion, arguments: []driver.NamedValue{positional(1, 1), positional(2, 2)},
			want: []*protocol.DataValue{signed(1), signed(2)}},
		{name: "positional under dollar-named", style: descriptor.PlaceholderDollarNamed,
			arguments: []driver.NamedValue{positional(1, 1), positional(2, 2)}, want: []*protocol.DataValue{signed(1), signed(2)}},
		{name: "named in order", style: descriptor.PlaceholderDollarNamed,
			arguments: []driver.NamedValue{named(1, "p1", 1), named(2, "p2", 2)}, want: []*protocol.DataValue{signed(1), signed(2)}},
		{name: "named out of order", style: descriptor.PlaceholderDollarNamed,
			arguments: []driver.NamedValue{named(1, "p2", 2), named(2, "p3", 3), named(3, "p1", 1)},
			want:      []*protocol.DataValue{signed(1), signed(2), signed(3)}},
		{name: "a number of two digits", style: descriptor.PlaceholderDollarNamed, arguments: ten, want: tenWant},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			got, err := statementargs.Arguments(test.style, test.arguments)
			c.Assert(err, qt.IsNil)
			c.Assert(got, qt.CmpEquals(protocmp.Transform()), test.want)
		})
	}
}

// A named argument the positions cannot say exactly is refused rather than
// sent at a position that binds another parameter or none.
func TestArguments_FailurePath(t *testing.T) {
	const prefix = `unsupported feature: the experimental provider contract does not support `
	tests := []struct {
		name      string
		style     descriptor.Placeholder
		arguments []driver.NamedValue
		wantErr   string
	}{
		{name: "a name under no style", style: "", arguments: []driver.NamedValue{named(1, "p1", 1)},
			wantErr: `the named argument "p1", since the engine declares no placeholder style that names a parameter`},
		{name: "a name under question", style: descriptor.PlaceholderQuestion, arguments: []driver.NamedValue{named(1, "p1", 1)},
			wantErr: `the named argument "p1", since the engine's placeholder style "question" binds arguments by position`},
		{name: "a name under dollar", style: descriptor.PlaceholderDollar, arguments: []driver.NamedValue{named(1, "p1", 1)},
			wantErr: `the named argument "p1", since the engine's placeholder style "dollar" binds arguments by position`},
		{name: "a name under at-p", style: descriptor.PlaceholderAtP, arguments: []driver.NamedValue{named(1, "p1", 1)},
			wantErr: `the named argument "p1", since the engine's placeholder style "at-p" binds arguments by position`},
		{name: "a name under colon", style: descriptor.PlaceholderColon, arguments: []driver.NamedValue{named(1, "p1", 1)},
			wantErr: `the named argument "p1", since the engine's placeholder style "colon" binds arguments by position`},
		{name: "a name of another kind", style: descriptor.PlaceholderDollarNamed, arguments: []driver.NamedValue{named(1, "code", 1)},
			wantErr: `the named argument "code", since the engine's placeholder style "dollar-named" gives a position only to the names p1, p2 and so on`},
		{name: "a prefix without a number", style: descriptor.PlaceholderDollarNamed, arguments: []driver.NamedValue{named(1, "p", 1)},
			wantErr: `the named argument "p", since the engine's placeholder style "dollar-named" gives a position only to the names p1, p2 and so on`},
		{name: "position zero", style: descriptor.PlaceholderDollarNamed, arguments: []driver.NamedValue{named(1, "p0", 1)},
			wantErr: `the named argument "p0", since the engine's placeholder style "dollar-named" gives a position only to the names p1, p2 and so on`},
		{name: "a leading zero", style: descriptor.PlaceholderDollarNamed, arguments: []driver.NamedValue{named(1, "p01", 1)},
			wantErr: `the named argument "p01", since the engine's placeholder style "dollar-named" gives a position only to the names p1, p2 and so on`},
		{name: "an upper-case prefix", style: descriptor.PlaceholderDollarNamed, arguments: []driver.NamedValue{named(1, "P1", 1)},
			wantErr: `the named argument "P1", since the engine's placeholder style "dollar-named" gives a position only to the names p1, p2 and so on`},
		{name: "a suffix after the number", style: descriptor.PlaceholderDollarNamed, arguments: []driver.NamedValue{named(1, "p1a", 1)},
			wantErr: `the named argument "p1a", since the engine's placeholder style "dollar-named" gives a position only to the names p1, p2 and so on`},
		{name: "a signed number", style: descriptor.PlaceholderDollarNamed, arguments: []driver.NamedValue{named(1, "p+1", 1)},
			wantErr: `the named argument "p\+1", since the engine's placeholder style "dollar-named" gives a position only to the names p1, p2 and so on`},
		{name: "a digit outside ASCII", style: descriptor.PlaceholderDollarNamed, arguments: []driver.NamedValue{named(1, "p١", 1)},
			wantErr: `the named argument "p١", since the engine's placeholder style "dollar-named" gives a position only to the names p1, p2 and so on`},
		{name: "one name twice", style: descriptor.PlaceholderDollarNamed, arguments: []driver.NamedValue{named(1, "p1", 1), named(2, "p1", 2)},
			wantErr: `the named argument "p1" twice`},
		{name: "a gap", style: descriptor.PlaceholderDollarNamed, arguments: []driver.NamedValue{named(1, "p1", 1), named(2, "p3", 3)},
			wantErr: `the named argument "p3" without one named "p2", since it carries arguments by position`},
		{name: "no first position", style: descriptor.PlaceholderDollarNamed, arguments: []driver.NamedValue{named(1, "p2", 2)},
			wantErr: `the named argument "p2" without one named "p1", since it carries arguments by position`},
		{name: "the lowest of two past the count", style: descriptor.PlaceholderDollarNamed,
			arguments: []driver.NamedValue{named(1, "p5", 5), named(2, "p1", 1), named(3, "p4", 4)},
			wantErr:   `the named argument "p4" without one named "p2", since it carries arguments by position`},
		{name: "a number past the int range", style: descriptor.PlaceholderDollarNamed,
			arguments: []driver.NamedValue{named(1, "p99999999999999999999", 1)},
			wantErr:   `the named argument "p99999999999999999999" without one named "p1", since it carries arguments by position`},
		{name: "a positional argument after a named one", style: descriptor.PlaceholderDollarNamed,
			arguments: []driver.NamedValue{named(1, "p1", 1), positional(2, 2)}, wantErr: `named and positional arguments in one statement`},
		{name: "a named argument after a positional one", style: descriptor.PlaceholderDollarNamed,
			arguments: []driver.NamedValue{positional(1, 1), named(2, "p2", 2)}, wantErr: `named and positional arguments in one statement`},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			got, err := statementargs.Arguments(test.style, test.arguments)
			c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
			c.Assert(err, qt.ErrorMatches, prefix+test.wantErr)
			c.Assert(got, qt.IsNil)
		})
	}
}

// An argument that does not hold a wire form never reached Wire, which is a
// defect of the caller rather than a limit of the contract.
func TestArguments_Unchecked(t *testing.T) {
	tests := []struct {
		name     string
		argument driver.NamedValue
		wantErr  string
	}{
		{name: "a Go value", argument: driver.NamedValue{Ordinal: 1, Name: "p1", Value: int64(1)},
			wantErr: `statement argument 1 of type int64 reached the provider handle unchecked`},
		{name: "no wire form", argument: driver.NamedValue{Ordinal: 1, Value: (*protocol.DataValue)(nil)},
			wantErr: `statement argument 1 of type \*protocol.DataValue reached the provider handle unchecked`},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			got, err := statementargs.Arguments(descriptor.PlaceholderDollarNamed, []driver.NamedValue{test.argument})
			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(err, qt.Not(qt.ErrorIs), ptaherr.ErrUnsupportedFeature)
			c.Assert(got, qt.IsNil)
		})
	}
}

package statementargs_test

import (
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"math"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"
	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
	"google.golang.org/protobuf/testing/protocmp"

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

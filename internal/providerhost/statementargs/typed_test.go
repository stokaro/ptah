package statementargs_test

import (
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"math"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"
	"google.golang.org/protobuf/testing/protocmp"

	"ptah.run/core/platform/descriptor"
	"ptah.run/core/ptaherr"
	"ptah.run/internal/providerhost/statementargs"
	protocol "ptah.run/provider-sdk/protocol/experimental"
)

func scalar(kind protocol.ArgumentType_Scalar, value *protocol.DataValue) *protocol.StatementArgument {
	return &protocol.StatementArgument{Type: &protocol.ArgumentType{Scalar: kind}, Value: value}
}

func optional(kind protocol.ArgumentType_Scalar, value *protocol.DataValue) *protocol.StatementArgument {
	return &protocol.StatementArgument{Type: &protocol.ArgumentType{Scalar: kind, Optional: true}, Value: value}
}

func list(kind protocol.ArgumentType_Scalar, nullable bool, elements ...*protocol.DataValue) *protocol.StatementArgument {
	return &protocol.StatementArgument{Type: &protocol.ArgumentType{Scalar: kind, Optional: nullable, List: true}, Elements: elements}
}

func double(v float64) *protocol.DataValue {
	return &protocol.DataValue{Value: &protocol.DataValue_DoubleValue{DoubleValue: v}}
}

func boolean(v bool) *protocol.DataValue {
	return &protocol.DataValue{Value: &protocol.DataValue_BoolValue{BoolValue: v}}
}

func timestamp(moment time.Time) *protocol.DataValue {
	return &protocol.DataValue{Value: &protocol.DataValue_TimestampValue{TimestampValue: &protocol.TimestampValue{
		Seconds: moment.Unix(), Nanos: int32(moment.Nanosecond())}}} // #nosec G115 -- Nanosecond is in [0, 1e9)
}

func interval(nanoseconds int64) *protocol.DataValue {
	return &protocol.DataValue{Value: &protocol.DataValue_IntervalValue{IntervalValue: &protocol.IntervalValue{Nanoseconds: nanoseconds}}}
}

// codes is a list of a named string type.
type codes []label

// NullCode looks like a database/sql Null type and is not one.
type NullCode struct {
	Code  string
	Valid bool
}

// nestedValuer answers another Valuer, which database/sql does not call.
type nestedValuer struct{}

func (nestedValuer) Value() (driver.Value, error) { return textValuer("CZ"), nil }

// arrayValuer is a list whose Value method decides what is bound.
type arrayValuer []string

func (arrayValuer) Value() (driver.Value, error) { return "{CZ,SK}", nil }

// Each argument carries the scalar of its width and sign, a type that admits
// null is optional with a typed null, and a slice is a list of its element's
// scalar.
func TestTyped_HappyPath(t *testing.T) {
	moment := time.Date(2026, 11, 3, 12, 30, 15, 250, time.FixedZone("CET", 3600))
	tests := []struct {
		name     string
		argument any
		want     *protocol.StatementArgument
	}{
		{name: "nil", argument: nil, want: scalar(protocol.ArgumentType_UNTYPED, null)},
		{name: "a bool", argument: true, want: scalar(protocol.ArgumentType_BOOL, boolean(true))},
		{name: "an int8", argument: int8(math.MinInt8), want: scalar(protocol.ArgumentType_INT8, signed(math.MinInt8))},
		{name: "an int16", argument: int16(math.MaxInt16), want: scalar(protocol.ArgumentType_INT16, signed(math.MaxInt16))},
		{name: "an int32", argument: int32(math.MinInt32), want: scalar(protocol.ArgumentType_INT32, signed(math.MinInt32))},
		{name: "an int64", argument: int64(math.MaxInt64), want: scalar(protocol.ArgumentType_INT64, signed(math.MaxInt64))},
		{name: "an int", argument: -5, want: scalar(protocol.ArgumentType_INT64, signed(-5))},
		{name: "a named int32", argument: small(5), want: scalar(protocol.ArgumentType_INT32, signed(5))},
		{name: "a uint8", argument: uint8(math.MaxUint8), want: scalar(protocol.ArgumentType_UINT8, unsigned(math.MaxUint8))},
		{name: "a uint16", argument: uint16(math.MaxUint16), want: scalar(protocol.ArgumentType_UINT16, unsigned(math.MaxUint16))},
		{name: "a uint32", argument: uint32(math.MaxUint32), want: scalar(protocol.ArgumentType_UINT32, unsigned(math.MaxUint32))},
		{name: "a uint64", argument: uint64(math.MaxUint64), want: scalar(protocol.ArgumentType_UINT64, unsigned(math.MaxUint64))},
		{name: "a uint", argument: uint(7), want: scalar(protocol.ArgumentType_UINT64, unsigned(7))},
		{name: "a named uint", argument: count(7), want: scalar(protocol.ArgumentType_UINT64, unsigned(7))},
		{name: "a float32", argument: float32(0.1), want: scalar(protocol.ArgumentType_FLOAT32, double(float64(float32(0.1))))},
		{name: "a float64", argument: 0.1, want: scalar(protocol.ArgumentType_FLOAT64, double(0.1))},
		{name: "a string", argument: "CZ", want: scalar(protocol.ArgumentType_STRING, text("CZ"))},
		{name: "a named string", argument: label("CZ"), want: scalar(protocol.ArgumentType_STRING, text("CZ"))},
		{name: "a byte slice", argument: []byte{1, 2}, want: scalar(protocol.ArgumentType_BYTES, binary([]byte{1, 2}))},
		{name: "a named byte slice", argument: json.RawMessage(`{}`), want: scalar(protocol.ArgumentType_BYTES, binary([]byte(`{}`)))},
		{name: "a time", argument: moment, want: scalar(protocol.ArgumentType_TIMESTAMP, timestamp(moment))},
		{name: "a duration", argument: 90*time.Second + 5, want: scalar(protocol.ArgumentType_INTERVAL, interval(90000000005))},
		{name: "a pointer", argument: new(uint(7)), want: optional(protocol.ArgumentType_UINT64, unsigned(7))},
		{name: "a nil pointer", argument: (*int32)(nil), want: optional(protocol.ArgumentType_INT32, null)},
		{name: "a pointer to a time", argument: &moment, want: optional(protocol.ArgumentType_TIMESTAMP, timestamp(moment))},
		{name: "a pointer to bytes", argument: &[]byte{1}, want: optional(protocol.ArgumentType_BYTES, binary([]byte{1}))},
		{name: "a valid generic null", argument: sql.Null[uint]{V: 7, Valid: true}, want: optional(protocol.ArgumentType_UINT64, unsigned(7))},
		{name: "an invalid generic null", argument: sql.Null[int16]{V: 7}, want: optional(protocol.ArgumentType_INT16, null)},
		{name: "a generic null of a time", argument: sql.Null[time.Time]{V: moment, Valid: true},
			want: optional(protocol.ArgumentType_TIMESTAMP, timestamp(moment))},
		{name: "a null int32", argument: sql.NullInt32{Int32: 5, Valid: true}, want: optional(protocol.ArgumentType_INT32, signed(5))},
		{name: "a null byte", argument: sql.NullByte{Byte: 5, Valid: true}, want: optional(protocol.ArgumentType_UINT8, unsigned(5))},
		{name: "an invalid null string", argument: sql.NullString{String: "CZ"}, want: optional(protocol.ArgumentType_STRING, null)},
		{name: "a null time", argument: sql.NullTime{Time: moment, Valid: true}, want: optional(protocol.ArgumentType_TIMESTAMP, timestamp(moment))},
		{name: "a null float64", argument: sql.NullFloat64{}, want: optional(protocol.ArgumentType_FLOAT64, null)},
		{name: "a slice of uint64", argument: []uint64{1, math.MaxUint64}, want: list(protocol.ArgumentType_UINT64, false, unsigned(1), unsigned(math.MaxUint64))},
		{name: "a slice of int32", argument: []int32{-1}, want: list(protocol.ArgumentType_INT32, false, signed(-1))},
		{name: "a named slice of a named string", argument: codes{"CZ", "SK"}, want: list(protocol.ArgumentType_STRING, false, text("CZ"), text("SK"))},
		{name: "a slice of byte slices", argument: [][]byte{{1}}, want: list(protocol.ArgumentType_BYTES, false, binary([]byte{1}))},
		{name: "an empty slice", argument: make([]string, 0), want: list(protocol.ArgumentType_STRING, false)},
		{name: "a nil slice", argument: []float32(nil), want: list(protocol.ArgumentType_FLOAT32, false)},
		{name: "a slice of pointers", argument: []*string{new("CZ"), nil}, want: list(protocol.ArgumentType_STRING, true, text("CZ"), null)},
		{name: "a slice of null values", argument: []sql.NullInt64{{Int64: 1, Valid: true}, {}},
			want: list(protocol.ArgumentType_INT64, true, signed(1), null)},
		{name: "a slice of generic nulls", argument: []sql.Null[uint8]{{V: 1, Valid: true}, {}},
			want: list(protocol.ArgumentType_UINT8, true, unsigned(1), null)},
		{name: "a valuer over a string", argument: textValuer("CZ"), want: scalar(protocol.ArgumentType_STRING, text("text:CZ"))},
		{name: "a valuer answering null", argument: absentValuer{}, want: scalar(protocol.ArgumentType_UNTYPED, null)},
		{name: "a valuer answering an int32", argument: narrowValuer{}, want: scalar(protocol.ArgumentType_INT32, signed(5))},
		{name: "a valuer on a pointer receiver", argument: &receiverValuer{n: 7}, want: scalar(protocol.ArgumentType_INT64, signed(7))},
		{name: "a slice with a Value method", argument: arrayValuer{"CZ"}, want: scalar(protocol.ArgumentType_STRING, text("{CZ,SK}"))},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			got, err := statementargs.Typed(test.argument)
			c.Assert(err, qt.IsNil)
			c.Assert(got, qt.CmpEquals(protocmp.Transform()), test.want)
		})
	}
}

// The typed form holds its own bytes, so a caller reusing its buffer after the
// call changes nothing that is sent.
func TestTyped_CopiesBytes(t *testing.T) {
	c := qt.New(t)
	buffer := []byte{1, 2}
	elements := [][]byte{{1, 2}}
	got, err := statementargs.Typed(buffer)
	c.Assert(err, qt.IsNil)
	gotList, err := statementargs.Typed(elements)
	c.Assert(err, qt.IsNil)
	buffer[0], elements[0][0] = 9, 9
	c.Assert(got.GetValue().GetBytesValue(), qt.DeepEquals, []byte{1, 2})
	c.Assert(gotList.GetElements()[0].GetBytesValue(), qt.DeepEquals, []byte{1, 2})
}

// What even the typed form cannot carry exactly is refused rather than sent as
// another type.
func TestTyped_FailurePath(t *testing.T) {
	tests := []struct {
		name     string
		argument any
		wantErr  string
	}{
		{name: "an array", argument: [2]int{1, 2}, wantErr: ` of type \[2\]int, since it carries no array`},
		{name: "a byte array", argument: [2]byte{1, 2}, wantErr: ` of type \[2\]uint8, since it carries no array`},
		{name: "a pointer to a pointer", argument: new(new(5)), wantErr: ` of type \*\*int, since it carries null at one level only`},
		{name: "a pointer to a null value", argument: &sql.NullString{String: "CZ", Valid: true},
			wantErr: ` of type \*sql.NullString, since a null of a type whose Value method decides what is bound has no type`},
		{name: "a pointer to a valuer", argument: new(textValuer("CZ")),
			wantErr: ` of type \*statementargs_test.textValuer, since a null of a type whose Value method decides what is bound has no type`},
		{name: "a pointer to a list", argument: &[]int{1}, wantErr: ` of type \*\[\]int, since it carries no list that admits null`},
		{name: "a pointer to a struct", argument: &struct{ A int }{A: 1}, wantErr: ` of type \*struct \{ A int \}`},
		{name: "a generic null of a pointer", argument: sql.Null[*int]{}, wantErr: ` of type sql.Null\[\*int\], since it carries null at one level only`},
		{name: "a generic null of a list", argument: sql.Null[[]int]{}, wantErr: ` of type sql.Null\[\[\]int\]`},
		{name: "a generic null of a valuer", argument: sql.Null[textValuer]{},
			wantErr: ` of type sql.Null\[ptah.run/internal/providerhost/statementargs_test.textValuer\], since a null of a type whose Value method decides what is bound has no type`},
		{name: "a nested list", argument: [][]int{{1}}, wantErr: ` of type \[\]\[\]int, since it carries no nested list`},
		{name: "a list of arrays", argument: [][2]byte{{1, 2}}, wantErr: ` of type \[\]\[2\]uint8, since it carries no nested list`},
		{name: "a list of pointers to lists", argument: []*[]int{{1}}, wantErr: ` of type \[\]\*\[\]int, since it carries no nested list`},
		{name: "a list of pointers to pointers", argument: []**int{nil}, wantErr: ` of type \[\]\*\*int, since it carries null at one level only`},
		{name: "a list of generic nulls of pointers", argument: []sql.Null[*int]{{}}, wantErr: ` of type \[\]sql.Null\[\*int\], since it carries null at one level only`},
		{name: "a list of valuers", argument: []textValuer{"CZ"},
			wantErr: ` of type \[\]statementargs_test.textValuer, since the Value method of its elements decides what each binds`},
		{name: "a list of pointers to null values", argument: []*sql.NullInt32{nil},
			wantErr: ` of type \[\]\*sql.NullInt32, since the Value method of its elements decides what each binds`},
		{name: "a list of generic nulls of valuers", argument: []sql.Null[textValuer]{{}},
			wantErr: ` of type \[\]sql.Null\[ptah.run/internal/providerhost/statementargs_test.textValuer\], since the Value method of its elements decides what each binds`},
		{name: "a list of anything", argument: []any{1}, wantErr: ` of type \[\]interface \{\}`},
		{name: "a list of structs", argument: []struct{ A int }{{A: 1}}, wantErr: ` of type \[\]struct \{ A int \}`},
		{name: "a valuer answering a pointer", argument: pointerValuer{}, wantErr: ` of type \*int64`},
		{name: "a valuer answering a valuer", argument: nestedValuer{}, wantErr: ` of type statementargs_test.textValuer`},
		{name: "a struct named like a null type", argument: NullCode{Code: "CZ", Valid: true}, wantErr: ` of type statementargs_test.NullCode`},
		{name: "a map", argument: map[string]int{"a": 1}, wantErr: ` of type map\[string\]int`},
		{name: "a struct", argument: struct{ A int }{A: 1}, wantErr: ` of type struct \{ A int \}`},
		{name: "a complex number", argument: complex(1, 2), wantErr: ` of type complex128`},
		{name: "a uintptr", argument: uintptr(1), wantErr: ` of type uintptr`},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			got, err := statementargs.Typed(test.argument)
			c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
			c.Assert(err, qt.ErrorMatches, `unsupported feature: the experimental provider contract does not support a statement argument`+test.wantErr)
			c.Assert(got, qt.IsNil)
		})
	}
}

// A Valuer that cannot answer stops the argument with its own error.
func TestTyped_ValuerFailure(t *testing.T) {
	c := qt.New(t)
	got, err := statementargs.Typed(failingValuer{})
	c.Assert(err, qt.ErrorIs, errValue)
	c.Assert(got, qt.IsNil)
}

// Typed arguments take their positions by the rules plain ones do.
func TestTypedArguments_HappyPath(t *testing.T) {
	first, second := scalar(protocol.ArgumentType_INT32, signed(1)), list(protocol.ArgumentType_STRING, false, text("CZ"))
	tests := []struct {
		name      string
		style     descriptor.Placeholder
		arguments []driver.NamedValue
		want      []*protocol.StatementArgument
	}{
		{name: "positional", style: descriptor.PlaceholderQuestion,
			arguments: []driver.NamedValue{{Ordinal: 1, Value: first}, {Ordinal: 2, Value: second}}, want: []*protocol.StatementArgument{first, second}},
		{name: "named out of order", style: descriptor.PlaceholderDollarNamed,
			arguments: []driver.NamedValue{{Ordinal: 1, Name: "p2", Value: second}, {Ordinal: 2, Name: "p1", Value: first}},
			want:      []*protocol.StatementArgument{first, second}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			got, err := statementargs.TypedArguments(test.style, test.arguments)
			c.Assert(err, qt.IsNil)
			c.Assert(got, qt.CmpEquals(protocmp.Transform()), test.want)
		})
	}
}

// A typed argument is placed or refused as a plain one is, and a value the
// handle's check did not give the typed form never reached Typed.
func TestTypedArguments_FailurePath(t *testing.T) {
	typed := scalar(protocol.ArgumentType_INT32, signed(1))
	tests := []struct {
		name      string
		style     descriptor.Placeholder
		arguments []driver.NamedValue
		wantErr   string
	}{
		{name: "a gap", style: descriptor.PlaceholderDollarNamed,
			arguments: []driver.NamedValue{{Ordinal: 1, Name: "p1", Value: typed}, {Ordinal: 2, Name: "p3", Value: typed}},
			wantErr:   `unsupported feature: the experimental provider contract does not support the named argument "p3" without one named "p2", since it carries arguments by position`},
		{name: "a name under question", style: descriptor.PlaceholderQuestion, arguments: []driver.NamedValue{{Ordinal: 1, Name: "p1", Value: typed}},
			wantErr: `unsupported feature: the experimental provider contract does not support the named argument "p1", since the engine's placeholder style "question" binds arguments by position`},
		{name: "a plain wire form", style: descriptor.PlaceholderQuestion, arguments: []driver.NamedValue{{Ordinal: 1, Value: signed(1)}},
			wantErr: `statement argument 1 of type \*protocol.DataValue reached the provider handle unchecked`},
		{name: "no typed form", style: descriptor.PlaceholderQuestion, arguments: []driver.NamedValue{{Ordinal: 1, Value: (*protocol.StatementArgument)(nil)}},
			wantErr: `statement argument 1 of type \*protocol.StatementArgument reached the provider handle unchecked`},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			got, err := statementargs.TypedArguments(test.style, test.arguments)
			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(got, qt.IsNil)
		})
	}
}

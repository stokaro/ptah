// Package statementargs gives an argument of a statement the operator wrote
// the wire form an executable provider binds, read from the Go type the caller
// passed. The provider host's database/sql statement handle calls it before
// database/sql converts the argument to a driver value. That conversion widens
// every integer to int64 and a float32 to float64, replaces a pointer with its
// target and an unsigned integer with a signed one, and leaves a provider no
// way to bind the type the caller chose. An argument whose type the contract
// cannot carry exactly is refused instead of converted.
package statementargs

import (
	"database/sql/driver"
	"fmt"
	"reflect"
	"slices"
	"strings"
	"time"

	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"

	"ptah.run/core/ptaherr"
)

var valuerType = reflect.TypeFor[driver.Valuer]()

// Wire returns the wire form of a statement argument, read from its Go type:
//
//   - nil is null;
//   - a bool is a bool, a string a string, a float64 a double, and a byte
//     slice bytes;
//   - an int or an int64 is a signed 64-bit integer, and a uint or a uint64 an
//     unsigned one, so an unsigned value above the int64 range keeps its value;
//   - a time.Time is a timestamp, and a time.Duration an interval of its
//     nanoseconds;
//   - a named type is its underlying kind, so a type declared over uint is an
//     unsigned 64-bit integer;
//   - a driver.Valuer is the value its Value method returns, read by the same
//     rules.
//
// The contract has no integer narrower than 64 bits, no float32, no type that
// admits null and no list. So an int8, int16, int32, uint8, uint16 or uint32, a
// float32, a pointer, a database/sql Null value, and a slice or an array of
// anything but bytes are refused with an error wrapping
// ptaherr.ErrUnsupportedFeature: sent, each would be bound as another type. A
// pointer whose own method is Value, rather than its target's, is a
// driver.Valuer.
func Wire(argument any) (*protocol.DataValue, error) {
	kind := reflect.TypeOf(argument)
	switch {
	case kind == nil:
		return null(), nil
	case nullable(kind):
		return nil, refuse(argument, "it cannot say that a type admits null")
	case kind.Implements(valuerType):
		value, err := argument.(driver.Valuer).Value()
		if err != nil {
			return nil, err
		}
		return plain(value)
	default:
		return plain(argument)
	}
}

// nullable reports a type that adds null to a driver.Valuer, which the Valuer
// check would otherwise read as the value it holds: a database/sql Null type,
// whose Value method answers that value at another width or none, and a
// pointer to a type with a Value method. plain refuses every other pointer.
func nullable(kind reflect.Type) bool {
	if kind.Kind() == reflect.Pointer {
		return kind.Elem().Implements(valuerType)
	}
	return kind.PkgPath() == "database/sql" && strings.HasPrefix(kind.Name(), "Null")
}

// plain is the wire form of a value that is not a driver.Valuer.
func plain(argument any) (*protocol.DataValue, error) {
	switch typed := argument.(type) {
	case nil:
		return null(), nil
	case time.Time:
		return &protocol.DataValue{Value: &protocol.DataValue_TimestampValue{TimestampValue: &protocol.TimestampValue{
			Seconds: typed.Unix(), Nanos: int32(typed.Nanosecond())}}}, nil // #nosec G115 -- Nanosecond is in [0, 1e9)
	case time.Duration:
		return &protocol.DataValue{Value: &protocol.DataValue_IntervalValue{IntervalValue: &protocol.IntervalValue{
			Nanoseconds: int64(typed)}}}, nil
	}
	value := reflect.ValueOf(argument)
	switch value.Kind() {
	case reflect.Bool:
		return &protocol.DataValue{Value: &protocol.DataValue_BoolValue{BoolValue: value.Bool()}}, nil
	case reflect.Int, reflect.Int64:
		return &protocol.DataValue{Value: &protocol.DataValue_Int64Value{Int64Value: value.Int()}}, nil
	case reflect.Uint, reflect.Uint64:
		return &protocol.DataValue{Value: &protocol.DataValue_Uint64Value{Uint64Value: value.Uint()}}, nil
	case reflect.Float64:
		return &protocol.DataValue{Value: &protocol.DataValue_DoubleValue{DoubleValue: value.Float()}}, nil
	case reflect.String:
		return &protocol.DataValue{Value: &protocol.DataValue_StringValue{StringValue: value.String()}}, nil
	case reflect.Int8, reflect.Int16, reflect.Int32, reflect.Uint8, reflect.Uint16, reflect.Uint32:
		return nil, refuse(argument, "it carries an integer only as int64 or uint64")
	case reflect.Float32:
		return nil, refuse(argument, "it carries a floating-point number only as float64")
	case reflect.Pointer:
		return nil, refuse(argument, "it cannot say that a type admits null")
	case reflect.Slice, reflect.Array:
		switch {
		case value.Type().Elem().Kind() != reflect.Uint8:
			return nil, refuse(argument, "it carries no list")
		case value.Kind() == reflect.Slice:
			return &protocol.DataValue{Value: &protocol.DataValue_BytesValue{BytesValue: slices.Clone(value.Bytes())}}, nil
		}
	}
	// A byte array, a map, a struct, a channel, a function or a complex
	// number: no case of the contract holds one.
	return nil, fmt.Errorf("%w: the experimental provider contract does not support a statement argument of type %T",
		ptaherr.ErrUnsupportedFeature, argument)
}

func null() *protocol.DataValue {
	return &protocol.DataValue{Value: &protocol.DataValue_Null{Null: &protocol.Empty{}}}
}

// refuse says which argument type the contract cannot carry, and why.
func refuse(argument any, reason string) error {
	return fmt.Errorf("%w: the experimental provider contract does not support a statement argument of type %T, since %s",
		ptaherr.ErrUnsupportedFeature, argument, reason)
}

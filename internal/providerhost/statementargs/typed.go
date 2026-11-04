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

var (
	timeType     = reflect.TypeFor[time.Time]()
	durationType = reflect.TypeFor[time.Duration]()
)

// Typed returns the typed wire form of a statement argument for a provider
// that negotiated typed arguments, read from its Go type: the scalar it binds
// at, whether the type admits null, and whether it is a list.
//
//   - nil is a null of no type, UNTYPED;
//   - a bool, a string and a byte slice are BOOL, STRING and BYTES;
//   - an integer is the scalar of its width and sign, an int INT64 and a uint
//     UINT64, and a float32 and a float64 are FLOAT32 and FLOAT64;
//   - a time.Time is a TIMESTAMP, and a time.Duration an INTERVAL of its
//     nanoseconds;
//   - a named type is its underlying kind;
//   - a pointer to a scalar and a database/sql Null value are optional: nil,
//     or a Null value that is not valid, is a null of the scalar;
//   - a slice other than a byte slice is a list of its element's scalar, and
//     its elements are optional when they are pointers or Null values;
//   - a driver.Valuer is the value its Value method returns, read by the same
//     rules for a scalar; a pointer whose own method is Value, rather than its
//     target's, is a driver.Valuer.
//
// What the typed form cannot carry exactly is refused with an error wrapping
// ptaherr.ErrUnsupportedFeature: an array, a nested list, a pointer to a
// pointer or to a Null value, a pointer to a list, a pointer to a type with a
// Value method, a list whose elements have one, and a map, a struct, a
// channel, a function or a complex number.
func Typed(argument any) (*protocol.StatementArgument, error) {
	if argument == nil {
		return &protocol.StatementArgument{Type: &protocol.ArgumentType{Scalar: protocol.ArgumentType_UNTYPED}, Value: null()}, nil
	}
	kind, value := reflect.TypeOf(argument), reflect.ValueOf(argument)
	switch {
	case nullType(kind):
		return typedNull(argument, value)
	case kind.Kind() == reflect.Pointer && !kind.Elem().Implements(valuerType) && kind.Implements(valuerType):
		// The pointer's own method is Value, so it decides what is bound.
		return typedAnswer(argument)
	case kind.Kind() == reflect.Pointer:
		return typedPointer(argument, value)
	case kind.Implements(valuerType):
		return typedAnswer(argument)
	case kind.Kind() == reflect.Slice && kind.Elem().Kind() != reflect.Uint8:
		return typedList(argument, value)
	case kind.Kind() == reflect.Array:
		return nil, refuseTyped(argument, "it carries no array")
	}
	scalar, ok := scalarOf(kind)
	if !ok {
		return nil, refuseTyped(argument, "")
	}
	return &protocol.StatementArgument{Type: &protocol.ArgumentType{Scalar: scalar}, Value: scalarValue(scalar, value)}, nil
}

// valuedNull is why a type that admits null cannot hold a type with a Value
// method: a null of it has no scalar, since only the method says one.
const valuedNull = "a null of a type whose Value method decides what is bound has no type"

// nullType reports a database/sql Null type, which adds null to the value of
// its first field.
func nullType(kind reflect.Type) bool {
	return kind.Kind() == reflect.Struct && kind.PkgPath() == "database/sql" && strings.HasPrefix(kind.Name(), "Null") && kind.NumField() == 2
}

// typedNull is a database/sql Null value: optional, and null unless valid.
func typedNull(argument any, value reflect.Value) (*protocol.StatementArgument, error) {
	field := value.Type().Field(0).Type
	switch {
	case field.Implements(valuerType):
		return nil, refuseTyped(argument, valuedNull)
	case field.Kind() == reflect.Pointer || nullType(field):
		return nil, refuseTyped(argument, "it carries null at one level only")
	}
	scalar, ok := scalarOf(field)
	if !ok {
		return nil, refuseTyped(argument, "")
	}
	kind := &protocol.ArgumentType{Scalar: scalar, Optional: true}
	if !value.FieldByName("Valid").Bool() {
		return &protocol.StatementArgument{Type: kind, Value: null()}, nil
	}
	return &protocol.StatementArgument{Type: kind, Value: scalarValue(scalar, value.Field(0))}, nil
}

// typedPointer is a pointer to a scalar: optional, and null when nil.
func typedPointer(argument any, value reflect.Value) (*protocol.StatementArgument, error) {
	target := value.Type().Elem()
	switch {
	case target.Implements(valuerType):
		return nil, refuseTyped(argument, valuedNull)
	case target.Kind() == reflect.Pointer || nullType(target):
		return nil, refuseTyped(argument, "it carries null at one level only")
	case target.Kind() == reflect.Slice && target.Elem().Kind() != reflect.Uint8:
		return nil, refuseTyped(argument, "it carries no list that admits null")
	}
	scalar, ok := scalarOf(target)
	if !ok {
		return nil, refuseTyped(argument, "")
	}
	kind := &protocol.ArgumentType{Scalar: scalar, Optional: true}
	if value.IsNil() {
		return &protocol.StatementArgument{Type: kind, Value: null()}, nil
	}
	return &protocol.StatementArgument{Type: kind, Value: scalarValue(scalar, value.Elem())}, nil
}

// typedAnswer is the scalar a driver.Valuer answers. A Valuer that answers nil
// binds a null of no type, since nothing else says one.
func typedAnswer(argument any) (*protocol.StatementArgument, error) {
	answer, err := argument.(driver.Valuer).Value()
	if err != nil {
		return nil, err
	}
	if answer == nil {
		return &protocol.StatementArgument{Type: &protocol.ArgumentType{Scalar: protocol.ArgumentType_UNTYPED}, Value: null()}, nil
	}
	scalar, ok := scalarOf(reflect.TypeOf(answer))
	if !ok {
		return nil, refuseTyped(answer, "")
	}
	return &protocol.StatementArgument{Type: &protocol.ArgumentType{Scalar: scalar}, Value: scalarValue(scalar, reflect.ValueOf(answer))}, nil
}

// typedList is a slice other than a byte slice: a list of the scalar of its
// element, whose elements are optional when they are pointers or Null values.
// A nil slice is an empty list.
func typedList(argument any, value reflect.Value) (*protocol.StatementArgument, error) {
	element := value.Type().Elem()
	held, optional := element, false
	switch {
	case nullType(element):
		held, optional = element.Field(0).Type, true
	case element.Kind() == reflect.Pointer && !element.Implements(valuerType):
		held, optional = element.Elem(), true
	}
	switch {
	case held.Implements(valuerType):
		return nil, refuseTyped(argument, "the Value method of its elements decides what each binds")
	case optional && (held.Kind() == reflect.Pointer || nullType(held)):
		return nil, refuseTyped(argument, "it carries null at one level only")
	case held.Kind() == reflect.Array || held.Kind() == reflect.Slice && held.Elem().Kind() != reflect.Uint8:
		return nil, refuseTyped(argument, "it carries no nested list")
	}
	scalar, ok := scalarOf(held)
	if !ok {
		return nil, refuseTyped(argument, "")
	}
	elements := make([]*protocol.DataValue, value.Len())
	for i := range elements {
		elements[i] = elementValue(scalar, value.Index(i))
	}
	return &protocol.StatementArgument{Type: &protocol.ArgumentType{Scalar: scalar, Optional: optional, List: true}, Elements: elements}, nil
}

// elementValue is one element of a list: a nil pointer and a Null value that
// is not valid are null.
func elementValue(scalar protocol.ArgumentType_Scalar, element reflect.Value) *protocol.DataValue {
	switch {
	case nullType(element.Type()):
		if !element.FieldByName("Valid").Bool() {
			return null()
		}
		return scalarValue(scalar, element.Field(0))
	case element.Kind() == reflect.Pointer:
		if element.IsNil() {
			return null()
		}
		return scalarValue(scalar, element.Elem())
	default:
		return scalarValue(scalar, element)
	}
}

// scalarOf is the scalar a Go type binds at, and whether it has one. A type
// with a Value method has none of its own: its method decides.
func scalarOf(kind reflect.Type) (protocol.ArgumentType_Scalar, bool) {
	switch {
	case kind.Implements(valuerType):
		return protocol.ArgumentType_SCALAR_UNSPECIFIED, false
	case kind == timeType:
		return protocol.ArgumentType_TIMESTAMP, true
	case kind == durationType:
		return protocol.ArgumentType_INTERVAL, true
	case kind.Kind() == reflect.Slice:
		return protocol.ArgumentType_BYTES, kind.Elem().Kind() == reflect.Uint8
	}
	scalar, ok := kindScalars[kind.Kind()]
	return scalar, ok
}

// kindScalars is the scalar of each Go kind that names one by itself.
var kindScalars = map[reflect.Kind]protocol.ArgumentType_Scalar{
	reflect.Bool: protocol.ArgumentType_BOOL,
	reflect.Int8: protocol.ArgumentType_INT8, reflect.Int16: protocol.ArgumentType_INT16, reflect.Int32: protocol.ArgumentType_INT32,
	reflect.Int64: protocol.ArgumentType_INT64, reflect.Int: protocol.ArgumentType_INT64,
	reflect.Uint8: protocol.ArgumentType_UINT8, reflect.Uint16: protocol.ArgumentType_UINT16, reflect.Uint32: protocol.ArgumentType_UINT32,
	reflect.Uint64: protocol.ArgumentType_UINT64, reflect.Uint: protocol.ArgumentType_UINT64,
	reflect.Float32: protocol.ArgumentType_FLOAT32, reflect.Float64: protocol.ArgumentType_FLOAT64,
	reflect.String: protocol.ArgumentType_STRING,
}

// scalarValue is the wire value of a Go value whose type binds at scalar, in
// the DataValue case the scalar names.
func scalarValue(scalar protocol.ArgumentType_Scalar, value reflect.Value) *protocol.DataValue {
	switch scalar {
	case protocol.ArgumentType_BOOL:
		return &protocol.DataValue{Value: &protocol.DataValue_BoolValue{BoolValue: value.Bool()}}
	case protocol.ArgumentType_UINT8, protocol.ArgumentType_UINT16, protocol.ArgumentType_UINT32, protocol.ArgumentType_UINT64:
		return &protocol.DataValue{Value: &protocol.DataValue_Uint64Value{Uint64Value: value.Uint()}}
	case protocol.ArgumentType_FLOAT32, protocol.ArgumentType_FLOAT64:
		return &protocol.DataValue{Value: &protocol.DataValue_DoubleValue{DoubleValue: value.Float()}}
	case protocol.ArgumentType_STRING:
		return &protocol.DataValue{Value: &protocol.DataValue_StringValue{StringValue: value.String()}}
	case protocol.ArgumentType_BYTES:
		return &protocol.DataValue{Value: &protocol.DataValue_BytesValue{BytesValue: slices.Clone(value.Bytes())}}
	case protocol.ArgumentType_TIMESTAMP:
		// scalarOf gives TIMESTAMP to time.Time alone.
		moment, _ := reflect.TypeAssert[time.Time](value)
		return &protocol.DataValue{Value: &protocol.DataValue_TimestampValue{TimestampValue: &protocol.TimestampValue{
			Seconds: moment.Unix(), Nanos: int32(moment.Nanosecond())}}} // #nosec G115 -- Nanosecond is in [0, 1e9)
	case protocol.ArgumentType_INTERVAL:
		return &protocol.DataValue{Value: &protocol.DataValue_IntervalValue{IntervalValue: &protocol.IntervalValue{Nanoseconds: value.Int()}}}
	default:
		return &protocol.DataValue{Value: &protocol.DataValue_Int64Value{Int64Value: value.Int()}}
	}
}

// refuseTyped says which argument type the typed form cannot carry, and why
// when a reason is given.
func refuseTyped(argument any, reason string) error {
	if reason == "" {
		return fmt.Errorf("%w: the experimental provider contract does not support a statement argument of type %T", ptaherr.ErrUnsupportedFeature, argument)
	}
	return refuse(argument, reason)
}

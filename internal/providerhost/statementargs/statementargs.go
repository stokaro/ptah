// Package statementargs gives an argument of a statement the operator wrote
// the wire form an executable provider binds, read from the Go type the caller
// passed, and puts the arguments of a statement at the positions the provider
// binds them in. The provider host's database/sql statement handle calls Wire,
// or Typed for a provider that negotiated typed arguments, before database/sql
// converts the argument to a driver value. That conversion widens every
// integer to int64 and a float32 to float64, replaces a pointer with its
// target and an unsigned integer with a signed one, and leaves a provider no
// way to bind the type the caller chose. An argument whose type the wire form
// cannot carry exactly is refused instead of converted. The contract carries
// arguments by position alone, so Arguments and TypedArguments read the
// position of a named argument from the placeholder style the provider's
// engine declares, and refuse a name that gives none.
package statementargs

import (
	"database/sql/driver"
	"fmt"
	"math"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"time"

	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"

	"ptah.run/core/platform/descriptor"
	"ptah.run/core/ptaherr"
)

var valuerType = reflect.TypeFor[driver.Valuer]()

// Wire returns the wire form of a statement argument for a provider that did
// not negotiate typed arguments, read from its Go type:
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

// Arguments returns the wire forms of a statement's arguments at the positions
// a provider binds them in, for an engine whose query dialect declares the
// placeholder style style; an empty style is an engine that declares none.
// Each argument holds the wire form Wire gave it.
//
// Positional arguments keep the order of the call. A named argument is read
// under the dollar-named style alone, the style whose Nth parameter is $pN and
// the one the query builder binds by name: the argument named pN takes
// position N, wherever the call put it, so the provider binds it to the
// parameter its name says. The names of N arguments have to be p1 to pN, each
// once, with the number in decimal and without a leading zero.
//
// What the positions cannot say exactly is refused with an error wrapping
// ptaherr.ErrUnsupportedFeature: a named argument under any other style or
// none, a name that is not a parameter of the style, two arguments with one
// name, a name whose position leaves a lower position without an argument,
// and named and positional arguments in one statement.
func Arguments(style descriptor.Placeholder, arguments []driver.NamedValue) ([]*protocol.DataValue, error) {
	return place[*protocol.DataValue](style, arguments)
}

// TypedArguments is Arguments for a provider that negotiated typed arguments:
// each argument holds the typed form Typed gave it, and a named argument takes
// its position by the same rules.
func TypedArguments(style descriptor.Placeholder, arguments []driver.NamedValue) ([]*protocol.StatementArgument, error) {
	return place[*protocol.StatementArgument](style, arguments)
}

// place puts the arguments of a statement, each holding the form V that the
// handle's check gave it, at the positions the provider binds them in.
func place[V *protocol.DataValue | *protocol.StatementArgument](style descriptor.Placeholder, arguments []driver.NamedValue) ([]V, error) {
	values := make([]V, 0, len(arguments))
	for _, argument := range arguments {
		value, checked := argument.Value.(V)
		if !checked || value == nil {
			return nil, fmt.Errorf("statement argument %d of type %T reached the provider handle unchecked", argument.Ordinal, argument.Value)
		}
		values = append(values, value)
	}
	if !slices.ContainsFunc(arguments, func(argument driver.NamedValue) bool { return argument.Name != "" }) {
		return values, nil
	}
	return byName(style, arguments, values)
}

// byName puts the wire form of each named argument at the position its name
// gives. An argument whose position is past the count of the arguments
// leaves a lower position without one, and the lowest such is the one the
// refusal names.
func byName[V *protocol.DataValue | *protocol.StatementArgument](style descriptor.Placeholder, arguments []driver.NamedValue, values []V) ([]V, error) {
	placed := make([]V, len(values))
	past, pastPosition := "", math.MaxInt
	for i, argument := range arguments {
		if argument.Name == "" {
			return nil, fmt.Errorf("%w: the experimental provider contract does not support named and positional arguments in one statement",
				ptaherr.ErrUnsupportedFeature)
		}
		position, err := namedPosition(style, argument.Name)
		if err != nil {
			return nil, err
		}
		switch {
		case position > len(placed):
			if position <= pastPosition {
				past, pastPosition = argument.Name, position
			}
		case placed[position-1] != nil:
			return nil, refuseName(argument.Name, " twice")
		default:
			placed[position-1] = values[i]
		}
	}
	if missing := slices.IndexFunc(placed, func(value V) bool { return value == nil }); missing >= 0 {
		return nil, refuseName(past, fmt.Sprintf(" without one named %q, since it carries arguments by position", "p"+strconv.Itoa(missing+1)))
	}
	return placed, nil
}

// namedPosition is the position, counted from one, that a named argument takes
// under style. The number of a name too long for an int is past every
// statement's arguments.
func namedPosition(style descriptor.Placeholder, name string) (int, error) {
	switch {
	case style == "":
		return 0, refuseName(name, ", since the engine declares no placeholder style that names a parameter")
	case style != descriptor.PlaceholderDollarNamed:
		return 0, refuseName(name, fmt.Sprintf(", since the engine's placeholder style %q binds arguments by position", style))
	}
	digits, ok := strings.CutPrefix(name, "p")
	if !ok || digits == "" || digits[0] == '0' || strings.ContainsFunc(digits, func(r rune) bool { return r < '0' || r > '9' }) {
		return 0, refuseName(name, fmt.Sprintf(", since the engine's placeholder style %q gives a position only to the names p1, p2 and so on", style))
	}
	position, err := strconv.Atoi(digits)
	if err != nil {
		position = math.MaxInt
	}
	return position, nil
}

// refuseName says which named argument the contract cannot place, and why.
func refuseName(name, reason string) error {
	return fmt.Errorf("%w: the experimental provider contract does not support the named argument %q%s",
		ptaherr.ErrUnsupportedFeature, name, reason)
}

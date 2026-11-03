package provider_test

import (
	"math"
	"regexp"
	"slices"
	"testing"

	qt "github.com/frankban/quicktest"
	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
	"github.com/stokaro/ptah/provider-sdk/provider"
	"google.golang.org/protobuf/proto"
)

func argumentType(scalar protocol.ArgumentType_Scalar, optional, list bool) *protocol.ArgumentType {
	return &protocol.ArgumentType{Scalar: scalar, Optional: optional, List: list}
}

func scalarArgument(scalar protocol.ArgumentType_Scalar, value *protocol.DataValue) *protocol.StatementArgument {
	return &protocol.StatementArgument{Type: argumentType(scalar, false, false), Value: value}
}

func signed(value int64) *protocol.DataValue {
	return &protocol.DataValue{Value: &protocol.DataValue_Int64Value{Int64Value: value}}
}

func unsigned(value uint64) *protocol.DataValue {
	return &protocol.DataValue{Value: &protocol.DataValue_Uint64Value{Uint64Value: value}}
}

func double(value float64) *protocol.DataValue {
	return &protocol.DataValue{Value: &protocol.DataValue_DoubleValue{DoubleValue: value}}
}

// withoutFeature returns features without feature, in a new slice.
func withoutFeature(features []string, feature string) []string {
	return slices.DeleteFunc(slices.Clone(features), func(listed string) bool { return listed == feature })
}

func typedStatementRequest(arguments ...*protocol.StatementArgument) *protocol.RunStatementRequest {
	request := statementRequest()
	request.Arguments, request.TypedArguments = nil, arguments
	return request
}

// typedArgumentsDescription serves operator statements with typed arguments.
func typedArgumentsDescription() *protocol.Description {
	d := description()
	d.Features = append(d.Features, provider.StatementsFeature, provider.TypedArgumentsFeature)
	d.Engines[0].Features = []string{provider.StatementsFeature}
	return d
}

// Every scalar holds its own case at the edges of its range, an optional type
// and an untyped argument hold null, and a list holds its elements, none of
// them at all, or nulls when its elements admit null.
func TestTypedStatementArguments_HappyPath(t *testing.T) {
	c := qt.New(t)
	when := &protocol.DataValue{Value: &protocol.DataValue_TimestampValue{TimestampValue: &protocol.TimestampValue{Seconds: 1, Nanos: 999_999_999}}}
	every := &protocol.DataValue{Value: &protocol.DataValue_IntervalValue{IntervalValue: &protocol.IntervalValue{Months: 1, Days: 2, Nanoseconds: 3}}}
	request := typedStatementRequest(
		&protocol.StatementArgument{Type: argumentType(protocol.ArgumentType_UNTYPED, false, false), Value: null()},
		scalarArgument(protocol.ArgumentType_BOOL, &protocol.DataValue{Value: &protocol.DataValue_BoolValue{BoolValue: true}}),
		scalarArgument(protocol.ArgumentType_INT8, signed(math.MinInt8)), scalarArgument(protocol.ArgumentType_INT8, signed(math.MaxInt8)),
		scalarArgument(protocol.ArgumentType_INT16, signed(math.MinInt16)), scalarArgument(protocol.ArgumentType_INT16, signed(math.MaxInt16)),
		scalarArgument(protocol.ArgumentType_INT32, signed(math.MinInt32)), scalarArgument(protocol.ArgumentType_INT32, signed(math.MaxInt32)),
		scalarArgument(protocol.ArgumentType_INT64, signed(math.MinInt64)), scalarArgument(protocol.ArgumentType_INT64, signed(math.MaxInt64)),
		scalarArgument(protocol.ArgumentType_UINT8, unsigned(math.MaxUint8)), scalarArgument(protocol.ArgumentType_UINT16, unsigned(math.MaxUint16)),
		scalarArgument(protocol.ArgumentType_UINT32, unsigned(math.MaxUint32)), scalarArgument(protocol.ArgumentType_UINT64, unsigned(math.MaxUint64)),
		scalarArgument(protocol.ArgumentType_FLOAT32, double(float64(float32(0.1)))), scalarArgument(protocol.ArgumentType_FLOAT32, double(math.NaN())),
		scalarArgument(protocol.ArgumentType_FLOAT32, double(math.Inf(-1))), scalarArgument(protocol.ArgumentType_FLOAT64, double(0.1)),
		scalarArgument(protocol.ArgumentType_STRING, text("CZ")),
		scalarArgument(protocol.ArgumentType_BYTES, &protocol.DataValue{Value: &protocol.DataValue_BytesValue{BytesValue: []byte{0}}}),
		scalarArgument(protocol.ArgumentType_TIMESTAMP, when), scalarArgument(protocol.ArgumentType_INTERVAL, every),
		&protocol.StatementArgument{Type: argumentType(protocol.ArgumentType_INT32, true, false), Value: null()},
		&protocol.StatementArgument{Type: argumentType(protocol.ArgumentType_INT32, true, false), Value: signed(7)},
		&protocol.StatementArgument{Type: argumentType(protocol.ArgumentType_UINT64, false, true), Elements: []*protocol.DataValue{unsigned(1), unsigned(math.MaxUint64)}},
		&protocol.StatementArgument{Type: argumentType(protocol.ArgumentType_STRING, false, true)},
		&protocol.StatementArgument{Type: argumentType(protocol.ArgumentType_STRING, true, true), Elements: []*protocol.DataValue{text("CZ"), null()}},
	)
	before := proto.CloneOf(request)
	c.Assert(provider.ValidateRunStatementRequest(request), qt.IsNil)
	c.Assert(provider.ValidateTypedArguments(provider.NegotiatedDescription(typedArgumentsDescription(), provider.TwoWayFeatures()), request), qt.IsNil)
	c.Assert(provider.ValidateStatementResult(request, statementResult()), qt.IsNil)
	c.Assert(proto.Equal(before, request), qt.IsTrue)

	// A request with plain arguments, or none, needs no negotiation.
	unlisted := provider.NegotiatedDescription(typedArgumentsDescription(), nil)
	c.Assert(provider.ValidateTypedArguments(unlisted, statementRequest()), qt.IsNil)
	c.Assert(provider.ValidateTypedArguments(unlisted, typedStatementRequest()), qt.IsNil)
}

// Each rule refuses the argument it names, with a reason a provider author
// can read.
func TestTypedStatementArguments_FailurePath(t *testing.T) {
	for _, test := range []struct {
		name     string
		argument *protocol.StatementArgument
		want     string
	}{
		{"no type", &protocol.StatementArgument{Value: signed(1)}, "needs a known scalar"},
		{"an unspecified scalar", scalarArgument(protocol.ArgumentType_SCALAR_UNSPECIFIED, signed(1)), "needs a known scalar"},
		{"an unknown scalar", scalarArgument(99, signed(1)), "needs a known scalar"},
		{"an optional untyped argument", &protocol.StatementArgument{Type: argumentType(protocol.ArgumentType_UNTYPED, true, false), Value: null()},
			"untyped statement argument is never optional or a list"},
		{"an untyped list", &protocol.StatementArgument{Type: argumentType(protocol.ArgumentType_UNTYPED, false, true)},
			"untyped statement argument is never optional or a list"},
		{"an untyped argument with a value", scalarArgument(protocol.ArgumentType_UNTYPED, signed(1)), "not one its scalar UNTYPED holds"},
		{"elements beside a value", &protocol.StatementArgument{Type: argumentType(protocol.ArgumentType_INT64, false, false), Value: signed(1),
			Elements: []*protocol.DataValue{signed(1)}}, "only a list statement argument has elements"},
		{"a value on a list", &protocol.StatementArgument{Type: argumentType(protocol.ArgumentType_INT64, false, true), Value: signed(1)},
			"list statement argument carries elements and no value"},
		{"an unset value", scalarArgument(protocol.ArgumentType_INT64, nil), "argument's value is unset"},
		{"a value with no case", scalarArgument(protocol.ArgumentType_INT64, &protocol.DataValue{}), "argument's value is unset"},
		{"an unset element", &protocol.StatementArgument{Type: argumentType(protocol.ArgumentType_INT64, true, true),
			Elements: []*protocol.DataValue{signed(1), {}}}, "argument's value is unset"},
		{"a null the type does not admit", scalarArgument(protocol.ArgumentType_INT64, null()), "null only when its type admits null"},
		{"a null element the type does not admit", &protocol.StatementArgument{Type: argumentType(protocol.ArgumentType_INT64, false, true),
			Elements: []*protocol.DataValue{null()}}, "null only when its type admits null"},
		{"an INT8 above its range", scalarArgument(protocol.ArgumentType_INT8, signed(math.MaxInt8+1)), "not one its scalar INT8 holds"},
		{"an INT8 below its range", scalarArgument(protocol.ArgumentType_INT8, signed(math.MinInt8-1)), "not one its scalar INT8 holds"},
		{"an INT16 above its range", scalarArgument(protocol.ArgumentType_INT16, signed(math.MaxInt16+1)), "not one its scalar INT16 holds"},
		{"an INT16 below its range", scalarArgument(protocol.ArgumentType_INT16, signed(math.MinInt16-1)), "not one its scalar INT16 holds"},
		{"an INT32 above its range", scalarArgument(protocol.ArgumentType_INT32, signed(math.MaxInt32+1)), "not one its scalar INT32 holds"},
		{"an INT32 below its range", scalarArgument(protocol.ArgumentType_INT32, signed(math.MinInt32-1)), "not one its scalar INT32 holds"},
		{"a UINT8 above its range", scalarArgument(protocol.ArgumentType_UINT8, unsigned(math.MaxUint8+1)), "not one its scalar UINT8 holds"},
		{"a UINT16 above its range", scalarArgument(protocol.ArgumentType_UINT16, unsigned(math.MaxUint16+1)), "not one its scalar UINT16 holds"},
		{"a UINT32 above its range", scalarArgument(protocol.ArgumentType_UINT32, unsigned(math.MaxUint32+1)), "not one its scalar UINT32 holds"},
		{"an element outside its range", &protocol.StatementArgument{Type: argumentType(protocol.ArgumentType_UINT8, false, true),
			Elements: []*protocol.DataValue{unsigned(1), unsigned(math.MaxUint8 + 1)}}, "not one its scalar UINT8 holds"},
		{"a signed value for an unsigned scalar", scalarArgument(protocol.ArgumentType_UINT64, signed(0)), "not one its scalar UINT64 holds"},
		{"an unsigned value for a signed scalar", scalarArgument(protocol.ArgumentType_INT64, unsigned(0)), "not one its scalar INT64 holds"},
		{"an integer for a float", scalarArgument(protocol.ArgumentType_FLOAT64, signed(1)), "not one its scalar FLOAT64 holds"},
		{"a double for an integer", scalarArgument(protocol.ArgumentType_INT64, double(1)), "not one its scalar INT64 holds"},
		{"a double a float32 does not hold", scalarArgument(protocol.ArgumentType_FLOAT32, double(0.1)), "not one its scalar FLOAT32 holds"},
		{"a double for a string", scalarArgument(protocol.ArgumentType_STRING, double(1)), "not one its scalar STRING holds"},
		{"a bool for an integer", scalarArgument(protocol.ArgumentType_INT8, &protocol.DataValue{Value: &protocol.DataValue_BoolValue{}}),
			"not one its scalar INT8 holds"},
		{"text for a bool", scalarArgument(protocol.ArgumentType_BOOL, text("true")), "not one its scalar BOOL holds"},
		{"bytes for text", scalarArgument(protocol.ArgumentType_STRING, &protocol.DataValue{Value: &protocol.DataValue_BytesValue{}}),
			"not one its scalar STRING holds"},
		{"text for bytes", scalarArgument(protocol.ArgumentType_BYTES, text("CZ")), "not one its scalar BYTES holds"},
		{"an interval for a timestamp", scalarArgument(protocol.ArgumentType_TIMESTAMP,
			&protocol.DataValue{Value: &protocol.DataValue_IntervalValue{IntervalValue: &protocol.IntervalValue{}}}), "not one its scalar TIMESTAMP holds"},
		{"a timestamp for an interval", scalarArgument(protocol.ArgumentType_INTERVAL,
			&protocol.DataValue{Value: &protocol.DataValue_TimestampValue{TimestampValue: &protocol.TimestampValue{}}}), "not one its scalar INTERVAL holds"},
		{"a decimal", scalarArgument(protocol.ArgumentType_STRING, decimal("1", 1, 0)), "not one its scalar STRING holds"},
		{"a date", scalarArgument(protocol.ArgumentType_INT64, &protocol.DataValue{Value: &protocol.DataValue_DateValue{DateValue: 1}}),
			"not one its scalar INT64 holds"},
		{"text that is not UTF-8", scalarArgument(protocol.ArgumentType_STRING, text("\xff")), "a string value is not UTF-8"},
		{"a timestamp out of range", scalarArgument(protocol.ArgumentType_TIMESTAMP,
			&protocol.DataValue{Value: &protocol.DataValue_TimestampValue{TimestampValue: &protocol.TimestampValue{Nanos: 1e9}}}), "nanoseconds are out of range"},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			request := typedStatementRequest(scalarArgument(protocol.ArgumentType_STRING, text("CZ")), test.argument)
			err := provider.ValidateRunStatementRequest(request)
			c.Assert(err, qt.ErrorIs, provider.ErrProtocol)
			c.Assert(err, qt.ErrorMatches, ".*"+regexp.QuoteMeta(test.want)+".*")
			c.Assert(provider.ValidateStatementResult(request, statementResult()), qt.ErrorMatches, ".*"+regexp.QuoteMeta(test.want)+".*")
		})
	}
}

// One request carries one form of arguments.
func TestStatementArgumentForms_FailurePath(t *testing.T) {
	c := qt.New(t)
	request := typedStatementRequest(scalarArgument(protocol.ArgumentType_STRING, text("CZ")))
	request.Arguments = []*protocol.DataValue{text("CZ")}
	err := provider.ValidateRunStatementRequest(request)
	c.Assert(err, qt.ErrorIs, provider.ErrProtocol)
	c.Assert(err, qt.ErrorMatches, `.*a statement carries arguments or typed arguments, never both`)
}

// Typed arguments need the feature in the negotiated description: a provider
// that does not list it, or a host that does not, gets none.
func TestTypedArgumentsNegotiation_FailurePath(t *testing.T) {
	request := typedStatementRequest(scalarArgument(protocol.ArgumentType_INT32, signed(1)))
	unlisted := typedArgumentsDescription()
	unlisted.Features = withoutFeature(unlisted.Features, provider.TypedArgumentsFeature)
	for _, test := range []struct {
		name        string
		description *protocol.Description
	}{
		{"a host that does not list it", provider.NegotiatedDescription(typedArgumentsDescription(), nil)},
		{"a provider that does not list it", provider.NegotiatedDescription(unlisted, provider.TwoWayFeatures())},
		{"no description", nil},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			err := provider.ValidateTypedArguments(test.description, request)
			c.Assert(err, qt.ErrorIs, provider.ErrProtocol)
			c.Assert(err, qt.ErrorMatches, `.*typed statement arguments were not negotiated`)
		})
	}
}

// The feature is two-way, and a provider lists it beside the statements
// service it extends.
func TestTypedArgumentsFeature_HappyPath(t *testing.T) {
	c := qt.New(t)
	c.Assert(provider.TwoWayFeatures(), qt.Contains, provider.TypedArgumentsFeature)
	c.Assert(provider.ValidateDescription(typedArgumentsDescription(), nil), qt.IsNil)
	listed := provider.NegotiatedDescription(typedArgumentsDescription(), []string{provider.TypedArgumentsFeature})
	c.Assert(listed.Features, qt.Contains, provider.TypedArgumentsFeature)
	unlisted := provider.NegotiatedDescription(typedArgumentsDescription(), nil)
	c.Assert(unlisted.Features, qt.Not(qt.Contains), provider.TypedArgumentsFeature)
	c.Assert(unlisted.Features, qt.Contains, provider.StatementsFeature)
}

func TestTypedArgumentsFeature_FailurePath(t *testing.T) {
	c := qt.New(t)
	d := typedArgumentsDescription()
	d.Features = withoutFeature(d.Features, provider.StatementsFeature)
	d.Engines[0].Features = nil
	c.Assert(provider.ValidateDescription(d, nil), qt.ErrorMatches, `.*sessions\.typed-arguments requires sessions\.statements`)
}

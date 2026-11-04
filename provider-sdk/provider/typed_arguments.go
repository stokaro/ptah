package provider

import (
	"fmt"
	"math"
	"slices"

	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
)

// TypedArgumentsFeature lets a statement carry each argument with the type its
// caller bound it at, in RunStatementRequest.typed_arguments. It is two-way: a
// provider lists it beside StatementsFeature, the host lists it in its host
// features, and a request carries typed arguments only when the negotiated
// description lists it. A request names no engine, so the feature is the
// provider's alone: a provider that lists it reads typed arguments on every
// session it opens.
const TypedArgumentsFeature = "sessions.typed-arguments"

// ValidateTypedArguments refuses typed arguments on a statement whose
// negotiated description does not list TypedArgumentsFeature: a provider built
// before the field would drop it and run the statement without its arguments.
// A request without typed arguments is valid here.
func ValidateTypedArguments(description *protocol.Description, request *protocol.RunStatementRequest) error {
	if len(request.GetTypedArguments()) == 0 || slices.Contains(description.GetFeatures(), TypedArgumentsFeature) {
		return nil
	}
	return statementError("typed statement arguments were not negotiated")
}

// validateTypedArgument checks one typed argument against its type.
func validateTypedArgument(argument *protocol.StatementArgument) error {
	kind := argument.GetType()
	scalar := kind.GetScalar()
	if _, known := protocol.ArgumentType_Scalar_name[int32(scalar)]; !known || scalar == protocol.ArgumentType_SCALAR_UNSPECIFIED {
		return statementError("a typed statement argument needs a known scalar")
	}
	if scalar == protocol.ArgumentType_UNTYPED && (kind.GetOptional() || kind.GetList()) {
		return statementError("an untyped statement argument is never optional or a list")
	}
	if !kind.GetList() {
		if len(argument.GetElements()) != 0 {
			return statementError("only a list statement argument has elements")
		}
		return validateArgumentValue(kind, argument.GetValue())
	}
	if argument.GetValue() != nil {
		return statementError("a list statement argument carries elements and no value")
	}
	for _, element := range argument.GetElements() {
		if err := validateArgumentValue(kind, element); err != nil {
			return err
		}
	}
	return nil
}

// validateArgumentValue checks one value of a typed argument: set, null only
// where its type admits null or is UNTYPED, and otherwise a valid value its
// scalar holds.
func validateArgumentValue(kind *protocol.ArgumentType, value *protocol.DataValue) error {
	if value.GetValue() == nil {
		return statementError("a typed statement argument's value is unset")
	}
	if _, null := value.GetValue().(*protocol.DataValue_Null); null {
		if kind.GetOptional() || kind.GetScalar() == protocol.ArgumentType_UNTYPED {
			return nil
		}
		return statementError("a typed statement argument is null only when its type admits null")
	}
	if !scalarHolds(kind.GetScalar(), value) {
		return statementError(fmt.Sprintf("a typed statement argument's value is not one its scalar %s holds", kind.GetScalar()))
	}
	return ValidateDataValue(value)
}

// scalarHolds reports whether a value that is not null is in the case scalar
// names and within its range. UNTYPED holds only null.
func scalarHolds(scalar protocol.ArgumentType_Scalar, value *protocol.DataValue) bool {
	switch v := value.GetValue().(type) {
	case *protocol.DataValue_BoolValue:
		return scalar == protocol.ArgumentType_BOOL
	case *protocol.DataValue_Int64Value:
		low, high, signed := signedRange(scalar)
		return signed && v.Int64Value >= low && v.Int64Value <= high
	case *protocol.DataValue_Uint64Value:
		high, unsigned := unsignedRange(scalar)
		return unsigned && v.Uint64Value <= high
	case *protocol.DataValue_DoubleValue:
		narrow := float64(float32(v.DoubleValue))
		return scalar == protocol.ArgumentType_FLOAT64 ||
			scalar == protocol.ArgumentType_FLOAT32 && (narrow == v.DoubleValue || math.IsNaN(v.DoubleValue))
	case *protocol.DataValue_StringValue:
		return scalar == protocol.ArgumentType_STRING
	case *protocol.DataValue_BytesValue:
		return scalar == protocol.ArgumentType_BYTES
	case *protocol.DataValue_TimestampValue:
		return scalar == protocol.ArgumentType_TIMESTAMP
	case *protocol.DataValue_IntervalValue:
		return scalar == protocol.ArgumentType_INTERVAL
	default:
		return false
	}
}

// signedRange is the range of a signed integer scalar, and whether scalar is
// one.
func signedRange(scalar protocol.ArgumentType_Scalar) (low, high int64, signed bool) {
	switch scalar {
	case protocol.ArgumentType_INT8:
		return math.MinInt8, math.MaxInt8, true
	case protocol.ArgumentType_INT16:
		return math.MinInt16, math.MaxInt16, true
	case protocol.ArgumentType_INT32:
		return math.MinInt32, math.MaxInt32, true
	case protocol.ArgumentType_INT64:
		return math.MinInt64, math.MaxInt64, true
	default:
		return 0, 0, false
	}
}

// unsignedRange is the largest value of an unsigned integer scalar, and
// whether scalar is one.
func unsignedRange(scalar protocol.ArgumentType_Scalar) (high uint64, unsigned bool) {
	switch scalar {
	case protocol.ArgumentType_UINT8:
		return math.MaxUint8, true
	case protocol.ArgumentType_UINT16:
		return math.MaxUint16, true
	case protocol.ArgumentType_UINT32:
		return math.MaxUint32, true
	case protocol.ArgumentType_UINT64:
		return math.MaxUint64, true
	default:
		return 0, false
	}
}

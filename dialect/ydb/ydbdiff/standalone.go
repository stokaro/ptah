package ydbdiff

import (
	"encoding/json"
	"fmt"

	"ptah.run/core/schemaext"
)

// Both operands must be explicit. Null establishes absence; an omitted field
// or an incomplete model must never turn into a creation or removal.
func decodeStandaloneOperands[B, A schemaext.Value](data json.RawMessage, family string, codecs []schemaext.Codec) (B, A, error) {
	var before B
	var after A
	fields, err := schemaext.DecodeJSON[map[string]json.RawMessage](data)
	if err != nil {
		return before, after, err
	}
	if len(fields) != 2 || len(fields["before"]) == 0 || len(fields["after"]) == 0 {
		return before, after, fmt.Errorf("%w: %s change requires explicit before and after fields", schemaext.ErrInvalidValue, family)
	}
	for _, codec := range codecs {
		switch codec.Representation {
		case schemaext.Observed:
			before, err = decodeStandaloneOperand[B](codec, fields["before"])
		case schemaext.Desired:
			after, err = decodeStandaloneOperand[A](codec, fields["after"])
		default:
			err = fmt.Errorf("%w: invalid %s operand codec", schemaext.ErrInvalidValue, family)
		}
		if err != nil {
			return before, after, err
		}
	}
	return before, after, nil
}

func decodeStandaloneOperand[T schemaext.Value](codec schemaext.Codec, data json.RawMessage) (T, error) {
	var zero T
	if string(data) == "null" {
		return zero, nil
	}
	value, err := codec.Decode(data)
	if err != nil {
		return zero, err
	}
	typed, ok := value.(T)
	if !ok {
		return zero, fmt.Errorf("%w: unexpected standalone operand %T", schemaext.ErrInvalidValue, value)
	}
	return typed, nil
}

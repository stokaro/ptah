package ydbdiff

import (
	"encoding/json"
	"fmt"

	"ptah.run/core/schemaext"
)

type workloadChange interface {
	schemaext.ChangeValue
	Validate() error
}

func workloadChangeCodec[T workloadChange, B, A schemaext.Value](prototype T, family string, models []schemaext.Codec, build func(B, A) T) schemaext.Codec {
	validated := func(payload schemaext.Payload) (T, error) {
		value, ok := payload.(T)
		if !ok {
			return value, fmt.Errorf("%w: expected %s change, got %T", schemaext.ErrInvalidValue, family, payload)
		}
		if err := value.Validate(); err != nil {
			return value, &schemaext.InvalidModelError{Kind: prototype.Kind(), Representation: schemaext.Change, Message: err.Error()}
		}
		return value, nil
	}
	encode := func(payload schemaext.Payload) (json.RawMessage, error) {
		value, err := validated(payload)
		if err != nil {
			return nil, err
		}
		return json.Marshal(value)
	}
	definition := `{"type":"object","required":["before","after"],"additionalProperties":false,"properties":{"before":{"anyOf":[{"type":"null"},%s]},"after":{"anyOf":[{"type":"null"},%s]}}}`
	return schemaext.Codec{Prototype: prototype, Representation: schemaext.Change, Version: 1,
		Definition: json.RawMessage(fmt.Sprintf(definition, models[1].Definition, models[0].Definition)),
		Encode:     encode, Canonical: encode,
		Clone: func(payload schemaext.Payload) (schemaext.Payload, error) {
			value, err := validated(payload)
			if err != nil {
				return nil, err
			}
			return value.CloneChange(), nil
		},
		Decode: func(data json.RawMessage) (schemaext.Payload, error) {
			before, after, err := decodeStandaloneOperands[B, A](data, family, models)
			if err != nil {
				return nil, err
			}
			value, err := validated(build(before, after))
			if err != nil {
				return nil, err
			}
			return value, nil
		},
	}
}

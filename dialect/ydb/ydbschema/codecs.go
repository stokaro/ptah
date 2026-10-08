package ydbschema

import (
	_ "embed" // Embed the codec definition that identifies this wire format.
	"encoding/json"
	"fmt"
	"slices"

	"ptah.run/core/schemaext"
)

//go:embed changefeed-codecs.json
var changefeedDefinition []byte

// WireDefinition returns an independent full description of the changefeed wire
// model. Operation codecs share this definition with desired/observed values.
func WireDefinition() json.RawMessage { return slices.Clone(changefeedDefinition) }

// Codecs declares the current desired and observed changefeed representations.
// These pure local codecs do not grant target capabilities or inspect a server.
func Codecs() []schemaext.Codec {
	return []schemaext.Codec{
		valueCodec(&DesiredChangefeed{}, schemaext.Desired, func(data json.RawMessage) (schemaext.Payload, error) {
			return schemaext.DecodeJSON[*DesiredChangefeed](data)
		}),
		valueCodec(&ObservedChangefeed{}, schemaext.Observed, func(data json.RawMessage) (schemaext.Payload, error) {
			return schemaext.DecodeJSON[*ObservedChangefeed](data)
		}),
	}
}

func valueCodec(prototype schemaext.Value, representation schemaext.Representation, decode func(json.RawMessage) (schemaext.Payload, error)) schemaext.Codec {
	return schemaext.Codec{Prototype: prototype, Representation: representation, Version: 1,
		Definition: WireDefinition(),
		Clone: func(payload schemaext.Payload) (schemaext.Payload, error) {
			value, ok := payload.(schemaext.Value)
			if !ok {
				return nil, fmt.Errorf("%w: expected a changefeed value", schemaext.ErrInvalidValue)
			}
			return schemaext.CloneValue(value)
		},
		Encode: encodeValue,
		Decode: func(data json.RawMessage) (schemaext.Payload, error) {
			value, err := decode(data)
			if err != nil {
				return nil, err
			}
			spec, err := valueSpec(value)
			if err != nil {
				return nil, err
			}
			if err := ValidateChangefeed(spec); err != nil {
				return nil, err
			}
			return value, nil
		},
		Canonical: func(payload schemaext.Payload) (json.RawMessage, error) {
			spec, err := valueSpec(payload)
			if err != nil {
				return nil, err
			}
			if err := ValidateChangefeed(spec); err != nil {
				return nil, err
			}
			spec = spec.Clone()
			CanonicalChangefeed(&spec)
			return marshalValue(payload, spec)
		},
	}
}

func encodeValue(payload schemaext.Payload) (json.RawMessage, error) {
	spec, err := valueSpec(payload)
	if err != nil {
		return nil, err
	}
	if err := ValidateChangefeed(spec); err != nil {
		return nil, err
	}
	return marshalValue(payload, spec)
}

func valueSpec(payload schemaext.Payload) (ChangefeedSpec, error) {
	if err := schemaext.ValidatePayload(payload); err != nil {
		return ChangefeedSpec{}, err
	}
	switch value := payload.(type) {
	case *DesiredChangefeed:
		return value.Spec, value.RetainedReplication.Validate()
	case *ObservedChangefeed:
		return value.Spec, value.Replication.Validate()
	default:
		return ChangefeedSpec{}, fmt.Errorf("%w: expected a changefeed value, got %T", schemaext.ErrInvalidValue, payload)
	}
}

// ChangefeedCoverage records that a source describes changefeeds, including an
// empty namespace. No other kind is enrolled when more codecs join the runtime.
// Callers provide explicit subject limitations for unread or unrepresentable state.
func ChangefeedCoverage(representation schemaext.Representation, subjects []schemaext.SubjectCoverage) (schemaext.Coverage, error) {
	var owned []schemaext.OwnedCodec
	for _, codec := range Codecs() {
		owned = append(owned, schemaext.OwnedCodec{Owner: "ptah.run/ydb", Codec: codec})
	}
	registry, err := schemaext.NewRegistry(owned...)
	if err != nil {
		return schemaext.Coverage{}, err
	}
	for _, model := range registry.Definitions() {
		if model.Kind == ChangefeedKind && model.Representation == representation {
			return schemaext.NewCoverage(representation, []schemaext.KindCoverage{{Model: model, Knowledge: schemaext.Knowledge{State: schemaext.Complete}}}, subjects)
		}
	}
	return schemaext.Coverage{}, fmt.Errorf("%w: changefeed coverage requires a schema representation", schemaext.ErrInvalidValue)
}

// Keep observation ownership and a retained planning binding distinct on the wire.
func marshalValue(payload schemaext.Payload, spec ChangefeedSpec) (json.RawMessage, error) {
	switch value := payload.(type) {
	case *DesiredChangefeed:
		return json.Marshal(&DesiredChangefeed{Spec: spec, RetainedReplication: value.RetainedReplication})
	case *ObservedChangefeed:
		return json.Marshal(&ObservedChangefeed{Spec: spec, Replication: value.Replication})
	default:
		return nil, fmt.Errorf("%w: expected a changefeed value, got %T", schemaext.ErrInvalidValue, payload)
	}
}

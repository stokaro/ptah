package spannerschema

import (
	_ "embed" // Embed the definition that identifies this wire format.
	"encoding/json"
	"fmt"
	"slices"

	"ptah.run/core/schemaext"
)

//go:embed row-deletion-codecs.json
var rowDeletionDefinition []byte

// WireDefinition returns an independent description of the row deletion
// policy wire model. The definition covers both representations.
func WireDefinition() json.RawMessage { return slices.Clone(rowDeletionDefinition) }

// Codecs returns the versioned row deletion policy codecs. Each call returns
// independent definitions. The codecs validate representation invariants; an
// observed interval is not read as a value.
func Codecs() []schemaext.Codec {
	return []schemaext.Codec{
		rowDeletionCodec(&DesiredRowDeletion{}, schemaext.Desired, decodeDesired, desiredPolicy),
		rowDeletionCodec(&ObservedRowDeletion{}, schemaext.Observed, decodeObserved, observedPolicy),
	}
}

func rowDeletionCodec(prototype schemaext.Value, representation schemaext.Representation,
	decode func(json.RawMessage) (schemaext.Payload, error), policy func(schemaext.Payload) (Policy, error),
) schemaext.Codec {
	encode := func(payload schemaext.Payload) (json.RawMessage, error) {
		p, err := policy(payload)
		if err != nil {
			return nil, err
		}
		return json.Marshal(wirePolicy(p))
	}
	return schemaext.Codec{
		Prototype: prototype, Representation: representation, Version: 1, Definition: WireDefinition(),
		Clone: func(payload schemaext.Payload) (schemaext.Payload, error) {
			if _, err := policy(payload); err != nil {
				return nil, err
			}
			value, ok := payload.(schemaext.Value)
			if !ok {
				return nil, fmt.Errorf("%w: expected a Spanner row deletion policy value", schemaext.ErrInvalidValue)
			}
			return schemaext.CloneValue(value)
		},
		Encode: encode, Decode: decode, Canonical: encode,
	}
}

// wirePolicy is the wire object. Its field order is the canonical encoding.
type wirePolicy struct {
	Column   string `json:"column"`
	Interval string `json:"interval"`
}

func desiredPolicy(payload schemaext.Payload) (Policy, error) {
	v, ok := payload.(*DesiredRowDeletion)
	if !ok {
		return Policy{}, fmt.Errorf("%w: expected a desired Spanner row deletion policy, got %T", schemaext.ErrInvalidValue, payload)
	}
	if err := ValidateDesired(v); err != nil {
		return Policy{}, err
	}
	return v.Policy, nil
}

func observedPolicy(payload schemaext.Payload) (Policy, error) {
	v, ok := payload.(*ObservedRowDeletion)
	if !ok {
		return Policy{}, fmt.Errorf("%w: expected an observed Spanner row deletion policy, got %T", schemaext.ErrInvalidValue, payload)
	}
	if err := ValidateObserved(v); err != nil {
		return Policy{}, err
	}
	return v.Policy, nil
}

func decodeDesired(data json.RawMessage) (schemaext.Payload, error) {
	p, err := decodePolicy(data)
	if err != nil {
		return nil, err
	}
	v := &DesiredRowDeletion{Policy: p}
	if err := ValidateDesired(v); err != nil {
		return nil, err
	}
	return v, nil
}

func decodeObserved(data json.RawMessage) (schemaext.Payload, error) {
	p, err := decodePolicy(data)
	if err != nil {
		return nil, err
	}
	v := &ObservedRowDeletion{Policy: p}
	if err := ValidateObserved(v); err != nil {
		return nil, err
	}
	return v, nil
}

// Keys are compared exactly: encoding/json would otherwise accept a
// case-insensitive spelling of a field name. Both fields are required, and
// null is refused.
func decodePolicy(data json.RawMessage) (Policy, error) {
	fields, err := schemaext.DecodeJSON[map[string]json.RawMessage](data)
	if err != nil {
		return Policy{}, err
	}
	if fields == nil {
		return Policy{}, fmt.Errorf("%w: expected a non-null Spanner row deletion policy object", schemaext.ErrInvalidValue)
	}
	var p Policy
	for _, field := range []struct {
		name  string
		value *string
	}{{"column", &p.Column}, {"interval", &p.Interval}} {
		raw, found := fields[field.name]
		if !found {
			return Policy{}, fmt.Errorf("%w: a Spanner row deletion policy needs %q", schemaext.ErrInvalidValue, field.name)
		}
		value, err := schemaext.DecodeJSON[*string](raw)
		if err != nil {
			return Policy{}, fmt.Errorf("Spanner row deletion policy field %q: %w", field.name, err)
		}
		if value == nil {
			return Policy{}, fmt.Errorf("%w: Spanner row deletion policy field %q cannot be null", schemaext.ErrInvalidValue, field.name)
		}
		*field.value = *value
	}
	if len(fields) != 2 {
		return Policy{}, fmt.Errorf("%w: unknown Spanner row deletion policy field", schemaext.ErrInvalidValue)
	}
	return p, nil
}

// Owner is the provider identity under which the bundled runtime registers
// these codecs. Coverage built here names it, so a runtime that registers the
// codecs under another identity must build its own coverage.
const Owner = "ptah.run/spanner"

// RowDeletionCoverage records what one source knows about row deletion
// policies. knowledge is the claim for every table the source describes;
// subjects override it for individual tables. A desired source that can
// declare the policy records complete knowledge, which makes a table without a
// value a request for no policy. A read records complete knowledge only for the
// tables it returned.
func RowDeletionCoverage(representation schemaext.Representation, knowledge schemaext.Knowledge, subjects []schemaext.SubjectCoverage) (schemaext.Coverage, error) {
	owned := make([]schemaext.OwnedCodec, 0, 2)
	for _, codec := range Codecs() {
		owned = append(owned, schemaext.OwnedCodec{Owner: Owner, Codec: codec})
	}
	registry, err := schemaext.NewRegistry(owned...)
	if err != nil {
		return schemaext.Coverage{}, err
	}
	for _, model := range registry.Definitions() {
		if model.Kind == RowDeletionKind && model.Representation == representation {
			return schemaext.NewCoverage(representation, []schemaext.KindCoverage{{Model: model, Knowledge: knowledge}}, subjects)
		}
	}
	return schemaext.Coverage{}, fmt.Errorf("%w: row deletion policy coverage requires a schema representation", schemaext.ErrInvalidValue)
}

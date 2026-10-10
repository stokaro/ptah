package crdbschema

import (
	"bytes"
	_ "embed" // Embed the definition that identifies this wire format.
	"encoding/json"
	"fmt"
	"slices"
	"strconv"
	"sync"

	"ptah.run/core/schemaext"
)

//go:embed row-ttl-codecs.json
var rowTTLDefinition []byte

// WireDefinition returns an independent description of the row-level TTL wire
// model. The definition covers both representations and their omission rules.
func WireDefinition() json.RawMessage { return slices.Clone(rowTTLDefinition) }

// Codecs returns the versioned row-level TTL codecs. Each call returns
// independent definitions. The codecs validate representation invariants; they
// do not read intervals as values or claim server support.
func Codecs() []schemaext.Codec {
	return []schemaext.Codec{
		rowTTLCodec(&DesiredRowTTL{}, schemaext.Desired, decodeDesired, desiredPolicy),
		rowTTLCodec(&ObservedRowTTL{}, schemaext.Observed, decodeObserved, observedPolicy),
	}
}

func rowTTLCodec(prototype schemaext.Value, representation schemaext.Representation,
	decode func(json.RawMessage) (schemaext.Payload, error), policy func(schemaext.Payload) (Policy, error),
) schemaext.Codec {
	encode := func(payload schemaext.Payload) (json.RawMessage, error) {
		p, err := policy(payload)
		if err != nil {
			return nil, err
		}
		return encodePolicy(p)
	}
	return schemaext.Codec{
		Prototype: prototype, Representation: representation, Version: 1, Definition: WireDefinition(),
		Clone: func(payload schemaext.Payload) (schemaext.Payload, error) {
			if _, err := policy(payload); err != nil {
				return nil, err
			}
			value, ok := payload.(schemaext.Value)
			if !ok {
				return nil, fmt.Errorf("%w: expected a CockroachDB row-level TTL value", schemaext.ErrInvalidValue)
			}
			return schemaext.CloneValue(value)
		},
		Encode: encode, Decode: decode, Canonical: encode,
	}
}

func desiredPolicy(payload schemaext.Payload) (Policy, error) {
	v, ok := payload.(*DesiredRowTTL)
	if !ok {
		return Policy{}, fmt.Errorf("%w: expected a desired CockroachDB row-level TTL, got %T", schemaext.ErrInvalidValue, payload)
	}
	if err := ValidateDesired(v); err != nil {
		return Policy{}, err
	}
	return v.Policy, nil
}

func observedPolicy(payload schemaext.Payload) (Policy, error) {
	v, ok := payload.(*ObservedRowTTL)
	if !ok {
		return Policy{}, fmt.Errorf("%w: expected an observed CockroachDB row-level TTL, got %T", schemaext.ErrInvalidValue, payload)
	}
	if err := ValidateObserved(v); err != nil {
		return Policy{}, err
	}
	return v.Policy, nil
}

// The wire object names each set parameter once, by its storage parameter
// name. A text value is a string, a count an integer, and a flag true. The
// map keys are sorted by encoding/json, which makes the encoding canonical.
func encodePolicy(p Policy) (json.RawMessage, error) {
	fields := make(map[string]any)
	for _, parameter := range p.Parameters() {
		switch parameter.Kind {
		case TextValue:
			fields[parameter.Name] = parameter.Value
		case CountValue:
			count, err := strconv.ParseInt(parameter.Value, 10, 64)
			if err != nil {
				return nil, fmt.Errorf("%w: %s", schemaext.ErrInvalidValue, parameter.Name)
			}
			fields[parameter.Name] = count
		case FlagValue:
			fields[parameter.Name] = true
		}
	}
	return json.Marshal(fields)
}

func decodeDesired(data json.RawMessage) (schemaext.Payload, error) {
	p, err := decodePolicy(data)
	if err != nil {
		return nil, err
	}
	v := &DesiredRowTTL{Policy: p}
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
	v := &ObservedRowTTL{Policy: p}
	if err := ValidateObserved(v); err != nil {
		return nil, err
	}
	return v, nil
}

// Keys are compared exactly: encoding/json would otherwise accept a
// case-insensitive spelling of a parameter name, which the server does not.
// Null, an empty text value, and a false flag are refused, because each has a
// single canonical spelling, which is omission.
func decodePolicy(data json.RawMessage) (Policy, error) {
	fields, err := schemaext.DecodeJSON[map[string]json.RawMessage](data)
	if err != nil {
		return Policy{}, err
	}
	if fields == nil {
		return Policy{}, fmt.Errorf("%w: expected a non-null CockroachDB row-level TTL object", schemaext.ErrInvalidValue)
	}
	var p Policy
	consumed := 0
	for _, text := range p.texts() {
		raw, found := fields[text.name]
		if !found {
			continue
		}
		consumed++
		value, err := decodeField[string](raw, text.name)
		if err != nil {
			return Policy{}, err
		}
		if value == "" {
			return Policy{}, fmt.Errorf("%w: %s cannot be empty; omit it instead", schemaext.ErrInvalidValue, text.name)
		}
		*text.value = value
	}
	for _, count := range p.counts() {
		raw, found := fields[count.name]
		if !found {
			continue
		}
		consumed++
		value, err := decodeField[int64](raw, count.name)
		if err != nil {
			return Policy{}, err
		}
		*count.value = &value
	}
	for _, flag := range p.flags() {
		raw, found := fields[flag.name]
		if !found {
			continue
		}
		consumed++
		value, err := decodeField[bool](raw, flag.name)
		if err != nil {
			return Policy{}, err
		}
		if !value {
			return Policy{}, fmt.Errorf("%w: %s is written by omission when false", schemaext.ErrInvalidValue, flag.name)
		}
		*flag.value = true
	}
	if consumed != len(fields) {
		return Policy{}, fmt.Errorf("%w: unknown CockroachDB row-level TTL property", schemaext.ErrInvalidValue)
	}
	return p, nil
}

func decodeField[T any](raw json.RawMessage, name string) (T, error) {
	var zero T
	if bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
		return zero, fmt.Errorf("%w: CockroachDB row-level TTL property %q cannot be null", schemaext.ErrInvalidValue, name)
	}
	value, err := schemaext.DecodeJSON[T](raw)
	if err != nil {
		return zero, fmt.Errorf("CockroachDB row-level TTL property %q: %w", name, err)
	}
	return value, nil
}

// Owner is the provider identity under which the bundled runtime registers
// these codecs. Coverage built here names it, so a runtime that registers the
// codecs under another identity must build its own coverage.
const Owner = "ptah.run/cockroachdb"

// rowTTLRegistry builds the model registry coverage names once. Building one
// validates and hashes every codec definition, and coverage is asked for on
// every read and every comparison.
var rowTTLRegistry = sync.OnceValues(func() (schemaext.Registry, error) {
	owned := make([]schemaext.OwnedCodec, 0, 2)
	for _, codec := range Codecs() {
		owned = append(owned, schemaext.OwnedCodec{Owner: Owner, Codec: codec})
	}
	return schemaext.NewRegistry(owned...)
})

// RowTTLCoverage records what one source knows about row-level TTL. knowledge
// is the claim for every table the source describes; subjects override it for
// individual tables. A desired source that can declare the policy records
// complete knowledge, which makes a table without a value a request for no TTL.
// A read records complete knowledge only for the tables it returned.
func RowTTLCoverage(representation schemaext.Representation, knowledge schemaext.Knowledge, subjects []schemaext.SubjectCoverage) (schemaext.Coverage, error) {
	registry, err := rowTTLRegistry()
	if err != nil {
		return schemaext.Coverage{}, err
	}
	for _, model := range registry.Definitions() {
		if model.Kind == RowTTLKind && model.Representation == representation {
			return schemaext.NewCoverage(representation, []schemaext.KindCoverage{{Model: model, Knowledge: knowledge}}, subjects)
		}
	}
	return schemaext.Coverage{}, fmt.Errorf("%w: row-level TTL coverage requires a schema representation", schemaext.ErrInvalidValue)
}

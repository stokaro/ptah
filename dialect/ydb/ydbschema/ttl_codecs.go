package ydbschema

import (
	"bytes"
	_ "embed" // Embed the definition that identifies the TTL wire format.
	"encoding/json"
	"fmt"
	"slices"

	"ptah.run/core/schemaext"
)

//go:embed ttl-codecs.json
var ttlDefinition []byte

// TTLWireDefinition returns an independent description of the TTL wire model.
// The definition covers both representations.
func TTLWireDefinition() json.RawMessage { return slices.Clone(ttlDefinition) }

// TTLCodecs returns the versioned TTL codecs. Each call returns independent
// definitions. The codecs validate representation invariants; they do not
// claim server support.
func TTLCodecs() []schemaext.Codec {
	return []schemaext.Codec{
		ttlCodec(&DesiredTTL{}, schemaext.Desired, decodeDesiredTTL, encodeDesiredTTL),
		ttlCodec(&ObservedTTL{}, schemaext.Observed, decodeObservedTTL, encodeObservedTTL),
	}
}

func ttlCodec(prototype schemaext.Value, representation schemaext.Representation,
	decode func(json.RawMessage) (schemaext.Payload, error), encode func(schemaext.Payload) (json.RawMessage, error),
) schemaext.Codec {
	return schemaext.Codec{
		Prototype: prototype, Representation: representation, Version: 1, Definition: TTLWireDefinition(),
		Clone: func(payload schemaext.Payload) (schemaext.Payload, error) {
			if _, err := encode(payload); err != nil {
				return nil, err
			}
			return schemaext.CloneValue(payload.(schemaext.Value))
		},
		Encode: encode, Decode: decode, Canonical: encode,
	}
}

// wireTTL is the wire object. Its field order is the canonical encoding.
type wireTTL struct {
	Column             string `json:"column"`
	Interval           string `json:"interval"`
	Unit               string `json:"unit,omitempty"`
	RunIntervalSeconds uint64 `json:"run_interval_seconds,omitempty"`
}

func encodeDesiredTTL(payload schemaext.Payload) (json.RawMessage, error) {
	v, ok := payload.(*DesiredTTL)
	if !ok {
		return nil, fmt.Errorf("%w: expected a desired YDB TTL, got %T", schemaext.ErrInvalidValue, payload)
	}
	if err := ValidateDesiredTTL(v); err != nil {
		return nil, err
	}
	return json.Marshal(wireTTL{Column: v.Policy.Column, Interval: v.Policy.Interval, Unit: v.Policy.Unit})
}

func encodeObservedTTL(payload schemaext.Payload) (json.RawMessage, error) {
	v, ok := payload.(*ObservedTTL)
	if !ok {
		return nil, fmt.Errorf("%w: expected an observed YDB TTL, got %T", schemaext.ErrInvalidValue, payload)
	}
	if err := ValidateObservedTTL(v); err != nil {
		return nil, err
	}
	return json.Marshal(wireTTL{Column: v.Policy.Column, Interval: v.Policy.Interval, Unit: v.Policy.Unit, RunIntervalSeconds: v.RunIntervalSeconds})
}

func decodeDesiredTTL(data json.RawMessage) (schemaext.Payload, error) {
	wire, err := decodeTTL(data, desiredTTLFields)
	if err != nil {
		return nil, err
	}
	v := &DesiredTTL{Policy: TTL{Column: wire.Column, Interval: wire.Interval, Unit: wire.Unit}}
	if err := ValidateDesiredTTL(v); err != nil {
		return nil, err
	}
	return v, nil
}

func decodeObservedTTL(data json.RawMessage) (schemaext.Payload, error) {
	wire, err := decodeTTL(data, observedTTLFields)
	if err != nil {
		return nil, err
	}
	v := &ObservedTTL{Policy: TTL{Column: wire.Column, Interval: wire.Interval, Unit: wire.Unit}, RunIntervalSeconds: wire.RunIntervalSeconds}
	if err := ValidateObservedTTL(v); err != nil {
		return nil, err
	}
	return v, nil
}

// desiredTTLFields and observedTTLFields are the wire fields each
// representation takes; only an observation records a run interval.
var (
	desiredTTLFields  = []string{"column", "interval", "unit"}
	observedTTLFields = []string{"column", "interval", "unit", "run_interval_seconds"}
)

// decodeTTL compares keys exactly against allowed, since encoding/json would
// accept a case-insensitive spelling. Null is refused, and so are an empty unit
// and a zero run interval, whose one spelling is omission.
func decodeTTL(data json.RawMessage, allowed []string) (wireTTL, error) {
	fields, err := schemaext.DecodeJSON[map[string]json.RawMessage](data)
	if err != nil {
		return wireTTL{}, err
	}
	if fields == nil {
		return wireTTL{}, fmt.Errorf("%w: expected a non-null YDB TTL object", schemaext.ErrInvalidValue)
	}
	for name, raw := range fields {
		if !slices.Contains(allowed, name) {
			return wireTTL{}, fmt.Errorf("%w: unknown YDB TTL field %q", schemaext.ErrInvalidValue, name)
		}
		if bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
			return wireTTL{}, fmt.Errorf("%w: YDB TTL field %q cannot be null", schemaext.ErrInvalidValue, name)
		}
	}
	wire, err := schemaext.DecodeJSON[wireTTL](data)
	if err != nil {
		return wireTTL{}, err
	}
	if raw, found := fields["unit"]; found && wire.Unit == "" {
		return wireTTL{}, fmt.Errorf("%w: an empty YDB TTL unit is written by omission (%s)", schemaext.ErrInvalidValue, raw)
	}
	if _, found := fields["run_interval_seconds"]; found && wire.RunIntervalSeconds == 0 {
		return wireTTL{}, fmt.Errorf("%w: a zero YDB TTL run interval is written by omission", schemaext.ErrInvalidValue)
	}
	return wire, nil
}

// TTLCoverage records what one source knows about YDB TTL. knowledge is the
// claim for every table the source describes; subjects override it for
// individual tables. A desired source that can declare the TTL records
// complete knowledge, which makes a table without a value a request for none.
// A read records complete knowledge only for the tables it returned.
func TTLCoverage(representation schemaext.Representation, knowledge schemaext.Knowledge, subjects []schemaext.SubjectCoverage) (schemaext.Coverage, error) {
	owned := make([]schemaext.OwnedCodec, 0, 2)
	for _, codec := range TTLCodecs() {
		owned = append(owned, schemaext.OwnedCodec{Owner: Owner, Codec: codec})
	}
	registry, err := schemaext.NewRegistry(owned...)
	if err != nil {
		return schemaext.Coverage{}, err
	}
	for _, model := range registry.Definitions() {
		if model.Kind == TTLKind && model.Representation == representation {
			return schemaext.NewCoverage(representation, []schemaext.KindCoverage{{Model: model, Knowledge: knowledge}}, subjects)
		}
	}
	return schemaext.Coverage{}, fmt.Errorf("%w: TTL coverage requires a schema representation", schemaext.ErrInvalidValue)
}

// Owner is the provider identity under which the bundled runtime registers the
// YDB codecs. Coverage built here names it, so a runtime that registers the
// codecs under another identity must build its own coverage.
const Owner = "ptah.run/ydb"

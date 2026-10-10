// Package tsdiff owns captured directional changes to TimescaleDB state: a
// surviving table's hypertable settings and a continuous aggregate. Each change
// carries complete operands and its conservative safety effect, and contains
// no plan, SQL or database access.
package tsdiff

import (
	"encoding/json"
	"fmt"
	"strings"

	"ptah.run/core/schemaext"
	"ptah.run/dialect/timescaledb/tsschema"
)

const (
	// HypertableKind identifies a change to a surviving table's partitioning.
	HypertableKind schemaext.Kind = "ptah.run/timescaledb/hypertable-change"
	// ContinuousAggregateKind identifies a change to one continuous aggregate.
	ContinuousAggregateKind schemaext.Kind = "ptah.run/timescaledb/continuous-aggregate-change"
)

// Hypertable records how a surviving table's partitioning differs. A nil
// Before is an ordinary table the declaration partitions; a nil After is a
// hypertable the declaration leaves ordinary. Both describe established state,
// never an unread one. Creation and removal of the table itself carry its
// settings with the common table lifecycle and never appear here.
type Hypertable struct {
	Before *tsschema.ObservedHypertable `json:"before"`
	After  *tsschema.DesiredHypertable  `json:"after"`
}

// Kind returns the stable change identity.
func (*Hypertable) Kind() schemaext.Kind { return HypertableKind }

// CloneChange returns independent operands. A nil receiver remains typed nil.
func (v *Hypertable) CloneChange() schemaext.ChangeValue {
	if v == nil {
		return (*Hypertable)(nil)
	}
	cloned := &Hypertable{}
	if v.Before != nil {
		cloned.Before = new(*v.Before)
	}
	if v.After != nil {
		cloned.After = new(*v.After)
	}
	return cloned
}

// Validate refuses missing or invalid operands and a pair that agrees.
func (v *Hypertable) Validate() error {
	if v == nil || (v.Before == nil && v.After == nil) {
		return fmt.Errorf("%w: a hypertable change requires a before or after operand", schemaext.ErrInvalidValue)
	}
	if v.Before != nil {
		if err := tsschema.ValidateObservedHypertable(v.Before); err != nil {
			return err
		}
	}
	if v.After != nil {
		if err := tsschema.ValidateDesiredHypertable(v.After); err != nil {
			return err
		}
	}
	if v.Before != nil && v.After != nil && tsschema.SamePartitioning(v.After, v.Before) {
		return fmt.Errorf("%w: hypertable operands describe the same partitioning", schemaext.ErrInvalidValue)
	}
	return nil
}

// Effect records the consequences the operands establish. Partitioning an
// existing table changes where its rows live; there is no statement that undoes
// it, which is why a removal or a repartitioning is refused by its planner.
func (v *Hypertable) Effect() schemaext.Effect {
	if v == nil || v.Validate() != nil {
		return schemaext.Effect{}
	}
	switch {
	case v.Before == nil:
		return schemaext.Effect{Impact: schemaext.Behavioral, Reason: "create_hypertable partitions the table into chunks; TimescaleDB has no statement that turns it back into an ordinary table"}
	case v.After == nil:
		return schemaext.Effect{Impact: schemaext.Destructive, Reason: "turning a hypertable back into an ordinary table needs its rows copied out of every chunk"}
	default:
		return schemaext.Effect{Impact: schemaext.Behavioral, Reason: "repartitioning a hypertable changes where its rows live"}
	}
}

// ContinuousAggregate records a change to one aggregate. A nil Before creates
// it, a nil After drops it, and both together replace it: there is no CREATE
// OR REPLACE for a continuous aggregate.
type ContinuousAggregate struct {
	Before *tsschema.ObservedContinuousAggregate `json:"before"`
	After  *tsschema.DesiredContinuousAggregate  `json:"after"`
}

// Kind returns the stable change identity.
func (*ContinuousAggregate) Kind() schemaext.Kind { return ContinuousAggregateKind }

// CloneChange returns independent operands. A nil receiver remains typed nil.
func (v *ContinuousAggregate) CloneChange() schemaext.ChangeValue { return v.Copy() }

// Copy is [ContinuousAggregate.CloneChange] without the interface: it shares
// no operand with v, and a nil receiver returns nil.
func (v *ContinuousAggregate) Copy() *ContinuousAggregate {
	if v == nil {
		return nil
	}
	return &ContinuousAggregate{Before: v.Before.Copy(), After: v.After.Copy()}
}

// Validate refuses missing or invalid operands.
func (v *ContinuousAggregate) Validate() error {
	if v == nil || (v.Before == nil && v.After == nil) {
		return fmt.Errorf("%w: a continuous aggregate change requires a before or after operand", schemaext.ErrInvalidValue)
	}
	if v.Before != nil {
		if err := tsschema.ValidateObservedContinuousAggregate(v.Before); err != nil {
			return err
		}
	}
	if v.After != nil {
		if err := tsschema.ValidateDesiredContinuousAggregate(v.After); err != nil {
			return err
		}
	}
	return nil
}

// Effect records what the change does to the aggregate's materialized data.
// The aggregate is created WITH NO DATA, so a replacement leaves it empty until
// an operator refreshes it.
func (v *ContinuousAggregate) Effect() schemaext.Effect {
	if v == nil || v.Validate() != nil {
		return schemaext.Effect{}
	}
	switch {
	case v.Before == nil:
		return schemaext.Effect{Impact: schemaext.Additive, Reason: "creates a continuous aggregate WITH NO DATA; its first refresh is run separately"}
	case v.After == nil:
		return schemaext.Effect{Impact: schemaext.Destructive, Reason: "DROP MATERIALIZED VIEW discards the aggregate and its materialized data"}
	default:
		return schemaext.Effect{Impact: schemaext.Destructive, Reason: "replacing a continuous aggregate drops its materialized data; the new one is empty until a refresh"}
	}
}

// Codecs returns the version-one change codecs.
func Codecs() []schemaext.Codec {
	return []schemaext.Codec{HypertableCodec(), ContinuousAggregateCodec()}
}

// HypertableCodec describes the complete before/after wire of a hypertable change.
func HypertableCodec() schemaext.Codec {
	return changeCodec(&Hypertable{}, tsschema.HypertableCodecs(), "hypertable", func(before, after schemaext.Value) (schemaext.ChangeValue, error) {
		value := &Hypertable{}
		var ok bool
		if value.Before, ok = operand[*tsschema.ObservedHypertable](before); !ok {
			return nil, fmt.Errorf("%w: unexpected hypertable operand %T", schemaext.ErrInvalidValue, before)
		}
		if value.After, ok = operand[*tsschema.DesiredHypertable](after); !ok {
			return nil, fmt.Errorf("%w: unexpected hypertable operand %T", schemaext.ErrInvalidValue, after)
		}
		return value, value.Validate()
	}, func(payload schemaext.Payload) error {
		value, ok := payload.(*Hypertable)
		if !ok {
			return fmt.Errorf("%w: expected a hypertable change, got %T", schemaext.ErrInvalidValue, payload)
		}
		return value.Validate()
	})
}

// ContinuousAggregateCodec describes the complete before/after wire of an
// aggregate change.
func ContinuousAggregateCodec() schemaext.Codec {
	return changeCodec(&ContinuousAggregate{}, tsschema.ContinuousAggregateCodecs(), "continuous aggregate", func(before, after schemaext.Value) (schemaext.ChangeValue, error) {
		value := &ContinuousAggregate{}
		var ok bool
		if value.Before, ok = operand[*tsschema.ObservedContinuousAggregate](before); !ok {
			return nil, fmt.Errorf("%w: unexpected continuous aggregate operand %T", schemaext.ErrInvalidValue, before)
		}
		if value.After, ok = operand[*tsschema.DesiredContinuousAggregate](after); !ok {
			return nil, fmt.Errorf("%w: unexpected continuous aggregate operand %T", schemaext.ErrInvalidValue, after)
		}
		return value, value.Validate()
	}, func(payload schemaext.Payload) error {
		value, ok := payload.(*ContinuousAggregate)
		if !ok {
			return fmt.Errorf("%w: expected a continuous aggregate change, got %T", schemaext.ErrInvalidValue, payload)
		}
		return value.Validate()
	})
}

func changeCodec(prototype schemaext.ChangeValue, models []schemaext.Codec, family string,
	build func(before, after schemaext.Value) (schemaext.ChangeValue, error), validate func(schemaext.Payload) error,
) schemaext.Codec {
	// tsschema returns each model's codecs as desired, then observed.
	desired, observed := models[0], models[1]
	encode := func(payload schemaext.Payload) (json.RawMessage, error) {
		if err := validate(payload); err != nil {
			return nil, err
		}
		return json.Marshal(payload)
	}
	definition := fmt.Sprintf(`{"type":"object","required":["before","after"],"additionalProperties":false,"properties":{`+
		`"before":{"anyOf":[{"type":"null"},%s]},"after":{"anyOf":[{"type":"null"},%s]}}}`, observed.Definition, desired.Definition)
	return schemaext.Codec{
		Prototype: prototype, Representation: schemaext.Change, Version: 1, Definition: json.RawMessage(definition),
		Encode: encode, Canonical: encode,
		Clone: func(payload schemaext.Payload) (schemaext.Payload, error) {
			if err := validate(payload); err != nil {
				return nil, err
			}
			return payload.(schemaext.ChangeValue).CloneChange(), nil
		},
		Decode: func(data json.RawMessage) (schemaext.Payload, error) {
			fields, err := schemaext.DecodeJSON[map[string]json.RawMessage](data)
			if err != nil {
				return nil, err
			}
			if len(fields) != 2 || len(fields["before"]) == 0 || len(fields["after"]) == 0 {
				return nil, fmt.Errorf("%w: a %s change requires exactly before and after", schemaext.ErrInvalidValue, family)
			}
			before, err := decodeOperand(observed, fields["before"])
			if err != nil {
				return nil, err
			}
			after, err := decodeOperand(desired, fields["after"])
			if err != nil {
				return nil, err
			}
			value, err := build(before, after)
			if err != nil {
				return nil, err
			}
			return value, nil
		},
	}
}

// operand converts a decoded side to its model type. An absent side is nil
// and acceptable; a value of another type is not.
func operand[T schemaext.Value](value schemaext.Value) (T, bool) {
	var zero T
	if value == nil {
		return zero, true
	}
	typed, ok := value.(T)
	return typed, ok
}

func decodeOperand(codec schemaext.Codec, data json.RawMessage) (schemaext.Value, error) {
	if strings.TrimSpace(string(data)) == "null" {
		return nil, nil
	}
	payload, err := codec.Decode(data)
	if err != nil {
		return nil, err
	}
	value, ok := payload.(schemaext.Value)
	if !ok {
		return nil, fmt.Errorf("%w: unexpected TimescaleDB operand %T", schemaext.ErrInvalidValue, payload)
	}
	return value, nil
}

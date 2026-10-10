package chdiff

import (
	"bytes"
	"encoding/json"
	"fmt"

	"ptah.run/core/schemaext"
	"ptah.run/dialect/clickhouse/chschema"
)

// RefreshKind identifies a change to a materialized view's refresh schedule.
const RefreshKind schemaext.Kind = "ptah.run/clickhouse/refresh-change"

// Refresh captures a materialized view's schedule transition. A nil Before is
// a plain view gaining its first schedule and a nil After is a refreshable
// view losing its last; both nil is not a change. The view's identity, body and
// lifecycle belong to the common materialized view.
type Refresh struct {
	Before *chschema.ObservedRefresh `json:"before"`
	After  *chschema.DesiredRefresh  `json:"after"`
}

// Kind returns the owned change identity.
func (*Refresh) Kind() schemaext.Kind { return RefreshKind }

// ReplacesOwner reports a change MODIFY REFRESH cannot make, which only
// replacing the view does: a schedule gained or lost, or APPEND added or
// removed. ClickHouse changes the rest of a refreshable view's schedule in
// place, keeping its rows. Measured on 26.7.3.19, MODIFY REFRESH on a plain
// view is answered `Code: 48 ... Alter of type 'MODIFY_REFRESH' is not
// supported by storage MaterializedView` (stokaro/ptah#1802); measured on
// 24.10.4.191 and 26.9.14.10, one that adds or removes APPEND is answered
// `Code: 48 ... Adding or removing APPEND is not supported` and `Changing
// APPEND or INCREMENTAL is not supported`.
func (v *Refresh) ReplacesOwner() bool {
	return v != nil && (v.Before == nil || v.After == nil || v.Before.Append != v.After.Append)
}

// Effect reports what applying the change does to the view's rows. Changing a
// schedule in place keeps them; a change that replaces the view discards the
// rows it accumulated.
func (v *Refresh) Effect() schemaext.Effect {
	if v.ReplacesOwner() {
		return schemaext.Effect{Impact: schemaext.Destructive, Reason: "adding or removing a refresh schedule, or its APPEND, replaces the materialized view and discards the rows it holds"}
	}
	return schemaext.Effect{Impact: schemaext.Behavioral, Reason: "changing a refresh schedule changes when the view is refreshed; its rows are kept"}
}

// String describes the transition as the two clauses, such as
// `EVERY 1 HOUR -> EVERY 2 HOUR`, with `none` for a plain view.
func (v *Refresh) String() string {
	before, after := "none", "none"
	if v != nil && v.Before != nil {
		before = v.Before.Clause()
	}
	if v != nil && v.After != nil {
		after = v.After.Clause()
	}
	return before + " -> " + after
}

// CloneChange returns independent operands. A nil receiver remains typed nil.
func (v *Refresh) CloneChange() schemaext.ChangeValue {
	if v == nil {
		return (*Refresh)(nil)
	}
	result := &Refresh{}
	if v.Before != nil {
		result.Before = &chschema.ObservedRefresh{Schedule: v.Before.Schedule.Clone()}
	}
	if v.After != nil {
		result.After = &chschema.DesiredRefresh{Schedule: v.After.Schedule.Clone()}
	}
	return result
}

// ValidateRefresh requires at least one operand and validates each present
// one. Invalid operands wrap schemaext.ErrInvalidValue.
func ValidateRefresh(v *Refresh) error {
	if v == nil || v.Before == nil && v.After == nil {
		return fmt.Errorf("%w: ClickHouse refresh change requires a schedule on at least one side", schemaext.ErrInvalidValue)
	}
	if v.Before != nil {
		if err := chschema.ValidateObservedRefresh(v.Before); err != nil {
			return err
		}
	}
	if v.After != nil {
		if err := chschema.ValidateDesiredRefresh(v.After); err != nil {
			return err
		}
	}
	return nil
}

// RefreshCodecs returns the versioned refresh-change codec. Each operand uses
// the strict refresh wire model for its representation; an absent schedule is
// null, and a change with null on both sides is refused.
func RefreshCodecs() []schemaext.Codec {
	value := func(payload schemaext.Payload) (*Refresh, error) {
		v, ok := payload.(*Refresh)
		if !ok {
			return nil, fmt.Errorf("%w: expected ClickHouse refresh change, got %T", schemaext.ErrInvalidValue, payload)
		}
		return v, ValidateRefresh(v)
	}
	encode := func(payload schemaext.Payload) (json.RawMessage, error) {
		v, err := value(payload)
		if err != nil {
			return nil, err
		}
		return json.Marshal(v)
	}
	models := chschema.RefreshCodecs()
	return []schemaext.Codec{{
		Prototype: &Refresh{}, Representation: schemaext.Change, Version: 1,
		Definition: json.RawMessage(fmt.Sprintf(`{"operands":%s,`+
			`"before":"observed refresh schedule, or null for a plain view",`+
			`"after":"desired refresh schedule, or null for a plain view",`+
			`"constraint":"at least one side is non-null"}`, chschema.RefreshWireDefinition())),
		Clone: func(payload schemaext.Payload) (schemaext.Payload, error) {
			v, err := value(payload)
			if err != nil {
				return nil, err
			}
			return v.CloneChange(), nil
		},
		Encode: encode, Canonical: encode,
		Decode: func(data json.RawMessage) (schemaext.Payload, error) {
			fields, err := schemaext.DecodeJSON[map[string]json.RawMessage](data)
			if err != nil {
				return nil, err
			}
			before, hasBefore := fields["before"]
			after, hasAfter := fields["after"]
			if len(fields) != 2 || !hasBefore || !hasAfter {
				return nil, fmt.Errorf("%w: ClickHouse refresh change requires exactly before and after", schemaext.ErrInvalidValue)
			}
			result := &Refresh{}
			for _, codec := range models {
				operand := after
				if codec.Representation == schemaext.Observed {
					operand = before
				}
				if bytes.Equal(bytes.TrimSpace(operand), []byte("null")) {
					continue
				}
				decoded, err := codec.Decode(operand)
				if err != nil {
					return nil, err
				}
				switch typed := decoded.(type) {
				case *chschema.ObservedRefresh:
					result.Before = typed
				case *chschema.DesiredRefresh:
					result.After = typed
				}
			}
			v, err := value(result)
			if err != nil {
				return nil, err
			}
			return v, nil
		},
	}}
}

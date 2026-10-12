package mssqldiff

import (
	"encoding/json"
	"fmt"

	"ptah.run/core/schemaext"
	"ptah.run/dialect/mssql/mssqlschema"
)

// RoutineOptionsKind identifies a change to the options a SQL Server
// routine's header writes, the principal it runs as among them. T-SQL changes
// them only by creating the routine again, so the change replaces the
// routine: a host plans it as the routine's CREATE OR ALTER.
const RoutineOptionsKind schemaext.Kind = "ptah.run/mssql/routine-options-change"

// RoutineOptions retains the options a routine holds and the ones a
// declaration writes. A nil Before is a routine that holds none, and a nil
// After one the declaration writes none for; at least one is present, and the
// two state different options.
type RoutineOptions struct {
	Before *mssqlschema.ObservedRoutineOptions `json:"before"`
	After  *mssqlschema.DesiredRoutineOptions  `json:"after"`
}

// Kind returns the owned change identity.
func (*RoutineOptions) Kind() schemaext.Kind { return RoutineOptionsKind }

// CloneChange returns independent operands. A nil receiver remains typed nil.
func (v *RoutineOptions) CloneChange() schemaext.ChangeValue { return v.Copy() }

// ReplacesOwner reports true: T-SQL has no statement that changes the header
// options of a routine that stays, so its CREATE OR ALTER writes them.
func (*RoutineOptions) ReplacesOwner() bool { return true }

// Effect reports what applying the change does: the routine is written again,
// and how it runs changes, the principal its body runs as among it.
func (*RoutineOptions) Effect() schemaext.Effect {
	return schemaext.Effect{Impact: schemaext.Behavioral,
		Reason: "the routine is written again with CREATE OR ALTER, with other header options, such as the principal its body runs as"}
}

// Copy is [RoutineOptions.CloneChange] without the interface. A nil receiver
// returns nil.
func (v *RoutineOptions) Copy() *RoutineOptions {
	if v == nil {
		return nil
	}
	result := &RoutineOptions{}
	if v.Before != nil {
		result.Before = new(*v.Before)
	}
	if v.After != nil {
		result.After = new(*v.After)
	}
	return result
}

// ValidateRoutineOptions requires an operand, each present one valid in its
// representation, and two different sets of options. Invalid operands wrap
// schemaext.ErrInvalidValue.
func ValidateRoutineOptions(v *RoutineOptions) error {
	if v == nil || (v.Before == nil && v.After == nil) {
		return fmt.Errorf("%w: a routine options change requires a before or after operand", schemaext.ErrInvalidValue)
	}
	if v.Before != nil {
		if err := mssqlschema.ValidateObservedRoutineOptions(v.Before); err != nil {
			return err
		}
	}
	if v.After != nil {
		if err := mssqlschema.ValidateDesiredRoutineOptions(v.After); err != nil {
			return err
		}
	}
	if v.Before != nil && v.After != nil && mssqlschema.RoutineOptions(*v.Before).Same(mssqlschema.RoutineOptions(*v.After)) {
		return fmt.Errorf("%w: a routine options change requires two different sets of options", schemaext.ErrInvalidValue)
	}
	return nil
}

var routineOptionsShape = schemaext.ObjectShape{
	Name: "routine options change", Allowed: []string{"before", "after"}, Required: []string{"before", "after"},
	Nullable: []string{"before", "after"},
}

// RoutineOptionsCodec returns the routine options change codec. Each present
// operand takes the exact desired or observed wire form, checked by that
// model's codec, and an absent one is null. Every refusal is a
// [schemaext.InvalidModelError].
func RoutineOptionsCodec() schemaext.Codec {
	return schemaext.ModelCodec[*RoutineOptions]{
		Prototype: &RoutineOptions{}, Representation: schemaext.Change, Version: 1,
		Definition: json.RawMessage(`{"before":"the observed options, or null","after":"the declared options, or null"}`),
		Shape: func(data json.RawMessage) error {
			fields, err := schemaext.DecodeObject(data, routineOptionsShape)
			if err != nil {
				return err
			}
			for _, codec := range mssqlschema.RoutineOptionsCodecs() {
				key := "after"
				if codec.Representation == schemaext.Observed {
					key = "before"
				}
				if string(fields[key]) == "null" {
					continue
				}
				if _, err := codec.Decode(fields[key]); err != nil {
					return err
				}
			}
			return nil
		},
		Validate: ValidateRoutineOptions,
		Clone:    (*RoutineOptions).Copy,
	}.Codec()
}

// RoutineOptionsReversal returns the change that undoes v: the options v
// declares become the observation, and the options v replaced become the
// declaration. An invalid v is refused with schemaext.ErrInvalidValue.
func RoutineOptionsReversal(v *RoutineOptions) (*RoutineOptions, error) {
	if err := ValidateRoutineOptions(v); err != nil {
		return nil, err
	}
	inverse := &RoutineOptions{}
	if v.After != nil {
		after, err := v.After.Observed()
		if err != nil {
			return nil, err
		}
		inverse.Before = after
	}
	if v.Before != nil {
		before, err := v.Before.Desired()
		if err != nil {
			return nil, err
		}
		inverse.After = before
	}
	return inverse, nil
}

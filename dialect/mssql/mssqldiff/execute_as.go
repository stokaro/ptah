package mssqldiff

import (
	"encoding/json"
	"fmt"

	"ptah.run/core/schemaext"
	"ptah.run/dialect/mssql/mssqlschema"
)

// ExecuteAsKind identifies a change to the principal a SQL Server routine runs
// as. T-SQL changes it only by creating the routine again, so the change
// replaces the routine: a host plans it as the routine's CREATE OR ALTER.
const ExecuteAsKind schemaext.Kind = "ptah.run/mssql/routine-execute-as-change"

// ExecuteAs retains the principal a routine runs as and the one a declaration
// names. A nil Before is a routine that runs as no named principal, and a nil
// After one the declaration names none for; at least one is present, and the
// two name different principals.
type ExecuteAs struct {
	Before *mssqlschema.ObservedExecuteAs `json:"before"`
	After  *mssqlschema.DesiredExecuteAs  `json:"after"`
}

// Kind returns the owned change identity.
func (*ExecuteAs) Kind() schemaext.Kind { return ExecuteAsKind }

// CloneChange returns independent operands. A nil receiver remains typed nil.
func (v *ExecuteAs) CloneChange() schemaext.ChangeValue { return v.Copy() }

// ReplacesOwner reports true: T-SQL has no statement that changes the
// principal of a routine that stays, so its CREATE OR ALTER writes it.
func (*ExecuteAs) ReplacesOwner() bool { return true }

// Effect reports what applying the change does: the routine is written again,
// and its body runs with another principal's rights.
func (*ExecuteAs) Effect() schemaext.Effect {
	return schemaext.Effect{Impact: schemaext.Behavioral,
		Reason: "the routine is written again with CREATE OR ALTER, and its body runs with the rights of another principal"}
}

// Copy is [ExecuteAs.CloneChange] without the interface. A nil receiver
// returns nil.
func (v *ExecuteAs) Copy() *ExecuteAs {
	if v == nil {
		return nil
	}
	result := &ExecuteAs{}
	if v.Before != nil {
		result.Before = new(*v.Before)
	}
	if v.After != nil {
		result.After = new(*v.After)
	}
	return result
}

// ValidateExecuteAs requires an operand, each present one valid in its
// representation, and two different principals. Invalid operands wrap
// schemaext.ErrInvalidValue.
func ValidateExecuteAs(v *ExecuteAs) error {
	if v == nil || (v.Before == nil && v.After == nil) {
		return fmt.Errorf("%w: an EXECUTE AS change requires a before or after operand", schemaext.ErrInvalidValue)
	}
	if v.Before != nil {
		if err := mssqlschema.ValidateObservedExecuteAs(v.Before); err != nil {
			return err
		}
	}
	if v.After != nil {
		if err := mssqlschema.ValidateDesiredExecuteAs(v.After); err != nil {
			return err
		}
	}
	if v.Before != nil && v.After != nil && mssqlschema.SamePrincipal(v.Before.Principal, v.After.Principal) {
		return fmt.Errorf("%w: an EXECUTE AS change requires two different principals", schemaext.ErrInvalidValue)
	}
	return nil
}

var executeAsShape = schemaext.ObjectShape{
	Name: "EXECUTE AS change", Allowed: []string{"before", "after"}, Required: []string{"before", "after"},
	Nullable: []string{"before", "after"},
}

// ExecuteAsCodec returns the EXECUTE AS change codec. Each present operand
// takes the exact desired or observed wire form, checked by that model's
// codec, and an absent one is null. Every refusal is a
// [schemaext.InvalidModelError].
func ExecuteAsCodec() schemaext.Codec {
	return schemaext.ModelCodec[*ExecuteAs]{
		Prototype: &ExecuteAs{}, Representation: schemaext.Change, Version: 1,
		Definition: json.RawMessage(`{"before":"the observed principal, or null","after":"the declared principal, or null"}`),
		Shape: func(data json.RawMessage) error {
			fields, err := schemaext.DecodeObject(data, executeAsShape)
			if err != nil {
				return err
			}
			for _, codec := range mssqlschema.ExecuteAsCodecs() {
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
		Validate: ValidateExecuteAs,
		Clone:    (*ExecuteAs).Copy,
	}.Codec()
}

// ExecuteAsReversal returns the change that undoes v: the principal v
// declares becomes the observation, and the principal v replaced becomes the
// declaration. An invalid v is refused with schemaext.ErrInvalidValue.
func ExecuteAsReversal(v *ExecuteAs) (*ExecuteAs, error) {
	if err := ValidateExecuteAs(v); err != nil {
		return nil, err
	}
	inverse := &ExecuteAs{}
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

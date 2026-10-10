package mssqlproperty

import (
	"encoding/json"
	"fmt"

	"ptah.run/core/ast"
	"ptah.run/core/schemaext"
)

// ChangeKind identifies a change of one extended property.
const ChangeKind schemaext.Kind = "ptah.run/mssql/extended-property-change"

// OperationKind identifies the statement that makes one change.
const OperationKind schemaext.Kind = "ptah.run/mssql/extended-property-operation"

const definition = `{"$id":"ptah.run/mssql/extended-property/v1",` +
	`"property":{"schema":"the schema the property is on or that holds its table; omitted for a database property",` +
	`"table":"the table the property is on; omitted for a schema or database property",` +
	`"column":"the column of table the property is on; omitted otherwise","name":"the property name","value":"the text it holds"},` +
	`"desired":{"comment":"optional documentation written above the statement","struct_name":"the Go struct that declared it"},` +
	`"observed":{"value_type":"the sql_variant base type in lower case"}}`

var (
	propertyFields = []string{"schema", "table", "column", "name", "value"}
	desiredShape   = schemaext.ObjectShape{Name: "SQL Server extended property",
		Allowed: append(propertyFields, "comment", "struct_name"), Required: []string{"name", "value"},
		NonEmpty: []string{"schema", "table", "column", "comment", "struct_name"}}
	observedShape = schemaext.ObjectShape{Name: "SQL Server extended property observation",
		Allowed: append(propertyFields, "value_type"), Required: []string{"name", "value", "value_type"},
		NonEmpty: []string{"schema", "table", "column"}}
)

// Codecs returns the version-one desired and observed property codecs, the
// change codec and the operation codec. A decoder accepts only what the
// encoder writes: a key in another letter case, a null, and an omitted value
// written out are refused. Every refusal is a [schemaext.InvalidModelError].
func Codecs() []schemaext.Codec {
	return []schemaext.Codec{
		schemaext.ModelCodec[*DesiredProperty]{
			Prototype: &DesiredProperty{}, Representation: schemaext.Desired, Version: 1, Definition: json.RawMessage(definition),
			Shape: func(data json.RawMessage) error { _, err := schemaext.DecodeObject(data, desiredShape); return err }, Validate: ValidateDesired,
		}.Codec(),
		schemaext.ModelCodec[*ObservedProperty]{
			Prototype: &ObservedProperty{}, Representation: schemaext.Observed, Version: 1, Definition: json.RawMessage(definition),
			Shape: func(data json.RawMessage) error { _, err := schemaext.DecodeObject(data, observedShape); return err }, Validate: ValidateObserved,
		}.Codec(),
		changeCodec(),
		operationCodec(),
	}
}

// Coverage records what one source knows about extended properties: knowledge
// for every property, with subjects overriding it. A source that can declare
// them records complete knowledge, which makes a property it leaves out one
// the plan drops.
func Coverage(representation schemaext.Representation, knowledge schemaext.Knowledge, subjects []schemaext.SubjectCoverage) (schemaext.Coverage, error) {
	codecs := Codecs()[:2]
	owned := make([]schemaext.OwnedCodec, 0, len(codecs))
	for _, codec := range codecs {
		owned = append(owned, schemaext.OwnedCodec{Owner: Owner, Codec: codec})
	}
	registry, err := schemaext.NewRegistry(owned...)
	if err != nil {
		return schemaext.Coverage{}, err
	}
	for _, model := range registry.Definitions() {
		if model.Kind == Kind && model.Representation == representation {
			return schemaext.NewCoverage(representation, []schemaext.KindCoverage{{Model: model, Knowledge: knowledge}}, subjects)
		}
	}
	return schemaext.Coverage{}, fmt.Errorf("%w: extended property coverage requires a schema representation", schemaext.ErrInvalidValue)
}

// Change retains the property the database holds and the one the
// declaration asks for. A nil Before adds the property and a nil After drops
// it; with both, the value changes.
type Change struct {
	Before *ObservedProperty `json:"before,omitzero"`
	After  *DesiredProperty  `json:"after,omitzero"`
}

// Kind returns the owned change identity.
func (*Change) Kind() schemaext.Kind { return ChangeKind }

// CloneChange returns independent operands. A nil receiver remains typed nil.
func (v *Change) CloneChange() schemaext.ChangeValue { return v.Copy() }

// Copy is [Change.CloneChange] without the interface: it shares nothing with
// v, and a nil receiver returns nil.
func (v *Change) Copy() *Change {
	if v == nil {
		return nil
	}
	result := &Change{}
	if v.Before != nil {
		before := *v.Before
		result.Before = &before
	}
	if v.After != nil {
		after := *v.After
		result.After = &after
	}
	return result
}

// ValidateChange requires an operand, valid operands, and one property on
// both sides.
func ValidateChange(v *Change) error {
	if v == nil || (v.Before == nil && v.After == nil) {
		return fmt.Errorf("%w: an extended property change requires an operand", schemaext.ErrInvalidValue)
	}
	if v.Before != nil {
		if err := ValidateObserved(v.Before); err != nil {
			return err
		}
	}
	if v.After != nil {
		if err := ValidateDesired(v.After); err != nil {
			return err
		}
	}
	if v.Before != nil && v.After != nil && v.Before.Ref().Key() != v.After.Ref().Key() {
		return fmt.Errorf("%w: extended property change operands name different properties", schemaext.ErrInvalidValue)
	}
	return nil
}

func changeCodec() schemaext.Codec {
	shape := schemaext.ObjectShape{Name: "SQL Server extended property change", Allowed: []string{"before", "after"}}
	return schemaext.ModelCodec[*Change]{
		Prototype: &Change{}, Representation: schemaext.Change, Version: 1,
		Definition: json.RawMessage(`{"before":"the observed property; omitted when the change adds it","after":"the declared property; omitted when the change drops it"}`),
		Shape: func(data json.RawMessage) error {
			fields, err := schemaext.DecodeObject(data, shape)
			if err != nil {
				return err
			}
			codecs := Codecs()
			for key, codec := range map[string]schemaext.Codec{"after": codecs[0], "before": codecs[1]} {
				if raw, found := fields[key]; found {
					if _, err := codec.Decode(raw); err != nil {
						return err
					}
				}
			}
			return nil
		},
		Validate: ValidateChange,
		Clone:    (*Change).Copy,
	}.Codec()
}

// Action is the procedure an [Operation] runs.
type Action string

// The procedures that add, change and drop an extended property.
const (
	Add    Action = "add"
	Update Action = "update"
	Drop   Action = "drop"
)

// Operation runs one of the three extended property procedures for Property.
// A drop writes no value.
type Operation struct {
	Action   Action          `json:"action"`
	Property DesiredProperty `json:"property"`
}

// Kind returns the stable operation identity.
func (*Operation) Kind() schemaext.Kind { return OperationKind }

// CloneExtension returns an independent operation; a nil receiver remains
// typed nil.
func (v *Operation) CloneExtension() ast.ExtensionPayload { return v.Copy() }

// Copy is [Operation.CloneExtension] without the interface.
func (v *Operation) Copy() *Operation {
	if v == nil {
		return nil
	}
	clone := *v
	return &clone
}

// Effect reports that an extended property is metadata: no statement here
// reads or writes a row.
func (*Operation) Effect() schemaext.Effect {
	return schemaext.Effect{Impact: schemaext.Additive, Reason: "an extended property is metadata on an object; changing it touches no row"}
}

// Validate requires a known action and a valid property.
func (v *Operation) Validate() error {
	if v == nil {
		return fmt.Errorf("%w: extended property operation is nil", schemaext.ErrInvalidValue)
	}
	if v.Action != Add && v.Action != Update && v.Action != Drop {
		return fmt.Errorf("%w: extended property operation has action %q", schemaext.ErrInvalidValue, v.Action)
	}
	return ValidateDesired(&v.Property)
}

func operationCodec() schemaext.Codec {
	shape := schemaext.ObjectShape{Name: "SQL Server extended property operation", Allowed: []string{"action", "property"}, Required: []string{"action", "property"}}
	return schemaext.ModelCodec[*Operation]{
		Prototype: &Operation{}, Representation: schemaext.Operation, Version: 1,
		Definition: json.RawMessage(`{"action":"add, update or drop","property":"the declared property the procedure names"}`),
		Shape: func(data json.RawMessage) error {
			fields, err := schemaext.DecodeObject(data, shape)
			if err != nil {
				return err
			}
			_, err = Codecs()[0].Decode(fields["property"])
			return err
		},
		Validate: (*Operation).Validate,
		Clone:    (*Operation).Copy,
	}.Codec()
}

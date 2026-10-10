package sqlitetable

import (
	_ "embed" // Embed the definition that identifies the table options wire format.
	"encoding/json"
	"fmt"
	"slices"

	"ptah.run/core/schemaext"
)

//go:embed table-codecs.json
var tableDefinition []byte

// TableWireDefinition returns an independent description of the table options
// wire model. The definition covers both representations.
func TableWireDefinition() json.RawMessage { return slices.Clone(tableDefinition) }

// tableShape is the wire shape of both representations: each option is
// written only when it is on.
func tableShape(name string) schemaext.ObjectShape {
	return schemaext.ObjectShape{Name: name, Allowed: []string{"strict", "without_rowid"}, NonEmpty: []string{"strict", "without_rowid"}}
}

// TableCodecs returns the version-one desired and observed table options
// codecs, in that order. Each call returns independent definitions. A decoder
// accepts only the spelling the encoder writes: a key in another letter case,
// a null, and an option written out as false are refused. Every refusal is a
// [schemaext.InvalidModelError] wrapping [schemaext.ErrInvalidValue].
func TableCodecs() []schemaext.Codec {
	return []schemaext.Codec{
		schemaext.ModelCodec[*DesiredTable]{
			Prototype: &DesiredTable{}, Representation: schemaext.Desired, Version: 1, Definition: TableWireDefinition(),
			Shape: shape(tableShape("SQLite table options")), Validate: ValidateDesiredTable,
		}.Codec(),
		schemaext.ModelCodec[*ObservedTable]{
			Prototype: &ObservedTable{}, Representation: schemaext.Observed, Version: 1, Definition: TableWireDefinition(),
			Shape: shape(tableShape("SQLite table options observation")), Validate: ValidateObservedTable,
		}.Codec(),
	}
}

// TableCoverage records what one source knows about SQLite table options.
// knowledge is the claim for every table the source describes; subjects
// override it for individual tables.
func TableCoverage(representation schemaext.Representation, knowledge schemaext.Knowledge, subjects []schemaext.SubjectCoverage) (schemaext.Coverage, error) {
	owned := make([]schemaext.OwnedCodec, 0, 2)
	for _, codec := range TableCodecs() {
		owned = append(owned, schemaext.OwnedCodec{Owner: Owner, Codec: codec})
	}
	registry, err := schemaext.NewRegistry(owned...)
	if err != nil {
		return schemaext.Coverage{}, err
	}
	for _, model := range registry.Definitions() {
		if model.Kind == TableKind && model.Representation == representation {
			return schemaext.NewCoverage(representation, []schemaext.KindCoverage{{Model: model, Knowledge: knowledge}}, subjects)
		}
	}
	return schemaext.Coverage{}, fmt.Errorf("%w: SQLite table options coverage requires a schema representation", schemaext.ErrInvalidValue)
}

func shape(object schemaext.ObjectShape) func(json.RawMessage) error {
	return func(data json.RawMessage) error {
		_, err := schemaext.DecodeObject(data, object)
		return err
	}
}

// ChangeKind identifies a change to a surviving table's options.
//
// No comparison produces one: SQLite has no statement that changes either
// option of a table that exists. The kind is the change vocabulary the
// owner's comparison declares, and no planner accepts it.
const ChangeKind schemaext.Kind = "ptah.run/sqlite/table-change"

// Change retains the options a table holds and the options a declaration
// states for it. Both are required.
type Change struct {
	Before *ObservedTable `json:"before"`
	After  *DesiredTable  `json:"after"`
}

// Kind returns the owned change identity.
func (*Change) Kind() schemaext.Kind { return ChangeKind }

// CloneChange returns independent operands. A nil receiver remains typed nil.
func (v *Change) CloneChange() schemaext.ChangeValue { return v.Copy() }

// Copy is [Change.CloneChange] without the interface. A nil receiver returns
// nil.
func (v *Change) Copy() *Change {
	if v == nil {
		return nil
	}
	result := &Change{}
	if v.Before != nil {
		result.Before = new(*v.Before)
	}
	if v.After != nil {
		result.After = new(*v.After)
	}
	return result
}

// ValidateChange requires both operands, each valid in its representation.
func ValidateChange(v *Change) error {
	if v == nil || v.Before == nil || v.After == nil {
		return fmt.Errorf("%w: a SQLite table options change requires both operands", schemaext.ErrInvalidValue)
	}
	if err := ValidateObservedTable(v.Before); err != nil {
		return err
	}
	return ValidateDesiredTable(v.After)
}

// ChangeCodec returns the table options change codec. Each operand takes the
// exact desired or observed wire form, checked by that model's codec.
func ChangeCodec() schemaext.Codec {
	changeShape := schemaext.ObjectShape{Name: "SQLite table options change", Allowed: []string{"before", "after"}, Required: []string{"before", "after"}}
	return schemaext.ModelCodec[*Change]{
		Prototype: &Change{}, Representation: schemaext.Change, Version: 1,
		Definition: json.RawMessage(fmt.Sprintf(`{"operands":%s,"before":"the observed options","after":"the desired options"}`, TableWireDefinition())),
		Shape: func(data json.RawMessage) error {
			fields, err := schemaext.DecodeObject(data, changeShape)
			if err != nil {
				return err
			}
			codecs := TableCodecs()
			for key, codec := range map[string]schemaext.Codec{"after": codecs[0], "before": codecs[1]} {
				if _, err := codec.Decode(fields[key]); err != nil {
					return err
				}
			}
			return nil
		},
		Validate: ValidateChange,
		Clone:    (*Change).Copy,
	}.Codec()
}

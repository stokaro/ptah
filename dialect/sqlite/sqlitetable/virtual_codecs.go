package sqlitetable

import (
	_ "embed" // Embed the definition that identifies the virtual table wire format.
	"encoding/json"
	"fmt"
	"slices"

	"ptah.run/core/schemaext"
)

//go:embed virtual-codecs.json
var virtualDefinition []byte

// VirtualWireDefinition returns an independent description of the virtual
// table wire model. The definition covers both representations.
func VirtualWireDefinition() json.RawMessage { return slices.Clone(virtualDefinition) }

func virtualShape(name string) schemaext.ObjectShape {
	return schemaext.ObjectShape{Name: name, Allowed: []string{"arguments", "module"}, Required: []string{"module"}, NonEmpty: []string{"arguments", "module"}}
}

// VirtualCodecs returns the version-one desired and observed virtual table
// codecs, in that order. Each call returns independent definitions. A decoder
// accepts only the spelling the encoder writes: a key in another letter case,
// a null, an empty module and empty arguments written out are refused. Every
// refusal is a [schemaext.InvalidModelError] wrapping
// [schemaext.ErrInvalidValue].
func VirtualCodecs() []schemaext.Codec {
	return []schemaext.Codec{
		schemaext.ModelCodec[*DesiredVirtual]{
			Prototype: &DesiredVirtual{}, Representation: schemaext.Desired, Version: 1, Definition: VirtualWireDefinition(),
			Shape: shape(virtualShape("SQLite virtual table")), Validate: ValidateDesiredVirtual,
		}.Codec(),
		schemaext.ModelCodec[*ObservedVirtual]{
			Prototype: &ObservedVirtual{}, Representation: schemaext.Observed, Version: 1, Definition: VirtualWireDefinition(),
			Shape: shape(virtualShape("SQLite virtual table observation")), Validate: ValidateObservedVirtual,
		}.Codec(),
	}
}

// VirtualCoverage records what one source knows about SQLite virtual tables.
// knowledge is the claim for every table the source describes; subjects
// override it for individual tables. A source that can state a virtual table
// claims [schemaext.Complete], and its silence about one asks for an ordinary
// table, or for no table; a source with no syntax for one makes no claim.
func VirtualCoverage(representation schemaext.Representation, knowledge schemaext.Knowledge, subjects []schemaext.SubjectCoverage) (schemaext.Coverage, error) {
	owned := make([]schemaext.OwnedCodec, 0, 2)
	for _, codec := range VirtualCodecs() {
		owned = append(owned, schemaext.OwnedCodec{Owner: Owner, Codec: codec})
	}
	registry, err := schemaext.NewRegistry(owned...)
	if err != nil {
		return schemaext.Coverage{}, err
	}
	for _, model := range registry.Definitions() {
		if model.Kind == VirtualKind && model.Representation == representation {
			return schemaext.NewCoverage(representation, []schemaext.KindCoverage{{Model: model, Knowledge: knowledge}}, subjects)
		}
	}
	return schemaext.Coverage{}, fmt.Errorf("%w: SQLite virtual table coverage requires a schema representation", schemaext.ErrInvalidValue)
}

// VirtualChangeKind identifies a difference in what a surviving table is: a
// virtual table declared with another module or other arguments, or a name
// that is a virtual table on one side and an ordinary table on the other.
//
// SQLite has no ALTER VIRTUAL TABLE and no statement that turns one kind of
// table into the other, so no planner accepts the change; the comparison
// reports it so that a plan cannot call the two tables equal.
const VirtualChangeKind schemaext.Kind = "ptah.run/sqlite/virtual-table-change"

// VirtualChange retains the declaration a table holds and the one a desired
// state states for it. A nil operand is an ordinary table on that side; at
// least one operand is present.
type VirtualChange struct {
	Before *ObservedVirtual `json:"before,omitempty"`
	After  *DesiredVirtual  `json:"after,omitempty"`
}

// Kind returns the owned change identity.
func (*VirtualChange) Kind() schemaext.Kind { return VirtualChangeKind }

// CloneChange returns independent operands. A nil receiver remains typed nil.
func (v *VirtualChange) CloneChange() schemaext.ChangeValue { return v.Copy() }

// Copy is [VirtualChange.CloneChange] without the interface. A nil receiver
// returns nil.
func (v *VirtualChange) Copy() *VirtualChange {
	if v == nil {
		return nil
	}
	result := &VirtualChange{}
	if v.Before != nil {
		result.Before = new(*v.Before)
	}
	if v.After != nil {
		result.After = new(*v.After)
	}
	return result
}

// ValidateVirtualChange requires at least one operand, each valid in its
// representation.
func ValidateVirtualChange(v *VirtualChange) error {
	if v == nil || (v.Before == nil && v.After == nil) {
		return fmt.Errorf("%w: a SQLite virtual table change requires an operand", schemaext.ErrInvalidValue)
	}
	if v.Before != nil {
		if err := ValidateObservedVirtual(v.Before); err != nil {
			return err
		}
	}
	if v.After != nil {
		return ValidateDesiredVirtual(v.After)
	}
	return nil
}

// VirtualChangeCodec returns the virtual table change codec. Each operand
// present takes the exact desired or observed wire form, checked by that
// model's codec.
func VirtualChangeCodec() schemaext.Codec {
	changeShape := schemaext.ObjectShape{Name: "SQLite virtual table change", Allowed: []string{"after", "before"}}
	return schemaext.ModelCodec[*VirtualChange]{
		Prototype: &VirtualChange{}, Representation: schemaext.Change, Version: 1,
		Definition: json.RawMessage(fmt.Sprintf(`{"operands":%s,"before":"the observed declaration, absent for an ordinary table","after":"the desired declaration, absent for an ordinary table"}`, VirtualWireDefinition())),
		Shape: func(data json.RawMessage) error {
			fields, err := schemaext.DecodeObject(data, changeShape)
			if err != nil {
				return err
			}
			codecs := VirtualCodecs()
			for key, codec := range map[string]schemaext.Codec{"after": codecs[0], "before": codecs[1]} {
				if operand, present := fields[key]; present {
					if _, err := codec.Decode(operand); err != nil {
						return err
					}
				}
			}
			return nil
		},
		Validate: ValidateVirtualChange,
		Clone:    (*VirtualChange).Copy,
	}.Codec()
}

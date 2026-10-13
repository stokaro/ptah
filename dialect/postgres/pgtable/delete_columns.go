package pgtable

import (
	"encoding/json"
	"fmt"
	"slices"

	"ptah.run/core/objectidentity"
	"ptah.run/core/schemaext"
)

// DeleteColumnsKind identifies PostgreSQL's column selection for ON DELETE.
const DeleteColumnsKind schemaext.Kind = "ptah.run/postgres/foreign-key-delete-columns"

// DeleteColumns limits ON DELETE SET NULL or SET DEFAULT to local columns of
// a foreign key. An empty list selects the entire key. Column names are logical
// identifiers, without SQL quoting. Order and repeated names do not change the
// action, but the model preserves their spelling for source rendering.
type DeleteColumns struct {
	Columns []string `json:"columns,omitempty"`
}

// DesiredDeleteColumns declares the columns a foreign key's delete action sets.
type DesiredDeleteColumns DeleteColumns

// ObservedDeleteColumns records the column selection a foreign key holds.
type ObservedDeleteColumns DeleteColumns

// Kind returns the owned column-selection identity.
func (*DesiredDeleteColumns) Kind() schemaext.Kind { return DeleteColumnsKind }

// Kind returns the owned column-selection identity.
func (*ObservedDeleteColumns) Kind() schemaext.Kind { return DeleteColumnsKind }

// Clone returns independent columns. A nil receiver remains typed nil.
func (d *DesiredDeleteColumns) Clone() schemaext.Value {
	if d == nil {
		return (*DesiredDeleteColumns)(nil)
	}
	return &DesiredDeleteColumns{Columns: slices.Clone(d.Columns)}
}

// Clone returns independent columns. A nil receiver remains typed nil.
func (d *ObservedDeleteColumns) Clone() schemaext.Value {
	if d == nil {
		return (*ObservedDeleteColumns)(nil)
	}
	return &ObservedDeleteColumns{Columns: slices.Clone(d.Columns)}
}

// Equal compares source declarations in order; semantic comparison also uses
// the common key's columns. Different representations are unequal.
func (d *DesiredDeleteColumns) Equal(other schemaext.Value) bool {
	v, ok := other.(*DesiredDeleteColumns)
	if !ok || d == nil || v == nil {
		return ok && d == nil && v == nil
	}
	return slices.Equal(d.Columns, v.Columns)
}

// Equal compares observed column spellings in order.
func (d *ObservedDeleteColumns) Equal(other schemaext.Value) bool {
	v, ok := other.(*ObservedDeleteColumns)
	if !ok || d == nil || v == nil {
		return ok && d == nil && v == nil
	}
	return slices.Equal(d.Columns, v.Columns)
}

// ValidateDesiredDeleteColumns refuses nil and invalid column names. Errors
// wrap schemaext.ErrInvalidValue. An empty selection means all key columns.
func ValidateDesiredDeleteColumns(d *DesiredDeleteColumns) error {
	if d == nil {
		return fmt.Errorf("%w: ON DELETE column selection requires a declaration", schemaext.ErrInvalidValue)
	}
	return validateDeleteColumns(d.Columns)
}

// ValidateObservedDeleteColumns applies the same checks to an observation.
func ValidateObservedDeleteColumns(d *ObservedDeleteColumns) error {
	if d == nil {
		return fmt.Errorf("%w: ON DELETE column selection requires an observation", schemaext.ErrInvalidValue)
	}
	return validateDeleteColumns(d.Columns)
}

func validateDeleteColumns(columns []string) error {
	for _, column := range columns {
		if column == "" {
			return fmt.Errorf("%w: ON DELETE column selection has an empty name", schemaext.ErrInvalidValue)
		}
		if err := schemaext.ValidText("ON DELETE column", column); err != nil {
			return err
		}
	}
	return nil
}

// WithDeleteColumns adds a declaration without changing facets. An empty
// selection adds nothing; an existing selection is refused by Facets.With.
func WithDeleteColumns(facets schemaext.Facets, columns []string) (schemaext.Facets, error) {
	if len(columns) == 0 {
		return facets, nil
	}
	value := &DesiredDeleteColumns{Columns: slices.Clone(columns)}
	if err := ValidateDesiredDeleteColumns(value); err != nil {
		return schemaext.Facets{}, err
	}
	return facets.With(value)
}

// WithObservedDeleteColumns adds an observation under WithDeleteColumns's rules.
func WithObservedDeleteColumns(facets schemaext.Facets, columns []string) (schemaext.Facets, error) {
	if len(columns) == 0 {
		return facets, nil
	}
	value := &ObservedDeleteColumns{Columns: slices.Clone(columns)}
	if err := ValidateObservedDeleteColumns(value); err != nil {
		return schemaext.Facets{}, err
	}
	return facets.With(value)
}

// DeleteColumnsOf returns independent columns from either representation. An
// absent facet returns nil and false. Invalid values wrap ErrInvalidValue.
func DeleteColumnsOf(facets schemaext.Facets) ([]string, bool, error) {
	value, found, err := facets.Get(DeleteColumnsKind)
	if err != nil || !found {
		return nil, found, err
	}
	switch d := value.(type) {
	case *DesiredDeleteColumns:
		if err := ValidateDesiredDeleteColumns(d); err != nil {
			return nil, true, err
		}
		return slices.Clone(d.Columns), true, nil
	case *ObservedDeleteColumns:
		if err := ValidateObservedDeleteColumns(d); err != nil {
			return nil, true, err
		}
		return slices.Clone(d.Columns), true, nil
	default:
		return nil, true, fmt.Errorf("%w: unexpected ON DELETE column selection %T", schemaext.ErrInvalidValue, value)
	}
}

const deleteColumnsDefinition = `{"type":"object","additionalProperties":false,"properties":{"columns":{"type":"array","items":{"type":"string","minLength":1}}}}`

// DeleteColumnsCodecs returns version-one codecs with exact object shapes.
// Unknown attributes, duplicate keys, null values and invalid names are refused.
func DeleteColumnsCodecs() []schemaext.Codec {
	return []schemaext.Codec{
		schemaext.ModelCodec[*DesiredDeleteColumns]{Prototype: &DesiredDeleteColumns{}, Representation: schemaext.Desired, Version: 1,
			Definition: json.RawMessage(deleteColumnsDefinition), Shape: deleteColumnsShape, Validate: ValidateDesiredDeleteColumns}.Codec(),
		schemaext.ModelCodec[*ObservedDeleteColumns]{Prototype: &ObservedDeleteColumns{}, Representation: schemaext.Observed, Version: 1,
			Definition: json.RawMessage(deleteColumnsDefinition), Shape: deleteColumnsShape, Validate: ValidateObservedDeleteColumns}.Codec(),
	}
}
func deleteColumnsShape(data json.RawMessage) error {
	_, err := schemaext.DecodeObject(data, schemaext.ObjectShape{Name: "ON DELETE column selection", Allowed: []string{"columns"}})
	return err
}

var deleteColumnsCoverage = schemaext.OwnedCoverageSource(Owner, DeleteColumnsCodecs)

// DeleteColumnsCoverage records source knowledge. Complete coverage interprets
// an absent facet as selection of all columns in the common foreign key.
func DeleteColumnsCoverage(representation schemaext.Representation, knowledge schemaext.Knowledge, subjects []schemaext.SubjectCoverage) (schemaext.Coverage, error) {
	return deleteColumnsCoverage(DeleteColumnsKind, representation, knowledge, subjects)
}

// HoldsDeleteColumns reports whether subject names a table constraint. The
// common definition must identify a FOREIGN KEY before the setting is applied.
func HoldsDeleteColumns(subject objectidentity.ID) bool {
	return subject.Kind == objectidentity.KindConstraint && !subject.Parent.Empty() && !subject.Name.Empty()
}

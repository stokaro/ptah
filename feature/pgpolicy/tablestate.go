package pgpolicy

import (
	"fmt"

	"ptah.run/core/objectidentity"
	"ptah.run/core/schemaext"
)

// DesiredTableState declares a table's row-security switches, a facet of the
// table. The two are independent flags of the relation, not two strengths of
// one setting: ENABLE makes the table's policies apply to every role but the
// table's owner, FORCE makes them apply to the owner too, and PostgreSQL
// accepts either without the other.
//
// Where a source describes the facet completely, a table without it declares
// both flags off.
type DesiredTableState struct {
	Enabled bool `json:"enabled"`
	Forced  bool `json:"forced"`
	// Comment is written before the statements. It is source documentation,
	// not a catalog property: a schema comparison ignores it, while Equal,
	// which compares values field by field, does not.
	Comment string `json:"comment,omitempty"`
	// StructName preserves the Go struct a declaration was read from.
	StructName string `json:"struct_name,omitempty"`
}

// DefaultedSwitches is the knowledge a source records for the switches of a
// table it declares policies for and no switches (stokaro/ptah#2048): the
// declaration requests the owner's default. The owner keeps the switches of a
// table that exists as they are, so a comparison plans neither ENABLE nor
// DISABLE, and enables a table the plan creates, whose policies would
// otherwise protect nothing. A switch value declared elsewhere for the same
// table takes precedence.
func DefaultedSwitches() schemaext.Knowledge {
	return schemaext.Knowledge{State: schemaext.Defaulted,
		Reason: "the declaration names the table's row-level security policies and not its switches"}
}

// ObservedTableState is a table's row-security switches as pg_class reports
// them: relrowsecurity and relforcerowsecurity.
type ObservedTableState struct {
	Enabled bool `json:"enabled"`
	Forced  bool `json:"forced"`
}

// Kind returns the table-state model identity.
func (*DesiredTableState) Kind() schemaext.Kind { return TableStateKind }

// Kind returns the table-state model identity.
func (*ObservedTableState) Kind() schemaext.Kind { return TableStateKind }

// Clone returns an independent declaration. A nil receiver remains typed nil.
func (v *DesiredTableState) Clone() schemaext.Value {
	if v == nil {
		return (*DesiredTableState)(nil)
	}
	return new(*v)
}

// Clone returns an independent observation. A nil receiver remains typed nil.
func (v *ObservedTableState) Clone() schemaext.Value {
	if v == nil {
		return (*ObservedTableState)(nil)
	}
	return new(*v)
}

// Equal compares declarations field by field.
func (v *DesiredTableState) Equal(other schemaext.Value) bool {
	right, ok := other.(*DesiredTableState)
	if !ok || v == nil || right == nil {
		return ok && v == nil && right == nil
	}
	return *v == *right
}

// Equal compares observations field by field.
func (v *ObservedTableState) Equal(other schemaext.Value) bool {
	right, ok := other.(*ObservedTableState)
	if !ok || v == nil || right == nil {
		return ok && v == nil && right == nil
	}
	return *v == *right
}

// ValidateDesiredTableState checks representation invariants.
func ValidateDesiredTableState(v *DesiredTableState) error {
	if v == nil {
		return modelError(TableStateKind, schemaext.Desired, fmt.Errorf("%w: nil row-security table declaration", schemaext.ErrInvalidValue))
	}
	if err := validTexts("comment", v.Comment, "struct name", v.StructName); err != nil {
		return modelError(TableStateKind, schemaext.Desired, err)
	}
	return nil
}

// ValidateObservedTableState checks representation invariants.
func ValidateObservedTableState(v *ObservedTableState) error {
	if v == nil {
		return modelError(TableStateKind, schemaext.Observed, fmt.Errorf("%w: nil row-security table observation", schemaext.ErrInvalidValue))
	}
	return nil
}

// TableStateSubject is the identity of a table's row-security switches in a
// plan's effects: the table's own identity under the switches' kind. The table
// belongs to the host, which alters it in steps of its own, so a switch change
// and a change to the table's definition stay separate writers of separate
// subjects.
func TableStateSubject(table objectidentity.ID) objectidentity.ID {
	table.Kind = objectidentity.Kind(TableStateKind)
	return table
}

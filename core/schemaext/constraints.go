package schemaext

import (
	"fmt"
	"slices"

	"ptah.run/core/objectidentity"
)

// DefinedConstraint is a feature object's participation in its parent table's
// constraint namespace. Ref retains the feature kind. Indexed reports a
// backing relation of the same name. Expressions carry column and routine
// references that must survive source edits and creation ordering.
type DefinedConstraint struct {
	Ref         objectidentity.ID
	Indexed     bool
	Expressions []string
}

// ConstraintDefiner exposes a validated, local projection of an owned table
// constraint. It performs no I/O and cannot call a provider. A provider adapter
// captures the projection with its decoded values; semantic changes use a
// batched service, not this accessor.
type ConstraintDefiner interface {
	DefinedConstraint(ref objectidentity.ID) DefinedConstraint
}

// DefinedConstraints returns independent projections in object identity
// order. The zero collection returns nil. Objects without ConstraintDefiner
// contribute no projection. Collections validate projections when they capture
// the values and return ErrInvalidValue for changed identities or absent parents.
func DefinedConstraints(objects Objects) []DefinedConstraint {
	var result []DefinedConstraint
	for _, ref := range objects.Refs() {
		if definer, ok := objects.values[ref.Key()].Value.(ConstraintDefiner); ok {
			definition := definer.DefinedConstraint(ref)
			definition.Expressions = slices.Clone(definition.Expressions)
			result = append(result, definition)
		}
	}
	return result
}

func validDefinedConstraint(object Object) error {
	definer, ok := object.Value.(ConstraintDefiner)
	if !ok {
		return nil
	}
	definition := definer.DefinedConstraint(object.Ref)
	if definition.Ref != object.Ref || definition.Ref.Parent.Empty() {
		return fmt.Errorf("%w: owned constraint projection must retain its identity and parent", ErrInvalidValue)
	}
	return nil
}

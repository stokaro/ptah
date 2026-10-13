package builtin

import (
	"slices"

	"ptah.run/core/ast"
	"ptah.run/core/platform/capability"
	"ptah.run/dialect/postgres/pgtablerender"
)

// Source declarations remain syntax until the SQL reader assigns identities.
// The selected built-in owner validates them without a synthetic object name.
func prepareOwnedConstraints(dialect string, caps capability.Capabilities, values []*ast.OwnedIndexConstraint) ([]*ast.OwnedIndexConstraint, error) {
	result := slices.Clone(values)
	for i, value := range values {
		cloned, err := value.Clone()
		if err != nil {
			return nil, err
		}
		result[i] = cloned
	}
	if err := pgtablerender.ValidateSourceExclusions(dialect, caps, result); err != nil {
		return nil, err
	}
	return result, nil
}

// Rebind the source order to the prepared nodes. A renderer must not recover
// unprepared caller nodes from Elements after validating their cloned slices.
func prepareTableElements(original, prepared *ast.CreateTableNode) {
	prepared.Elements = slices.Clone(original.Elements)
	for i, element := range original.Elements {
		element.Column = preparedElement(element.Column, original.Columns, prepared.Columns)
		element.Constraint = preparedElement(element.Constraint, original.Constraints, prepared.Constraints)
		element.Index = preparedElement(element.Index, original.Indexes, prepared.Indexes)
		element.OwnedConstraint = preparedElement(element.OwnedConstraint, original.OwnedConstraints, prepared.OwnedConstraints)
		element.Objects = slices.Clone(element.Objects)
		prepared.Elements[i] = element
	}
}

func preparedElement[T any](node *T, original, prepared []*T) *T {
	position := slices.Index(original, node)
	if position >= 0 {
		return prepared[position]
	}
	return node
}

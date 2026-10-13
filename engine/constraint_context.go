package engine

import (
	"fmt"
	"slices"
	"strings"

	"ptah.run/core/objectidentity"
	"ptah.run/core/schemaext"
	"ptah.run/internal/constraintkind"
)

func snapshotConstraintContexts(contexts []schemaext.ConstraintContext, owners []schemaext.ParentState, representation schemaext.Representation) ([]schemaext.ConstraintContext, error) {
	result := slices.Clone(contexts)
	seen := make(map[objectidentity.Key]bool, len(contexts))
	for i, definition := range contexts {
		ref := definition.Subject
		definition.Type = strings.ToUpper(strings.TrimSpace(definition.Type))
		owner, found := facetOwner(owners, ref)
		if !found || representation == schemaext.Desired && !owner.Desired || representation == schemaext.Observed && !owner.Current ||
			(ref.Kind != objectidentity.KindConstraint && ref.Kind != objectidentity.KindPrimaryKey) ||
			seen[ref.Key()] || !constraintkind.Valid(definition.Type) ||
			(ref.Kind == objectidentity.KindPrimaryKey) != (definition.Type == "PRIMARY KEY") {
			return nil, fmt.Errorf("%w: invalid or duplicate constraint context %s", schemaext.ErrInvalidValue, ref)
		}
		if err := schemaext.ValidText("constraint type", definition.Type); err != nil {
			return nil, err
		}
		for _, column := range definition.Columns {
			if column == "" {
				return nil, fmt.Errorf("%w: constraint context %s has an empty column name", schemaext.ErrInvalidValue, ref)
			}
			if err := schemaext.ValidText("constraint column", column); err != nil {
				return nil, err
			}
		}
		seen[ref.Key()] = true
		result[i] = definition.Clone()
	}
	slices.SortFunc(result, func(a, b schemaext.ConstraintContext) int { return schemaext.CompareRefs(a.Subject, b.Subject) })
	return result, nil
}

func constraintContextsOfKinds(contexts []schemaext.ConstraintContext, kinds []objectidentity.Kind) []schemaext.ConstraintContext {
	return slices.DeleteFunc(slices.Clone(contexts), func(definition schemaext.ConstraintContext) bool {
		return !slices.Contains(kinds, definition.Subject.Kind)
	})
}

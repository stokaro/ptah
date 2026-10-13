package schemadiff

import (
	"slices"
	"strings"

	"ptah.run/catalog"
	"ptah.run/core/objectidentity"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
)

// Use the same slot identities as feature capture. Rebuilding names here
// would let quoted names or primary-key aliases bind to different subjects.
func comparisonConstraintContexts(desired *schemamodel.Database, current *catalog.Database, target string, semantics identifier.Semantics) (declared, observed []schemaext.ConstraintContext, err error) {
	declaredSlots, err := declaredFacetSlots(desired, target, semantics)
	if err != nil {
		return nil, nil, err
	}
	declaredSubjects := facetSlotSubjects(declaredSlots)
	observedSubjects := facetSlotSubjects(observedFacetSlots(current, target, semantics))
	for i := range desired.Constraints {
		constraint := &desired.Constraints[i]
		if subject, found := declaredSubjects[&constraint.Facets]; found {
			declared = append(declared, schemaext.ConstraintContext{Subject: subject,
				Type: strings.ToUpper(strings.TrimSpace(constraint.Type)), Columns: slices.Clone(constraint.Columns)})
		}
	}
	for i := range current.Constraints {
		constraint := &current.Constraints[i]
		if subject, found := observedSubjects[&constraint.Facets]; found {
			observed = append(observed, schemaext.ConstraintContext{Subject: subject,
				Type: strings.ToUpper(strings.TrimSpace(constraint.Type)), Columns: slices.Clone(constraint.ColumnNamesOrDefault())})
		}
	}
	return declared, observed, nil
}

func facetSlotSubjects(slots []facetOwnerSlot) map[*schemaext.Facets]objectidentity.ID {
	result := make(map[*schemaext.Facets]objectidentity.ID, len(slots))
	for _, slot := range slots {
		result[slot.values] = slot.subject
	}
	return result
}

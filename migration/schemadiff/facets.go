package schemadiff

import (
	"fmt"
	"slices"

	"ptah.run/catalog"
	"ptah.run/core/objectidentity"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
)

func captureFeatureStates(desired *schemamodel.Database, current *catalog.Database, target string, semantics identifier.Semantics) (declared, observed schemaext.FeatureState, err error) {
	declared = schemaext.FeatureState{Objects: desired.FeatureObjects, Coverage: desired.FeatureCoverage}
	observed = schemaext.FeatureState{Objects: current.FeatureObjects, Coverage: current.FeatureCoverage}
	// Other attachment points require their own identity capture before they can
	// participate. Refuse them here so no attached value silently disappears.
	declaredSlots, err := declaredFacetSlots(desired, target, semantics)
	if err != nil {
		return schemaext.FeatureState{}, schemaext.FeatureState{}, err
	}
	captured := make(map[*schemaext.Facets]bool)
	for _, slot := range declaredSlots {
		captured[slot.values] = true
		if !slot.values.IsZero() {
			declared.Facets = append(declared.Facets, schemaext.FacetRecord{Subject: slot.subject, Values: *slot.values})
		}
	}
	observedSlots := observedFacetSlots(current, target, semantics)
	for _, slot := range observedSlots {
		captured[slot.values] = true
		if !slot.values.IsZero() {
			observed.Facets = append(observed.Facets, schemaext.FacetRecord{Subject: slot.subject, Values: *slot.values})
		}
	}
	declared.Coverage = followOwners(declared.Coverage, declaredSlots)
	observed.Coverage = followOwners(observed.Coverage, observedSlots)
	// A field of a struct that declares no table is no column: nothing renders
	// or compares it, so its facets go nowhere either, like the rest of its
	// declaration. An embedded type's own fields are the case; the columns
	// they become are fields of the embedding table and are captured above.
	for _, slot := range tablelessFieldFacets(desired) {
		captured[slot] = true
	}
	for _, slots := range [][]*schemaext.Facets{desired.FacetSlots(), current.FacetSlots()} {
		for _, slot := range slots {
			if !captured[slot] && !slot.IsZero() {
				return schemaext.FeatureState{}, schemaext.FeatureState{}, fmt.Errorf("%w: no comparison identity capture for attached facets %v", ptaherr.ErrUnsupportedFeature, slot.Kinds())
			}
		}
	}
	return declared, observed, nil
}

func effectiveFeatureState(desired *schemamodel.Database, state schemaext.FeatureState, target string, semantics identifier.Semantics) (*schemamodel.Database, error) {
	effective := *desired
	effective.FeatureObjects, effective.FeatureCoverage = state.Objects, state.Coverage
	effective.Tables = slices.Clone(desired.Tables)
	effective.Fields = slices.Clone(desired.Fields)
	effective.Indexes = slices.Clone(desired.Indexes)
	effective.Constraints = slices.Clone(desired.Constraints)
	effective.MaterializedViews = slices.Clone(desired.MaterializedViews)
	effective.Functions = slices.Clone(desired.Functions)
	declared, err := declaredFacetSlots(&effective, target, semantics)
	if err != nil {
		return nil, err
	}
	positions := make(map[objectidentity.Key]*schemaext.Facets)
	for _, slot := range declared {
		if _, duplicate := positions[slot.subject.Key()]; duplicate {
			return nil, fmt.Errorf("%w: duplicate facet owner %s", ptaherr.ErrInvalidSchemaDiff, slot.subject)
		}
		positions[slot.subject.Key()] = slot.values
		*slot.values = schemaext.Facets{}
	}
	seen := make(map[objectidentity.Key]bool)
	for _, record := range state.Facets {
		key := record.Subject.Key()
		values, found := positions[key]
		if !found || seen[key] {
			return nil, fmt.Errorf("%w: missing or duplicate facet owner %s", ptaherr.ErrInvalidSchemaDiff, record.Subject)
		}
		seen[key] = true
		*values = record.Values
	}
	return &effective, nil
}

// followOwners drops the knowledge a side holds about the attached settings
// of an owner outside that side of the comparison: a table, column, index,
// primary key, materialized view, function, or procedure. A dialect scope,
// exclusion, or schema selection can remove the owner. Its attached knowledge
// follows it, so the comparison describes the selected objects. Knowledge of
// named feature objects belongs to those objects (stokaro/ptah#4278).
func followOwners(coverage schemaext.Coverage, slots []facetOwnerSlot) schemaext.Coverage {
	owners := make(map[objectidentity.Key]bool, len(slots))
	for _, slot := range slots {
		owners[slot.subject.Key()] = true
	}
	return coverage.SelectSubjects(func(subject objectidentity.ID) bool {
		switch subject.Kind {
		case objectidentity.KindTable, objectidentity.KindColumn, objectidentity.KindIndex, objectidentity.KindPrimaryKey, objectidentity.KindMatView, objectidentity.KindFunction, objectidentity.KindProcedure:
			return owners[subject.Key()]
		default:
			return true
		}
	})
}

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
	captured := make(map[*schemaext.Facets]bool)
	for _, slot := range declaredFacetSlots(desired, target, semantics) {
		captured[slot.values] = true
		if !slot.values.IsZero() {
			declared.Facets = append(declared.Facets, schemaext.FacetRecord{Subject: slot.subject, Values: *slot.values})
		}
	}
	for _, slot := range observedFacetSlots(current, target, semantics) {
		captured[slot.values] = true
		if !slot.values.IsZero() {
			observed.Facets = append(observed.Facets, schemaext.FacetRecord{Subject: slot.subject, Values: *slot.values})
		}
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
	effective.Indexes = slices.Clone(desired.Indexes)
	positions := make(map[objectidentity.Key]*schemaext.Facets)
	for _, slot := range declaredFacetSlots(&effective, target, semantics) {
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

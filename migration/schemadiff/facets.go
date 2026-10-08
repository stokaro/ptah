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
	"ptah.run/migration/schemadiff/internal/compare"
)

func captureFeatureStates(desired *schemamodel.Database, current *catalog.Database, target string, semantics identifier.Semantics) (declared, observed schemaext.FeatureState, err error) {
	declared = schemaext.FeatureState{Objects: desired.FeatureObjects, Coverage: desired.FeatureCoverage}
	observed = schemaext.FeatureState{Objects: current.FeatureObjects, Coverage: current.FeatureCoverage}
	// Table identity is shared with named child comparison and common changes.
	// Other attachment points require their own identity capture before they can
	// participate. Refuse them here so no attached value silently disappears.
	captured := make(map[*schemaext.Facets]bool)
	for i := range desired.Tables {
		table := &desired.Tables[i]
		captured[&table.Facets] = true
		if !table.Facets.IsZero() {
			declared.Facets = append(declared.Facets, schemaext.FacetRecord{Subject: compare.TableSubject(table.Schema, table.Name, target, semantics), Values: table.Facets})
		}
	}
	for i := range current.Tables {
		table := &current.Tables[i]
		captured[&table.Facets] = true
		if !table.Facets.IsZero() {
			observed.Facets = append(observed.Facets, schemaext.FacetRecord{Subject: compare.TableSubject(table.Schema, table.Name, target, semantics), Values: table.Facets})
		}
	}
	for _, slots := range [][]*schemaext.Facets{desired.FacetSlots(), current.FacetSlots()} {
		for _, slot := range slots {
			if !captured[slot] && !slot.IsZero() {
				return schemaext.FeatureState{}, schemaext.FeatureState{}, fmt.Errorf("%w: no comparison identity capture for non-table facets %v", ptaherr.ErrUnsupportedFeature, slot.Kinds())
			}
		}
	}
	return declared, observed, nil
}

func effectiveFeatureState(desired *schemamodel.Database, state schemaext.FeatureState, target string, semantics identifier.Semantics) (*schemamodel.Database, error) {
	effective := *desired
	effective.FeatureObjects, effective.FeatureCoverage = state.Objects, state.Coverage
	effective.Tables = slices.Clone(desired.Tables)
	positions := make(map[objectidentity.Key]int, len(effective.Tables))
	for i := range effective.Tables {
		table := &effective.Tables[i]
		positions[compare.TableSubject(table.Schema, table.Name, target, semantics).Key()] = i
		table.Facets = schemaext.Facets{}
	}
	seen := make(map[objectidentity.Key]bool)
	for _, record := range state.Facets {
		key := record.Subject.Key()
		i, found := positions[key]
		if !found || seen[key] {
			return nil, fmt.Errorf("%w: missing or duplicate table facet owner %s", ptaherr.ErrInvalidSchemaDiff, record.Subject)
		}
		seen[key] = true
		effective.Tables[i].Facets = record.Values
	}
	return &effective, nil
}

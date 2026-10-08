package engine

import (
	"slices"

	"ptah.run/core/schemaext"
)

// Scope precedes codec validation on both sides. Otherwise an excluded foreign
// model still needs its implementation, or complete coverage turns it into a
// removal request. The retained binding makes repeated selection idempotent.
func scopeFacetComparison(request schemaext.FacetComparisonRequest, target schemaext.TargetSelection) (schemaext.FacetComparisonRequest, error) {
	request.Desired.Records = slices.Clone(request.Desired.Records)
	for i, record := range request.Desired.Records {
		projected, err := record.Values.ForTarget(target)
		if err != nil {
			return schemaext.FacetComparisonRequest{}, err
		}
		request.Desired.Records[i].Values = projected
	}
	request.Current.Records = slices.Clone(request.Current.Records)
	for i, record := range request.Current.Records {
		for _, kind := range record.Values.DeclaredKinds() {
			if !request.Includes(kind, record.Subject) {
				record.Values = record.Values.Without(kind)
			}
		}
		request.Current.Records[i] = record
	}
	var err error
	request.Desired.Coverage, err = selectedFacetCoverage(request, request.Desired.Coverage)
	if err != nil {
		return schemaext.FacetComparisonRequest{}, err
	}
	request.Current.Coverage, err = selectedFacetCoverage(request, request.Current.Coverage)
	if err != nil {
		return schemaext.FacetComparisonRequest{}, err
	}
	return request, nil
}

func selectedFacetCoverage(request schemaext.FacetComparisonRequest, coverage schemaext.Coverage) (schemaext.Coverage, error) {
	if coverage.IsZero() {
		return coverage, nil
	}
	kinds := slices.DeleteFunc(coverage.KindRecords(), func(record schemaext.KindCoverage) bool {
		return facetKindExcluded(request, record.Model.Kind)
	})
	subjects := slices.DeleteFunc(coverage.SubjectRecords(), func(record schemaext.SubjectCoverage) bool {
		return !request.Includes(record.Kind, record.Subject)
	})
	return schemaext.NewCoverage(coverage.Representation(), kinds, subjects)
}

func facetKindExcluded(request schemaext.FacetComparisonRequest, kind schemaext.Kind) bool {
	return len(request.Owners) != 0 && !slices.ContainsFunc(request.Owners, func(owner schemaext.ParentState) bool {
		return request.Includes(kind, owner.Subject)
	})
}

func sameFacetScope(first, second schemaext.Facets, kind schemaext.Kind) bool {
	return slices.Equal(first.TargetScope(kind), second.TargetScope(kind)) &&
		slices.Contains(first.Kinds(), kind) == slices.Contains(second.Kinds(), kind)
}

package engine

import (
	"cmp"
	"context"
	"fmt"
	"maps"
	"slices"

	"ptah.run/core/objectidentity"
	"ptah.run/core/schemaext"
)

// CompareFeatures accounts for named feature objects and attached settings in
// one operation. Both input surfaces are validated before either kind of owner
// runs. An error on either surface discards all changes from both surfaces.
// Each side's not-described records are folded into the coverage of the
// models a provider maps them onto before any owner runs; see
// [Provider.CoverageLimits].
func (r *Runtime) CompareFeatures(ctx context.Context, request schemaext.ComparisonRequest) (schemaext.ComparisonResult, error) {
	if err := schemaext.RequireRuntime(ctx, r); err != nil {
		return schemaext.ComparisonResult{}, err
	}
	selected, err := r.ResolveTarget(request.Target)
	if err != nil {
		return schemaext.ComparisonResult{}, err
	}
	request.Target = selected.Name()
	if err := r.validateExclusions(request.Exclusions); err != nil {
		return schemaext.ComparisonResult{}, err
	}
	if request, err = r.foldCoverageLimits(request); err != nil {
		return schemaext.ComparisonResult{}, err
	}
	request, skipped, err := r.scopeComparisonOffTarget(request)
	if err != nil {
		return schemaext.ComparisonResult{}, err
	}
	facetKinds, objectKinds, err := r.featureComparisonKinds(request)
	if err != nil {
		return schemaext.ComparisonResult{}, err
	}
	objects, facets, err := r.prepareFeatureComparisons(ctx, request, objectKinds, facetKinds)
	if err != nil {
		return schemaext.ComparisonResult{}, err
	}
	objectResult, err := r.CompareObjects(ctx, objects)
	if err != nil {
		return schemaext.ComparisonResult{}, err
	}
	facetResult, err := r.CompareFacets(ctx, facets)
	if err != nil {
		return schemaext.ComparisonResult{}, err
	}
	coverage, err := objectResult.Desired.Coverage.Combine(facetResult.Desired.Coverage)
	if err != nil {
		return schemaext.ComparisonResult{}, err
	}
	result := schemaext.ComparisonResult{
		Complete: true,
		Desired: schemaext.FeatureState{
			Objects: objectResult.Desired.Objects, Facets: facetResult.Desired.Records, Coverage: coverage,
		},
		Changes: objectResult.Changes, FacetChanges: facetResult.Changes,
		Undecided: append(slices.Clone(objectResult.Undecided), facetResult.Undecided...),
		Skipped:   skipped,
	}
	if err := ctx.Err(); err != nil {
		return schemaext.ComparisonResult{}, err
	}
	slices.SortFunc(result.Undecided, func(a, b schemaext.UndecidedChange) int {
		return cmp.Or(cmp.Compare(a.Kind, b.Kind), schemaext.CompareRefs(a.Subject, b.Subject))
	})
	return result, nil
}

// scopeComparisonOffTarget leaves out, on both sides, the objects the target
// does not serve whose owners answer [schemaext.OffTargetSkip], and the
// coverage of their models, which no owner on the target reads. It returns
// the subjects it left out, each once, desired side first. An object whose
// owner answers [schemaext.OffTargetRefuse] is refused.
func (r *Runtime) scopeComparisonOffTarget(request schemaext.ComparisonRequest) (schemaext.ComparisonRequest, []schemaext.SkippedObject, error) {
	desired, skipped, err := r.ScopeOffTarget(request.Target, request.Desired.Objects)
	if err != nil {
		return schemaext.ComparisonRequest{}, nil, err
	}
	current, skippedCurrent, err := r.ScopeOffTarget(request.Target, request.Current.Objects)
	if err != nil {
		return schemaext.ComparisonRequest{}, nil, err
	}
	for _, object := range skippedCurrent {
		if !slices.ContainsFunc(skipped, func(held schemaext.SkippedObject) bool { return held.Subject.Key() == object.Subject.Key() }) {
			skipped = append(skipped, object)
		}
	}
	request.Desired.Objects, request.Current.Objects = desired, current
	request.Desired.Coverage = r.coverageOnTarget(request.Target, request.Desired.Coverage)
	request.Current.Coverage = r.coverageOnTarget(request.Target, request.Current.Coverage)
	return request, skipped, nil
}

// coverageOnTarget returns coverage without the records of the models target
// skips: the ones it does not serve whose owners answer
// [schemaext.OffTargetSkip] there. Coverage that holds none comes back as it
// is.
func (r *Runtime) coverageOnTarget(target string, coverage schemaext.Coverage) schemaext.Coverage {
	selected, _ := r.lookup(target)
	skips := func(kind schemaext.Kind) bool {
		policy, stated := r.offTarget[kind]
		return stated && policy.Action == schemaext.OffTargetSkip && !selected.refusesSkips && !r.serves(selected.name, kind)
	}
	var kept []schemaext.Kind
	dropped := false
	for _, record := range coverage.KindRecords() {
		if skips(record.Model.Kind) {
			dropped = true
			continue
		}
		kept = append(kept, record.Model.Kind)
	}
	if !dropped {
		return coverage
	}
	return coverage.SelectKinds(kept)
}

func (r *Runtime) featureComparisonKinds(request schemaext.ComparisonRequest) (facetKinds, objectKinds []schemaext.Kind, err error) {
	all, facets := make(map[schemaext.Kind]bool), make(map[schemaext.Kind]bool)
	for _, state := range []schemaext.FeatureState{request.Desired, request.Current} {
		for _, record := range state.Coverage.KindRecords() {
			kind := record.Model.Kind
			all[kind] = true
			if _, found := r.facetComparisons[conversionKey{target: request.Target, kind: kind}]; found {
				facets[kind] = true
			}
		}
		for _, record := range state.Facets {
			for _, kind := range record.Values.DeclaredKinds() {
				if _, found := r.comparisons[conversionKey{target: request.Target, kind: kind}]; found {
					return nil, nil, fmt.Errorf("%w: named-object model %q is attached as a facet", schemaext.ErrInvalidValue, kind)
				}
				all[kind], facets[kind] = true, true
			}
		}
		for _, ref := range state.Objects.Refs() {
			kind := schemaext.Kind(ref.Kind)
			all[kind] = true
			if _, found := r.facetComparisons[conversionKey{target: request.Target, kind: kind}]; found {
				facets[kind] = true
			}
		}
	}
	if err := r.requestKinds(request, facets, all); err != nil {
		return nil, nil, err
	}
	for _, state := range []schemaext.FeatureState{request.Desired, request.Current} {
		for _, ref := range state.Objects.Refs() {
			if facets[schemaext.Kind(ref.Kind)] {
				return nil, nil, fmt.Errorf("%w: facet model %q is declared as a named object", schemaext.ErrInvalidValue, ref.Kind)
			}
		}
	}
	var objects []schemaext.Kind
	for _, kind := range slices.Sorted(maps.Keys(all)) {
		if !facets[kind] {
			objects = append(objects, kind)
		}
	}
	return slices.Sorted(maps.Keys(facets)), objects, nil
}

// requestKinds adds the kind of each change request's subject to all. A
// request names a named-object model; one that names a facet model is
// refused, since a facet owner takes no request.
func (r *Runtime) requestKinds(request schemaext.ComparisonRequest, facets, all map[schemaext.Kind]bool) error {
	for _, change := range request.Requests {
		kind := schemaext.Kind(change.Subject.Kind)
		if _, found := r.facetComparisons[conversionKey{target: request.Target, kind: kind}]; found || facets[kind] {
			return fmt.Errorf("%w: a change request names facet model %q", schemaext.ErrInvalidValue, kind)
		}
		all[kind] = true
	}
	return nil
}

func (r *Runtime) prepareFeatureComparisons(ctx context.Context, request schemaext.ComparisonRequest, objectKinds, facetKinds []schemaext.Kind) (schemaext.ObjectComparisonRequest, schemaext.FacetComparisonRequest, error) {
	objects := schemaext.ObjectComparisonRequest{
		Target: request.Target, Identifiers: request.Identifiers, Capabilities: request.Capabilities,
		Desired:           schemaext.ObjectState{Objects: request.Desired.Objects, Coverage: request.Desired.Coverage.SelectKinds(objectKinds)},
		Current:           schemaext.ObjectState{Objects: request.Current.Objects, Coverage: request.Current.Coverage.SelectKinds(objectKinds)},
		Requests:          request.Requests,
		DeclaredRelations: request.DeclaredRelations,
		DatabasePath:      request.DatabasePath,
		Exclusions:        request.Exclusions,
		DeclaredColumns:   request.DeclaredColumns,
	}
	for _, owner := range request.Owners {
		if owner.Subject.Kind == objectidentity.KindTable {
			objects.Parents = append(objects.Parents, owner)
		}
	}
	facets := schemaext.FacetComparisonRequest{
		Target: request.Target, Identifiers: request.Identifiers, Capabilities: request.Capabilities,
		Owners:  request.Owners,
		Desired: schemaext.FacetState{Records: request.Desired.Facets, Coverage: request.Desired.Coverage.SelectKinds(facetKinds)},
		Current: schemaext.FacetState{Records: request.Current.Facets, Coverage: request.Current.Coverage.SelectKinds(facetKinds)},
	}
	var err error
	objects, err = r.snapshotComparison(ctx, objects)
	if err != nil {
		return schemaext.ObjectComparisonRequest{}, schemaext.FacetComparisonRequest{}, err
	}
	if _, _, err := r.comparisonBatches(objects); err != nil {
		return schemaext.ObjectComparisonRequest{}, schemaext.FacetComparisonRequest{}, err
	}
	facets, err = r.snapshotFacetComparison(ctx, facets)
	if err != nil {
		return schemaext.ObjectComparisonRequest{}, schemaext.FacetComparisonRequest{}, err
	}
	if _, _, err := r.facetComparisonBatches(facets); err != nil {
		return schemaext.ObjectComparisonRequest{}, schemaext.FacetComparisonRequest{}, err
	}
	return objects, facets, nil
}

// validateExclusions refuses an exclusion of a kind the runtime's documents
// cannot name, spelled exactly as the vocabulary spells it.
func (r *Runtime) validateExclusions(exclusions []schemaext.Exclusion) error {
	for _, exclusion := range exclusions {
		kind, err := r.vocabulary.ParseKind(string(exclusion.Kind))
		if err != nil || kind != exclusion.Kind {
			return fmt.Errorf("%w: comparison exclusion of %q, which is not a coverage kind this runtime reads", schemaext.ErrInvalidValue, exclusion.Kind)
		}
	}
	return nil
}

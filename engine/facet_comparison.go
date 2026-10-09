package engine

import (
	"context"
	"fmt"
	"maps"
	"slices"

	"ptah.run/core/objectidentity"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
)

// FacetComparison assigns attached models and their directional change codecs
// to one contextual owner. A model cannot also be a named-object model on the
// same target. A service may compare several related facet kinds in one batch.
type FacetComparison struct {
	Target      string
	Kinds       []schemaext.Kind
	ChangeKinds []schemaext.Kind
	Service     schemaext.FacetComparisonService
	// OwnerKinds declares the common object kinds these models attach to.
	// It is required even for a service that accepts only table owners.
	OwnerKinds []objectidentity.Kind
}

func (r *Runtime) registerFacetComparison(owner string, declaration FacetComparison) error {
	target, found := r.targets[declaration.Target]
	if !found || target.name != declaration.Target || len(declaration.Kinds) == 0 || len(declaration.ChangeKinds) == 0 || declaration.Service == nil || nilService(declaration.Service) {
		return fmt.Errorf("%w: incomplete facet comparison for %q", ErrInvalidRegistration, declaration.Target)
	}
	if err := validateFacetOwnerKinds(declaration.OwnerKinds); err != nil {
		return err
	}
	for _, kind := range declaration.Kinds {
		if !r.ownsCodec(owner, kind, schemaext.Desired) || !r.ownsCodec(owner, kind, schemaext.Observed) {
			return fmt.Errorf("%w: %q does not own both representations of facet %q", ErrInvalidRegistration, owner, kind)
		}
		key := conversionKey{target: target.name, kind: kind}
		if _, exists := r.facetComparisons[key]; exists {
			return fmt.Errorf("%w: duplicate facet comparison for %q/%q", ErrInvalidRegistration, target.name, kind)
		}
		if _, exists := r.comparisons[key]; exists {
			return fmt.Errorf("%w: model %q is registered as both an object and a facet", ErrInvalidRegistration, kind)
		}
		r.facetComparisons[key] = len(r.facetServices)
	}
	seen := make(map[schemaext.Kind]bool)
	for _, kind := range declaration.ChangeKinds {
		if seen[kind] || !r.ownsCodec(owner, kind, schemaext.Change) {
			return fmt.Errorf("%w: invalid facet change ownership for %q/%q", ErrInvalidRegistration, owner, kind)
		}
		seen[kind] = true
	}
	declaration.Kinds = slices.Clone(declaration.Kinds)
	declaration.ChangeKinds = slices.Clone(declaration.ChangeKinds)
	declaration.OwnerKinds = slices.Clone(declaration.OwnerKinds)
	r.facetServices = append(r.facetServices, declaration)
	return nil
}

// CompareFacets compares attached settings through the selected owners. Every
// input and reply is checked before changes escape. Missing handlers never
// turn a concrete value, explicit default, or knowledge limit into a no-op.
func (r *Runtime) CompareFacets(ctx context.Context, request schemaext.FacetComparisonRequest) (schemaext.FacetComparisonResult, error) {
	if err := schemaext.RequireRuntime(ctx, r); err != nil {
		return schemaext.FacetComparisonResult{}, err
	}
	selected, err := r.ResolveTarget(request.Target)
	if err != nil {
		return schemaext.FacetComparisonResult{}, err
	}
	request.Target = selected.Name()
	request, err = r.snapshotFacetComparison(ctx, request)
	if err != nil {
		return schemaext.FacetComparisonResult{}, err
	}
	batches, inactive, err := r.facetComparisonBatches(request)
	if err != nil {
		return schemaext.FacetComparisonResult{}, err
	}
	result := schemaext.FacetComparisonResult{Complete: true, Desired: selectFacetState(request.Desired, inactive)}
	for service, kinds := range batches {
		if len(kinds) == 0 {
			continue
		}
		batch := request
		batch.Kinds = kinds
		batch.Desired, batch.Current = selectFacetState(request.Desired, kinds), selectFacetState(request.Current, kinds)
		var retained schemaext.FacetState
		batch, retained = selectFacetOwners(batch, r.facetServices[service].OwnerKinds)
		result.Desired, err = mergeFacetStates(result.Desired, retained)
		if err != nil {
			return schemaext.FacetComparisonResult{}, err
		}
		sent, err := r.snapshotFacetComparison(ctx, batch)
		if err != nil {
			return schemaext.FacetComparisonResult{}, err
		}
		reply, err := r.facetServices[service].Service.CompareFacets(ctx, sent)
		if err != nil {
			return schemaext.FacetComparisonResult{}, err
		}
		reply, err = r.validateFacetReply(ctx, service, batch, reply)
		if err != nil {
			return schemaext.FacetComparisonResult{}, err
		}
		result.Desired, err = mergeFacetStates(result.Desired, reply.Desired)
		if err != nil {
			return schemaext.FacetComparisonResult{}, err
		}
		result.Changes = append(result.Changes, reply.Changes...)
		result.Undecided = append(result.Undecided, reply.Undecided...)
	}
	if err := ctx.Err(); err != nil {
		return schemaext.FacetComparisonResult{}, err
	}
	if err := distinctFacetChanges(result.Changes); err != nil {
		return schemaext.FacetComparisonResult{}, err
	}
	sortFacetReply(&result)
	return result, nil
}

func (r *Runtime) snapshotFacetComparison(ctx context.Context, request schemaext.FacetComparisonRequest) (schemaext.FacetComparisonRequest, error) {
	target, err := r.ResolveTarget(request.Target)
	if err != nil {
		return schemaext.FacetComparisonRequest{}, err
	}
	request, err = r.scopeFacetComparison(request, target)
	if err != nil {
		return schemaext.FacetComparisonRequest{}, err
	}
	request.Desired, err = r.codecs.SnapshotFacetState(ctx, schemaext.Desired, request.Desired)
	if err != nil {
		return schemaext.FacetComparisonRequest{}, err
	}
	request.Current, err = r.codecs.SnapshotFacetState(ctx, schemaext.Observed, request.Current)
	if err != nil {
		return schemaext.FacetComparisonRequest{}, err
	}
	request.Kinds = slices.Clone(request.Kinds)
	request.Owners = slices.Clone(request.Owners)
	request.Capabilities, request.Identifiers = request.Capabilities.Clone(), request.Identifiers.Clone()
	seen := make(map[objectidentity.Key]bool)
	for _, owner := range request.Owners {
		ref := owner.Subject
		if ref.Kind == "" || ref.Name.Source == "" || ref.Name.Normalized == "" || (!owner.Desired && !owner.Current) || seen[ref.Key()] {
			return schemaext.FacetComparisonRequest{}, fmt.Errorf("%w: invalid or duplicate facet owner %s", schemaext.ErrInvalidValue, ref)
		}
		seen[ref.Key()] = true
	}
	slices.SortFunc(request.Owners, func(a, b schemaext.ParentState) int { return schemaext.CompareRefs(a.Subject, b.Subject) })
	if err := facetInputSubjects(request); err != nil {
		return schemaext.FacetComparisonRequest{}, err
	}
	if err := r.facetInputOwnerKinds(request); err != nil {
		return schemaext.FacetComparisonRequest{}, err
	}
	return request, nil
}

func facetInputSubjects(request schemaext.FacetComparisonRequest) error {
	for _, source := range []struct {
		state   schemaext.FacetState
		desired bool
	}{{request.Desired, true}, {request.Current, false}} {
		for _, record := range source.state.Records {
			owner, found := facetOwner(request.Owners, record.Subject)
			if !found || (source.desired && !owner.Desired) || (!source.desired && !owner.Current) {
				return fmt.Errorf("%w: facet has no owner on its source side: %s", schemaext.ErrInvalidValue, record.Subject)
			}
			for _, kind := range record.Values.Kinds() {
				if source.state.Coverage.Lookup(kind, record.Subject).State == schemaext.Absent {
					return fmt.Errorf("%w: present facet %q is marked absent on %s", schemaext.ErrInvalidValue, kind, record.Subject)
				}
			}
		}
		for _, record := range source.state.Coverage.SubjectRecords() {
			if _, found := facetOwner(request.Owners, record.Subject); !found {
				return fmt.Errorf("%w: facet coverage has no common owner: %s", schemaext.ErrInvalidValue, record.Subject)
			}
		}
	}
	return nil
}

func (r *Runtime) facetComparisonBatches(request schemaext.FacetComparisonRequest) ([][]schemaext.Kind, []schemaext.Kind, error) {
	kinds, required := make(map[schemaext.Kind]bool), make(map[schemaext.Kind]bool)
	for _, kind := range request.Kinds {
		if !kind.Valid() || kinds[kind] {
			return nil, nil, fmt.Errorf("%w: invalid or duplicate facet comparison kind %q", schemaext.ErrInvalidValue, kind)
		}
		kinds[kind], required[kind] = true, !r.facetKindExcluded(request, kind)
	}
	for _, state := range []schemaext.FacetState{request.Desired, request.Current} {
		for _, record := range state.Records {
			for _, kind := range record.Values.DeclaredKinds() {
				kinds[kind] = true
				if slices.Contains(record.Values.Kinds(), kind) {
					required[kind] = true
				}
			}
		}
		comparisonCoverageRequirements(state.Coverage, kinds, required)
	}
	batches := make([][]schemaext.Kind, len(r.facetServices))
	var inactive []schemaext.Kind
	for _, kind := range slices.Sorted(maps.Keys(kinds)) {
		if r.facetKindExcluded(request, kind) {
			inactive = append(inactive, kind)
			continue
		}
		service, found := r.facetComparisons[conversionKey{target: request.Target, kind: kind}]
		if !found {
			if required[kind] {
				return nil, nil, fmt.Errorf("%w: no facet comparison for %q/%q", ptaherr.ErrUnsupportedFeature, request.Target, kind)
			}
			inactive = append(inactive, kind)
			continue
		}
		batches[service] = append(batches[service], kind)
	}
	return batches, inactive, nil
}

func facetOwner(owners []schemaext.ParentState, subject objectidentity.ID) (schemaext.ParentState, bool) {
	for _, owner := range owners {
		if owner.Subject.Key() == subject.Key() {
			return owner, true
		}
	}
	return schemaext.ParentState{}, false
}

func selectFacetState(state schemaext.FacetState, kinds []schemaext.Kind) schemaext.FacetState {
	result := schemaext.FacetState{Coverage: state.Coverage.SelectKinds(kinds)}
	for _, record := range state.Records {
		selected := record.Values
		for _, kind := range selected.DeclaredKinds() {
			if !slices.Contains(kinds, kind) {
				selected = selected.Without(kind)
			}
		}
		if !selected.IsZero() {
			result.Records = append(result.Records, schemaext.FacetRecord{Subject: record.Subject, Values: selected})
		}
	}
	return result
}

func mergeFacetStates(first, second schemaext.FacetState) (schemaext.FacetState, error) {
	coverage, err := first.Coverage.Combine(second.Coverage)
	if err != nil {
		return schemaext.FacetState{}, err
	}
	records := make(map[objectidentity.Key]schemaext.FacetRecord)
	for _, state := range []schemaext.FacetState{first, second} {
		for _, record := range state.Records {
			prior, found := records[record.Subject.Key()]
			if found {
				record.Values, err = prior.Values.Merge(record.Values)
				if err != nil {
					return schemaext.FacetState{}, err
				}
			}
			records[record.Subject.Key()] = record
		}
	}
	result := schemaext.FacetState{Coverage: coverage, Records: slices.Collect(maps.Values(records))}
	slices.SortFunc(result.Records, func(a, b schemaext.FacetRecord) int { return schemaext.CompareRefs(a.Subject, b.Subject) })
	return result, nil
}

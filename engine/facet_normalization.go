package engine

import (
	"context"
	"fmt"
	"slices"

	"ptah.run/core/objectidentity"
	"ptah.run/core/schemaext"
)

// FacetNormalization assigns attached-setting probes to one owner per target
// and kind. The provider owns both model representations. OwnerKinds limits
// which common objects receive probes; it cannot be empty.
type FacetNormalization struct {
	Target     string
	Kinds      []schemaext.Kind
	OwnerKinds []objectidentity.Kind
	Service    schemaext.FacetNormalizationService
}

func (r *Runtime) registerFacetNormalization(owner string, declaration FacetNormalization) error {
	target, found := r.targets[declaration.Target]
	if !found || target.name != declaration.Target || len(declaration.Kinds) == 0 || declaration.Service == nil || nilService(declaration.Service) {
		return fmt.Errorf("%w: incomplete facet normalization for %q", ErrInvalidRegistration, declaration.Target)
	}
	if err := validateFacetOwnerKinds(declaration.OwnerKinds); err != nil {
		return err
	}
	seen := make(map[schemaext.Kind]bool)
	for _, kind := range declaration.Kinds {
		if seen[kind] || !r.ownsCodec(owner, kind, schemaext.Desired) || !r.ownsCodec(owner, kind, schemaext.Observed) {
			return fmt.Errorf("%w: duplicate or unowned normalization facet %q", ErrInvalidRegistration, kind)
		}
		seen[kind] = true
		for _, existing := range r.facetProbes {
			if existing.Target == declaration.Target && slices.Contains(existing.Kinds, kind) {
				return fmt.Errorf("%w: duplicate facet normalization for %q/%q", ErrInvalidRegistration, declaration.Target, kind)
			}
		}
	}
	declaration.Kinds = slices.Clone(declaration.Kinds)
	declaration.OwnerKinds = slices.Clone(declaration.OwnerKinds)
	r.facetProbes = append(r.facetProbes, declaration)
	return nil
}

// NormalizeFacets sends each owner one isolated batch of active declarations
// and observations. Undeclared kinds and excluded target bindings pass through
// without probes. Replies cannot add, remove or rebind settings, change their
// common subjects, or alter source coverage. Errors and cancellation return no
// partial state. Input records and provider replies never back returned slices.
func (r *Runtime) NormalizeFacets(ctx context.Context, request schemaext.FacetNormalizationRequest) (schemaext.FacetNormalizationResult, error) {
	if err := schemaext.RequireRuntime(ctx, r); err != nil {
		return schemaext.FacetNormalizationResult{}, err
	}
	selected, err := r.ResolveTarget(request.Target)
	if err != nil {
		return schemaext.FacetNormalizationResult{}, err
	}
	request.Target = selected.Name()
	result := request.Desired
	result.Records = slices.Clone(result.Records)
	for _, service := range r.facetProbes {
		if service.Target != request.Target {
			continue
		}
		batch, err := r.facetNormalizationBatch(ctx, request, selected, service)
		if err != nil {
			return schemaext.FacetNormalizationResult{}, err
		}
		if len(batch.Desired.Records) == 0 {
			continue
		}
		// The validation reference is separate from the provider's mutable snapshot.
		sent, err := r.snapshotFacetNormalization(ctx, batch)
		if err != nil {
			return schemaext.FacetNormalizationResult{}, err
		}
		reply, err := service.Service.NormalizeFacets(ctx, sent)
		if err != nil {
			return schemaext.FacetNormalizationResult{}, err
		}
		returned, err := r.acceptFacetNormalization(ctx, batch.Desired, reply)
		if err != nil {
			return schemaext.FacetNormalizationResult{}, err
		}
		result, err = replaceNormalizedFacets(result, returned)
		if err != nil {
			return schemaext.FacetNormalizationResult{}, err
		}
	}
	if err := ctx.Err(); err != nil {
		return schemaext.FacetNormalizationResult{}, err
	}
	return schemaext.FacetNormalizationResult{Complete: true, Desired: result}, nil
}

func (r *Runtime) facetNormalizationBatch(ctx context.Context, request schemaext.FacetNormalizationRequest, target schemaext.TargetSelection, service FacetNormalization) (schemaext.FacetNormalizationRequest, error) {
	var err error
	request.Desired, err = selectNormalizationFacets(request.Desired, target, service)
	if err != nil {
		return schemaext.FacetNormalizationRequest{}, err
	}
	request.Current, err = selectNormalizationFacets(request.Current, target, service)
	if err != nil {
		return schemaext.FacetNormalizationRequest{}, err
	}
	request.Owners = slices.DeleteFunc(slices.Clone(request.Owners), func(owner schemaext.ParentState) bool {
		return !slices.Contains(service.OwnerKinds, owner.Subject.Kind)
	})
	return r.snapshotFacetNormalization(ctx, request)
}

func selectNormalizationFacets(state schemaext.FacetState, target schemaext.TargetSelection, service FacetNormalization) (schemaext.FacetState, error) {
	result := schemaext.FacetState{Coverage: state.Coverage.SelectKinds(service.Kinds).SelectSubjects(func(subject objectidentity.ID) bool { return slices.Contains(service.OwnerKinds, subject.Kind) })}
	for _, record := range selectFacetState(state, service.Kinds).Records {
		if !slices.Contains(service.OwnerKinds, record.Subject.Kind) {
			continue
		}
		projected, err := record.Values.ForTarget(target)
		if err != nil {
			return schemaext.FacetState{}, err
		}
		for _, kind := range projected.DeclaredKinds() {
			if !slices.Contains(projected.Kinds(), kind) {
				projected = projected.Without(kind)
			}
		}
		if projected.Len() > 0 {
			result.Records = append(result.Records, schemaext.FacetRecord{Subject: record.Subject, Values: projected})
		}
	}
	return result, nil
}

func (r *Runtime) snapshotFacetNormalization(ctx context.Context, request schemaext.FacetNormalizationRequest) (schemaext.FacetNormalizationRequest, error) {
	var err error
	request.Desired, err = r.codecs.SnapshotFacetState(ctx, schemaext.Desired, request.Desired)
	if err != nil {
		return schemaext.FacetNormalizationRequest{}, err
	}
	request.Current, err = r.codecs.SnapshotFacetState(ctx, schemaext.Observed, request.Current)
	if err != nil {
		return schemaext.FacetNormalizationRequest{}, err
	}
	request.Owners = slices.Clone(request.Owners)
	request.Identifiers = request.Identifiers.Clone()
	seen := make(map[objectidentity.Key]bool)
	for _, owner := range request.Owners {
		ref := owner.Subject
		if ref.Kind == "" || ref.Name.Source == "" || ref.Name.Normalized == "" || (!owner.Desired && !owner.Current) || seen[ref.Key()] {
			return schemaext.FacetNormalizationRequest{}, fmt.Errorf("%w: invalid or duplicate normalization parent %s", schemaext.ErrInvalidValue, ref)
		}
		seen[ref.Key()] = true
	}
	if err := facetInputSubjects(schemaext.FacetComparisonRequest{Desired: request.Desired, Current: request.Current, Owners: request.Owners}); err != nil {
		return schemaext.FacetNormalizationRequest{}, err
	}
	return request, nil
}

func (r *Runtime) acceptFacetNormalization(ctx context.Context, original schemaext.FacetState, reply schemaext.FacetNormalizationResult) (schemaext.FacetState, error) {
	if !reply.Complete {
		return schemaext.FacetState{}, fmt.Errorf("%w: facet normalization did not complete", schemaext.ErrInvalidValue)
	}
	returned, err := r.codecs.SnapshotFacetState(ctx, schemaext.Desired, reply.Desired)
	if err != nil {
		return schemaext.FacetState{}, err
	}
	if !returned.Coverage.Equal(original.Coverage) || len(returned.Records) != len(original.Records) {
		return schemaext.FacetState{}, fmt.Errorf("%w: facet normalization changed declarations or source coverage", schemaext.ErrInvalidValue)
	}
	for i, record := range returned.Records {
		before := original.Records[i]
		if record.Subject != before.Subject || !slices.Equal(record.Values.DeclaredKinds(), before.Values.DeclaredKinds()) || !slices.Equal(record.Values.Kinds(), before.Values.Kinds()) {
			return schemaext.FacetState{}, fmt.Errorf("%w: facet normalization changed a subject or kind", schemaext.ErrInvalidValue)
		}
		for _, kind := range record.Values.DeclaredKinds() {
			if !slices.Equal(record.Values.TargetScope(kind), before.Values.TargetScope(kind)) {
				return schemaext.FacetState{}, fmt.Errorf("%w: facet normalization changed a target binding", schemaext.ErrInvalidValue)
			}
		}
	}
	return returned, nil
}

func replaceNormalizedFacets(state, normalized schemaext.FacetState) (schemaext.FacetState, error) {
	for _, record := range normalized.Records {
		position := slices.IndexFunc(state.Records, func(original schemaext.FacetRecord) bool { return original.Subject.Key() == record.Subject.Key() })
		if position < 0 {
			return schemaext.FacetState{}, fmt.Errorf("%w: normalization added a subject", schemaext.ErrInvalidValue)
		}
		values, err := record.Values.Values()
		if err != nil {
			return schemaext.FacetState{}, err
		}
		for _, value := range values {
			state.Records[position].Values, err = state.Records[position].Values.Replace(value)
			if err != nil {
				return schemaext.FacetState{}, err
			}
		}
	}
	return state, nil
}

package engine

import (
	"context"
	"fmt"
	"slices"

	"ptah.run/core/objectidentity"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
)

// Normalization assigns connected-target normalization of declared named
// objects to one owner per target and kind. The provider owns the desired
// codec of every kind it normalizes.
type Normalization struct {
	Target  string
	Kinds   []schemaext.Kind
	Service schemaext.NormalizationService
}

func (r *Runtime) registerNormalization(owner string, declaration Normalization) error {
	target, found := r.targets[declaration.Target]
	if !found || target.name != declaration.Target || len(declaration.Kinds) == 0 || declaration.Service == nil || nilService(declaration.Service) {
		return fmt.Errorf("%w: incomplete normalization registration for %q", ErrInvalidRegistration, declaration.Target)
	}
	index := len(r.probeServices)
	for _, kind := range declaration.Kinds {
		if !r.ownsCodec(owner, kind, schemaext.Desired) {
			return fmt.Errorf("%w: %q does not own %q/%s", ErrInvalidRegistration, owner, kind, schemaext.Desired)
		}
		key := conversionKey{target: target.name, kind: kind}
		if _, exists := r.probes[key]; exists {
			return fmt.Errorf("%w: duplicate normalization for %q/%q", ErrInvalidRegistration, target.name, kind)
		}
		r.probes[key] = index
	}
	declaration.Kinds = slices.Clone(declaration.Kinds)
	r.probeServices = append(r.probeServices, declaration)
	return nil
}

// NormalizeObjects sends each registered owner one batch of the declared
// objects of its kinds, with the observations of the same kinds. A kind no
// owner normalizes passes through unchanged and is not snapshotted:
// normalization adds target facts to a declaration and is never required to
// read it, so a target with no normalizer returns the declaration as it came.
// Replies must return every object under its identity, kind and source
// coverage. Errors and cancellation discard the whole result.
func (r *Runtime) NormalizeObjects(ctx context.Context, request schemaext.NormalizationRequest) (schemaext.NormalizationResult, error) {
	if err := schemaext.RequireRuntime(ctx, r); err != nil {
		return schemaext.NormalizationResult{}, err
	}
	target, found := r.lookup(request.Target)
	if !found {
		return schemaext.NormalizationResult{}, fmt.Errorf("%w: %q", ptaherr.ErrUnsupportedDialect, request.Target)
	}
	request.Target = target.name
	var kinds []schemaext.Kind
	for _, service := range r.probeServices {
		if service.Target == target.name {
			kinds = append(kinds, service.Kinds...)
		}
	}
	owned := func(ref objectidentity.ID) bool { return slices.Contains(kinds, schemaext.Kind(ref.Kind)) }
	if len(kinds) == 0 || !slices.ContainsFunc(request.Desired.Objects.Refs(), owned) {
		return schemaext.NormalizationResult{Complete: true, Desired: request.Desired}, ctx.Err()
	}
	desired, err := r.codecs.SnapshotObjectState(ctx, schemaext.Desired, schemaext.ObjectState{
		Objects: request.Desired.Objects.Select(owned), Coverage: request.Desired.Coverage.SelectKinds(kinds),
	})
	if err != nil {
		return schemaext.NormalizationResult{}, err
	}
	current, err := r.codecs.SnapshotObjectState(ctx, schemaext.Observed, schemaext.ObjectState{
		Objects: request.Current.Objects.Select(owned), Coverage: request.Current.Coverage.SelectKinds(kinds),
	})
	if err != nil {
		return schemaext.NormalizationResult{}, err
	}
	result := request.Desired
	for index, service := range r.probeServices {
		if service.Target != target.name {
			continue
		}
		selected := func(ref objectidentity.ID) bool {
			owner, found := r.probes[conversionKey{target.name, schemaext.Kind(ref.Kind)}]
			return found && owner == index
		}
		batch := request
		batch.Desired = schemaext.ObjectState{Objects: desired.Objects.Select(selected), Coverage: desired.Coverage.SelectKinds(service.Kinds)}
		batch.Current = schemaext.ObjectState{Objects: current.Objects.Select(selected), Coverage: current.Coverage.SelectKinds(service.Kinds)}
		batch.Capabilities = request.Capabilities.Clone()
		batch.Identifiers = request.Identifiers.Clone()
		if batch.Desired.Objects.Len() == 0 {
			continue
		}
		reply, err := service.Service.NormalizeObjects(ctx, batch)
		if err != nil {
			return schemaext.NormalizationResult{}, err
		}
		normalized, err := r.acceptNormalization(ctx, batch, reply)
		if err != nil {
			return schemaext.NormalizationResult{}, err
		}
		for _, object := range normalized {
			result.Objects, err = result.Objects.Replace(object)
			if err != nil {
				return schemaext.NormalizationResult{}, err
			}
		}
	}
	if err := ctx.Err(); err != nil {
		return schemaext.NormalizationResult{}, err
	}
	return schemaext.NormalizationResult{Complete: true, Desired: result}, nil
}

func (r *Runtime) acceptNormalization(ctx context.Context, request schemaext.NormalizationRequest, reply schemaext.NormalizationResult) ([]schemaext.Object, error) {
	if !reply.Complete {
		return nil, fmt.Errorf("%w: normalization did not complete", schemaext.ErrInvalidValue)
	}
	returned, err := r.codecs.SnapshotObjectState(ctx, schemaext.Desired, reply.Desired)
	if err != nil {
		return nil, err
	}
	if !slices.Equal(returned.Objects.Refs(), request.Desired.Objects.Refs()) {
		return nil, fmt.Errorf("%w: normalization changed the declared objects", schemaext.ErrInvalidValue)
	}
	if !slices.Equal(returned.Coverage.KindRecords(), request.Desired.Coverage.KindRecords()) ||
		!slices.Equal(returned.Coverage.SubjectRecords(), request.Desired.Coverage.SubjectRecords()) {
		return nil, fmt.Errorf("%w: normalization changed source coverage", schemaext.ErrInvalidValue)
	}
	objects, err := returned.Objects.All()
	if err != nil {
		return nil, err
	}
	for _, object := range objects {
		original, _, err := request.Desired.Objects.Get(object.Ref)
		if err != nil {
			return nil, err
		}
		if object.Value.Kind() != original.Value.Kind() {
			return nil, fmt.Errorf("%w: normalization changed the kind of %s", schemaext.ErrInvalidValue, object.Ref)
		}
	}
	return objects, ctx.Err()
}

package engine

import (
	"cmp"
	"context"
	"fmt"
	"maps"
	"slices"

	"ptah.run/core/objectidentity"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
)

// ObjectComparison assigns named schema kinds and their change representations
// to a contextual service. One call contains all assigned kinds present in the
// request, including enrolled empty namespaces. The provider owns every codec.
type ObjectComparison struct {
	Target      string
	Kinds       []schemaext.Kind
	ChangeKinds []schemaext.Kind
	Service     schemaext.ObjectComparisonService
}

func (r *Runtime) registerComparison(owner string, declaration ObjectComparison) error {
	target, found := r.targets[declaration.Target]
	if !found || target.name != declaration.Target || len(declaration.Kinds) == 0 || len(declaration.ChangeKinds) == 0 || declaration.Service == nil || nilService(declaration.Service) {
		return fmt.Errorf("%w: incomplete object comparison for %q", ErrInvalidRegistration, declaration.Target)
	}
	for _, kind := range declaration.Kinds {
		for _, representation := range []schemaext.Representation{schemaext.Desired, schemaext.Observed} {
			if !r.ownsCodec(owner, kind, representation) {
				return fmt.Errorf("%w: %q does not own %q/%s", ErrInvalidRegistration, owner, kind, representation)
			}
		}
		key := conversionKey{target: target.name, kind: kind}
		if _, exists := r.facetComparisons[key]; exists {
			return fmt.Errorf("%w: model %q is registered as both an object and a facet", ErrInvalidRegistration, kind)
		}
		if _, exists := r.comparisons[key]; exists {
			return fmt.Errorf("%w: duplicate comparison for %q/%q", ErrInvalidRegistration, target.name, kind)
		}
		r.comparisons[key] = len(r.comparisonServices)
	}
	seen := make(map[schemaext.Kind]bool)
	for _, kind := range declaration.ChangeKinds {
		if seen[kind] || !r.ownsCodec(owner, kind, schemaext.Change) {
			return fmt.Errorf("%w: invalid change ownership for %q/%q", ErrInvalidRegistration, owner, kind)
		}
		seen[kind] = true
	}
	declaration.Kinds = slices.Clone(declaration.Kinds)
	declaration.ChangeKinds = slices.Clone(declaration.ChangeKinds)
	r.comparisonServices = append(r.comparisonServices, declaration)
	return nil
}

func (r *Runtime) ownsCodec(owner string, kind schemaext.Kind, representation schemaext.Representation) bool {
	return slices.ContainsFunc(r.codecs.Definitions(), func(model schemaext.CodecIdentity) bool {
		return model.Owner == owner && model.Kind == kind && model.Representation == representation
	})
}

// CompareObjects validates a complete comparison before dispatch. Each selected
// owner runs once, and every reply is checked before any result escapes. Unknown
// source kinds stay unknown when a provider is added to the runtime. Unsupported
// concrete objects and explicit default requests are errors, even on an otherwise
// empty target. Empty codec namespaces alone do not assert target support.
func (r *Runtime) CompareObjects(ctx context.Context, request schemaext.ObjectComparisonRequest) (schemaext.ObjectComparisonResult, error) {
	if ctx == nil {
		return schemaext.ObjectComparisonResult{}, fmt.Errorf("%w: comparison requires a context", schemaext.ErrInvalidValue)
	}
	if err := ctx.Err(); err != nil {
		return schemaext.ObjectComparisonResult{}, err
	}
	target, found := r.lookup(request.Target)
	if !found {
		return schemaext.ObjectComparisonResult{}, fmt.Errorf("%w: %q", ptaherr.ErrUnsupportedDialect, request.Target)
	}
	request.Target = target.name
	request, err := r.snapshotComparison(ctx, request)
	if err != nil {
		return schemaext.ObjectComparisonResult{}, err
	}
	batches, inactive, err := r.comparisonBatches(request)
	if err != nil {
		return schemaext.ObjectComparisonResult{}, err
	}
	result := schemaext.ObjectComparisonResult{Complete: true, Desired: selectObjectState(request.Desired, inactive)}
	for service, kinds := range batches {
		if len(kinds) == 0 {
			continue
		}
		batch := request
		batch.Kinds = kinds
		batch.Desired = selectObjectState(request.Desired, kinds)
		batch.Current = selectObjectState(request.Current, kinds)
		// The host's validation copy never aliases service-owned slices or maps.
		sent, err := r.snapshotComparison(ctx, batch)
		if err != nil {
			return schemaext.ObjectComparisonResult{}, err
		}
		reply, err := r.comparisonServices[service].Service.CompareObjects(ctx, sent)
		if err != nil {
			return schemaext.ObjectComparisonResult{}, err
		}
		reply, err = r.validateComparisonReply(ctx, service, batch, reply)
		if err != nil {
			return schemaext.ObjectComparisonResult{}, err
		}
		result.Desired.Objects, err = result.Desired.Objects.Merge(reply.Desired.Objects)
		if err != nil {
			return schemaext.ObjectComparisonResult{}, err
		}
		result.Desired.Coverage, err = result.Desired.Coverage.Combine(reply.Desired.Coverage)
		if err != nil {
			return schemaext.ObjectComparisonResult{}, err
		}
		result.Changes = append(result.Changes, reply.Changes...)
		result.Undecided = append(result.Undecided, reply.Undecided...)
	}
	if err := ctx.Err(); err != nil {
		return schemaext.ObjectComparisonResult{}, err
	}
	slices.SortFunc(result.Changes, func(a, b schemaext.ChangeRecord) int { return schemaext.CompareRefs(a.Subject, b.Subject) })
	slices.SortFunc(result.Undecided, func(a, b schemaext.UndecidedChange) int {
		return cmp.Or(cmp.Compare(a.Kind, b.Kind), schemaext.CompareRefs(a.Subject, b.Subject))
	})
	return result, nil
}

func (r *Runtime) snapshotComparison(ctx context.Context, request schemaext.ObjectComparisonRequest) (schemaext.ObjectComparisonRequest, error) {
	var err error
	request.Desired, err = r.codecs.SnapshotObjectState(ctx, schemaext.Desired, request.Desired)
	if err != nil {
		return schemaext.ObjectComparisonRequest{}, err
	}
	request.Current, err = r.codecs.SnapshotObjectState(ctx, schemaext.Observed, request.Current)
	if err != nil {
		return schemaext.ObjectComparisonRequest{}, err
	}
	request.Capabilities = request.Capabilities.Clone()
	request.Identifiers = request.Identifiers.Clone()
	request.Kinds = slices.Clone(request.Kinds)
	request.Parents = slices.Clone(request.Parents)
	seen := make(map[objectidentity.Key]bool)
	for _, parent := range request.Parents {
		ref := parent.Subject
		if ref.Kind != objectidentity.KindTable || ref.Name.Source == "" || ref.Name.Normalized == "" || !ref.Parent.Empty() || ref.Signature != "" || (!parent.Desired && !parent.Current) || seen[ref.Key()] {
			return schemaext.ObjectComparisonRequest{}, fmt.Errorf("%w: invalid or duplicate comparison parent %s", schemaext.ErrInvalidValue, ref)
		}
		seen[ref.Key()] = true
	}
	slices.SortFunc(request.Parents, func(a, b schemaext.ParentState) int { return schemaext.CompareRefs(a.Subject, b.Subject) })
	if err := comparisonInputSubjects(request); err != nil {
		return schemaext.ObjectComparisonRequest{}, err
	}
	return request, nil
}

func comparisonInputSubjects(request schemaext.ObjectComparisonRequest) error {
	for _, source := range []struct {
		state     schemaext.ObjectState
		direction schemaext.Representation
	}{
		{request.Desired, schemaext.Desired}, {request.Current, schemaext.Observed},
	} {
		for _, ref := range source.state.Objects.Refs() {
			if source.state.Coverage.Lookup(schemaext.Kind(ref.Kind), ref).State == schemaext.Absent {
				return fmt.Errorf("%w: present object is marked absent: %s", schemaext.ErrInvalidValue, ref)
			}
			if ref.Parent.Empty() {
				continue
			}
			parent, found := comparisonParent(request.Parents, ref)
			if !found || (source.direction == schemaext.Desired && !parent.Desired) || (source.direction == schemaext.Observed && !parent.Current) {
				return fmt.Errorf("%w: feature object has no parent on its source side: %s", schemaext.ErrInvalidValue, ref)
			}
		}
	}
	return nil
}

func (r *Runtime) comparisonBatches(request schemaext.ObjectComparisonRequest) ([][]schemaext.Kind, []schemaext.Kind, error) {
	kinds := make(map[schemaext.Kind]bool)
	required := make(map[schemaext.Kind]bool)
	for _, kind := range request.Kinds {
		if !kind.Valid() || kinds[kind] {
			return nil, nil, fmt.Errorf("%w: invalid or duplicate requested comparison kind %q", schemaext.ErrInvalidValue, kind)
		}
		kinds[kind], required[kind] = true, true
	}
	for _, state := range []schemaext.ObjectState{request.Desired, request.Current} {
		for _, ref := range state.Objects.Refs() {
			kind := schemaext.Kind(ref.Kind)
			kinds[kind], required[kind] = true, true
		}
		comparisonCoverageRequirements(state.Coverage, kinds, required)
	}
	batches := make([][]schemaext.Kind, len(r.comparisonServices))
	var inactive []schemaext.Kind
	for _, kind := range slices.Sorted(maps.Keys(kinds)) {
		service, found := r.comparisons[conversionKey{target: request.Target, kind: kind}]
		if !found {
			if required[kind] {
				return nil, nil, fmt.Errorf("%w: no object comparison for %q/%q", ptaherr.ErrUnsupportedFeature, request.Target, kind)
			}
			inactive = append(inactive, kind)
			continue
		}
		batches[service] = append(batches[service], kind)
	}
	return batches, inactive, nil
}

func selectObjectState(state schemaext.ObjectState, kinds []schemaext.Kind) schemaext.ObjectState {
	return schemaext.ObjectState{Objects: state.Objects.Select(func(ref objectidentity.ID) bool { return slices.Contains(kinds, schemaext.Kind(ref.Kind)) }), Coverage: state.Coverage.SelectKinds(kinds)}
}

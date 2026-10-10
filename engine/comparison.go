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
//
// Actions lists the [schemaext.ChangeRequest] actions the service accepts for
// its kinds. A request whose action the owner of its kind does not list is
// refused before any service runs. A service with no actions accepts none.
type ObjectComparison struct {
	Target      string
	Kinds       []schemaext.Kind
	ChangeKinds []schemaext.Kind
	Actions     []string
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
	actions := make(map[string]bool, len(declaration.Actions))
	for _, action := range declaration.Actions {
		if actions[action] || !reversalText(action) {
			return fmt.Errorf("%w: invalid or duplicate change request action %q for %q", ErrInvalidRegistration, action, owner)
		}
		actions[action] = true
	}
	declaration.Kinds = slices.Clone(declaration.Kinds)
	declaration.ChangeKinds = slices.Clone(declaration.ChangeKinds)
	declaration.Actions = slices.Clone(declaration.Actions)
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
		batch.Requests = selectRequests(request.Requests, kinds)
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
	request.Requests, err = snapshotRequests(request.Requests)
	if err != nil {
		return schemaext.ObjectComparisonRequest{}, err
	}
	request.DeclaredRelations, err = snapshotDeclaredRelations(request.DeclaredRelations)
	if err != nil {
		return schemaext.ObjectComparisonRequest{}, err
	}
	if err := comparisonInputSubjects(request); err != nil {
		return schemaext.ObjectComparisonRequest{}, err
	}
	return request, nil
}

// snapshotDeclaredRelations copies and orders the declared views and
// materialized views, refusing a reference that is not one.
func snapshotDeclaredRelations(refs []objectidentity.ID) ([]objectidentity.ID, error) {
	refs = slices.Clone(refs)
	for _, ref := range refs {
		if (ref.Kind != objectidentity.KindView && ref.Kind != objectidentity.KindMatView) || ref.Name.Source == "" || ref.Name.Normalized == "" || !ref.Parent.Empty() || ref.Signature != "" {
			return nil, fmt.Errorf("%w: invalid declared relation %s", schemaext.ErrInvalidValue, ref)
		}
	}
	slices.SortFunc(refs, schemaext.CompareRefs)
	return refs, nil
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
	for _, change := range request.Requests {
		kind := schemaext.Kind(change.Subject.Kind)
		kinds[kind], required[kind] = true, true
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
	for _, change := range request.Requests {
		service := r.comparisons[conversionKey{target: request.Target, kind: schemaext.Kind(change.Subject.Kind)}]
		if !slices.Contains(r.comparisonServices[service].Actions, change.Action) {
			return nil, nil, fmt.Errorf("%w: the owner of %q accepts no %q request, for %s", ptaherr.ErrUnsupportedFeature, change.Subject.Kind, change.Action, change.Subject)
		}
	}
	return batches, inactive, nil
}

// snapshotRequests copies the change requests in subject and action order. A
// request needs a named subject of a valid kind and an action, and asking for
// one action on one subject twice is refused rather than merged.
func snapshotRequests(requests []schemaext.ChangeRequest) ([]schemaext.ChangeRequest, error) {
	if len(requests) == 0 {
		return nil, nil
	}
	type requestKey struct {
		subject objectidentity.Key
		action  string
	}
	seen := make(map[requestKey]bool, len(requests))
	for _, change := range requests {
		key := requestKey{change.Subject.Key(), change.Action}
		if !schemaext.Kind(change.Subject.Kind).Valid() || change.Subject.Name.Source == "" || !reversalText(change.Action) || seen[key] {
			return nil, fmt.Errorf("%w: invalid or duplicate change request %q for %s", schemaext.ErrInvalidValue, change.Action, change.Subject)
		}
		seen[key] = true
	}
	requests = slices.Clone(requests)
	slices.SortFunc(requests, func(a, b schemaext.ChangeRequest) int {
		return cmp.Or(schemaext.CompareRefs(a.Subject, b.Subject), cmp.Compare(a.Action, b.Action))
	})
	return requests, nil
}

func selectRequests(requests []schemaext.ChangeRequest, kinds []schemaext.Kind) []schemaext.ChangeRequest {
	var selected []schemaext.ChangeRequest
	for _, change := range requests {
		if slices.Contains(kinds, schemaext.Kind(change.Subject.Kind)) {
			selected = append(selected, change)
		}
	}
	return selected
}

func selectObjectState(state schemaext.ObjectState, kinds []schemaext.Kind) schemaext.ObjectState {
	return schemaext.ObjectState{Objects: state.Objects.Select(func(ref objectidentity.ID) bool { return slices.Contains(kinds, schemaext.Kind(ref.Kind)) }), Coverage: state.Coverage.SelectKinds(kinds)}
}

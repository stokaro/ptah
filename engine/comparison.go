package engine

import (
	"cmp"
	"context"
	"fmt"
	"maps"
	"slices"
	"strconv"

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
//
// DeclaredKinds lists the models, beside its own, whose declared objects the
// service reads: the runtime gives it the identity of every desired object of
// those kinds in [schemaext.ObjectComparisonRequest.DeclaredObjects]. Each is a
// model the same owner registers, named once, and none is one of Kinds.
//
// DeclaredFacetKinds lists the facet models whose declared facets the service
// reads: the runtime gives it every desired facet of those kinds, with its
// owner and value, in [schemaext.ObjectComparisonRequest.DeclaredFacets]. Each
// is a model the same owner registers, named once, and none is one of Kinds or
// DeclaredKinds.
//
// CurrentContext reports that the service reads the current state of its
// declared kinds and declared facet kinds too: the runtime gives it the current
// objects and facets of those kinds, and their coverage, in
// [schemaext.ObjectComparisonRequest.CurrentContext]. Such a service names at
// least one declared kind or declared facet kind, each with an observed model
// its owner registers.
type ObjectComparison struct {
	Target             string
	Kinds              []schemaext.Kind
	ChangeKinds        []schemaext.Kind
	Actions            []string
	DeclaredKinds      []schemaext.Kind
	DeclaredFacetKinds []schemaext.Kind
	CurrentContext     bool
	Service            schemaext.ObjectComparisonService
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
	if err := r.validateDeclaredKinds(owner, declaration); err != nil {
		return err
	}
	declaration.DeclaredKinds = slices.Clone(declaration.DeclaredKinds)
	declaration.DeclaredFacetKinds = slices.Clone(declaration.DeclaredFacetKinds)
	declaration.Kinds = slices.Clone(declaration.Kinds)
	declaration.ChangeKinds = slices.Clone(declaration.ChangeKinds)
	declaration.Actions = slices.Clone(declaration.Actions)
	r.comparisonServices = append(r.comparisonServices, declaration)
	return nil
}

// validateDeclaredKinds refuses a declared kind or declared facet kind of
// declaration that its owner does not register a desired model for, one named
// twice, and one of the service's own kinds, whose objects its batch of the
// desired state holds. A service that reads a current context needs a
// declared kind or declared facet kind, each with an observed model its owner
// registers, since the context holds the current values of those kinds.
func (r *Runtime) validateDeclaredKinds(owner string, declaration ObjectComparison) error {
	kinds := slices.Concat(declaration.DeclaredKinds, declaration.DeclaredFacetKinds)
	declared := make(map[schemaext.Kind]bool, len(kinds))
	for _, kind := range kinds {
		if declared[kind] || slices.Contains(declaration.Kinds, kind) || !r.ownsCodec(owner, kind, schemaext.Desired) ||
			declaration.CurrentContext && !r.ownsCodec(owner, kind, schemaext.Observed) {
			return fmt.Errorf("%w: invalid declared kind %q for %q", ErrInvalidRegistration, kind, owner)
		}
		declared[kind] = true
	}
	if declaration.CurrentContext && len(kinds) == 0 {
		return fmt.Errorf("%w: object comparison of %q reads a current context and names no declared kind", ErrInvalidRegistration, owner)
	}
	return nil
}

func (r *Runtime) ownsCodec(owner string, kind schemaext.Kind, representation schemaext.Representation) bool {
	codecOwner, found := r.codecs.Owner(kind, representation)
	return found && codecOwner == owner
}

// CompareObjects validates a complete comparison before dispatch. Each selected
// owner runs once, and every reply is checked before any result escapes. Unknown
// source kinds stay unknown when a provider is added to the runtime. Unsupported
// concrete objects and explicit default requests are errors, even on an otherwise
// empty target. Empty codec namespaces alone do not assert target support.
func (r *Runtime) CompareObjects(ctx context.Context, request schemaext.ObjectComparisonRequest) (schemaext.ObjectComparisonResult, error) {
	result, counts, err := r.compareObjects(ctx, request)
	if err != nil {
		return schemaext.ObjectComparisonResult{}, err
	}
	result.Counts = counts.result()
	return result, nil
}

// compareObjects is [Runtime.CompareObjects] with the counts of its services
// apart from the result, for a caller that joins them with another comparison's.
func (r *Runtime) compareObjects(ctx context.Context, request schemaext.ObjectComparisonRequest) (schemaext.ObjectComparisonResult, objectCounts, error) {
	if ctx == nil {
		return schemaext.ObjectComparisonResult{}, objectCounts{}, fmt.Errorf("%w: comparison requires a context", schemaext.ErrInvalidValue)
	}
	if err := ctx.Err(); err != nil {
		return schemaext.ObjectComparisonResult{}, objectCounts{}, err
	}
	target, found := r.lookup(request.Target)
	if !found {
		return schemaext.ObjectComparisonResult{}, objectCounts{}, fmt.Errorf("%w: %q", ptaherr.ErrUnsupportedDialect, request.Target)
	}
	request.Target = target.name
	// A resolution names a request by its place in the caller's list, which
	// the snapshot sorts.
	asked := slices.Clone(request.Requests)
	request, err := r.snapshotComparison(ctx, request)
	if err != nil {
		return schemaext.ObjectComparisonResult{}, objectCounts{}, err
	}
	batches, inactive, err := r.comparisonBatches(request)
	if err != nil {
		return schemaext.ObjectComparisonResult{}, objectCounts{}, err
	}
	result := schemaext.ObjectComparisonResult{Complete: true, Desired: selectObjectState(request.Desired, inactive)}
	var counts objectCounts
	for service, kinds := range batches {
		if len(kinds) == 0 {
			continue
		}
		batch := request
		batch.Kinds = kinds
		batch.Desired = selectObjectState(request.Desired, kinds)
		batch.Current = selectObjectState(request.Current, kinds)
		batch.Requests = selectRequests(request.Requests, kinds, r.comparisonServices[service].Actions)
		batch.DeclaredObjects = declaredObjects(request.Desired, r.comparisonServices[service].DeclaredKinds)
		batch.DeclaredFacets, err = declaredFacets(request.DeclaredFacets, r.comparisonServices[service].DeclaredFacetKinds)
		if err != nil {
			return schemaext.ObjectComparisonResult{}, objectCounts{}, err
		}
		batch.CurrentFacets = schemaext.FacetState{}
		batch.CurrentContext, err = currentContext(request, r.comparisonServices[service])
		if err != nil {
			return schemaext.ObjectComparisonResult{}, objectCounts{}, err
		}
		// The host's validation copy never aliases service-owned slices or maps.
		sent, err := r.snapshotComparison(ctx, batch)
		if err != nil {
			return schemaext.ObjectComparisonResult{}, objectCounts{}, err
		}
		reply, err := r.comparisonServices[service].Service.CompareObjects(ctx, sent)
		if err != nil {
			return schemaext.ObjectComparisonResult{}, objectCounts{}, err
		}
		reply, err = r.validateComparisonReply(ctx, service, batch, reply)
		if err != nil {
			return schemaext.ObjectComparisonResult{}, objectCounts{}, err
		}
		result.Desired.Objects, err = result.Desired.Objects.Merge(reply.Desired.Objects)
		if err != nil {
			return schemaext.ObjectComparisonResult{}, objectCounts{}, err
		}
		result.Desired.Coverage, err = result.Desired.Coverage.Combine(reply.Desired.Coverage)
		if err != nil {
			return schemaext.ObjectComparisonResult{}, objectCounts{}, err
		}
		result.Changes = append(result.Changes, reply.Changes...)
		result.Undecided = append(result.Undecided, reply.Undecided...)
		counts.add(r.comparisonServices[service].ChangeKinds, reply.Counts)
		for _, answer := range reply.Resolved {
			answered := batch.Requests[answer.Request]
			index := slices.IndexFunc(asked, func(change schemaext.ChangeRequest) bool {
				return change.Written == answered.Written && change.Action == answered.Action
			})
			result.Resolved = append(result.Resolved, schemaext.ResolvedRequest{Request: index, Subject: answer.Subject})
		}
	}
	if err := ctx.Err(); err != nil {
		return schemaext.ObjectComparisonResult{}, objectCounts{}, err
	}
	if err := requireResolved(request.Target, asked, result.Resolved); err != nil {
		return schemaext.ObjectComparisonResult{}, objectCounts{}, err
	}
	slices.SortFunc(result.Resolved, func(a, b schemaext.ResolvedRequest) int { return cmp.Compare(a.Request, b.Request) })
	slices.SortFunc(result.Changes, func(a, b schemaext.ChangeRecord) int { return schemaext.CompareRefs(a.Subject, b.Subject) })
	slices.SortFunc(result.Undecided, func(a, b schemaext.UndecidedChange) int {
		return cmp.Or(cmp.Compare(a.Kind, b.Kind), schemaext.CompareRefs(a.Subject, b.Subject))
	})
	return result, counts, nil
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
	request.Identifiers = request.Identifiers.Clone()
	request.Kinds = slices.Clone(request.Kinds)
	request.Parents = slices.Clone(request.Parents)
	seen := make(map[objectidentity.Key]bool)
	for _, parent := range request.Parents {
		ref := parent.Subject
		if ref.Kind != objectidentity.KindTable || ref.Name.Source == "" || ref.Name.Normalized == "" || !ref.Parent.Empty() || !ref.Signature.IsZero() || (!parent.Desired && !parent.Current) || seen[ref.Key()] {
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
	request.DeclaredRoles, err = snapshotDeclaredRoles(request.DeclaredRoles)
	if err != nil {
		return schemaext.ObjectComparisonRequest{}, err
	}
	request.Exclusions = slices.Clone(request.Exclusions)
	request.DeclaredColumns = slices.Clone(request.DeclaredColumns)
	request.Required, err = snapshotRequired(request.Required)
	if err != nil {
		return schemaext.ObjectComparisonRequest{}, err
	}
	request.DeclaredObjects = slices.Clone(request.DeclaredObjects)
	request.DeclaredFacets, err = r.snapshotFacetRecords(ctx, schemaext.Desired, request.DeclaredFacets)
	if err != nil {
		return schemaext.ObjectComparisonRequest{}, err
	}
	request.CurrentFacets, err = r.snapshotCurrentFacets(ctx, request.CurrentFacets)
	if err != nil {
		return schemaext.ObjectComparisonRequest{}, err
	}
	request.CurrentContext, err = r.snapshotCurrentContext(ctx, request.CurrentContext)
	if err != nil {
		return schemaext.ObjectComparisonRequest{}, err
	}
	if err := comparisonInputSubjects(request); err != nil {
		return schemaext.ObjectComparisonRequest{}, err
	}
	return request, nil
}

// declaredObjects returns the identity of every object of desired whose kind
// is one of kinds, in identity order: the declared objects a service that
// lists kinds as its declared kinds receives. The request's desired state is
// the whole one, before the runtime selects a service's own kinds.
func declaredObjects(desired schemaext.ObjectState, kinds []schemaext.Kind) []objectidentity.ID {
	if len(kinds) == 0 {
		return nil
	}
	var result []objectidentity.ID
	for _, ref := range desired.Objects.Refs() {
		if slices.Contains(kinds, schemaext.Kind(ref.Kind)) {
			result = append(result, ref)
		}
	}
	slices.SortFunc(result, schemaext.CompareRefs)
	return result
}

// readsDeclaredFacets reports whether an object comparison of target lists a
// declared facet kind, so a comparison hands its desired facets to object
// services, and copies them, only when one reads them.
func (r *Runtime) readsDeclaredFacets(target string) bool {
	selected, found := r.lookup(target)
	return found && slices.ContainsFunc(r.comparisonServices, func(service ObjectComparison) bool {
		return service.Target == selected.name && len(service.DeclaredFacetKinds) != 0
	})
}

// declaredFacets returns the facets of records whose kind is one of kinds,
// each record holding only those kinds with a value, in owner order: the
// declared facets a service that lists kinds as its declared facet kinds
// receives. A facet the declaration excludes on purpose has no value and is
// left out.
func declaredFacets(records []schemaext.FacetRecord, kinds []schemaext.Kind) ([]schemaext.FacetRecord, error) {
	if len(kinds) == 0 {
		return nil, nil
	}
	var result []schemaext.FacetRecord
	for _, record := range records {
		var values []schemaext.Value
		for _, kind := range kinds {
			value, found, err := record.Values.Get(kind)
			if err != nil {
				return nil, err
			}
			if found {
				values = append(values, value)
			}
		}
		if len(values) == 0 {
			continue
		}
		facets, err := schemaext.NewFacets(values...)
		if err != nil {
			return nil, err
		}
		result = append(result, schemaext.FacetRecord{Subject: record.Subject, Values: facets})
	}
	slices.SortFunc(result, func(a, b schemaext.FacetRecord) int { return schemaext.CompareRefs(a.Subject, b.Subject) })
	return result, nil
}

// snapshotFacetRecords copies facet records of representation through the
// owners' codecs, so a service never aliases the caller's values.
func (r *Runtime) snapshotFacetRecords(ctx context.Context, representation schemaext.Representation, records []schemaext.FacetRecord) ([]schemaext.FacetRecord, error) {
	if len(records) == 0 {
		return nil, nil
	}
	result := make([]schemaext.FacetRecord, 0, len(records))
	for _, record := range records {
		values, err := r.codecs.SnapshotFacets(ctx, representation, record.Values)
		if err != nil {
			return nil, err
		}
		result = append(result, schemaext.FacetRecord{Subject: record.Subject, Values: values})
	}
	return result, nil
}

// snapshotCurrentFacets copies the current facet state a caller names for the
// services that read a current context. A request without one keeps the zero
// value.
func (r *Runtime) snapshotCurrentFacets(ctx context.Context, state schemaext.FacetState) (schemaext.FacetState, error) {
	if len(state.Records) == 0 && state.Coverage.IsZero() {
		return schemaext.FacetState{}, nil
	}
	return r.codecs.SnapshotFacetState(ctx, schemaext.Observed, state)
}

// snapshotCurrentContext copies a service's current context: its objects with
// the coverage of every kind it holds, and its facets. The zero value stays
// the zero value.
func (r *Runtime) snapshotCurrentContext(ctx context.Context, state schemaext.FeatureState) (schemaext.FeatureState, error) {
	if state.Objects.IsZero() && len(state.Facets) == 0 && state.Coverage.IsZero() {
		return schemaext.FeatureState{}, nil
	}
	objects, err := r.codecs.SnapshotObjectState(ctx, schemaext.Observed, schemaext.ObjectState{Objects: state.Objects, Coverage: state.Coverage})
	if err != nil {
		return schemaext.FeatureState{}, err
	}
	facets, err := r.snapshotFacetRecords(ctx, schemaext.Observed, state.Facets)
	if err != nil {
		return schemaext.FeatureState{}, err
	}
	return schemaext.FeatureState{Objects: objects.Objects, Facets: facets, Coverage: objects.Coverage}, nil
}

// readsCurrentContext reports whether an object comparison of target reads a
// current context, so a comparison hands its current facets to object
// services, and copies them, only when one reads them.
func (r *Runtime) readsCurrentContext(target string) bool {
	selected, found := r.lookup(target)
	return found && slices.ContainsFunc(r.comparisonServices, func(service ObjectComparison) bool {
		return service.Target == selected.name && service.CurrentContext
	})
}

// currentContext is the current context of service in request: for a service
// that reads one, the current objects of its declared kinds, the current
// facets of its declared facet kinds, each record holding only those kinds
// with a value, in owner order, and the current coverage of all those kinds;
// the zero value for any other service. The objects and their coverage come
// from the request's whole current state, before the runtime selects the
// service's own kinds, and the facets from the current facets a caller named.
func currentContext(request schemaext.ObjectComparisonRequest, service ObjectComparison) (schemaext.FeatureState, error) {
	if !service.CurrentContext {
		return schemaext.FeatureState{}, nil
	}
	objects := selectObjectState(request.Current, service.DeclaredKinds)
	facets, err := declaredFacets(request.CurrentFacets.Records, service.DeclaredFacetKinds)
	if err != nil {
		return schemaext.FeatureState{}, err
	}
	coverage, err := objects.Coverage.Combine(request.CurrentFacets.Coverage.SelectKinds(service.DeclaredFacetKinds))
	if err != nil {
		return schemaext.FeatureState{}, err
	}
	return schemaext.FeatureState{Objects: objects.Objects, Facets: facets, Coverage: coverage}, nil
}

// snapshotRequired copies the objects the desired values require, ordered by
// identity and each once, refusing an identity without a kind or a name.
func snapshotRequired(refs []objectidentity.ID) ([]objectidentity.ID, error) {
	refs = slices.Clone(refs)
	for _, ref := range refs {
		if ref.Kind == "" || ref.Name.Source == "" || ref.Name.Normalized == "" {
			return nil, fmt.Errorf("%w: invalid required object %s", schemaext.ErrInvalidValue, ref)
		}
	}
	slices.SortFunc(refs, schemaext.CompareRefs)
	return slices.CompactFunc(refs, func(a, b objectidentity.ID) bool { return schemaext.CompareRefs(a, b) == 0 }), nil
}

// snapshotDeclaredRelations copies and orders the declared tables, views and
// materialized views, refusing a reference that is not one.
func snapshotDeclaredRelations(refs []objectidentity.ID) ([]objectidentity.ID, error) {
	refs = slices.Clone(refs)
	for _, ref := range refs {
		if !slices.Contains([]objectidentity.Kind{objectidentity.KindTable, objectidentity.KindView, objectidentity.KindMatView}, ref.Kind) ||
			ref.Name.Source == "" || ref.Name.Normalized == "" || !ref.Parent.Empty() || !ref.Signature.IsZero() {
			return nil, fmt.Errorf("%w: invalid declared relation %s", schemaext.ErrInvalidValue, ref)
		}
	}
	slices.SortFunc(refs, schemaext.CompareRefs)
	return refs, nil
}

// snapshotDeclaredRoles copies and orders the managed roles. A role has no
// catalog, schema, parent, or overload signature.
func snapshotDeclaredRoles(refs []objectidentity.ID) ([]objectidentity.ID, error) {
	refs = slices.Clone(refs)
	for _, ref := range refs {
		if ref.Kind != objectidentity.KindRole || ref.Name.Source == "" || ref.Name.Normalized == "" ||
			!ref.Database.Empty() || !ref.Namespace.IsZero() || !ref.Catalog.Empty() || !ref.Schema.Empty() || !ref.Parent.Empty() || !ref.Signature.IsZero() {
			return nil, fmt.Errorf("%w: invalid declared role %s", schemaext.ErrInvalidValue, ref)
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
		if change.Written != "" {
			// A spelling nobody can read is the first thing wrong with the
			// request, whatever else the states hold.
			if !r.acceptsAction(request.Target, change.Action) {
				return nil, nil, fmt.Errorf("%w: no owner on %q reads the %q request %q", ptaherr.ErrUnsupportedFeature, request.Target, change.Action, change.Written)
			}
			continue
		}
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
		if change.Written != "" {
			r.offerWritten(request.Target, change, batches)
			continue
		}
		service := r.comparisons[conversionKey{target: request.Target, kind: schemaext.Kind(change.Subject.Kind)}]
		if !slices.Contains(r.comparisonServices[service].Actions, change.Action) {
			return nil, nil, fmt.Errorf("%w: the owner of %q accepts no %q request, for %s", ptaherr.ErrUnsupportedFeature, change.Subject.Kind, change.Action, change.Subject)
		}
	}
	return batches, inactive, nil
}

// acceptsAction reports whether a comparison on target accepts action.
func (r *Runtime) acceptsAction(target, action string) bool {
	return slices.ContainsFunc(r.comparisonServices, func(declaration ObjectComparison) bool {
		return declaration.Target == target && slices.Contains(declaration.Actions, action)
	})
}

// offerWritten adds to batches every kind of each service on target that
// accepts the action of a written request, so each of them can resolve it
// against the desired objects it compares.
func (r *Runtime) offerWritten(target string, change schemaext.ChangeRequest, batches [][]schemaext.Kind) {
	for service, declaration := range r.comparisonServices {
		if declaration.Target != target || !slices.Contains(declaration.Actions, change.Action) {
			continue
		}
		for _, kind := range declaration.Kinds {
			if !slices.Contains(batches[service], kind) {
				batches[service] = append(batches[service], kind)
			}
		}
		slices.Sort(batches[service])
	}
}

// requireResolved refuses a written request that no owner resolved, or that
// more than one resolved, naming the operator's spelling: a spelling no owner
// reads must not read as a change done.
func requireResolved(target string, requests []schemaext.ChangeRequest, resolved []schemaext.ResolvedRequest) error {
	for index, change := range requests {
		if change.Written == "" {
			continue
		}
		owners := 0
		for _, answer := range resolved {
			if answer.Request == index {
				owners++
			}
		}
		switch {
		case owners == 0:
			return fmt.Errorf("%w: no owner on %q reads the %q request %q", ptaherr.ErrUnsupportedFeature, target, change.Action, change.Written)
		case owners > 1:
			return fmt.Errorf("%w: %d owners on %q read the %q request %q", schemaext.ErrInvalidValue, owners, target, change.Action, change.Written)
		}
	}
	return nil
}

// snapshotRequests copies the change requests in subject, action and
// spelling order. A request needs an action and either a named subject of a
// valid kind or a written spelling of one line with no surrounding space, and
// asking for one action on one subject, or on one spelling, twice is refused
// rather than merged.
func snapshotRequests(requests []schemaext.ChangeRequest) ([]schemaext.ChangeRequest, error) {
	if len(requests) == 0 {
		return nil, nil
	}
	type requestKey struct {
		subject objectidentity.Key
		action  string
		written string
	}
	seen := make(map[requestKey]bool, len(requests))
	for _, change := range requests {
		key := requestKey{change.Subject.Key(), change.Action, change.Written}
		if change.Written != "" && (change.Subject.Kind != "" || change.Subject.Name.Source != "") {
			return nil, fmt.Errorf("%w: change request %q names its subject both as %s and as written %q", schemaext.ErrInvalidValue, change.Action, change.Subject, change.Written)
		}
		named := schemaext.Kind(change.Subject.Kind).Valid() && change.Subject.Name.Source != ""
		if change.Written != "" {
			named = reversalText(change.Written)
		}
		if !named || !reversalText(change.Action) || seen[key] {
			return nil, fmt.Errorf("%w: invalid or duplicate change request %q for %s", schemaext.ErrInvalidValue, change.Action, requestName(change))
		}
		seen[key] = true
	}
	requests = slices.Clone(requests)
	slices.SortFunc(requests, func(a, b schemaext.ChangeRequest) int {
		return cmp.Or(schemaext.CompareRefs(a.Subject, b.Subject), cmp.Compare(a.Action, b.Action), cmp.Compare(a.Written, b.Written))
	})
	return requests, nil
}

// requestName is how a message names the object of a request.
func requestName(change schemaext.ChangeRequest) string {
	if change.Written != "" {
		return strconv.Quote(change.Written)
	}
	return change.Subject.String()
}

// selectRequests returns the requests of a batch: those that name a subject
// of its kinds, and the written ones whose action its service accepts.
func selectRequests(requests []schemaext.ChangeRequest, kinds []schemaext.Kind, actions []string) []schemaext.ChangeRequest {
	var selected []schemaext.ChangeRequest
	for _, change := range requests {
		if change.Written != "" && slices.Contains(actions, change.Action) || change.Written == "" && slices.Contains(kinds, schemaext.Kind(change.Subject.Kind)) {
			selected = append(selected, change)
		}
	}
	return selected
}

func selectObjectState(state schemaext.ObjectState, kinds []schemaext.Kind) schemaext.ObjectState {
	return schemaext.ObjectState{Objects: state.Objects.Select(func(ref objectidentity.ID) bool { return slices.Contains(kinds, schemaext.Kind(ref.Kind)) }), Coverage: state.Coverage.SelectKinds(kinds)}
}

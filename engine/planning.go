package engine

import (
	"context"
	"fmt"
	"slices"

	"ptah.run/core/featureplan"
	"ptah.run/core/objectidentity"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
)

// Planning assigns change kinds to one contextual planning service on a target.
// The provider owns every change and operation codec. OperationKinds declares
// the complete output vocabulary; registration does not grant execution rights.
type Planning struct {
	Target string
	Kinds  []schemaext.Kind
	// ParentKinds assigns attached model assessment independently of child
	// deltas. Each model requires owned desired and observed codecs.
	ParentKinds []schemaext.Kind
	// ContextKinds assigns named object assessment without requiring a change.
	// Each kind has one planner and owned desired and observed codecs.
	ContextKinds   []schemaext.Kind
	OperationKinds []schemaext.Kind
	Service        featureplan.Service
}

type ownedPlanning struct {
	owner string
	Planning
}

func (r *Runtime) registerPlanning(owner string, declaration Planning) error {
	target, found := r.targets[declaration.Target]
	if !found || target.name != declaration.Target || (len(declaration.Kinds) == 0 && len(declaration.ParentKinds) == 0 && len(declaration.ContextKinds) == 0) || declaration.Service == nil || nilService(declaration.Service) {
		return fmt.Errorf("%w: incomplete planning registration for %q", ErrInvalidRegistration, declaration.Target)
	}
	if len(declaration.Kinds) > 0 && len(declaration.OperationKinds) == 0 {
		return fmt.Errorf("%w: change planning requires an operation vocabulary", ErrInvalidRegistration)
	}
	for _, kind := range declaration.ParentKinds {
		if !r.ownsCodec(owner, kind, schemaext.Desired) || !r.ownsCodec(owner, kind, schemaext.Observed) {
			return fmt.Errorf("%w: %q does not own parent model %q", ErrInvalidRegistration, owner, kind)
		}
		key := conversionKey{target: target.name, kind: kind}
		if _, found := r.parentPlanning[key]; found {
			return fmt.Errorf("%w: duplicate parent planning for %q/%q", ErrInvalidRegistration, target.name, kind)
		}
		r.parentPlanning[key] = len(r.planningServices)
	}
	if err := r.registerObjectPlanning(owner, target.name, declaration.ContextKinds); err != nil {
		return err
	}
	for _, kind := range declaration.Kinds {
		if !r.ownsCodec(owner, kind, schemaext.Change) {
			return fmt.Errorf("%w: %q does not own change %q", ErrInvalidRegistration, owner, kind)
		}
		key := conversionKey{target: target.name, kind: kind}
		if _, found := r.planning[key]; found {
			return fmt.Errorf("%w: duplicate planning for %q/%q", ErrInvalidRegistration, target.name, kind)
		}
		r.planning[key] = len(r.planningServices)
	}
	if err := r.validatePlanningOperations(owner, declaration.OperationKinds); err != nil {
		return err
	}
	declaration.Kinds = slices.Clone(declaration.Kinds)
	declaration.ParentKinds = slices.Clone(declaration.ParentKinds)
	declaration.ContextKinds = slices.Clone(declaration.ContextKinds)
	slices.Sort(declaration.ContextKinds)
	declaration.OperationKinds = slices.Clone(declaration.OperationKinds)
	r.planningServices = append(r.planningServices, ownedPlanning{owner, declaration})
	return nil
}

func (r *Runtime) validatePlanningOperations(owner string, kinds []schemaext.Kind) error {
	seen := make(map[schemaext.Kind]bool)
	for _, kind := range kinds {
		if seen[kind] || !r.ownsCodec(owner, kind, schemaext.Operation) {
			return fmt.Errorf("%w: duplicate or unowned planning operation %q", ErrInvalidRegistration, kind)
		}
		seen[kind] = true
	}
	return nil
}

func (r *Runtime) registerObjectPlanning(owner, target string, kinds []schemaext.Kind) error {
	for _, kind := range kinds {
		if !r.ownsCodec(owner, kind, schemaext.Desired) || !r.ownsCodec(owner, kind, schemaext.Observed) {
			return fmt.Errorf("%w: %q does not own context model %q", ErrInvalidRegistration, owner, kind)
		}
		key := conversionKey{target: target, kind: kind}
		if _, found := r.objectPlanning[key]; found {
			return fmt.Errorf("%w: duplicate object context planning for %q/%q", ErrInvalidRegistration, target, kind)
		}
		r.objectPlanning[key] = len(r.planningServices)
	}
	return nil
}

// PlanFeatures validates the complete request before dispatching one batch per
// selected service. Inputs and replies are isolated through local codecs. Every
// change is accounted for; missing ownership, malformed replies, and cancellation
// return no result. Contributions still require scheduling with the host graph.
// Completed refusals retain diagnostics from every selected service, with change
// indexes remapped to the caller's batch. Any refusal discards all contributions
// and receipts. Provider failures discard diagnostics too.
func (r *Runtime) PlanFeatures(ctx context.Context, request featureplan.Request) (featureplan.Result, error) {
	if err := schemaext.RequireRuntime(ctx, r); err != nil {
		return featureplan.Result{}, err
	}
	target, found := r.lookup(request.Target)
	if !found {
		return featureplan.Result{}, fmt.Errorf("%w: %q", ptaherr.ErrUnsupportedDialect, request.Target)
	}
	callerObjects := request.Objects.AllSubjects()
	request.Target = target.name
	// Assignment is runtime-owned. An input cannot suppress a registered model.
	request.ParentKinds = nil
	request.ContextKinds = nil
	request.Objects.ChangedSubjects = nil
	request, err := r.snapshotPlanning(ctx, request)
	if err != nil {
		return featureplan.Result{}, err
	}
	request.Objects.ChangedSubjects = changedObjectSubjects(request)
	batches, err := r.planningBatches(request)
	if err != nil {
		return featureplan.Result{}, err
	}
	result := featureplan.Result{Complete: true, Changes: make([]featureplan.ChangePlan, len(request.Changes))}
	for service, indices := range batches {
		owner := r.planningServices[service]
		assessParents := owner.Target == target.name && len(owner.ParentKinds) > 0 && hasParentActions(request.Tables)
		assessObjects := owner.Target == target.name && len(owner.ContextKinds) > 0
		if len(indices) == 0 && !assessParents && !assessObjects {
			continue
		}
		batch := r.planningBatch(owner, request, indices)
		sent, err := r.snapshotPlanning(ctx, batch)
		if err != nil {
			return featureplan.Result{}, err
		}
		reply, err := r.planningServices[service].Service.PlanFeatures(ctx, sent)
		if err != nil {
			return featureplan.Result{}, err
		}
		reply, err = r.validatePlanningReply(ctx, service, batch, reply)
		if err != nil {
			return featureplan.Result{}, err
		}
		if len(reply.Diagnostics) != 0 {
			result.Diagnostics = append(result.Diagnostics, remapPlanningDiagnostics(reply.Diagnostics, indices, batch.Objects.AllSubjects(), callerObjects)...)
			continue
		}
		result.Contributions = append(result.Contributions, reply.Contributions...)
		result.Rewrites = append(result.Rewrites, reply.Rewrites...)
		result.Parents = append(result.Parents, reply.Parents...)
		result.Objects = append(result.Objects, reply.Objects...)
		for i, index := range indices {
			result.Changes[index] = reply.Changes[i]
		}
	}
	if err := ctx.Err(); err != nil {
		return featureplan.Result{}, err
	}
	if len(result.Diagnostics) != 0 {
		return featureplan.Result{Complete: true, Diagnostics: result.Diagnostics}, nil
	}
	return result, nil
}

func (r *Runtime) planningBatch(owner ownedPlanning, request featureplan.Request, indices []int) featureplan.Request {
	batch := request
	batch.ContextKinds = slices.Clone(owner.ContextKinds)
	batch.Objects = featureplan.ObjectContext{}
	if len(owner.ContextKinds) > 0 {
		batch.Objects = r.planningObjectsFor(owner.owner, request.Target, request.Objects)
	}
	if len(owner.ParentKinds) > 0 && hasParentActions(request.Tables) {
		batch.ParentKinds = slices.Clone(owner.ParentKinds)
	}
	batch.Changes = make([]schemaext.ChangeRecord, len(indices))
	for i, index := range indices {
		batch.Changes[i] = request.Changes[index]
	}
	return batch
}

func remapPlanningDiagnostics(diagnostics []featureplan.Diagnostic, indices []int, batchObjects, allObjects []objectidentity.ID) []featureplan.Diagnostic {
	result := make([]featureplan.Diagnostic, len(diagnostics))
	for i, diagnostic := range diagnostics {
		if diagnostic.Change != nil {
			diagnostic.Change = new(indices[*diagnostic.Change])
		}
		if diagnostic.ContextObject != nil {
			subject := batchObjects[*diagnostic.ContextObject]
			diagnostic.ContextObject = new(slices.IndexFunc(allObjects, func(candidate objectidentity.ID) bool { return candidate.Key() == subject.Key() }))
		}
		result[i] = diagnostic
	}
	return result
}

func (r *Runtime) planningBatches(request featureplan.Request) ([][]int, error) {
	if err := r.validateParentPlanningOwnership(request); err != nil {
		return nil, err
	}
	batches := make([][]int, len(r.planningServices))
	type key struct {
		subject objectidentity.Key
		kind    schemaext.Kind
	}
	seen := make(map[key]bool)
	for index, change := range request.Changes {
		identity := key{change.Subject.Key(), change.Value.Kind()}
		if seen[identity] {
			return nil, fmt.Errorf("%w: duplicate planning input for %s/%q", schemaext.ErrInvalidValue, change.Subject, identity.kind)
		}
		seen[identity] = true
		service, found := r.planning[conversionKey{request.Target, identity.kind}]
		if !found {
			return nil, fmt.Errorf("%w: no planning service for %q/%q", ptaherr.ErrUnsupportedFeature, request.Target, identity.kind)
		}
		batches[service] = append(batches[service], index)
	}
	return batches, nil
}

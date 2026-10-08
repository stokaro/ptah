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
	ParentKinds    []schemaext.Kind
	OperationKinds []schemaext.Kind
	Service        featureplan.Service
}

type ownedPlanning struct {
	owner string
	Planning
}

func (r *Runtime) registerPlanning(owner string, declaration Planning) error {
	target, found := r.targets[declaration.Target]
	if !found || target.name != declaration.Target || (len(declaration.Kinds) == 0 && len(declaration.ParentKinds) == 0) || declaration.Service == nil || nilService(declaration.Service) {
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
	seen := make(map[schemaext.Kind]bool)
	for _, kind := range declaration.OperationKinds {
		if seen[kind] || !r.ownsCodec(owner, kind, schemaext.Operation) {
			return fmt.Errorf("%w: duplicate or unowned planning operation %q", ErrInvalidRegistration, kind)
		}
		seen[kind] = true
	}
	declaration.Kinds = slices.Clone(declaration.Kinds)
	declaration.ParentKinds = slices.Clone(declaration.ParentKinds)
	declaration.OperationKinds = slices.Clone(declaration.OperationKinds)
	r.planningServices = append(r.planningServices, ownedPlanning{owner, declaration})
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
	request.Target = target.name
	// Assignment is runtime-owned. An input cannot suppress a registered model.
	request.ParentKinds = nil
	request, err := r.snapshotPlanning(ctx, request)
	if err != nil {
		return featureplan.Result{}, err
	}
	batches, err := r.planningBatches(request)
	if err != nil {
		return featureplan.Result{}, err
	}
	result := featureplan.Result{Complete: true, Changes: make([]featureplan.ChangePlan, len(request.Changes))}
	for service, indices := range batches {
		owner := r.planningServices[service]
		assessParents := owner.Target == target.name && len(owner.ParentKinds) > 0 && hasParentActions(request.Tables)
		if len(indices) == 0 && !assessParents {
			continue
		}
		batch := request
		if assessParents {
			batch.ParentKinds = slices.Clone(owner.ParentKinds)
		}
		batch.Changes = make([]schemaext.ChangeRecord, len(indices))
		for i, index := range indices {
			batch.Changes[i] = request.Changes[index]
		}
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
			result.Diagnostics = append(result.Diagnostics, remapPlanningDiagnostics(reply.Diagnostics, indices)...)
			continue
		}
		result.Contributions = append(result.Contributions, reply.Contributions...)
		result.Rewrites = append(result.Rewrites, reply.Rewrites...)
		result.Parents = append(result.Parents, reply.Parents...)
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

func remapPlanningDiagnostics(diagnostics []featureplan.Diagnostic, indices []int) []featureplan.Diagnostic {
	result := make([]featureplan.Diagnostic, len(diagnostics))
	for i, diagnostic := range diagnostics {
		if diagnostic.Change != nil {
			diagnostic.Change = new(indices[*diagnostic.Change])
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

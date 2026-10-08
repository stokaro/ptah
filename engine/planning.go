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
	Target         string
	Kinds          []schemaext.Kind
	OperationKinds []schemaext.Kind
	Service        featureplan.Service
}

type ownedPlanning struct {
	owner string
	Planning
}

func (r *Runtime) registerPlanning(owner string, declaration Planning) error {
	target, found := r.targets[declaration.Target]
	if !found || target.name != declaration.Target || len(declaration.Kinds) == 0 || len(declaration.OperationKinds) == 0 || declaration.Service == nil || nilService(declaration.Service) {
		return fmt.Errorf("%w: incomplete planning registration for %q", ErrInvalidRegistration, declaration.Target)
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
	declaration.OperationKinds = slices.Clone(declaration.OperationKinds)
	r.planningServices = append(r.planningServices, ownedPlanning{owner, declaration})
	return nil
}

// PlanFeatures validates the complete request before dispatching one batch per
// selected service. Inputs and replies are isolated through local codecs. Every
// change is accounted for; missing ownership, malformed replies, and cancellation
// return no result. Contributions still require scheduling with the host graph.
func (r *Runtime) PlanFeatures(ctx context.Context, request featureplan.Request) (featureplan.Result, error) {
	if err := schemaext.RequireRuntime(ctx, r); err != nil {
		return featureplan.Result{}, err
	}
	target, found := r.lookup(request.Target)
	if !found {
		return featureplan.Result{}, fmt.Errorf("%w: %q", ptaherr.ErrUnsupportedDialect, request.Target)
	}
	request.Target = target.name
	request, err := r.snapshotPlanning(ctx, request)
	if err != nil {
		return featureplan.Result{}, err
	}
	batches, err := r.planningBatches(request)
	if err != nil {
		return featureplan.Result{}, err
	}
	result := featureplan.Result{Changes: make([]featureplan.ChangePlan, len(request.Changes))}
	for service, indices := range batches {
		if len(indices) == 0 {
			continue
		}
		batch := request
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
		result.Contributions = append(result.Contributions, reply.Contributions...)
		for i, index := range indices {
			result.Changes[index] = reply.Changes[i]
		}
	}
	if err := ctx.Err(); err != nil {
		return featureplan.Result{}, err
	}
	return result, nil
}

func (r *Runtime) planningBatches(request featureplan.Request) ([][]int, error) {
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

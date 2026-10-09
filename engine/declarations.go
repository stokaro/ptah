package engine

import (
	"context"
	"fmt"
	"slices"

	"ptah.run/core/featureplan"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
)

// DeclarationPlanning assigns desired standalone model kinds to one batched
// creation service. Its provider owns the desired and operation codecs. It
// neither requires an observed representation nor claims live absence.
type DeclarationPlanning struct {
	Target         string
	Kinds          []schemaext.Kind
	OperationKinds []schemaext.Kind
	Service        featureplan.DeclarationService
}

type ownedDeclarationPlanning struct {
	owner string
	DeclarationPlanning
}

func (r *Runtime) registerDeclarationPlanning(owner string, declaration DeclarationPlanning) error {
	target, found := r.targets[declaration.Target]
	if !found || target.name != declaration.Target || len(declaration.Kinds) == 0 || len(declaration.OperationKinds) == 0 || declaration.Service == nil || nilService(declaration.Service) {
		return fmt.Errorf("%w: incomplete declaration planning registration for %q", ErrInvalidRegistration, declaration.Target)
	}
	for _, kind := range declaration.Kinds {
		if !r.ownsCodec(owner, kind, schemaext.Desired) {
			return fmt.Errorf("%w: %q does not own declaration %q", ErrInvalidRegistration, owner, kind)
		}
		key := conversionKey{target.name, kind}
		if _, found := r.declarations[key]; found {
			return fmt.Errorf("%w: duplicate declaration planning for %q/%q", ErrInvalidRegistration, target.name, kind)
		}
		r.declarations[key] = len(r.declarationServices)
	}
	seen := make(map[schemaext.Kind]bool)
	for _, kind := range declaration.OperationKinds {
		if seen[kind] || !r.ownsCodec(owner, kind, schemaext.Operation) {
			return fmt.Errorf("%w: duplicate or unowned declaration operation %q", ErrInvalidRegistration, kind)
		}
		seen[kind] = true
	}
	declaration.Kinds = slices.Clone(declaration.Kinds)
	declaration.OperationKinds = slices.Clone(declaration.OperationKinds)
	r.declarationServices = append(r.declarationServices, ownedDeclarationPlanning{owner, declaration})
	return nil
}

// PlanDeclarations dispatches authored standalone objects to their selected
// owners after validating the entire request. Services receive isolated batches;
// every declaration and contributed step requires a receipt. Refusals discard all
// operations and retain diagnostic indexes in the caller's order. Errors discard
// the whole reply. Successful contributions still need the complete host graph.
func (r *Runtime) PlanDeclarations(ctx context.Context, request featureplan.DeclarationRequest) (featureplan.DeclarationResult, error) {
	if err := schemaext.RequireRuntime(ctx, r); err != nil {
		return featureplan.DeclarationResult{}, err
	}
	target, found := r.lookup(request.Target)
	if !found {
		return featureplan.DeclarationResult{}, fmt.Errorf("%w: %q", ptaherr.ErrUnsupportedDialect, request.Target)
	}
	request.Target = target.name
	request, err := r.snapshotDeclarations(ctx, request)
	if err != nil {
		return featureplan.DeclarationResult{}, err
	}
	batches := make([][]int, len(r.declarationServices))
	for index, object := range request.Objects {
		service, found := r.declarations[conversionKey{request.Target, object.Value.Kind()}]
		if !found {
			return featureplan.DeclarationResult{}, fmt.Errorf("%w: no declaration planning for %q/%q", ptaherr.ErrUnsupportedFeature, request.Target, object.Value.Kind())
		}
		batches[service] = append(batches[service], index)
	}
	result := featureplan.DeclarationResult{Complete: true, Declarations: make([]featureplan.DeclarationPlan, len(request.Objects))}
	for service, indices := range batches {
		if len(indices) == 0 {
			continue
		}
		batch := request
		batch.Objects = make([]schemaext.Object, len(indices))
		for i, index := range indices {
			batch.Objects[i] = request.Objects[index]
		}
		sent, err := r.snapshotDeclarations(ctx, batch)
		if err != nil {
			return featureplan.DeclarationResult{}, err
		}
		reply, err := r.declarationServices[service].Service.PlanDeclarations(ctx, sent)
		if err != nil {
			return featureplan.DeclarationResult{}, err
		}
		reply, err = r.validateDeclarationReply(ctx, service, batch, reply)
		if err != nil {
			return featureplan.DeclarationResult{}, err
		}
		for _, diagnostic := range reply.Diagnostics {
			if diagnostic.Object != nil {
				diagnostic.Object = new(indices[*diagnostic.Object])
			}
			result.Diagnostics = append(result.Diagnostics, diagnostic)
		}
		if len(reply.Diagnostics) != 0 {
			continue
		}
		result.Contributions = append(result.Contributions, reply.Contributions...)
		for i, index := range indices {
			result.Declarations[index] = reply.Declarations[i]
		}
	}
	if err := ctx.Err(); err != nil {
		return featureplan.DeclarationResult{}, err
	}
	if len(result.Diagnostics) != 0 {
		return featureplan.DeclarationResult{Complete: true, Diagnostics: result.Diagnostics}, nil
	}
	return result, nil
}

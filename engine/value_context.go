package engine

import (
	"context"
	"fmt"

	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
)

// ContextBinding assigns database interpretation to one owner for each target
// and model kind. The owner must register a desired or observed codec for each
// kind. Registration grants no inspection or dependency authority.
type ContextBinding struct {
	Target  string
	Kinds   []schemaext.Kind
	Service schemaext.ValueContextService
}

func (r *Runtime) registerContextBindings(provider Provider) error {
	for _, binding := range provider.ContextBindings {
		if err := r.registerContextBinding(provider.ID, binding); err != nil {
			return err
		}
	}
	return nil
}

func (r *Runtime) registerContextBinding(owner string, declaration ContextBinding) error {
	target, found := r.targets[declaration.Target]
	if !found || target.name != declaration.Target || len(declaration.Kinds) == 0 || declaration.Service == nil || nilService(declaration.Service) {
		return fmt.Errorf("%w: incomplete context binding registration", ErrInvalidRegistration)
	}
	index := len(r.bindingServices)
	for _, kind := range declaration.Kinds {
		owned := false
		for _, representation := range []schemaext.Representation{schemaext.Desired, schemaext.Observed} {
			codecOwner, exists := r.codecs.Owner(kind, representation)
			if exists && codecOwner != owner {
				return fmt.Errorf("%w: context binding does not own %q/%s", ErrInvalidRegistration, kind, representation)
			}
			owned = owned || exists
		}
		if !owned {
			return fmt.Errorf("%w: context binding has no schema codec for %q", ErrInvalidRegistration, kind)
		}
		key := conversionKey{target: target.name, kind: kind}
		if _, exists := r.bindings[key]; exists {
			return fmt.Errorf("%w: duplicate context binding for %q/%q", ErrInvalidRegistration, target.name, kind)
		}
		r.bindings[key] = index
	}
	r.bindingServices = append(r.bindingServices, declaration.Service)
	return nil
}

// ResolveDatabaseContext resolves aliases before calling the selected target's
// pure resolver. An unregistered target refuses; a target without this service
// returns the zero value. No other target or built-in service is consulted.
func (r *Runtime) ResolveDatabaseContext(ctx context.Context, request schemaext.DatabaseContextRequest) (schemaext.DatabaseContext, error) {
	if err := schemaext.RequireRuntime(ctx, r); err != nil {
		return schemaext.DatabaseContext{}, err
	}
	selected, found := r.lookup(request.Target)
	if !found {
		return schemaext.DatabaseContext{}, fmt.Errorf("%w: %q", ptaherr.ErrUnsupportedDialect, request.Target)
	}
	if selected.databaseContext == nil {
		return schemaext.DatabaseContext{}, nil
	}
	request.Target = selected.name
	bound, err := selected.databaseContext.ResolveDatabaseContext(ctx, request)
	if err != nil {
		return schemaext.DatabaseContext{}, err
	}
	if err := ctx.Err(); err != nil {
		return schemaext.DatabaseContext{}, err
	}
	if err := bound.Validate(); err != nil {
		return schemaext.DatabaseContext{}, err
	}
	return bound, nil
}

// BindValueContexts snapshots all inputs before dispatch and restores their
// order after owner batches complete. Models without a binding service retain
// their snapshots. Cancellation or any malformed result discards the batch.
func (r *Runtime) BindValueContexts(ctx context.Context, request schemaext.ValueContextRequest) ([]schemaext.Value, error) {
	if err := schemaext.RequireRuntime(ctx, r); err != nil {
		return nil, err
	}
	if err := request.Context.Validate(); err != nil {
		return nil, err
	}
	selected, found := r.lookup(request.Target)
	if !found {
		return nil, fmt.Errorf("%w: %q", ptaherr.ErrUnsupportedDialect, request.Target)
	}
	values, err := r.codecs.SnapshotValues(ctx, request.Representation, request.Values)
	if err != nil {
		return nil, err
	}
	batches := make([][]int, len(r.bindingServices))
	for i, value := range values {
		if service, exists := r.bindings[conversionKey{target: selected.name, kind: value.Kind()}]; exists {
			batches[service] = append(batches[service], i)
		}
	}
	for service, indexes := range batches {
		if len(indexes) == 0 {
			continue
		}
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		batch := schemaext.ValueContextRequest{Target: selected.name, Context: request.Context, Representation: request.Representation, Values: make([]schemaext.Value, len(indexes))}
		kinds := make([]schemaext.Kind, len(indexes))
		for i, index := range indexes {
			batch.Values[i], kinds[i] = values[index], values[index].Kind()
		}
		bound, err := r.bindingServices[service].BindValueContexts(ctx, batch)
		if err != nil {
			return nil, err
		}
		if len(bound) != len(kinds) {
			return nil, fmt.Errorf("%w: context binding changed the value count", schemaext.ErrInvalidValue)
		}
		bound, err = r.codecs.SnapshotValues(ctx, request.Representation, bound)
		if err != nil {
			return nil, err
		}
		for i, index := range indexes {
			if bound[i].Kind() != kinds[i] {
				return nil, fmt.Errorf("%w: context binding changed the ordered kind", schemaext.ErrInvalidValue)
			}
			values[index] = bound[i]
		}
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return values, nil
}

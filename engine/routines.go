package engine

import (
	"context"
	"fmt"
	"slices"

	"ptah.run/core/ptaherr"
	"ptah.run/core/routineext"
)

// ReadRoutineBodies reads the request's bodies with the reader of its target,
// named canonically, and returns the reader's answer. A target registered
// without a reader, and an unregistered one, wrap ptaherr.ErrUnsupportedDialect.
// A reply that does not answer the request wraps routineext.ErrInvalid. An
// error returns no result.
func (r *Runtime) ReadRoutineBodies(ctx context.Context, request routineext.ReadRequest) (routineext.ReadResult, error) {
	reader, err := r.routineReader(ctx, request.Target)
	if err != nil {
		return routineext.ReadResult{}, err
	}
	request.Target = reader.name
	request.Bodies = slices.Clone(request.Bodies)
	result, err := reader.routineBodies.ReadBodies(ctx, request)
	if err != nil {
		return routineext.ReadResult{}, err
	}
	if err := routineext.ValidateRead(request, result); err != nil {
		return routineext.ReadResult{}, fmt.Errorf("routine body reader of %q: %w", reader.owner, err)
	}
	return result, ctx.Err()
}

// routineReader is the target whose reader reads the bodies of the target a
// request names.
func (r *Runtime) routineReader(ctx context.Context, name string) (target, error) {
	if ctx == nil {
		return target{}, fmt.Errorf("%w: reading routine bodies requires a context", routineext.ErrInvalid)
	}
	selected, found := r.lookup(name)
	if !found || selected.routineBodies == nil {
		return target{}, fmt.Errorf("%w: no routine body reader for target %q", ptaherr.ErrUnsupportedDialect, name)
	}
	return selected, nil
}

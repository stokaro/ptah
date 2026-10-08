package engine

import (
	"fmt"

	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
)

// ResolveTarget resolves a name or alias using this runtime's frozen selection.
// It performs no provider call or database access. Unregistered targets satisfy
// errors.Is(err, ptaherr.ErrUnsupportedDialect), including on an empty runtime.
func (r *Runtime) ResolveTarget(name string) (schemaext.TargetSelection, error) {
	selected, found := r.lookup(name)
	if !found {
		return schemaext.TargetSelection{}, fmt.Errorf("%w: %q", ptaherr.ErrUnsupportedDialect, name)
	}
	return selected.selection, nil
}

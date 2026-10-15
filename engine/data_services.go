package engine

import (
	"fmt"

	"ptah.run/core/dataext"
	"ptah.run/core/ptaherr"
)

// DataServices returns the managed-data and seed services target registered,
// under any of its names: the zero Services when it registered none. An
// unregistered target fails with [ptaherr.ErrUnsupportedDialect].
func (r *Runtime) DataServices(target string) (dataext.Services, error) {
	selected, found := r.lookup(target)
	if !found {
		return dataext.Services{}, fmt.Errorf("%w: %q", ptaherr.ErrUnsupportedDialect, target)
	}
	return selected.data, nil
}

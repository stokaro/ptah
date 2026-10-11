package engine

import (
	"fmt"

	"ptah.run/core/ptaherr"
	"ptah.run/core/schemacomparison"
)

// ComparisonRules returns the comparison rules target registered, under any of
// its names: the zero Rules when it registered none. An unregistered target
// fails with [ptaherr.ErrUnsupportedDialect].
func (r *Runtime) ComparisonRules(target string) (schemacomparison.Rules, error) {
	selected, found := r.lookup(target)
	if !found {
		return schemacomparison.Rules{}, fmt.Errorf("%w: %q", ptaherr.ErrUnsupportedDialect, target)
	}
	return selected.comparison, nil
}

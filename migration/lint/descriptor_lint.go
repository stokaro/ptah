package lint

import (
	"ptah.run/core/platform"
	"ptah.run/core/platform/descriptor"
)

// lintOf returns what dialect's descriptor states about linting it, and the
// zero value for a dialect the registry does not know.
func lintOf(dialect string) descriptor.Lint {
	described, _ := platform.Lookup(dialect)
	return described.Lint
}

// lintDialects returns the canonical names of the dialects whose lint
// description satisfies states, in the order the registry holds them.
func lintDialects(states func(descriptor.Lint) bool) []string {
	var names []string
	for _, described := range platform.Dialects().Dialects() {
		if states(described.Lint) {
			names = append(names, described.Name)
		}
	}
	return names
}

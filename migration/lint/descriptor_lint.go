package lint

import (
	"slices"

	"ptah.run/core/lintext"
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

// soleSchema returns the one namespace of an engine that has no other, such as
// SQLite's main, and "" for an engine whose URL can select among several.
func soleSchema(dialect string) string {
	described, _ := platform.Lookup(dialect)
	if described.Schemas.Selection != descriptor.SchemaSelectionSingle {
		return ""
	}
	return described.DefaultSchema
}

// listsObjects reports whether a read of dialect's dev database lists the
// objects of kind in full ([descriptor.Lint.ListedObjects]), so the starting
// state can show one absent.
func listsObjects(dialect string, kind lintext.BaselineObjectKind) bool {
	return slices.Contains(lintOf(dialect).ListedObjects, string(kind))
}

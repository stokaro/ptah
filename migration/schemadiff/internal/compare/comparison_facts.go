package compare

import (
	"ptah.run/core/platform"
	"ptah.run/core/platform/descriptor"
)

// comparisonOf returns what dialect's descriptor states about comparing a
// declaration with its catalog, folding the spelling as
// platform.NormalizeDialect does, and the zero value for a dialect the
// registry does not know.
func comparisonOf(dialect string) descriptor.Comparison {
	described, _ := platform.Lookup(dialect)
	return described.Comparison
}

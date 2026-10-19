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

// foreignKeyNameRulesOf returns the names dialect's server gives an unnamed
// foreign key beside the ones its ForeignKeyNaming gives, and the zero value
// for a dialect the registry does not know.
func foreignKeyNameRulesOf(dialect string) descriptor.ForeignKeyNameRules {
	described, _ := platform.Lookup(dialect)
	return described.ForeignKeyNameRules
}

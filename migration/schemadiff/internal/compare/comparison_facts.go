package compare

import (
	"ptah.run/core/platform/descriptor"
	"ptah.run/internal/descriptortable"
)

// The tables below hold what each descriptor of the process registry states
// about comparing, for the helpers that read them once per column, index and
// key a comparison visits. platform.Lookup copies the whole descriptor on every
// call, which costs more than most of the decisions these helpers serve. A
// caller must not modify what a helper returns: the answers are shared.
var (
	comparisons = descriptortable.New(func(described descriptor.Dialect) descriptor.Comparison {
		return described.Comparison
	})
	columnFacts = descriptortable.New(func(described descriptor.Dialect) descriptor.Columns {
		return described.Columns
	})
	foreignKeyNameRules = descriptortable.New(func(described descriptor.Dialect) descriptor.ForeignKeyNameRules {
		return described.ForeignKeyNameRules
	})
	constraintNamings = descriptortable.New(func(described descriptor.Dialect) descriptor.ConstraintNaming {
		return described.ConstraintNaming
	})
	foreignKeyNamings = descriptortable.New(func(described descriptor.Dialect) descriptor.ForeignKeyNaming {
		return described.ForeignKeyNaming
	})
	describedDialects = descriptortable.New(func(described descriptor.Dialect) bool {
		return described.Name != ""
	})
	qualifiedCollations = descriptortable.New(func(described descriptor.Dialect) bool {
		return described.Grammar.Columns.QualifiedCollations
	})
)

// comparisonOf returns what dialect's descriptor states about comparing a
// declaration with its catalog, folding the spelling as
// platform.NormalizeDialect does, and the zero value for a dialect the
// registry does not know.
func comparisonOf(dialect string) descriptor.Comparison {
	return comparisons.Get(dialect)
}

// columnFactsOf returns what dialect's descriptor states about how its server
// reads a column definition, and the zero value for a dialect the registry
// does not know.
func columnFactsOf(dialect string) descriptor.Columns {
	return columnFacts.Get(dialect)
}

// foreignKeyNameRulesOf returns the names dialect's server gives an unnamed
// foreign key beside the ones its ForeignKeyNaming gives, and the zero value
// for a dialect the registry does not know.
func foreignKeyNameRulesOf(dialect string) descriptor.ForeignKeyNameRules {
	return foreignKeyNameRules.Get(dialect)
}

// constraintNamingOf returns how dialect's server names a constraint a schema
// leaves unnamed, and the zero value for a dialect the registry does not know.
func constraintNamingOf(dialect string) descriptor.ConstraintNaming {
	return constraintNamings.Get(dialect)
}

// foreignKeyNamingOf returns how dialect's server names an unnamed foreign key,
// and the zero value for a dialect the registry does not know.
func foreignKeyNamingOf(dialect string) descriptor.ForeignKeyNaming {
	return foreignKeyNamings.Get(dialect)
}

// isDescribed reports whether the process registry describes dialect.
func isDescribed(dialect string) bool {
	return describedDialects.Get(dialect)
}

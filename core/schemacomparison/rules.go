// Package schemacomparison declares the rules a target gives the shared
// comparison of the common schema model, where the target's catalog reports a
// declaration in a form the shared comparison cannot read on its own. A target
// registers its rules with its runtime, and a comparison asks the runtime for
// the rules of the target it compares for. A target that registers none is
// compared by the shared rules alone.
package schemacomparison

import (
	"ptah.run/catalog"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
)

// Rules are one target's comparison rules. A nil member leaves the shared
// comparison's own rule for that area in place. The zero value is a target
// with no rules of its own.
type Rules struct {
	// Columns compares column definitions and reads a reported default.
	Columns Columns
	// Indexes compares index definitions.
	Indexes Indexes
	// Views compares the queries of views and materialized views.
	Views Views
	// Routines gives a routine the attributes the target applies where its
	// declaration states none.
	Routines Routines
}

// Columns compares the column definitions of one target, and reads back the
// defaults its catalog reports.
type Columns interface {
	// TypeChange returns how the type of current, which the catalog reports
	// as currentType, differs from the type desired declares, written as
	// "<current> -> <desired>", or "" when current holds the declared type.
	// It replaces the shared type comparison of a column that is no domain.
	TypeChange(desired schemamodel.Field, current catalog.Column, currentType string) string
	// DefaultChange returns how held, the default current holds, differs
	// from declared, the default desired declares, written as
	// "<held> -> <declared>", or "" when they are one default. Either side
	// may be empty. It replaces the shared default comparison.
	DefaultChange(desired schemamodel.Field, current catalog.Column, declared, held string) string
	// AlwaysNotNull reports whether the target creates a column declared as
	// desired NOT NULL whatever its declaration says about nullability.
	AlwaysNotNull(desired schemamodel.Field) bool
	// SequenceChanges returns how the start and the increment of the
	// sequence current draws its values from differ from what desired
	// declares, keyed by the attribute a declaration spells each with, or
	// nil when they agree. The shared comparison asks it on a target with
	// [ptah.run/core/platform/capability.SerialSequenceOptions] only.
	SequenceChanges(desired schemamodel.Field, current catalog.Column) map[string]string
	// DefaultValue returns the value a default the catalog reported was
	// written from, where the target stores every default as a typed value
	// and reports it as a literal, and false for a default that is no such
	// literal. A column read back into a declaration carries that value.
	DefaultValue(reported string) (value string, ok bool)
}

// Indexes compares the index definitions of one target.
type Indexes interface {
	// OptionsEqual reports whether current holds the index options desired
	// declares: its kind and the settings fixed when the index is built. A
	// target that registers it has the whole definition compared, its
	// uniqueness, key parts and covered columns included, and an index whose
	// definition differs is dropped and built again. It must not modify
	// either index.
	OptionsEqual(desired schemamodel.Index, current catalog.Index) bool
	// RenameCarries reports whether an index renamed in place keeps a facet
	// of kind, so that the comparison may pair a dropped and an added index
	// as a rename. Without it, an index that carries any facet is dropped and
	// built again rather than renamed.
	RenameCarries(kind schemaext.Kind) bool
}

// Views compares view queries on one target.
type Views interface {
	// QueryEqual reports whether desired and current are one query on the
	// target. It replaces the shared comparison's reading of a view's or a
	// materialized view's query.
	QueryEqual(desired, current string) bool
}

// Routines states what a target applies to a routine whose declaration leaves
// an attribute out. The shared comparison reads an omitted attribute as the
// target's on both sides, so a declaration that leaves it out compares equal
// to the routine the catalog reports, and the declaration is still written as
// it was declared.
type Routines interface {
	// Defaults returns the values the target applies, as its reader reports
	// them. An empty value leaves the attribute as declared.
	Defaults() RoutineDefaults
}

// RoutineDefaults are the values a target applies to a routine that states
// none. It is itself a [Routines] that answers with its own values, which is
// what a target with fixed defaults registers.
type RoutineDefaults struct {
	// Language is the routine language, such as plpgsql or sql.
	Language string
}

// Defaults returns d.
func (d RoutineDefaults) Defaults() RoutineDefaults {
	return d
}

// Runtime selects the rules a registered target gives the comparison.
type Runtime interface {
	// ComparisonRules returns the rules of target, the zero Rules for a
	// target that registers none. An unregistered target is an error.
	ComparisonRules(target string) (Rules, error)
}

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
	// Indexes compares index definitions.
	Indexes Indexes
	// Views compares the queries of views and materialized views.
	Views Views
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

// Runtime selects the rules a registered target gives the comparison.
type Runtime interface {
	// ComparisonRules returns the rules of target, the zero Rules for a
	// target that registers none. An unregistered target is an error.
	ComparisonRules(target string) (Rules, error)
}

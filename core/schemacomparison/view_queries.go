package schemacomparison

import (
	"context"

	"ptah.run/core/objectidentity"
	"ptah.run/core/platform/capability"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemaext"
)

// ViewQueries decides whether the query a declared view states and the query
// of the view the database holds under its name are one query, on a target
// whose engine reads a query by rules of its own, such as names that keep
// their case. A target that registers it has every view and materialized
// view query the comparison pairs decided by it rather than by a comparison
// of the two texts. It cannot be registered beside [Views].
type ViewQueries interface {
	// EquivalentViewQueries answers each pair of request, in order: whether
	// the declared query and the held one are one query on the target, or
	// that the target cannot decide it. The answer has one entry for each
	// pair. A failure decides nothing, and the comparison fails with it. It
	// must not modify the request.
	EquivalentViewQueries(ctx context.Context, request ViewQueriesRequest) ([]ViewQueryAnswer, error)
}

// ViewQueryAnswer is a target's answer about one view query pair. The zero
// value says the two queries differ, so an answer nobody set replaces the
// view rather than hiding a change.
type ViewQueryAnswer int

// The answers a target gives.
const (
	// ViewQueryDifferent says the queries are not one query: the view is
	// replaced.
	ViewQueryDifferent ViewQueryAnswer = iota
	// ViewQueryEquivalent says the queries are one query: the view stays.
	ViewQueryEquivalent
	// ViewQueryUndecided says the target cannot decide the pair, and the
	// comparison reads the two texts as it does for a target that registers
	// no view rules.
	ViewQueryUndecided
)

// ViewQueriesRequest asks a target about the view queries a comparison pairs.
type ViewQueriesRequest struct {
	// Target is the target the comparison compares for.
	Target string
	// Identifiers are the identifier rules of the comparison.
	Identifiers identifier.Semantics
	// Capabilities are the capabilities the comparison runs with.
	Capabilities capability.Capabilities
	// Pairs are the queries to decide, each held view named once.
	Pairs []ViewQueryPair
	// Columns are the columns of every table the desired schema declares,
	// each named by its table under Identifiers, in the order the declaration
	// lists them: what a `*` of a declared query stands for, and the relation
	// a column a server qualifies belongs to. A target that does not read
	// them ignores them.
	Columns []schemaext.DeclaredColumn
}

// ViewQueryPair is a declared view's query and the view the database holds
// under its name.
type ViewQueryPair struct {
	// Kind is [objectidentity.KindView] for a view and
	// [objectidentity.KindMatView] for a materialized view.
	Kind objectidentity.Kind
	// Schema and Name name the held view as the database reports it.
	Schema, Name string
	// Held is the query the database holds, as its read reports it.
	Held string
	// Declared is the query the declaration states, without the view's
	// name, its options and, for a materialized view, its data clause.
	Declared string
}

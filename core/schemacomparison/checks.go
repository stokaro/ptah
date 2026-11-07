package schemacomparison

import (
	"context"

	"ptah.run/catalog"
	"ptah.run/core/platform/capability"
	"ptah.run/core/platform/identifier"
)

// Checks decides whether a declared CHECK and the CHECK a table holds under
// the same name are one constraint, on a target whose server keeps a CHECK in
// a form of its own: with literals cast, a list rewritten or parentheses
// added. A target that registers it has every CHECK the comparison pairs
// decided by it rather than by a comparison of the two texts.
type Checks interface {
	// EquivalentChecks answers each pair of request, in order: whether the
	// declared expression and the held CHECK are one constraint on the
	// target. The answer has one entry for each pair. A failure decides
	// nothing, and the comparison fails with it. It must not modify the
	// request.
	EquivalentChecks(ctx context.Context, request ChecksRequest) ([]bool, error)
}

// ChecksRequest asks a target about the CHECKs a comparison pairs.
type ChecksRequest struct {
	// Target is the target the comparison compares for.
	Target string
	// Identifiers are the identifier rules of the comparison.
	Identifiers identifier.Semantics
	// Capabilities are the capabilities the comparison runs with.
	Capabilities capability.Capabilities
	// Current is the database the comparison reads. It holds the table and
	// the CHECK of each pair.
	Current *catalog.Database
	// Pairs are the CHECKs to decide, each held CHECK named once.
	Pairs []CheckPair
}

// CheckPair is a declared CHECK expression and the CHECK a table holds under
// the same name.
type CheckPair struct {
	// Held is the CHECK the database holds, as Current reports it.
	Held catalog.Constraint
	// Declared is the declared expression, without the CHECK keyword and
	// its enclosing parentheses.
	Declared string
}

package difftypes

import "ptah.run/core/schemamodel"

// StreamingQueryChange holds both declarations for an in-place change.
// AllowStateReset belongs to Desired; it is never read from the database.
type StreamingQueryChange struct {
	// Desired is the query the target schema declares.
	Desired schemamodel.StreamingQuery
	// Current is the query the database holds.
	Current schemamodel.StreamingQuery
}

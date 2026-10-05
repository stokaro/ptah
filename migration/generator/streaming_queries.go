package generator

import (
	"ptah.run/core/schemamodel"
	"ptah.run/migration/schemadiff/difftypes"
)

func cloneStreamingQueries(queries []schemamodel.StreamingQuery) []schemamodel.StreamingQuery {
	if queries == nil {
		return nil
	}
	out := make([]schemamodel.StreamingQuery, len(queries))
	for i, query := range queries {
		query.Spec = query.Spec.Clone()
		out[i] = query
	}
	return out
}

func cloneStreamingQueryChanges(changes []difftypes.StreamingQueryChange) []difftypes.StreamingQueryChange {
	if changes == nil {
		return nil
	}
	out := make([]difftypes.StreamingQueryChange, len(changes))
	for i, change := range changes {
		change.Desired.Spec = change.Desired.Spec.Clone()
		change.Current.Spec = change.Current.Spec.Clone()
		out[i] = change
	}
	return out
}

// Reversing a body change also resets aggregation state. The explicit permission
// that made the forward change possible travels with its reverse; a rollback
// restores the declaration, never a checkpoint that was discarded.
func reverseStreamingQueryChanges(changes []difftypes.StreamingQueryChange) []difftypes.StreamingQueryChange {
	out := cloneStreamingQueryChanges(changes)
	for i, change := range out {
		change.Current.AllowStateReset = change.Desired.AllowStateReset
		out[i] = difftypes.StreamingQueryChange{Desired: change.Current, Current: change.Desired}
	}
	return out
}

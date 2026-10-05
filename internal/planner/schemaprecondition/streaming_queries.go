package schemaprecondition

import (
	"fmt"

	"ptah.run/core/platform/capability"
	"ptah.run/core/ptaherr"
	"ptah.run/migration/schemadiff/difftypes"
)

// RefuseStreamingQueries prevents a planner without this family from silently
// discarding its changes. Only the YDB planner emits streaming-query statements.
func RefuseStreamingQueries(dialect string, diff *difftypes.SchemaDiff) error {
	if diff == nil || (len(diff.StreamingQueriesAdded) == 0 && len(diff.StreamingQueriesRemoved) == 0 && len(diff.StreamingQueriesChanged) == 0) {
		return nil
	}
	return &ptaherr.CapabilityError{Dialect: dialect, Feature: string(capability.StreamingQueries), Err: ptaherr.ErrUnsupportedFeature,
		Message: fmt.Sprintf("streaming query changes require capability %s, unavailable on this %s target", capability.StreamingQueries, dialect)}
}

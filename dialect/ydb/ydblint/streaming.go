package ydblint

import (
	"ptah.run/core/lintext"
	"ptah.run/dialect/ydb/ydbstreaming"
	"ptah.run/internal/lintkit"
)

// YDB 26.2 resets aggregation state when a query body changes under FORCE.
// CREATE OR REPLACE also retains topic offsets while replacing the query.
func ydbStreamingCheckpointRule() lintkit.Rule {
	return lintkit.Rule{
		Rule: lintext.Rule{
			Code:          "YD160",
			Title:         "streaming query checkpoint state discarded",
			Severity:      lintext.Warning,
			Targets:       ydbOnly,
			AppliesToDown: true,
		},
		Statement: func(file *lintkit.File, stmt *lintext.Statement) (bool, string) {
			if !ydbRun(file.Target) || lintkit.HasWordPrefix(stmt.Words, "DROP") || !ydbstreaming.LosesCheckpoint(stmt.Words) {
				return false, ""
			}
			return true, "replacing a streaming query or changing its body retains topic offsets but resets aggregation state; rollback cannot recover that state"
		},
	}
}

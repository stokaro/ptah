package lint

import "ptah.run/dialect/ydb/ydbstreaming"

// YDB 26.2 resets aggregation state when a query body changes under FORCE.
// CREATE OR REPLACE also retains topic offsets while replacing the query.
func ydbStreamingCheckpointRule() Rule {
	return Rule{
		Code: "YD160", Title: "streaming query checkpoint state discarded", Severity: SeverityWarning,
		Dialects: ydbOnly, AppliesToDown: true,
		CheckStatement: func(stmt *Statement) (bool, string) {
			if !ydbRun(stmt.Target) || hasWordPrefix(stmt.Words, "DROP") || !ydbstreaming.LosesCheckpoint(stmt.Words) {
				return false, ""
			}
			return true, "replacing a streaming query or changing its body retains topic offsets but resets aggregation state; rollback cannot recover that state"
		},
	}
}

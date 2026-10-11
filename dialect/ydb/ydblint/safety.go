package ydblint

import (
	"slices"

	"ptah.run/core/lintext"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/ydb/ydbdiff"
	"ptah.run/dialect/ydb/ydbexternal"
	"ptah.run/dialect/ydb/ydbsecret"
	"ptah.run/dialect/ydb/ydbstreaming"
	"ptah.run/internal/lintkit"
)

// safetyEffect reads the judgment of a YQL statement whose effect only this
// owner knows ([lintext.FactSafetyEffect]): the drops that lose what a new
// object of the same name does not get back, the drops and replacements that
// change what YDB reads from another system, and the changes that reset or
// change a streaming query's continuous work. A safety assessment takes the
// judgment in place of its own reading of the words, and a statement this
// reads nothing of is left to that reading.
//
// An async replication dropped with CASCADE drops its replica tables, and one
// dropped without it leaves them read-only for good unless it was failed over
// first, which a statement read alone cannot tell. The reasons are those the
// planned changes of these objects report, so a written statement and a
// planned one read alike.
func safetyEffect(stmt *lintext.Statement) (lintext.Effect, bool) {
	w := stmt.Words
	switch {
	case lintkit.HasWordPrefix(w, "DROP", "ASYNC", "REPLICATION") && !slices.Contains(w, "CASCADE"):
		return judgment(schemaext.Behavioral, ydbdiff.KeepReplicaTablesReason)
	case lintkit.HasWordPrefix(w, "DROP", "ASYNC", "REPLICATION"):
		return judgment(schemaext.Destructive, ydbdiff.DropReplicationReason)
	case ydbstreaming.LosesCheckpoint(w):
		return judgment(schemaext.Destructive, ydbstreaming.CheckpointLoss)
	case lintkit.HasWordPrefix(w, "ALTER", "STREAMING", "QUERY"):
		return judgment(schemaext.Behavioral, ydbstreaming.ExecutionChange)
	case lintkit.HasWordPrefix(w, "DROP", "TOPIC"):
		return judgment(schemaext.Destructive, ydbdiff.DropTopicReason)
	case lintkit.HasWordPrefix(w, "ALTER", "TOPIC") && lintkit.HasWordSeq(w, "DROP", "CONSUMER"):
		return judgment(schemaext.Destructive, ydbdiff.DropConsumerReason)
	case lintkit.HasWordPrefix(w, "DROP", "TRANSFER"):
		return judgment(schemaext.Destructive, ydbdiff.DropTransferReason)
	case lintkit.HasWordPrefix(w, "DROP", "COORDINATION", "NODE"):
		return judgment(schemaext.Destructive, ydbdiff.DropCoordinationNodeReason)
	case lintkit.HasWordPrefix(w, "DROP", "SECRET"):
		return judgment(schemaext.Destructive, ydbsecret.DropReason)
	case lintkit.HasWordPrefix(w, "DROP", "EXTERNAL", "DATA", "SOURCE"):
		return judgment(schemaext.Behavioral, ydbexternal.DropSourceReason)
	case lintkit.HasWordPrefix(w, "DROP", "EXTERNAL", "TABLE"):
		return judgment(schemaext.Behavioral, ydbexternal.DropTableReason)
	case lintkit.HasWordPrefix(w, "CREATE", "OR", "REPLACE", "EXTERNAL"):
		return judgment(schemaext.Behavioral, ydbexternal.ReplaceReason)
	default:
		return lintext.Effect{}, false
	}
}

// judgment is a safety effect of impact, for reason.
func judgment(impact schemaext.Impact, reason string) (lintext.Effect, bool) {
	return lintext.Effect{Fact: lintext.FactSafetyEffect, Impact: impact, Reason: reason}, true
}

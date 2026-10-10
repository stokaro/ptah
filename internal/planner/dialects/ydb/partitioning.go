package ydb

import (
	"fmt"

	"ptah.run/core/objectidentity"
	"ptah.run/dialect/ydb/ydbdiff"
	"ptah.run/dialect/ydb/ydbplan"
	"ptah.run/migration/schemadiff/difftypes"
)

// How a YDB plan changes a table's settings: how it splits into partitions,
// its read replicas and its key bloom filter.
//
// The settings are the YDB owner's facet of the table, and the owner plans
// their change: one `ALTER TABLE t SET (...)`, written with the table's other
// facets at the table's place in the plan (see [Planner.planTableFacets]).
// The settings depend on no column.
//
// A starting layout (UNIFORM_PARTITIONS, PARTITION_AT_KEYS) is taken only by
// CREATE TABLE. A change that asks for one the table was not created with is
// refused here, with how to ask for a rebuild, or rebuilds the table when
// asked, which writes the declared settings, and the held value of every
// other one, into the new table's CREATE TABLE (see
// [ydbplan.RebuiltTablePartitioning]).

// partitioningChange is the change of a table's own settings the diff
// carries, or nil.
func partitioningChange(tableDiff difftypes.TableDiff) *ydbdiff.TablePartitioning {
	for _, record := range tableDiff.FeatureChanges {
		if change, ok := record.Value.(*ydbdiff.TablePartitioning); ok && record.Subject.Kind == objectidentity.KindTable {
			return change
		}
	}
	return nil
}

// refusePartitioningChange refuses, before anything is emitted, a change of a
// table's settings that only a rebuild makes, saying how to ask for one. The
// owner refuses every other change of the settings it cannot make.
func (p *Planner) refusePartitioningChange(tableDiff difftypes.TableDiff) error {
	if reason := ydbplan.TablePartitioningRebuildReason(partitioningChange(tableDiff)); reason != "" {
		return p.rebuildableFact(fmt.Sprintf("table %q", tableDiff.TableName), reason)
	}
	return nil
}

// partitioningNeedsRebuild reports whether a table's settings change in a way
// only a rebuild makes.
func partitioningNeedsRebuild(tableDiff difftypes.TableDiff) bool {
	return ydbplan.TablePartitioningRebuildReason(partitioningChange(tableDiff)) != ""
}

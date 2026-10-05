//go:build integration

package ydb_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ast"
)

// A new table takes its settings from the cluster's table profile, not from
// YDB's documentation. With the cluster's configuration replaced, as the
// EnableMoveIndex test replaces it, a new table does not split by size, and a
// declaration that names no setting keeps that: nothing is planned against
// it. Declaring AUTO_PARTITIONING_BY_SIZE = ENABLED plans exactly that change,
// with the held value of the rest of the splitting group named so nothing
// else moves, and nothing is left to plan once it ran.
func TestYDBTablePartitioning_ClusterProfileIsKept(t *testing.T) {
	for _, line := range ydbLines {
		t.Run(line.name, func(t *testing.T) {
			c := qt.New(t)
			clusterFlagOff(c, line, "enable_move_index", "EnableMoveIndex")
			conn := openYDB(c, line)
			dropTables(c, conn, partitioningSchemas)
			c.Cleanup(func() { dropTables(c, conn, partitioningSchemas) })

			silent := partitionedItems(nil)
			apply(c, conn, planAgainst(c, conn, silent, partitioningSchemas))
			c.Assert(partitioningOf(c, conn), qt.DeepEquals, &ast.YDBTablePartitioningSpec{BySize: new(false)})
			c.Assert(planAgainst(c, conn, silent, partitioningSchemas), qt.HasLen, 0)

			bySize := partitionedItems(&ast.YDBTablePartitioningSpec{BySize: new(true)})
			changes := planAgainst(c, conn, bySize, partitioningSchemas)
			c.Assert(changes, qt.DeepEquals, []string{
				"ALTER TABLE `ptah_ydb_partitioning/items` SET (AUTO_PARTITIONING_BY_SIZE = ENABLED, " +
					"AUTO_PARTITIONING_PARTITION_SIZE_MB = 2048, AUTO_PARTITIONING_BY_LOAD = DISABLED, " +
					"AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 1)",
			})
			apply(c, conn, changes)
			c.Assert(planAgainst(c, conn, bySize, partitioningSchemas), qt.HasLen, 0)
			c.Assert(planAgainst(c, conn, silent, partitioningSchemas), qt.HasLen, 0)
			c.Assert(partitioningOf(c, conn), qt.IsNil)
		})
	}
}

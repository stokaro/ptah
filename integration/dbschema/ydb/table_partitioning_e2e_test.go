//go:build integration

package ydb_test

import (
	"context"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ast"
	"ptah.run/internal/dbtarget"
)

// HCL has no spelling for a YDB table's settings. `ptah-compat schema inspect`
// says so for each table it leaves them out of, and the document applied back
// to the database it came from plans nothing: a setting a description leaves
// out keeps what the table holds, so the table does not return to YDB's
// defaults.
func TestYDBCompatBinary_KeepsTableSettingsHCLCannotWrite(t *testing.T) {
	c := qt.New(t)
	binary := buildCompatBinary(c, c.Context())
	for _, line := range ydbLines {
		t.Run(line.name, func(t *testing.T) {
			url := dbtarget.URL(t, line.engine)
			c := qt.New(t)
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
			defer cancel()
			conn := openYDB(c, line)
			dropTables(c, conn, partitioningSchemas)
			c.Cleanup(func() { dropTables(c, conn, partitioningSchemas) })
			settings := &ast.YDBTablePartitioningSpec{ByLoad: new(true), MinPartitions: 4, KeyBloomFilter: new(true)}
			apply(c, conn, planAgainst(c, conn, partitionedItems(settings), partitioningSchemas))

			inspected, notes, inspectErr := runCompat(ctx, binary, "schema", "inspect", "--url", url, "--schema", partitioningSchema)
			c.Assert(inspectErr, qt.IsNil, qt.Commentf("schema inspect:\n%s", notes))
			c.Assert(notes, qt.Contains, "warning: table."+partitioningSchema+".items: YDB table settings "+
				"(AUTO_PARTITIONING_BY_LOAD = ENABLED, AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 4, KEY_BLOOM_FILTER = ENABLED) "+
				"are not represented in HCL")
			reapplied, _, reapplyErr := runCompat(ctx, binary, "schema", "apply", "--url", url, "--schema", partitioningSchema,
				"--to", "file://"+writeCompatFile(c, c.TempDir(), "inspected.hcl", inspected), "--dry-run")
			c.Assert(reapplyErr, qt.IsNil, qt.Commentf("schema apply of the inspected document:\n%s", reapplied))
			c.Assert(reapplied, qt.Equals, "Schema is synced, no changes to be made\n")
			c.Assert(partitioningOf(c, conn), qt.DeepEquals, settings)
		})
	}
}

// partitioningRebuildDesired is the items table with its label column made
// NOT NULL, in HCL, which has no spelling for the table's settings.
const partitioningRebuildDesired = `schema "` + partitioningSchema + `" {
}

table "items" {
  schema = schema.` + partitioningSchema + `
  column "id" {
    type = Uint64
  }
  column "code" {
    type = Utf8
  }
  column "label" {
    type = Utf8
  }
  primary_key {
    columns = [column.id, column.code]
  }
}
`

// A table rebuilt from an HCL desired state keeps its settings: the document
// cannot spell them, so it leaves each one out, and the rebuild writes the
// settings the table holds on the new table rather than dropping them with the
// old one.
func TestYDBCompatBinary_RebuildKeepsTableSettingsHCLCannotWrite(t *testing.T) {
	c := qt.New(t)
	binary := buildCompatBinary(c, c.Context())
	for _, line := range ydbLines {
		t.Run(line.name, func(t *testing.T) {
			url := dbtarget.URL(t, line.engine)
			c := qt.New(t)
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
			defer cancel()
			conn := openYDB(c, line)
			cleanup := func() {
				dropDirectory(c, conn, partitioningSchema, "items", "__ptah_rebuild_items", "__ptah_replaced_items")
			}
			cleanup()
			c.Cleanup(cleanup)
			c.Assert(conn.Writer().ExecuteSQL(ctx, "CREATE TABLE `"+partitioningSchema+"/items` "+
				"(`id` Uint64 NOT NULL, `code` Utf8 NOT NULL, `label` Utf8, PRIMARY KEY (`id`, `code`)) "+
				"WITH (AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 3, READ_REPLICAS_SETTINGS = 'PER_AZ:1')"), qt.IsNil)
			c.Assert(conn.Writer().ExecuteSQL(ctx, "UPSERT INTO `"+partitioningSchema+"/items` (id, code, label) "+
				"VALUES (1ul, 'a'u, 'one'u)"), qt.IsNil)
			desired := "file://" + writeCompatFile(c, c.TempDir(), "desired.hcl", partitioningRebuildDesired)

			rebuilt, notes, rebuildErr := runCompatWithEnv(ctx, binary, []string{"PTAH_ALLOW_TABLE_REBUILD=1"},
				"schema", "apply", "--url", url, "--schema", partitioningSchema, "--to", desired, "--auto-approve")

			c.Assert(rebuildErr, qt.IsNil, qt.Commentf("schema apply:\n%s\n%s", rebuilt, notes))
			c.Assert(rebuilt, qt.Contains,
				") WITH (AUTO_PARTITIONING_BY_SIZE = ENABLED, AUTO_PARTITIONING_PARTITION_SIZE_MB = 2048, "+
					"AUTO_PARTITIONING_BY_LOAD = DISABLED, AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 3, "+
					"READ_REPLICAS_SETTINGS = \"PER_AZ:1\", KEY_BLOOM_FILTER = DISABLED);")
			c.Assert(partitioningOf(c, conn), qt.DeepEquals,
				&ast.YDBTablePartitioningSpec{MinPartitions: 3, ReadReplicas: "PER_AZ:1"})
			synced, _, syncedErr := runCompat(ctx, binary,
				"schema", "apply", "--url", url, "--schema", partitioningSchema, "--to", desired, "--dry-run")
			c.Assert(syncedErr, qt.IsNil, qt.Commentf("schema apply again:\n%s", synced))
			c.Assert(synced, qt.Equals, "Schema is synced, no changes to be made\n")
		})
	}
}

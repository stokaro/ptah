//go:build integration

package ydb_test

import (
	"slices"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ast"
	"ptah.run/core/schemamodel"
)

// globalIndexSchema is the directory the global-index tests write into.
const globalIndexSchema = "ptah_ydb_global_indexes"

var globalIndexSchemas = []string{globalIndexSchema}

// globalIndexDeclaration is a table carrying a synchronous, an asynchronous
// covering and a unique index, each with the partitioning partitioning gives
// it, keyed by index name; an index partitioning leaves out takes YDB's
// defaults.
func globalIndexDeclaration(partitioning map[string]*ast.IndexPartitioningSpec, renamed map[string]string) *schemamodel.Database {
	name := func(index string) string {
		if to, ok := renamed[index]; ok {
			return to
		}
		return index
	}
	db := &schemamodel.Database{
		Tables: []schemamodel.Table{{StructName: "Item", Name: "items", Schema: globalIndexSchema}},
		Fields: []schemamodel.Field{
			{StructName: "Item", Name: "id", Type: "BIGINT", Primary: true},
			{StructName: "Item", Name: "sku", Type: "VARCHAR(64)", Nullable: true},
			{StructName: "Item", Name: "kind", Type: "TEXT", Nullable: true},
			{StructName: "Item", Name: "price", Type: "BIGINT", Nullable: true},
		},
		Indexes: []schemamodel.Index{
			{StructName: "Item", Name: name("idx_items_kind"), Fields: []string{"kind"},
				Partitioning: partitioning["idx_items_kind"]},
			{StructName: "Item", Name: name("idx_items_price"), Fields: []string{"price"}, Type: "async",
				IncludeColumns: []string{"kind", "sku"}, Partitioning: partitioning["idx_items_price"]},
			{StructName: "Item", Name: name("uq_items_sku"), Fields: []string{"sku"}, Unique: true,
				Partitioning: partitioning["uq_items_sku"]},
		},
	}
	schemamodel.Finalize(db)
	return db
}

// TestYDBGlobalIndexes_PartitioningRoundTrip applies indexes that declare their
// partitioning, reads the settings back from the server, and plans nothing
// after; then it changes the settings in place, and then removes a maximum,
// which only a rebuild reaches. Each step ends with nothing left to plan, and
// applying the same declaration again plans nothing too.
func TestYDBGlobalIndexes_PartitioningRoundTrip(t *testing.T) {
	for _, line := range ydbLines {
		t.Run(line.name, func(t *testing.T) {
			c := qt.New(t)
			conn := openYDB(c, line)
			dropTables(c, conn, globalIndexSchemas)
			c.Cleanup(func() { dropTables(c, conn, globalIndexSchemas) })

			tuned := map[string]*ast.IndexPartitioningSpec{
				"idx_items_kind":  {ByLoad: new(true), MinPartitions: 3, MaxPartitions: 9},
				"idx_items_price": {BySize: new(false), ReadReplicas: "PER_AZ:1"},
				"uq_items_sku":    {PartitionSizeMB: 512, MinPartitions: 2},
			}
			declared := globalIndexDeclaration(tuned, nil)
			apply(c, conn, planAgainst(c, conn, declared, globalIndexSchemas))
			c.Assert(planAgainst(c, conn, declared, globalIndexSchemas), qt.HasLen, 0)
			apply(c, conn, planAgainst(c, conn, declared, globalIndexSchemas))
			c.Assert(planAgainst(c, conn, declared, globalIndexSchemas), qt.HasLen, 0)

			live := readScoped(c, conn, globalIndexSchemas)
			c.Assert(indexNamed(c, live, "idx_items_kind").Partitioning, qt.DeepEquals,
				&ast.IndexPartitioningSpec{ByLoad: new(true), MinPartitions: 3, MaxPartitions: 9})
			c.Assert(indexNamed(c, live, "idx_items_price").Partitioning, qt.DeepEquals,
				&ast.IndexPartitioningSpec{BySize: new(false), ReadReplicas: "PER_AZ:1"})
			c.Assert(indexNamed(c, live, "uq_items_sku").Partitioning, qt.DeepEquals,
				&ast.IndexPartitioningSpec{PartitionSizeMB: 512, MinPartitions: 2})

			// In place: one ALTER INDEX per changed index, and each names every
			// setting, because setting AUTO_PARTITIONING_BY_SIZE resets the size and
			// the minimum the index holds.
			retuned := map[string]*ast.IndexPartitioningSpec{
				"idx_items_kind":  {ByLoad: new(true), MinPartitions: 4, MaxPartitions: 9},
				"idx_items_price": nil,
				"uq_items_sku":    {PartitionSizeMB: 512, MinPartitions: 2},
			}
			declared = globalIndexDeclaration(retuned, nil)
			changes := planAgainst(c, conn, declared, globalIndexSchemas)
			c.Assert(changes, qt.DeepEquals, []string{
				"ALTER TABLE `ptah_ydb_global_indexes/items` ALTER INDEX `idx_items_kind` SET (AUTO_PARTITIONING_BY_SIZE = ENABLED, " +
					"AUTO_PARTITIONING_PARTITION_SIZE_MB = 2048, AUTO_PARTITIONING_BY_LOAD = ENABLED, " +
					"AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 4, AUTO_PARTITIONING_MAX_PARTITIONS_COUNT = 9)",
				"ALTER TABLE `ptah_ydb_global_indexes/items` ALTER INDEX `idx_items_price` SET (AUTO_PARTITIONING_BY_SIZE = ENABLED, " +
					"AUTO_PARTITIONING_PARTITION_SIZE_MB = 2048, AUTO_PARTITIONING_BY_LOAD = DISABLED, " +
					"AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 1, READ_REPLICAS_SETTINGS = \"PER_AZ:0\")",
			})
			apply(c, conn, changes)
			c.Assert(planAgainst(c, conn, declared, globalIndexSchemas), qt.HasLen, 0)
			live = readScoped(c, conn, globalIndexSchemas)
			c.Assert(indexNamed(c, live, "idx_items_price").Partitioning, qt.IsNil)

			// A maximum YDB cannot remove in place: the index is rebuilt, which gives
			// it the defaults, and the rest of the declaration is set on the new one.
			declared = globalIndexDeclaration(map[string]*ast.IndexPartitioningSpec{
				"idx_items_kind": {ByLoad: new(true), MinPartitions: 4},
				"uq_items_sku":   {PartitionSizeMB: 512, MinPartitions: 2},
			}, nil)
			rebuild := planAgainst(c, conn, declared, globalIndexSchemas)
			c.Assert(rebuild, qt.DeepEquals, []string{
				"ALTER TABLE `ptah_ydb_global_indexes/items` DROP INDEX `idx_items_kind`",
				"ALTER TABLE `ptah_ydb_global_indexes/items` ADD INDEX `idx_items_kind` GLOBAL SYNC ON (`kind`)",
				"ALTER TABLE `ptah_ydb_global_indexes/items` ALTER INDEX `idx_items_kind` SET (AUTO_PARTITIONING_BY_SIZE = ENABLED, " +
					"AUTO_PARTITIONING_PARTITION_SIZE_MB = 2048, AUTO_PARTITIONING_BY_LOAD = ENABLED, " +
					"AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 4)",
			})
			apply(c, conn, rebuild)
			c.Assert(planAgainst(c, conn, declared, globalIndexSchemas), qt.HasLen, 0)
			live = readScoped(c, conn, globalIndexSchemas)
			c.Assert(indexNamed(c, live, "idx_items_kind").Partitioning, qt.DeepEquals,
				&ast.IndexPartitioningSpec{ByLoad: new(true), MinPartitions: 4})
		})
	}
}

// TestYDBGlobalIndexes_RenameRoundTrip renames an index in the declaration and
// plans one RENAME INDEX for it, not a drop and a create: the server keeps
// the index, its kind, its covered columns and its partitioning under the new
// name, and the table's rows stay readable through it. A rename that also
// changes the partitioning is a rename and an ALTER INDEX.
func TestYDBGlobalIndexes_RenameRoundTrip(t *testing.T) {
	for _, line := range ydbLines {
		t.Run(line.name, func(t *testing.T) {
			c := qt.New(t)
			conn := openYDB(c, line)
			dropTables(c, conn, globalIndexSchemas)
			c.Cleanup(func() { dropTables(c, conn, globalIndexSchemas) })

			partitioning := map[string]*ast.IndexPartitioningSpec{"idx_items_price": {MinPartitions: 3}}
			apply(c, conn, planAgainst(c, conn, globalIndexDeclaration(partitioning, nil), globalIndexSchemas))
			apply(c, conn, []string{"UPSERT INTO `ptah_ydb_global_indexes/items` (id, sku, kind, price) " +
				"VALUES (1, 'a'u, 'tool'u, 10), (2, 'b'u, 'part'u, 20)"})

			renamed := globalIndexDeclaration(partitioning, map[string]string{
				"idx_items_price": "idx_items_by_price",
				"uq_items_sku":    "uq_items_code",
			})
			renames := planAgainst(c, conn, renamed, globalIndexSchemas)
			c.Assert(renames, qt.DeepEquals, []string{
				"ALTER TABLE `ptah_ydb_global_indexes/items` RENAME INDEX `idx_items_price` TO `idx_items_by_price`",
				"ALTER TABLE `ptah_ydb_global_indexes/items` RENAME INDEX `uq_items_sku` TO `uq_items_code`",
			})
			apply(c, conn, renames)
			c.Assert(planAgainst(c, conn, renamed, globalIndexSchemas), qt.HasLen, 0)
			apply(c, conn, planAgainst(c, conn, renamed, globalIndexSchemas))
			c.Assert(planAgainst(c, conn, renamed, globalIndexSchemas), qt.HasLen, 0)

			live := readScoped(c, conn, globalIndexSchemas)
			c.Assert(indexNamesOf(live), qt.DeepEquals, []string{"idx_items_by_price", "idx_items_kind", "uq_items_code"})
			byPrice := indexNamed(c, live, "idx_items_by_price")
			c.Assert(byPrice.Method, qt.Equals, "GLOBAL ASYNC")
			c.Assert(byPrice.IncludeColumns, qt.DeepEquals, []string{"kind", "sku"})
			c.Assert(byPrice.Partitioning, qt.DeepEquals, &ast.IndexPartitioningSpec{MinPartitions: 3})
			c.Assert(indexNamed(c, live, "uq_items_code").IsUnique, qt.IsTrue)
			var count int64
			c.Assert(conn.QueryRowContext(c.Context(),
				"SELECT COUNT(*) FROM `ptah_ydb_global_indexes/items` VIEW `uq_items_code` WHERE sku = 'b'u").Scan(&count), qt.IsNil)
			c.Assert(count, qt.Equals, int64(1))

			// Back to the first names, with the partitioning changed on the way.
			partitioning["idx_items_price"] = &ast.IndexPartitioningSpec{MinPartitions: 5}
			back := planAgainst(c, conn, globalIndexDeclaration(partitioning, nil), globalIndexSchemas)
			c.Assert(back, qt.DeepEquals, []string{
				"ALTER TABLE `ptah_ydb_global_indexes/items` RENAME INDEX `idx_items_by_price` TO `idx_items_price`",
				"ALTER TABLE `ptah_ydb_global_indexes/items` RENAME INDEX `uq_items_code` TO `uq_items_sku`",
				"ALTER TABLE `ptah_ydb_global_indexes/items` ALTER INDEX `idx_items_price` SET (AUTO_PARTITIONING_BY_SIZE = ENABLED, " +
					"AUTO_PARTITIONING_PARTITION_SIZE_MB = 2048, AUTO_PARTITIONING_BY_LOAD = DISABLED, " +
					"AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 5)",
			})
			apply(c, conn, back)
			c.Assert(planAgainst(c, conn, globalIndexDeclaration(partitioning, nil), globalIndexSchemas), qt.HasLen, 0)
		})
	}
}

// TestYDBGlobalIndexes_AddedToATableThatExists adds an asynchronous covering
// index with its partitioning to a table that holds rows, through ADD INDEX
// and the ALTER INDEX after it, and plans nothing after.
func TestYDBGlobalIndexes_AddedToATableThatExists(t *testing.T) {
	for _, line := range ydbLines {
		t.Run(line.name, func(t *testing.T) {
			c := qt.New(t)
			conn := openYDB(c, line)
			dropTables(c, conn, globalIndexSchemas)
			c.Cleanup(func() { dropTables(c, conn, globalIndexSchemas) })

			before := globalIndexDeclaration(nil, nil)
			before.Indexes = slices.DeleteFunc(before.Indexes, func(index schemamodel.Index) bool {
				return index.Name == "idx_items_price"
			})
			apply(c, conn, planAgainst(c, conn, before, globalIndexSchemas))
			apply(c, conn, []string{"UPSERT INTO `ptah_ydb_global_indexes/items` (id, sku, kind, price) VALUES (1, 'a'u, 'tool'u, 10)"})

			after := globalIndexDeclaration(map[string]*ast.IndexPartitioningSpec{
				"idx_items_price": {ByLoad: new(true), MinPartitions: 2},
			}, nil)
			added := planAgainst(c, conn, after, globalIndexSchemas)
			c.Assert(added, qt.DeepEquals, []string{
				"ALTER TABLE `ptah_ydb_global_indexes/items` ADD INDEX `idx_items_price` GLOBAL ASYNC ON (`price`) COVER (`kind`, `sku`)",
				"ALTER TABLE `ptah_ydb_global_indexes/items` ALTER INDEX `idx_items_price` SET (AUTO_PARTITIONING_BY_SIZE = ENABLED, " +
					"AUTO_PARTITIONING_PARTITION_SIZE_MB = 2048, AUTO_PARTITIONING_BY_LOAD = ENABLED, " +
					"AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 2)",
			})
			apply(c, conn, added)
			c.Assert(planAgainst(c, conn, after, globalIndexSchemas), qt.HasLen, 0)
			apply(c, conn, planAgainst(c, conn, after, globalIndexSchemas))
			c.Assert(planAgainst(c, conn, after, globalIndexSchemas), qt.HasLen, 0)
		})
	}
}

// TestYDBGlobalIndexes_SettingOneResetsAnother changes an index's settings
// where writing only the ones that differ would leave others reset: setting
// AUTO_PARTITIONING_BY_LOAD resets the minimum partition count, and setting
// AUTO_PARTITIONING_BY_SIZE resets it and the size. Each change converges in
// one apply, which it does only because the statement names every setting.
func TestYDBGlobalIndexes_SettingOneResetsAnother(t *testing.T) {
	steps := []struct {
		name   string
		before map[string]*ast.IndexPartitioningSpec
		after  map[string]*ast.IndexPartitioningSpec
	}{
		{
			name:   "splitting by load turned on beside a minimum",
			before: map[string]*ast.IndexPartitioningSpec{"idx_items_kind": {MinPartitions: 5}},
			after:  map[string]*ast.IndexPartitioningSpec{"idx_items_kind": {ByLoad: new(true), MinPartitions: 5}},
		},
		{
			name:   "splitting by size turned back on beside a minimum",
			before: map[string]*ast.IndexPartitioningSpec{"idx_items_kind": {BySize: new(false), MinPartitions: 6}},
			after:  map[string]*ast.IndexPartitioningSpec{"idx_items_kind": {PartitionSizeMB: 100, MinPartitions: 6}},
		},
	}

	for _, line := range ydbLines {
		t.Run(line.name, func(t *testing.T) {
			for _, step := range steps {
				t.Run(step.name, func(t *testing.T) {
					c := qt.New(t)
					conn := openYDB(c, line)
					dropTables(c, conn, globalIndexSchemas)
					c.Cleanup(func() { dropTables(c, conn, globalIndexSchemas) })
					apply(c, conn, planAgainst(c, conn, globalIndexDeclaration(step.before, nil), globalIndexSchemas))

					declared := globalIndexDeclaration(step.after, nil)
					apply(c, conn, planAgainst(c, conn, declared, globalIndexSchemas))

					c.Assert(planAgainst(c, conn, declared, globalIndexSchemas), qt.HasLen, 0)
					c.Assert(indexNamed(c, readScoped(c, conn, globalIndexSchemas), "idx_items_kind").Partitioning, qt.DeepEquals,
						step.after["idx_items_kind"])
				})
			}
		})
	}
}

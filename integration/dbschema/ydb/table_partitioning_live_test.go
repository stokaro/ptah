//go:build integration

package ydb_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ast"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemamodel"
	"ptah.run/dbschema"
	"ptah.run/migration/planner"
	"ptah.run/migration/schemadiff"
)

// partitioningSchema is the directory the table-settings tests own.
const partitioningSchema = "ptah_ydb_partitioning"

var partitioningSchemas = []string{partitioningSchema}

// partitionedItems is a table keyed on an unsigned id and a text code, which
// declares partitioning.
func partitionedItems(partitioning *ast.YDBTablePartitioningSpec) *schemamodel.Database {
	db := &schemamodel.Database{
		Tables: []schemamodel.Table{{StructName: "Item", Name: "items", Schema: partitioningSchema,
			PrimaryKey: []string{"id", "code"}, YDBPartitioning: partitioning}},
		Fields: []schemamodel.Field{
			{StructName: "Item", Name: "id", Type: "BIGINT UNSIGNED"},
			{StructName: "Item", Name: "code", Type: "TEXT"},
			{StructName: "Item", Name: "label", Type: "TEXT", Nullable: true},
		},
	}
	schemamodel.Finalize(db)
	return db
}

// partitioningOf reads the settings of the items table back from the server.
func partitioningOf(c *qt.C, conn *dbschema.DatabaseConnection) *ast.YDBTablePartitioningSpec {
	c.Helper()
	return tableNamed(c, readScoped(c, conn, partitioningSchemas), partitioningSchema, "items").YDBPartitioning
}

// planPartitioning plans the migration that takes the items table to declared,
// with table rebuilds allowed or not.
func planPartitioning(c *qt.C, conn *dbschema.DatabaseConnection, declared *schemamodel.Database, rebuild bool) ([]string, error) {
	c.Helper()
	info := conn.Info()
	diff, err := schemadiff.CompareWithDatabaseInfo(declared, readScoped(c, conn, partitioningSchemas), info, nil)
	c.Assert(err, qt.IsNil)
	return planner.GenerateSchemaDiffSQLStatementsWithOptions(diff, info.Dialect, planner.Options{
		Capabilities: info.Capabilities, AllowTableRebuild: rebuild,
	})
}

// TestYDBTablePartitioning_RoundTrip creates a table with every setting, reads
// the settings back, and plans nothing after, applied twice. It then changes
// them in place: the change turns splitting by load on, which alone would
// reset the minimum to 1, so the statement names the minimum and the size, and
// the read replicas and the key bloom filter, which the declaration leaves
// out, stay as the table holds them. Declared away through PER_AZ:0 and
// DISABLED, since neither can be reset, they go. Removing every declaration
// plans nothing, the maximum YDB cannot remove included.
func TestYDBTablePartitioning_RoundTrip(t *testing.T) {
	for _, line := range ydbLines {
		t.Run(line.name, func(t *testing.T) {
			c := qt.New(t)
			conn := openYDB(c, line)
			dropTables(c, conn, partitioningSchemas)
			c.Cleanup(func() { dropTables(c, conn, partitioningSchemas) })

			tuned := &ast.YDBTablePartitioningSpec{PartitionSizeMB: 100, MinPartitions: 6, MaxPartitions: 20,
				ReadReplicas: "per_az:1", KeyBloomFilter: new(true)}
			declared := partitionedItems(tuned)
			apply(c, conn, planAgainst(c, conn, declared, partitioningSchemas))
			c.Assert(planAgainst(c, conn, declared, partitioningSchemas), qt.HasLen, 0)
			apply(c, conn, planAgainst(c, conn, declared, partitioningSchemas))
			c.Assert(planAgainst(c, conn, declared, partitioningSchemas), qt.HasLen, 0)
			c.Assert(partitioningOf(c, conn), qt.DeepEquals, &ast.YDBTablePartitioningSpec{PartitionSizeMB: 100,
				MinPartitions: 6, MaxPartitions: 20, ReadReplicas: "PER_AZ:1", KeyBloomFilter: new(true)})

			changes := planAgainst(c, conn, partitionedItems(&ast.YDBTablePartitioningSpec{ByLoad: new(true)}), partitioningSchemas)
			c.Assert(changes, qt.DeepEquals, []string{
				"ALTER TABLE `ptah_ydb_partitioning/items` SET (AUTO_PARTITIONING_BY_SIZE = ENABLED, " +
					"AUTO_PARTITIONING_PARTITION_SIZE_MB = 100, AUTO_PARTITIONING_BY_LOAD = ENABLED, " +
					"AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 6, AUTO_PARTITIONING_MAX_PARTITIONS_COUNT = 20)",
			})
			apply(c, conn, changes)
			c.Assert(partitioningOf(c, conn), qt.DeepEquals, &ast.YDBTablePartitioningSpec{PartitionSizeMB: 100,
				ByLoad: new(true), MinPartitions: 6, MaxPartitions: 20, ReadReplicas: "PER_AZ:1", KeyBloomFilter: new(true)})

			away := partitionedItems(&ast.YDBTablePartitioningSpec{ReadReplicas: "PER_AZ:0", KeyBloomFilter: new(false)})
			changes = planAgainst(c, conn, away, partitioningSchemas)
			c.Assert(changes, qt.DeepEquals, []string{
				"ALTER TABLE `ptah_ydb_partitioning/items` SET (READ_REPLICAS_SETTINGS = \"PER_AZ:0\", KEY_BLOOM_FILTER = DISABLED)",
			})
			apply(c, conn, changes)
			c.Assert(planAgainst(c, conn, away, partitioningSchemas), qt.HasLen, 0)
			retuned := &ast.YDBTablePartitioningSpec{PartitionSizeMB: 100, ByLoad: new(true), MinPartitions: 6, MaxPartitions: 20}
			c.Assert(partitioningOf(c, conn), qt.DeepEquals, retuned)
			declared = partitionedItems(retuned)

			// Turning splitting by size on again resets the size and the
			// minimum unless the statement names them.
			bySize := &ast.YDBTablePartitioningSpec{BySize: new(false), ByLoad: new(true), MinPartitions: 6, MaxPartitions: 20}
			apply(c, conn, planAgainst(c, conn, partitionedItems(bySize), partitioningSchemas))
			c.Assert(partitioningOf(c, conn), qt.DeepEquals, bySize)
			apply(c, conn, planAgainst(c, conn, declared, partitioningSchemas))
			c.Assert(planAgainst(c, conn, declared, partitioningSchemas), qt.HasLen, 0)
			c.Assert(partitioningOf(c, conn), qt.DeepEquals, retuned)

			c.Assert(planAgainst(c, conn, partitionedItems(nil), partitioningSchemas), qt.HasLen, 0)
		})
	}
}

// TestYDBTablePartitioning_StartingLayout creates tables with each starting
// layout and plans nothing after: YDB keeps no record of the layout, and the
// minimum partition count it set reads back as the one the declaration gives a
// new table. The split points are written in the key's own types, a text value
// on the second key column included.
func TestYDBTablePartitioning_StartingLayout(t *testing.T) {
	tests := []struct {
		name         string
		partitioning *ast.YDBTablePartitioningSpec
		want         *ast.YDBTablePartitioningSpec
	}{
		{name: "uniform partitions", partitioning: &ast.YDBTablePartitioningSpec{UniformPartitions: 4},
			want: &ast.YDBTablePartitioningSpec{MinPartitions: 4}},
		{name: "split points", partitioning: &ast.YDBTablePartitioningSpec{PartitionAtKeys: [][]string{{"10", "it's"}, {"20"}}},
			want: &ast.YDBTablePartitioningSpec{MinPartitions: 3}},
		{name: "a layout beside a declared minimum",
			partitioning: &ast.YDBTablePartitioningSpec{UniformPartitions: 4, MinPartitions: 2, BySize: new(false)},
			want:         &ast.YDBTablePartitioningSpec{BySize: new(false), MinPartitions: 2}},
	}

	for _, line := range ydbLines {
		for _, test := range tests {
			t.Run(line.name+"/"+test.name, func(t *testing.T) {
				c := qt.New(t)
				conn := openYDB(c, line)
				dropTables(c, conn, partitioningSchemas)
				c.Cleanup(func() { dropTables(c, conn, partitioningSchemas) })

				declared := partitionedItems(test.partitioning)
				apply(c, conn, planAgainst(c, conn, declared, partitioningSchemas))
				c.Assert(planAgainst(c, conn, declared, partitioningSchemas), qt.HasLen, 0)
				apply(c, conn, planAgainst(c, conn, declared, partitioningSchemas))
				c.Assert(planAgainst(c, conn, declared, partitioningSchemas), qt.HasLen, 0)
				c.Assert(partitioningOf(c, conn), qt.DeepEquals, test.want)
			})
		}
	}
}

// TestYDBTablePartitioning_ChangesOnlyARebuildMakes refuses a starting layout
// on a table created without it, naming the flag that asks for a rebuild;
// asked for one, the plan recreates the table with the declared settings and
// the held value of every other one, keeps its rows, and leaves nothing to
// plan.
func TestYDBTablePartitioning_ChangesOnlyARebuildMakes(t *testing.T) {
	tests := []struct {
		name    string
		before  *ast.YDBTablePartitioningSpec
		after   *ast.YDBTablePartitioningSpec
		want    *ast.YDBTablePartitioningSpec
		wantErr string
	}{
		{
			name:  "a starting layout",
			after: &ast.YDBTablePartitioningSpec{UniformPartitions: 4},
			want:  &ast.YDBTablePartitioningSpec{MinPartitions: 4},
			wantErr: `.*table "ptah_ydb_partitioning.items": it declares UNIFORM_PARTITIONS = 4, which YDB takes only ` +
				`when it creates a table .*; YDB makes it by rebuilding the table, which Ptah plans when asked with --allow-table-rebuild`,
		},
		{
			name:   "a starting layout over held settings",
			before: &ast.YDBTablePartitioningSpec{BySize: new(false), MaxPartitions: 9, KeyBloomFilter: new(true)},
			after:  &ast.YDBTablePartitioningSpec{PartitionAtKeys: [][]string{{"10"}}},
			want: &ast.YDBTablePartitioningSpec{BySize: new(false), MinPartitions: 2, MaxPartitions: 9,
				KeyBloomFilter: new(true)},
			wantErr: `.*table "ptah_ydb_partitioning.items": it declares PARTITION_AT_KEYS with one split point, .*; YDB ` +
				`makes it by rebuilding the table, which Ptah plans when asked with --allow-table-rebuild`,
		},
	}

	for _, line := range ydbLines {
		for _, test := range tests {
			t.Run(line.name+"/"+test.name, func(t *testing.T) {
				c := qt.New(t)
				conn := openYDB(c, line)
				dropDirectory(c, conn, partitioningSchema, "items", "__ptah_rebuild_items", "__ptah_replaced_items")
				c.Cleanup(func() {
					dropDirectory(c, conn, partitioningSchema, "items", "__ptah_rebuild_items", "__ptah_replaced_items")
				})
				apply(c, conn, planAgainst(c, conn, partitionedItems(test.before), partitioningSchemas))
				apply(c, conn, []string{"UPSERT INTO `" + partitioningSchema + "/items` (id, code, label) " +
					"VALUES (1ul, 'a'u, 'one'u), (2ul, 'b'u, 'two'u)"})
				declared := partitionedItems(test.after)

				refused, err := planPartitioning(c, conn, declared, false)
				c.Assert(err, qt.ErrorMatches, test.wantErr)
				c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
				c.Assert(refused, qt.IsNil)

				rebuild, err := planPartitioning(c, conn, declared, true)
				c.Assert(err, qt.IsNil)
				apply(c, conn, rebuild)
				c.Assert(planAgainst(c, conn, declared, partitioningSchemas), qt.HasLen, 0)
				c.Assert(partitioningOf(c, conn), qt.DeepEquals, test.want)
				var count int64
				c.Assert(conn.QueryRowContext(c.Context(),
					"SELECT COUNT(*) FROM `"+partitioningSchema+"/items` WHERE label IS NOT NULL").Scan(&count), qt.IsNil)
				c.Assert(count, qt.Equals, int64(2))
			})
		}
	}
}

// TestYDBTablePartitioning_RebuildCarriesTheSettings rebuilds a table for a
// column type change and keeps its settings: the new table is created with
// them, and nothing is left to plan.
func TestYDBTablePartitioning_RebuildCarriesTheSettings(t *testing.T) {
	for _, line := range ydbLines {
		t.Run(line.name, func(t *testing.T) {
			c := qt.New(t)
			conn := openYDB(c, line)
			dropDirectory(c, conn, partitioningSchema, "items", "__ptah_rebuild_items", "__ptah_replaced_items")
			c.Cleanup(func() {
				dropDirectory(c, conn, partitioningSchema, "items", "__ptah_rebuild_items", "__ptah_replaced_items")
			})
			settings := &ast.YDBTablePartitioningSpec{ByLoad: new(true), MinPartitions: 3, ReadReplicas: "ANY_AZ:1"}
			before := partitionedItems(settings)
			before.Fields[2].Type = "INTEGER"
			apply(c, conn, planAgainst(c, conn, before, partitioningSchemas))
			apply(c, conn, []string{"UPSERT INTO `" + partitioningSchema + "/items` (id, code, label) VALUES (1ul, 'a'u, 7)"})

			declared := partitionedItems(settings)
			declared.Fields[2].Type = "BIGINT"
			rebuild, err := planPartitioning(c, conn, declared, true)
			c.Assert(err, qt.IsNil)
			apply(c, conn, rebuild)

			c.Assert(planAgainst(c, conn, declared, partitioningSchemas), qt.HasLen, 0)
			c.Assert(partitioningOf(c, conn), qt.DeepEquals, settings)
		})
	}
}

// TestYDBTablePartitioning_RebuildKeepsWhatTheDeclarationLeavesOut rebuilds a
// table whose declaration names none of its settings, for a column type
// change. The new table and its index take what the old ones hold, so the
// rebuild changes no setting nobody declared, and nothing is left to plan.
func TestYDBTablePartitioning_RebuildKeepsWhatTheDeclarationLeavesOut(t *testing.T) {
	for _, line := range ydbLines {
		t.Run(line.name, func(t *testing.T) {
			c := qt.New(t)
			conn := openYDB(c, line)
			dropDirectory(c, conn, partitioningSchema, "items", "__ptah_rebuild_items", "__ptah_replaced_items")
			c.Cleanup(func() {
				dropDirectory(c, conn, partitioningSchema, "items", "__ptah_rebuild_items", "__ptah_replaced_items")
			})
			tuned := &ast.YDBTablePartitioningSpec{BySize: new(false), ByLoad: new(true), MinPartitions: 3,
				MaxPartitions: 9, ReadReplicas: "ANY_AZ:1", KeyBloomFilter: new(true)}
			indexTuned := &ast.IndexPartitioningSpec{ByLoad: new(true), MinPartitions: 2}
			before := partitionedItems(tuned)
			before.Fields[2].Type = "INTEGER"
			before.Indexes = []schemamodel.Index{{StructName: "Item", Name: "items_label", Fields: []string{"label"},
				Partitioning: indexTuned}}
			apply(c, conn, planAgainst(c, conn, before, partitioningSchemas))
			apply(c, conn, []string{"UPSERT INTO `" + partitioningSchema + "/items` (id, code, label) VALUES (1ul, 'a'u, 7)"})

			declared := partitionedItems(nil)
			declared.Fields[2].Type = "BIGINT"
			declared.Indexes = []schemamodel.Index{{StructName: "Item", Name: "items_label", Fields: []string{"label"}}}
			rebuild, err := planPartitioning(c, conn, declared, true)
			c.Assert(err, qt.IsNil)
			apply(c, conn, rebuild)

			c.Assert(planAgainst(c, conn, declared, partitioningSchemas), qt.HasLen, 0)
			live := readScoped(c, conn, partitioningSchemas)
			c.Assert(tableNamed(c, live, partitioningSchema, "items").YDBPartitioning, qt.DeepEquals, tuned)
			c.Assert(indexNamed(c, live, "items_label").Partitioning, qt.DeepEquals, indexTuned)
		})
	}
}

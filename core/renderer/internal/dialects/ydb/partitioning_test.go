package ydb_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ast"
	"ptah.run/core/platform/capability"
	"ptah.run/core/ptaherr"
	"ptah.run/core/renderer/internal/dialects/ydb"
)

// TestRender_IndexPartitioning_HappyPath pins how an index's partitioning is
// written: never inside the clause that creates the index, which takes no
// settings on any line, but by an ALTER INDEX after it that names every
// setting. Each rendering was applied to local-ydb 26.2.1.14 and 25.1.4.7 one
// statement per query and read back from the index's implementation table.
func TestRender_IndexPartitioning_HappyPath(t *testing.T) {
	tuned := &ast.IndexPartitioningSpec{ByLoad: new(true), MinPartitions: 3, MaxPartitions: 9, ReadReplicas: "PER_AZ:1"}

	tests := []struct {
		name string
		caps capability.Capabilities
		node ast.Node
		want string
	}{
		{
			name: "a new table's index on 25.1",
			caps: capability.YDB251(),
			node: withIndex(&ast.IndexNode{Name: "i", Columns: []string{"a"}, Partitioning: tuned}, ast.NewColumn("a", "TEXT")),
			want: "CREATE TABLE `t` (\n" +
				"    `id` Int64 NOT NULL,\n" +
				"    `a` Utf8,\n" +
				"    PRIMARY KEY (`id`),\n" +
				"    INDEX `i` GLOBAL SYNC ON (`a`)\n" +
				");\n" +
				"ALTER TABLE `t` ALTER INDEX `i` SET (AUTO_PARTITIONING_BY_SIZE = ENABLED, " +
				"AUTO_PARTITIONING_PARTITION_SIZE_MB = 2048, AUTO_PARTITIONING_BY_LOAD = ENABLED, " +
				"AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 3, AUTO_PARTITIONING_MAX_PARTITIONS_COUNT = 9, " +
				"READ_REPLICAS_SETTINGS = \"PER_AZ:1\");\n",
		},
		{
			name: "a declaration of the defaults writes nothing more",
			caps: capability.YDB262(),
			node: withIndex(&ast.IndexNode{Name: "i", Columns: []string{"a"},
				Partitioning: &ast.IndexPartitioningSpec{BySize: new(true), MinPartitions: 1}}, ast.NewColumn("a", "TEXT")),
			want: "CREATE TABLE `t` (\n" +
				"    `id` Int64 NOT NULL,\n" +
				"    `a` Utf8,\n" +
				"    PRIMARY KEY (`id`),\n" +
				"    INDEX `i` GLOBAL SYNC ON (`a`)\n" +
				");\n",
		},
		{
			name: "an index added to a table that exists",
			caps: capability.YDB262(),
			node: &ast.IndexNode{Name: "i", Table: "dir.t", Columns: []string{"a"}, Type: "async",
				Partitioning: &ast.IndexPartitioningSpec{BySize: new(false)}},
			want: "ALTER TABLE `dir/t` ADD INDEX `i` GLOBAL ASYNC ON (`a`);\n" +
				"ALTER TABLE `dir/t` ALTER INDEX `i` SET (AUTO_PARTITIONING_BY_SIZE = DISABLED, " +
				"AUTO_PARTITIONING_BY_LOAD = DISABLED, AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 1);\n",
		},
		{
			name: "an index tuned back to the defaults",
			caps: capability.YDB251(),
			node: alter(&ast.SetIndexPartitioningOperation{IndexName: "i",
				Partitioning: &ast.IndexPartitioningSpec{MaxPartitions: 9}, Previous: tuned}),
			want: "ALTER TABLE `t` ALTER INDEX `i` SET (AUTO_PARTITIONING_BY_SIZE = ENABLED, " +
				"AUTO_PARTITIONING_PARTITION_SIZE_MB = 2048, AUTO_PARTITIONING_BY_LOAD = DISABLED, " +
				"AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 1, AUTO_PARTITIONING_MAX_PARTITIONS_COUNT = 9, " +
				"READ_REPLICAS_SETTINGS = \"PER_AZ:0\");\n",
		},
		{
			name: "a change to settings the index already holds writes nothing",
			caps: capability.YDB262(),
			node: alter(&ast.SetIndexPartitioningOperation{IndexName: "i", Partitioning: tuned, Previous: tuned}),
			want: "",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			got, err := ydb.NewWithCapabilities(test.caps).Render(test.node)
			c.Assert(err, qt.IsNil)
			c.Assert(got, qt.Equals, test.want)
		})
	}
}

// TestRender_IndexPartitioning_FailurePath refuses partitioning a target
// cannot carry, by its key, and a change YDB refuses, by the server's reason.
func TestRender_IndexPartitioning_FailurePath(t *testing.T) {
	withoutKey := capability.YDB262().With(capability.IndexPartitioning, false)
	tests := []struct {
		name    string
		caps    capability.Capabilities
		node    ast.Node
		wantErr string
	}{
		{
			name: "an index's partitioning without the key",
			caps: withoutKey,
			node: withIndex(&ast.IndexNode{Name: "i", Columns: []string{"a"},
				Partitioning: &ast.IndexPartitioningSpec{MinPartitions: 3}}, ast.NewColumn("a", "TEXT")),
			wantErr: `index "i" declares its partitioning, which requires target capability index_partitioning, unavailable on this ydb target`,
		},
		{
			name:    "a change of partitioning without the key",
			caps:    withoutKey,
			node:    alter(&ast.SetIndexPartitioningOperation{IndexName: "i", Partitioning: &ast.IndexPartitioningSpec{MinPartitions: 3}}),
			wantErr: `changing the partitioning of index "i" of table "t", which requires target capability index_partitioning, .*`,
		},
		{
			name: "a size on an index that does not split by size",
			caps: capability.YDB262(),
			node: &ast.IndexNode{Name: "i", Table: "t", Columns: []string{"a"},
				Partitioning: &ast.IndexPartitioningSpec{BySize: new(false), PartitionSizeMB: 100}},
			wantErr: `index "i": auto_partitioning_partition_size_mb is set while auto_partitioning_by_size is disabled, .*`,
		},
		{
			name: "a maximum removed in place",
			caps: capability.YDB262(),
			node: alter(&ast.SetIndexPartitioningOperation{IndexName: "i",
				Previous: &ast.IndexPartitioningSpec{MaxPartitions: 9}}),
			wantErr: `index "i" of table "t": its maximum of 9 partitions cannot be removed in place .*`,
		},
		{
			name:    "a change naming no index",
			caps:    capability.YDB262(),
			node:    alter(&ast.SetIndexPartitioningOperation{Partitioning: &ast.IndexPartitioningSpec{MinPartitions: 3}}),
			wantErr: `table "t": ALTER INDEX \.\.\. SET names no index`,
		},
		{
			name: "PostgreSQL storage parameters",
			caps: capability.YDB262(),
			node: withIndex(&ast.IndexNode{Name: "i", Columns: []string{"a"}, StorageParams: map[string]string{"fillfactor": "70"}},
				ast.NewColumn("a", "TEXT")),
			wantErr: `index "i": a YDB global index takes no storage parameters; .*`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			got, err := ydb.NewWithCapabilities(test.caps).Render(test.node)
			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
			c.Assert(got, qt.Equals, "")
		})
	}
}

package ydb_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ast"
	"ptah.run/core/platform/capability"
	"ptah.run/core/ptaherr"
	"ptah.run/engine/builtin/internal/dialects/ydb"
)

// partitionedTable is a table keyed on id, of keyType, and a second key
// column, which declares partitioning.
func partitionedTable(keyType string, partitioning *ast.YDBTablePartitioningSpec) *ast.CreateTableNode {
	return &ast.CreateTableNode{Name: "t", YDBPartitioning: partitioning, Columns: []*ast.ColumnNode{
		ast.NewColumn("id", keyType).SetPrimary(),
		ast.NewColumn("k", "TEXT").SetPrimary(),
		ast.NewColumn("v", "TEXT"),
	}}
}

// TestRender_TablePartitioning_HappyPath pins how a row table's settings are
// written: every setting the declaration names inside CREATE TABLE's WITH
// clause, the starting layout in the key's own types, and a change in place as
// one ALTER TABLE ... SET naming every setting of each group it touches: the
// declared ones, and the held value of every other one. A setting the
// declaration leaves out is never changed. Each rendering was applied to
// local-ydb 26.2.1.14 and 25.1.4.7 and read back.
func TestRender_TablePartitioning_HappyPath(t *testing.T) {
	tuned := &ast.YDBTablePartitioningSpec{
		PartitionSizeMB: 100, ByLoad: new(true), MinPartitions: 6, MaxPartitions: 20, ReadReplicas: "per_az:1",
		KeyBloomFilter: new(true),
	}
	tests := []struct {
		name string
		caps capability.Capabilities
		node ast.Node
		want string
	}{
		{
			name: "every setting at CREATE on 25.1",
			caps: capability.YDB251(),
			node: partitionedTable("BIGINT", tuned),
			want: "CREATE TABLE `t` (\n" +
				"    `id` Int64 NOT NULL,\n" +
				"    `k` Utf8 NOT NULL,\n" +
				"    `v` Utf8,\n" +
				"    PRIMARY KEY (`id`, `k`)\n" +
				") WITH (AUTO_PARTITIONING_PARTITION_SIZE_MB = 100, AUTO_PARTITIONING_BY_LOAD = ENABLED, " +
				"AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 6, AUTO_PARTITIONING_MAX_PARTITIONS_COUNT = 20, " +
				"READ_REPLICAS_SETTINGS = \"PER_AZ:1\", KEY_BLOOM_FILTER = ENABLED);\n",
		},
		{
			name: "uniform partitions on an unsigned key",
			caps: capability.YDB262(),
			node: partitionedTable("BIGINT UNSIGNED", &ast.YDBTablePartitioningSpec{UniformPartitions: 4, BySize: new(false)}),
			want: "CREATE TABLE `t` (\n" +
				"    `id` Uint64 NOT NULL,\n" +
				"    `k` Utf8 NOT NULL,\n" +
				"    `v` Utf8,\n" +
				"    PRIMARY KEY (`id`, `k`)\n" +
				") WITH (AUTO_PARTITIONING_BY_SIZE = DISABLED, UNIFORM_PARTITIONS = 4);\n",
		},
		{
			name: "split points on the leading key columns",
			caps: capability.YDB262(),
			node: partitionedTable("BIGINT", &ast.YDBTablePartitioningSpec{PartitionAtKeys: [][]string{{"10", "a"}, {"20"}}}),
			want: "CREATE TABLE `t` (\n" +
				"    `id` Int64 NOT NULL,\n" +
				"    `k` Utf8 NOT NULL,\n" +
				"    `v` Utf8,\n" +
				"    PRIMARY KEY (`id`, `k`)\n" +
				") WITH (PARTITION_AT_KEYS = ((10, 'a'u), (20)));\n",
		},
		{
			name: "settings beside a TTL share the WITH clause",
			caps: capability.YDB262(),
			node: func() *ast.CreateTableNode {
				table := partitionedTable("BIGINT", &ast.YDBTablePartitioningSpec{MinPartitions: 3})
				table.Columns = append(table.Columns, ast.NewColumn("ts", "TIMESTAMP"))
				table.RowDeletionPolicy = &ast.RowDeletionPolicySpec{Column: "ts", Interval: "P1D"}
				return table
			}(),
			want: "CREATE TABLE `t` (\n" +
				"    `id` Int64 NOT NULL,\n" +
				"    `k` Utf8 NOT NULL,\n" +
				"    `v` Utf8,\n" +
				"    `ts` Timestamp64,\n" +
				"    PRIMARY KEY (`id`, `k`)\n" +
				") WITH (TTL = Interval(\"P1D\") ON `ts`, AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 3);\n",
		},
		{
			name: "a changed minimum names the whole splitting group",
			caps: capability.YDB262(),
			node: alter(&ast.SetYDBTablePartitioningOperation{
				Partitioning: &ast.YDBTablePartitioningSpec{ByLoad: new(true), MinPartitions: 4},
				Previous:     &ast.YDBTablePartitioningSpec{ByLoad: new(true), MinPartitions: 6},
			}),
			want: "ALTER TABLE `t` SET (AUTO_PARTITIONING_BY_SIZE = ENABLED, AUTO_PARTITIONING_PARTITION_SIZE_MB = 2048, " +
				"AUTO_PARTITIONING_BY_LOAD = ENABLED, AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 4);\n",
		},
		{
			name: "replicas and a filter declared away",
			caps: capability.YDB251(),
			node: alter(&ast.SetYDBTablePartitioningOperation{
				Partitioning: &ast.YDBTablePartitioningSpec{ReadReplicas: "PER_AZ:0", KeyBloomFilter: new(false)},
				Previous:     tuned,
			}),
			want: "ALTER TABLE `t` SET (READ_REPLICAS_SETTINGS = \"PER_AZ:0\", KEY_BLOOM_FILTER = DISABLED);\n",
		},
		{
			name: "settings left out keep what the table holds",
			caps: capability.YDB251(),
			node: alter(&ast.SetYDBTablePartitioningOperation{
				Partitioning: &ast.YDBTablePartitioningSpec{MinPartitions: 6},
				Previous:     tuned,
			}),
			want: "",
		},
		{
			// The table held no size, and the size named is the one YDB sets
			// when splitting by size turns on.
			name: "splitting by size turned on over a table that did not split by size",
			caps: capability.YDB262(),
			node: alter(&ast.SetYDBTablePartitioningOperation{
				Partitioning: &ast.YDBTablePartitioningSpec{BySize: new(true)},
				Previous:     &ast.YDBTablePartitioningSpec{BySize: new(false), MinPartitions: 3},
			}),
			want: "ALTER TABLE `t` SET (AUTO_PARTITIONING_BY_SIZE = ENABLED, AUTO_PARTITIONING_PARTITION_SIZE_MB = 2048, " +
				"AUTO_PARTITIONING_BY_LOAD = DISABLED, AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 3);\n",
		},
		{
			// The table holds the minimum its starting layout set, so the layout
			// is not a change.
			name: "a starting layout the table holds the minimum of",
			caps: capability.YDB262(),
			node: alter(&ast.SetYDBTablePartitioningOperation{
				Partitioning: &ast.YDBTablePartitioningSpec{UniformPartitions: 4, KeyBloomFilter: new(true)},
				Previous:     &ast.YDBTablePartitioningSpec{MinPartitions: 4},
			}),
			want: "ALTER TABLE `t` SET (KEY_BLOOM_FILTER = ENABLED);\n",
		},
		{
			name: "settings the table already holds write nothing",
			caps: capability.YDB262(),
			node: alter(&ast.SetYDBTablePartitioningOperation{Partitioning: tuned, Previous: tuned}),
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

// TestRender_TablePartitioning_FailurePath refuses a setting the target has no
// key for, by the key, and a declaration or a change YDB refuses, by the
// server's reason.
func TestRender_TablePartitioning_FailurePath(t *testing.T) {
	tests := []struct {
		name    string
		caps    capability.Capabilities
		node    ast.Node
		wantErr string
	}{
		{
			name:    "partitioning without the key",
			caps:    capability.YDB262().With(capability.PartitioningOptions, false),
			node:    partitionedTable("BIGINT", &ast.YDBTablePartitioningSpec{MinPartitions: 3}),
			wantErr: `table "t" declares its partitioning, which requires target capability partitioning_options, unavailable on this ydb target`,
		},
		{
			name:    "read replicas without the key",
			caps:    capability.YDB262().With(capability.ReadReplicas, false),
			node:    partitionedTable("BIGINT", &ast.YDBTablePartitioningSpec{ReadReplicas: "ANY_AZ:1"}),
			wantErr: `table "t" declares its read replicas, which requires target capability read_replicas, .*`,
		},
		{
			name:    "a key bloom filter without the key",
			caps:    capability.YDB262().With(capability.KeyBloomFilter, false),
			node:    partitionedTable("BIGINT", &ast.YDBTablePartitioningSpec{KeyBloomFilter: new(false)}),
			wantErr: `table "t" declares its key bloom filter, which requires target capability key_bloom_filter, .*`,
		},
		{
			name: "replicas going away without the key",
			caps: capability.YDB262().With(capability.ReadReplicas, false),
			node: alter(&ast.SetYDBTablePartitioningOperation{
				Previous: &ast.YDBTablePartitioningSpec{ReadReplicas: "PER_AZ:1"},
			}),
			wantErr: `table "t" declares its read replicas, which requires target capability read_replicas, .*`,
		},
		{
			name:    "uniform partitions on a signed key",
			caps:    capability.YDB262(),
			node:    partitionedTable("BIGINT", &ast.YDBTablePartitioningSpec{UniformPartitions: 4}),
			wantErr: `table "t": uniform_partitions splits the range of the first key column, .* this one is Int64 .*`,
		},
		{
			name:    "a split point longer than the key",
			caps:    capability.YDB262(),
			node:    partitionedTable("BIGINT", &ast.YDBTablePartitioningSpec{PartitionAtKeys: [][]string{{"1", "a", "x"}}}),
			wantErr: `table "t": split point 1 of partition_at_keys: it holds 3 values, and the key has 2 columns .*`,
		},
		{
			name:    "both starting layouts",
			caps:    capability.YDB262(),
			node:    partitionedTable("BIGINT UNSIGNED", &ast.YDBTablePartitioningSpec{UniformPartitions: 2, PartitionAtKeys: [][]string{{"1"}}}),
			wantErr: `table "t": uniform_partitions and partition_at_keys are both declared, which YDB refuses .*`,
		},
		{
			name:    "a size on a table that does not split by size",
			caps:    capability.YDB262(),
			node:    partitionedTable("BIGINT", &ast.YDBTablePartitioningSpec{BySize: new(false), PartitionSizeMB: 100}),
			wantErr: `table "t": auto_partitioning_partition_size_mb is set while auto_partitioning_by_size is disabled, .*`,
		},
		{
			name: "a starting layout on a table created without it",
			caps: capability.YDB262(),
			node: alter(&ast.SetYDBTablePartitioningOperation{
				Partitioning: &ast.YDBTablePartitioningSpec{UniformPartitions: 4},
			}),
			wantErr: `table "t": it declares UNIFORM_PARTITIONS = 4, which YDB takes only when it creates a table .*`,
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

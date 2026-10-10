package ydb_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/ast"
	"ptah.run/core/platform/capability"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/ydb/ydbast"
	"ptah.run/dialect/ydb/ydbdiff"
	"ptah.run/dialect/ydb/ydbschema"
	"ptah.run/engine/builtin/internal/dialects/ydb"
)

// indexSettings is an index's settings as the YDB owner's facet.
func indexSettings(settings ydbschema.IndexPartitioning) schemaext.Facets {
	return must.Must(schemaext.NewFacets(&ydbschema.DesiredIndexPartitioning{IndexPartitioning: settings}))
}

// withIndexSettings adds settings to facets as the YDB owner's facet.
func withIndexSettings(facets schemaext.Facets, settings ydbschema.IndexPartitioning) schemaext.Facets {
	return must.Must(facets.With(&ydbschema.DesiredIndexPartitioning{IndexPartitioning: settings}))
}

// changeIndexSettings is the YDB owner's in-place change of index name's
// settings, from what the index holds, nil for YDB's defaults, to what the
// declaration states.
func changeIndexSettings(name string, after ydbschema.IndexPartitioning, before *ydbschema.IndexPartitioning) *ast.AlterTableNode {
	change := ydbdiff.IndexPartitioning{After: &ydbschema.DesiredIndexPartitioning{IndexPartitioning: after}}
	if before != nil {
		change.Before = &ydbschema.ObservedIndexPartitioning{IndexPartitioning: *before}
	}
	return alter(&ast.ExtensionAlterOperation{Payload: &ydbast.AlterIndexPartitioning{Index: name, Change: change}})
}

// TestRender_IndexPartitioning_HappyPath pins how an index's partitioning is
// written: never inside the clause that creates the index, which takes no
// settings on any line, but by an ALTER INDEX after it that names each setting
// the declaration names. A change in place names what the declaration names
// and the held value of every other setting of its group, and a setting the
// declaration leaves out is never changed; a change that would change nothing
// is refused, see [TestRender_IndexPartitioning_RefusesAChangeThatWritesNothing].
// Each rendering was applied to
// local-ydb 26.2.1.14 and 25.1.4.7 one statement per query and read back from
// the index's implementation table.
func TestRender_IndexPartitioning_HappyPath(t *testing.T) {
	tuned := ydbschema.IndexPartitioning{ByLoad: new(true), MinPartitions: 3, MaxPartitions: 9, ReadReplicas: "PER_AZ:1"}

	tests := []struct {
		name string
		caps capability.Capabilities
		node ast.Node
		want string
	}{
		{
			name: "a new table's index on 25.1",
			caps: capability.YDB251(),
			node: withIndex(&ast.IndexNode{Name: "i", Columns: []string{"a"}, Facets: indexSettings(tuned)}, ast.NewColumn("a", "TEXT")),
			want: "CREATE TABLE `t` (\n" +
				"    `id` Int64 NOT NULL,\n" +
				"    `a` Utf8,\n" +
				"    PRIMARY KEY (`id`),\n" +
				"    INDEX `i` GLOBAL SYNC ON (`a`)\n" +
				");\n" +
				"ALTER TABLE `t` ALTER INDEX `i` SET (AUTO_PARTITIONING_BY_LOAD = ENABLED, " +
				"AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 3, AUTO_PARTITIONING_MAX_PARTITIONS_COUNT = 9, " +
				"READ_REPLICAS_SETTINGS = \"PER_AZ:1\");\n",
		},
		{
			name: "a declaration of the defaults is written as declared",
			caps: capability.YDB262(),
			node: withIndex(&ast.IndexNode{Name: "i", Columns: []string{"a"},
				Facets: indexSettings(ydbschema.IndexPartitioning{BySize: new(true), MinPartitions: 1})}, ast.NewColumn("a", "TEXT")),
			want: "CREATE TABLE `t` (\n" +
				"    `id` Int64 NOT NULL,\n" +
				"    `a` Utf8,\n" +
				"    PRIMARY KEY (`id`),\n" +
				"    INDEX `i` GLOBAL SYNC ON (`a`)\n" +
				");\n" +
				"ALTER TABLE `t` ALTER INDEX `i` SET (AUTO_PARTITIONING_BY_SIZE = ENABLED, " +
				"AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 1);\n",
		},
		{
			name: "an index added to a table that exists",
			caps: capability.YDB262(),
			node: &ast.IndexNode{Name: "i", Table: "dir.t", Columns: []string{"a"}, Type: "async",
				Facets: indexSettings(ydbschema.IndexPartitioning{BySize: new(false)})},
			want: "ALTER TABLE `dir/t` ADD INDEX `i` GLOBAL ASYNC ON (`a`);\n" +
				"ALTER TABLE `dir/t` ALTER INDEX `i` SET (AUTO_PARTITIONING_BY_SIZE = DISABLED);\n",
		},
		{
			name: "an index declared back to the defaults",
			caps: capability.YDB251(),
			node: changeIndexSettings("i", ydbschema.IndexPartitioning{BySize: new(true), ByLoad: new(false), MinPartitions: 1,
				ReadReplicas: "PER_AZ:0"}, &tuned),
			want: "ALTER TABLE `t` ALTER INDEX `i` SET (AUTO_PARTITIONING_BY_SIZE = ENABLED, " +
				"AUTO_PARTITIONING_PARTITION_SIZE_MB = 2048, AUTO_PARTITIONING_BY_LOAD = DISABLED, " +
				"AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 1, AUTO_PARTITIONING_MAX_PARTITIONS_COUNT = 9, " +
				"READ_REPLICAS_SETTINGS = \"PER_AZ:0\");\n",
		},
		{
			// The minimum, the maximum and the replicas are named with the
			// values the index holds: setting AUTO_PARTITIONING_BY_LOAD alone
			// would reset the minimum to 1.
			name: "one setting names the held rest of its group",
			caps: capability.YDB262(),
			node: changeIndexSettings("i", ydbschema.IndexPartitioning{ByLoad: new(false)}, &tuned),
			want: "ALTER TABLE `t` ALTER INDEX `i` SET (AUTO_PARTITIONING_BY_SIZE = ENABLED, " +
				"AUTO_PARTITIONING_PARTITION_SIZE_MB = 2048, AUTO_PARTITIONING_BY_LOAD = DISABLED, " +
				"AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 3, AUTO_PARTITIONING_MAX_PARTITIONS_COUNT = 9, " +
				"READ_REPLICAS_SETTINGS = \"PER_AZ:1\");\n",
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
				Facets: indexSettings(ydbschema.IndexPartitioning{MinPartitions: 3})}, ast.NewColumn("a", "TEXT")),
			wantErr: `index "i" declares its partitioning, which requires target capability index_partitioning, unavailable on this ydb target`,
		},
		{
			name:    "a change of partitioning without the key",
			caps:    withoutKey,
			node:    changeIndexSettings("i", ydbschema.IndexPartitioning{MinPartitions: 3}, nil),
			wantErr: `changing the partitioning of index "i" of table "t", which requires target capability index_partitioning, .*`,
		},
		{
			name: "a size on an index that does not split by size",
			caps: capability.YDB262(),
			node: &ast.IndexNode{Name: "i", Table: "t", Columns: []string{"a"},
				Facets: indexSettings(ydbschema.IndexPartitioning{BySize: new(false), PartitionSizeMB: 100})},
			wantErr: `index "i": auto_partitioning_partition_size_mb is set while auto_partitioning_by_size is disabled, .*`,
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

// TestRender_IndexPartitioning_RefusesAChangeThatWritesNothing refuses an
// in-place change after which the index holds what it held, and one naming no
// index: the planner emits neither.
func TestRender_IndexPartitioning_RefusesAChangeThatWritesNothing(t *testing.T) {
	tuned := ydbschema.IndexPartitioning{ByLoad: new(true), MinPartitions: 3, MaxPartitions: 9, ReadReplicas: "PER_AZ:1"}
	tests := []struct {
		name    string
		node    ast.Node
		wantErr string
	}{
		{name: "settings left out keep what the index holds", node: changeIndexSettings("i", ydbschema.IndexPartitioning{MaxPartitions: 9}, &tuned),
			wantErr: `.*the settings of index "i" of table "t": .*operands contain no change`},
		{name: "settings the index already holds", node: changeIndexSettings("i", tuned, &tuned),
			wantErr: `.*the settings of index "i" of table "t": .*operands contain no change`},
		{name: "a change naming no index", node: changeIndexSettings("", ydbschema.IndexPartitioning{MinPartitions: 3}, nil),
			wantErr: `.*operation names no index`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			got, err := ydb.NewWithCapabilities(capability.YDB262()).Render(test.node)
			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(err, qt.ErrorIs, schemaext.ErrInvalidValue)
			c.Assert(got, qt.Equals, "")
		})
	}
}

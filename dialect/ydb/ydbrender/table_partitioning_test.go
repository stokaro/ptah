package ydbrender_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/ast"
	"ptah.run/core/platform/capability"
	"ptah.run/core/renderer"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/ydb/ydbast"
	"ptah.run/dialect/ydb/ydbdiff"
	"ptah.run/dialect/ydb/ydbrender"
	"ptah.run/dialect/ydb/ydbschema"
)

func partitioningOperation(after ydbschema.TablePartitioning, before *ydbschema.TablePartitioning) *ydbast.AlterTablePartitioning {
	op := &ydbast.AlterTablePartitioning{Change: ydbdiff.TablePartitioning{After: &ydbschema.DesiredTablePartitioning{TablePartitioning: after}}}
	if before != nil {
		op.Change.Before = &ydbschema.ObservedTablePartitioning{TablePartitioning: *before}
	}
	return op
}

// TestTablePartitioningHandler_RendersOneStatement pins the one ALTER TABLE
// ... SET a change lowers to, under the table's path.
func TestTablePartitioningHandler_RendersOneStatement(t *testing.T) {
	c := qt.New(t)
	registry := must.Must(renderer.NewExtensions(ydbrender.TablePartitioningHandler()))

	statements, err := registry.Render(renderer.ExtensionContext{Target: "ydb", Capabilities: capability.YDB262(), Parent: &ast.AlterTableNode{Name: "app.items"}},
		ast.AlterExtension, partitioningOperation(ydbschema.TablePartitioning{ReadReplicas: "PER_AZ:0", KeyBloomFilter: new(true)},
			&ydbschema.TablePartitioning{ReadReplicas: "ANY_AZ:1"}))

	c.Assert(err, qt.IsNil)
	c.Assert(statements, qt.DeepEquals, []string{"ALTER TABLE `app/items` SET (READ_REPLICAS_SETTINGS = \"PER_AZ:0\", KEY_BLOOM_FILTER = ENABLED);"})
}

func TestTablePartitioningHandler_FailurePath(t *testing.T) {
	parent := &ast.AlterTableNode{Name: "items"}
	tests := []struct {
		name    string
		ctx     renderer.ExtensionContext
		op      *ydbast.AlterTablePartitioning
		wantErr string
	}{
		{name: "another target", ctx: renderer.ExtensionContext{Target: "spanner", Capabilities: capability.SpannerPostgres(), Parent: parent},
			op: partitioningOperation(ydbschema.TablePartitioning{MinPartitions: 2}, nil), wantErr: `YDB table partitioning cannot be rendered for "spanner"`},
		{name: "a setting the target has no key for", ctx: renderer.ExtensionContext{Target: "ydb", Capabilities: capability.YDB262().With(capability.KeyBloomFilter, false), Parent: parent},
			op:      partitioningOperation(ydbschema.TablePartitioning{KeyBloomFilter: new(true)}, nil),
			wantErr: `changing the key bloom filter of table "items", which requires target capability key_bloom_filter, unavailable on this ydb target`},
		{name: "read replicas going away without the key", ctx: renderer.ExtensionContext{Target: "ydb", Capabilities: capability.YDB262().With(capability.ReadReplicas, false), Parent: parent},
			op:      partitioningOperation(ydbschema.TablePartitioning{MinPartitions: 2}, &ydbschema.TablePartitioning{ReadReplicas: "PER_AZ:1"}),
			wantErr: `changing the read replicas of table "items", which requires target capability read_replicas, .*`},
		{name: "a size YDB refuses", ctx: renderer.ExtensionContext{Target: "ydb", Capabilities: capability.YDB262(), Parent: parent},
			op:      partitioningOperation(ydbschema.TablePartitioning{BySize: new(false), PartitionSizeMB: 64}, nil),
			wantErr: `table "items": auto_partitioning_partition_size_mb is set while auto_partitioning_by_size is disabled, .*`},
		{name: "a starting layout", ctx: renderer.ExtensionContext{Target: "ydb", Capabilities: capability.YDB262(), Parent: parent},
			op:      partitioningOperation(ydbschema.TablePartitioning{UniformPartitions: 4}, nil),
			wantErr: `table "items": it declares UNIFORM_PARTITIONS = 4, which YDB takes only when it creates a table .*`},
		{name: "no change", ctx: renderer.ExtensionContext{Target: "ydb", Capabilities: capability.YDB262(), Parent: parent},
			op:      partitioningOperation(ydbschema.TablePartitioning{MinPartitions: 2}, &ydbschema.TablePartitioning{MinPartitions: 2}),
			wantErr: `the settings of table "items": .*operands contain no change`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			statements, err := must.Must(renderer.NewExtensions(ydbrender.TablePartitioningHandler())).Render(test.ctx, ast.AlterExtension, test.op)

			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(statements, qt.IsNil)
		})
	}
}

// TestCreateTablePartitioning_HappyPath writes each setting the declaration
// names, then its starting layout in the key's own types, and nothing for a
// table without the facet.
func TestCreateTablePartitioning_HappyPath(t *testing.T) {
	c := qt.New(t)
	facets := must.Must(schemaext.NewFacets(&ydbschema.DesiredTablePartitioning{TablePartitioning: ydbschema.TablePartitioning{
		ByLoad: new(true), MinPartitions: 2, PartitionAtKeys: [][]string{{"10"}},
	}}))

	settings, err := ydbrender.CreateTablePartitioning(capability.YDB262(), "items", facets, []string{"Uint64"})
	none, noneErr := ydbrender.CreateTablePartitioning(capability.Capabilities{}, "items", schemaext.Facets{}, []string{"Uint64"})

	c.Assert(err, qt.IsNil)
	c.Assert(settings, qt.DeepEquals, []string{"AUTO_PARTITIONING_BY_LOAD = ENABLED", "AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 2", "PARTITION_AT_KEYS = ((10))"})
	c.Assert(noneErr, qt.IsNil)
	c.Assert(none, qt.IsNil)
}

func TestCreateTablePartitioning_FailurePath(t *testing.T) {
	declared := func(settings ydbschema.TablePartitioning) schemaext.Facets {
		return must.Must(schemaext.NewFacets(&ydbschema.DesiredTablePartitioning{TablePartitioning: settings}))
	}
	tests := []struct {
		name    string
		caps    capability.Capabilities
		facets  schemaext.Facets
		wantErr string
	}{
		{name: "a setting without its key", caps: capability.YDB262().With(capability.PartitioningOptions, false),
			facets: declared(ydbschema.TablePartitioning{MinPartitions: 2}), wantErr: `table "t" declares its partitioning, which requires target capability partitioning_options, .*`},
		{name: "two starting layouts", caps: capability.YDB262(), facets: declared(ydbschema.TablePartitioning{UniformPartitions: 2, PartitionAtKeys: [][]string{{"1"}}}),
			wantErr: `table "t": uniform_partitions and partition_at_keys are both declared, .*`},
		{name: "a layout the key cannot take", caps: capability.YDB262(), facets: declared(ydbschema.TablePartitioning{UniformPartitions: 2}),
			wantErr: `table "t": uniform_partitions splits the range of the first key column, .*`},
		{name: "an invalid declaration", caps: capability.YDB262(), facets: declared(ydbschema.TablePartitioning{ReadReplicas: "x"}),
			wantErr: `the settings of table "t": .*read replicas "x" .*`},
		{name: "an observation", caps: capability.YDB262(),
			facets:  must.Must(schemaext.NewFacets(&ydbschema.ObservedTablePartitioning{TablePartitioning: ydbschema.TablePartitioning{MinPartitions: 2}})),
			wantErr: `.*table-partitioning.*`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			settings, err := ydbrender.CreateTablePartitioning(test.caps, "t", test.facets, []string{"Utf8"})

			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(settings, qt.IsNil)
		})
	}
}

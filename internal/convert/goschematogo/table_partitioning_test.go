package goschematogo_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ast"
	"ptah.run/core/goschema"
	"ptah.run/core/schemamodel"
	"ptah.run/internal/convert/goschematogo"
)

// TestRender_TablePartitioning writes a YDB row table's settings as the
// attributes the annotation parser reads them back from, the split points in
// YQL's own list, so a schema read from a database and written as Go keeps its
// settings.
func TestRender_TablePartitioning(t *testing.T) {
	tests := []struct {
		name         string
		partitioning *ast.YDBTablePartitioningSpec
		wantText     string
	}{
		{
			name: "every setting but the split points",
			partitioning: &ast.YDBTablePartitioningSpec{
				BySize: new(false), ByLoad: new(true), MinPartitions: 3, MaxPartitions: 9, ReadReplicas: "PER_AZ:1",
				KeyBloomFilter: new(true), UniformPartitions: 4,
			},
			wantText: `key_bloom_filter="ENABLED" uniform_partitions="4"`,
		},
		{
			name:         "split points",
			partitioning: &ast.YDBTablePartitioningSpec{PartitionAtKeys: [][]string{{"10", "it's"}, {"20"}}},
			wantText:     `partition_at_keys="(10, 'it\\'s'), 20"`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			db := &schemamodel.Database{
				Tables: []schemamodel.Table{{StructName: "Item", Name: "items", PrimaryKey: []string{"id", "kind"},
					YDBPartitioning: test.partitioning}},
				Fields: []schemamodel.Field{
					{StructName: "Item", FieldName: "ID", Name: "id", Type: "BIGINT UNSIGNED", Primary: true},
					{StructName: "Item", FieldName: "Kind", Name: "kind", Type: "TEXT"},
				},
			}

			files, err := goschematogo.Render(db, goschematogo.Options{SingleFile: true})
			c.Assert(err, qt.IsNil)
			c.Assert(files, qt.HasLen, 1)
			reparsed, err := goschema.ParseSource("schema.go", string(files[0].Data))

			c.Assert(err, qt.IsNil)
			c.Assert(string(files[0].Data), qt.Contains, test.wantText)
			c.Assert(reparsed.Tables, qt.HasLen, 1)
			c.Assert(reparsed.Tables[0].YDBPartitioning, qt.DeepEquals, test.partitioning)
		})
	}
}

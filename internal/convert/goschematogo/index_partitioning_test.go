package goschematogo_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ast"
	"ptah.run/core/goschema"
	"ptah.run/core/schemamodel"
	"ptah.run/internal/convert/goschematogo"
)

// TestRender_IndexPartitioning writes a YDB index's partitioning as the
// attributes the annotation parser reads it back from, so a schema read from
// a database and written as Go keeps its settings.
func TestRender_IndexPartitioning(t *testing.T) {
	c := qt.New(t)
	partitioning := &ast.IndexPartitioningSpec{
		BySize: new(false), ByLoad: new(true), MinPartitions: 3, MaxPartitions: 9, ReadReplicas: "PER_AZ:1",
	}
	db := &schemamodel.Database{
		Tables: []schemamodel.Table{{StructName: "Item", Name: "items", PrimaryKey: []string{"id"}}},
		Fields: []schemamodel.Field{
			{StructName: "Item", FieldName: "ID", Name: "id", Type: "BIGINT", Primary: true},
			{StructName: "Item", FieldName: "Kind", Name: "kind", Type: "TEXT"},
		},
		Indexes: []schemamodel.Index{{
			StructName: "Item", Name: "idx_items_kind", TableName: "items", Fields: []string{"kind"}, Partitioning: partitioning,
		}},
	}

	files, err := goschematogo.Render(c.Context(), db, goschematogo.Options{SingleFile: true})
	c.Assert(err, qt.IsNil)
	c.Assert(files, qt.HasLen, 1)
	reparsed, err := goschema.ParseSource("schema.go", string(files[0].Data))

	c.Assert(err, qt.IsNil)
	c.Assert(string(files[0].Data), qt.Contains, `auto_partitioning_by_size="DISABLED"`)
	c.Assert(reparsed.Indexes, qt.HasLen, 1)
	c.Assert(reparsed.Indexes[0].Partitioning, qt.DeepEquals, partitioning)
}

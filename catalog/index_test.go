package catalog_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/core/ast"
)

func indexWithMutableFields() catalog.Index {
	return catalog.Index{
		Name: "by_label", TableName: "items", Schema: "app", Columns: []string{"label"},
		Parts: []catalog.IndexPart{{Name: "label", Desc: true}}, IncludeColumns: []string{"id"},
		StorageParams: map[string]string{"fillfactor": "70"}, RequiresExtensions: []string{"bloom"},
		NullsDistinct: new(false), Partitioning: &ast.IndexPartitioningSpec{BySize: new(true), ByLoad: new(false)},
		Vector: &ast.VectorIndexSpec{Dimension: 32},
	}
}

func TestIndexCloneOwnsEveryMutableField(t *testing.T) {
	c := qt.New(t)
	original := indexWithMutableFields()
	clone := original.Clone()
	c.Assert(clone, qt.DeepEquals, original)
	clone.Columns[0], clone.Parts[0].Name, clone.IncludeColumns[0] = "changed", "changed", "changed"
	clone.StorageParams["fillfactor"], clone.RequiresExtensions[0] = "20", "changed"
	*clone.NullsDistinct, *clone.Partitioning.BySize, *clone.Partitioning.ByLoad = true, false, true
	clone.Vector.Dimension = 64
	c.Assert(original, qt.DeepEquals, indexWithMutableFields())
}

func TestIndexClonePreservesAbsentFields(t *testing.T) {
	c := qt.New(t)
	c.Assert((catalog.Index{}).Clone(), qt.DeepEquals, catalog.Index{})
}

package schemamodel_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ast"
	"ptah.run/core/schemamodel"
)

func mutableIndexDeclaration() schemamodel.Index {
	return schemamodel.Index{
		Name: "by_label", TableName: "items", Fields: []string{"label"},
		Parts: []schemamodel.IndexPart{{Name: "label", Prefix: "20"}}, IncludeColumns: []string{"id"},
		StorageParams: map[string]string{"fillfactor": "70"}, RequiresExtensions: []string{"bloom"},
		NullsDistinct: new(false), Partitioning: &ast.IndexPartitioningSpec{BySize: new(true), ByLoad: new(false)},
		Vector: &ast.VectorIndexSpec{Dimension: 32},
	}
}

func TestIndexCloneIsolatesTheDeclaredDefinition(t *testing.T) {
	c := qt.New(t)
	original := mutableIndexDeclaration()
	clone := original.Clone()
	c.Assert(clone, qt.DeepEquals, original)
	clone.Fields[0], clone.Parts[0].Name, clone.IncludeColumns[0] = "changed", "changed", "changed"
	clone.StorageParams["fillfactor"], clone.RequiresExtensions[0] = "20", "changed"
	*clone.NullsDistinct, *clone.Partitioning.BySize, *clone.Partitioning.ByLoad = true, false, true
	clone.Vector.Dimension = 64
	c.Assert(original, qt.DeepEquals, mutableIndexDeclaration())
}

func TestIndexCloneKeepsAbsentDeclarationFields(t *testing.T) {
	c := qt.New(t)
	c.Assert((schemamodel.Index{}).Clone(), qt.DeepEquals, schemamodel.Index{})
}

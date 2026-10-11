package catalog_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/core/objectidentity"
)

func indexWithMutableFields() catalog.Index {
	return catalog.Index{
		Name: "by_label", TableName: "items", Schema: "app", Columns: []string{"label"},
		Parts: []catalog.IndexPart{{Name: "label", Desc: true}}, IncludeColumns: []string{"id"},
		StorageParams: map[string]string{"fillfactor": "70"}, Requires: []objectidentity.ID{{Kind: objectidentity.KindExtension, Name: objectidentity.Part{Source: "bloom", Normalized: "bloom"}}},
		NullsDistinct: new(false),
	}
}

func TestIndexCloneOwnsEveryMutableField(t *testing.T) {
	c := qt.New(t)
	original := indexWithMutableFields()
	clone := original.Clone()
	c.Assert(clone, qt.DeepEquals, original)
	clone.Columns[0], clone.Parts[0].Name, clone.IncludeColumns[0] = "changed", "changed", "changed"
	clone.StorageParams["fillfactor"], clone.Requires[0].Name.Source = "20", "changed"
	*clone.NullsDistinct = true
	c.Assert(original, qt.DeepEquals, indexWithMutableFields())
}

func TestIndexClonePreservesAbsentFields(t *testing.T) {
	c := qt.New(t)
	c.Assert((catalog.Index{}).Clone(), qt.DeepEquals, catalog.Index{})
}

package schemadiff_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/catalog"
	"go.5x5.cz/ptah/core/schemamodel"
	"go.5x5.cz/ptah/migration/schemadiff"
)

// TestCompare_AnIndexAdditionCarriesItsDeclaration is the property that lets a
// planner render a CREATE INDEX without the document it came from.
//
// The owner is the half worth asserting alongside the definition. An index
// declaration need not name its table -- this one is resolved from the struct
// it was written on -- and a materialized view is an owner too, so the answer
// is derived rather than copied off the index (stokaro/ptah#2315).
func TestCompare_AnIndexAdditionCarriesItsDeclaration(t *testing.T) {
	c := qt.New(t)
	desired := &schemamodel.Database{
		Tables: []schemamodel.Table{{StructName: "Widget", Name: "widgets"}},
		Fields: []schemamodel.Field{{StructName: "Widget", Name: "code", Type: "TEXT"}},
		Indexes: []schemamodel.Index{
			{StructName: "Widget", Name: "idx_widgets_code", Fields: []string{"code"}},
		},
	}
	schemamodel.Finalize(desired)

	diff := schemadiff.Compare(desired, &catalog.Database{})

	c.Assert(diff.IndexAdditions(), qt.HasLen, 1)
	c.Assert(diff.IndexesAdded[0].Index.Name, qt.Equals, "idx_widgets_code")
	c.Assert(diff.IndexesAdded[0].Index.Fields, qt.DeepEquals, []string{"code"},
		qt.Commentf("the definition travels with the addition, not looked up from the document"))
	c.Assert(diff.IndexesAdded[0].TableName, qt.Equals, "widgets",
		qt.Commentf("the owner is resolved where the declaration is"))
}

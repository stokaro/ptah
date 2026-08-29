package schemadiff_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/catalog"
	"go.5x5.cz/ptah/core/schemamodel"
	"go.5x5.cz/ptah/migration/schemadiff"
)

// TestCompare_CarriesTheDeclaredIndexesWithTheirOwners asserts the comparison
// itself fills the carry, which nothing else in the suite can.
//
// `migration/planner` completes an incomplete diff from the declaration it is
// still handed, so every planner test passes whether the comparison filled this
// or not. Zeroing the comparison's fill reddened NOTHING before this test
// existed: two answers, and neither measured (stokaro/ptah#2315).
//
// The owner is the half worth asserting. An index declaration need not name its
// table -- this one is resolved from the struct it was written on -- and a
// materialized view is an owner too.
func TestCompare_CarriesTheDeclaredIndexesWithTheirOwners(t *testing.T) {
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

	c.Assert(diff.DeclaredIndexes, qt.HasLen, 1)
	c.Assert(diff.DeclaredIndexes[0].Index.Name, qt.Equals, "idx_widgets_code")
	c.Assert(diff.DeclaredIndexes[0].TableName, qt.Equals, "widgets",
		qt.Commentf("the owner is resolved where the declaration is, not looked up again by a planner"))
}

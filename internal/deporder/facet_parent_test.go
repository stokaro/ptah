package deporder_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/dialect/postgres/pgtable"
	"ptah.run/internal/deporder"
)

// TestGeneratedTableDependencies_OrdersAPartitionAfterItsParent derives the edge from
// the declaration, for a schema assembled in memory that nothing finalized. A parent
// spelled with the default schema pairs with the table that leaves it out.
func TestGeneratedTableDependencies_OrdersAPartitionAfterItsParent(t *testing.T) {
	for _, test := range []struct {
		name                 string
		parentSchema, parent string
		want                 []string
	}{
		{name: "the same spelling", parent: "events", want: []string{"events"}},
		{name: "the default schema on the reference", parentSchema: "public", parent: "events", want: []string{"events"}},
		{name: "a parent no table declares", parentSchema: "other", parent: "elsewhere", want: []string{"other.elsewhere"}},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			schema := &schemamodel.Database{Tables: []schemamodel.Table{
				{StructName: "E1", Name: "e1", Facets: must.Must(schemaext.NewFacets(&pgtable.DesiredPartitionMember{
					PartitionMember: pgtable.PartitionMember{ParentSchema: test.parentSchema, Parent: test.parent, Bound: "DEFAULT"},
				}))},
				{StructName: "Events", Name: "events"},
			}}

			got := deporder.GeneratedTableDependencies(schema)

			c.Assert(got["e1"], qt.DeepEquals, test.want)
			c.Assert(got["events"], qt.HasLen, 0)
		})
	}
}

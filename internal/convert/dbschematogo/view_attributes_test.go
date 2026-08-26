package dbschematogo_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/catalog"
	"go.5x5.cz/ptah/internal/convert/dbschematogo"
)

// TestConvertViews_CarriesTheAttributes pins the layer between the reader and
// the renderer.
//
// Both ends were covered and this was not: a conversion that dropped the WITH
// clause left the reader reading it correctly, the renderer able to write it,
// and the replay unbound anyway. The mutation that removes the field from the
// conversion survived every other test in the tree (stokaro/ptah#2125).
func TestConvertViews_CarriesTheAttributes(t *testing.T) {
	c := qt.New(t)

	converted := dbschematogo.ConvertCatalogToSchema(&catalog.Database{
		Views: []catalog.View{
			{
				Schema:     "dbo",
				Name:       "bound_orders",
				Body:       "SELECT id FROM dbo.orders",
				Attributes: []string{"SCHEMABINDING"},
			},
			{
				Schema: "dbo",
				Name:   "plain_orders",
				Body:   "SELECT id FROM dbo.orders",
			},
		},
	})

	c.Assert(converted.Views, qt.HasLen, 2)
	c.Assert(converted.Views[0].Attributes, qt.DeepEquals, []string{"SCHEMABINDING"})
	// The view beside it keeps none, so a conversion that attached the same
	// clause to everything fails here rather than passing on the row above.
	c.Assert(converted.Views[1].Attributes, qt.IsNil)
}

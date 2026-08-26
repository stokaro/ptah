package fromschema_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/schemamodel"
	"go.5x5.cz/ptah/internal/convert/fromschema"
)

// TestFromView_CarriesTheAttributes pins the last hop before the renderer.
//
// The renderer writes what the node carries, so a conversion that drops the
// clause here produces a correct renderer, a correct reader, and an unbound
// view (stokaro/ptah#2125).
func TestFromView_CarriesTheAttributes(t *testing.T) {
	c := qt.New(t)

	node := fromschema.FromView(schemamodel.View{
		Name:       "dbo.bound_orders",
		Body:       "SELECT id FROM dbo.orders",
		Attributes: []string{"SCHEMABINDING", "VIEW_METADATA"},
	})

	c.Assert(node.Attributes, qt.DeepEquals, []string{"SCHEMABINDING", "VIEW_METADATA"})
}

// TestFromView_WithoutAttributesCarriesNone is the control: a view that has no
// clause must not gain one.
func TestFromView_WithoutAttributesCarriesNone(t *testing.T) {
	c := qt.New(t)

	node := fromschema.FromView(schemamodel.View{Name: "dbo.v", Body: "SELECT id FROM dbo.t"})

	c.Assert(node.Attributes, qt.IsNil)
}

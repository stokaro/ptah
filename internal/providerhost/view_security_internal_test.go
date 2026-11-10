package providerhost

// White-box testing required: the provider wire conversion is package-local,
// and its SQLView message has no MySQL SQL SECURITY field.

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ast"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemamodel"
)

func TestProviderViewSecurity_IsRefusedBeforeWireConversion(t *testing.T) {
	c := qt.New(t)
	target := commonObjectTarget(everyCommonKind...)
	view := ast.NewCreateView("recent").SetBody("SELECT 1")
	view.Security = "INVOKER"
	change, _, err := target.commonObjectChange(view)
	c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
	c.Assert(change, qt.IsNil)

	declared, err := target.declaredCommonObjects(&schemamodel.Database{Views: []schemamodel.View{{
		Name: "recent", Body: "SELECT 1", Security: "INVOKER",
	}}})
	c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
	c.Assert(declared, qt.IsNil)
}

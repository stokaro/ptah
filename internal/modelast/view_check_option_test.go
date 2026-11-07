package modelast_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/schemamodel"
	"ptah.run/internal/modelast"
)

// TestFromView_CarriesTheCheckOptionLevel pins the last hop before the
// renderer: a LOCAL view reaches it as a node asking for WITH LOCAL CHECK
// OPTION, and LOCAL without a check option asks for nothing.
func TestFromView_CarriesTheCheckOptionLevel(t *testing.T) {
	tests := []struct {
		name          string
		view          schemamodel.View
		wantWithCheck bool
		wantLocal     bool
	}{
		{name: "LOCAL", view: schemamodel.View{Name: "v", Body: "SELECT 1", WithCheck: true, LocalCheck: true}, wantWithCheck: true, wantLocal: true},
		{name: "CASCADED", view: schemamodel.View{Name: "v", Body: "SELECT 1", WithCheck: true}, wantWithCheck: true},
		{name: "LOCAL without a check option", view: schemamodel.View{Name: "v", Body: "SELECT 1", LocalCheck: true}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			node := modelast.FromView(test.view)

			c.Assert(node.WithCheck, qt.Equals, test.wantWithCheck)
			c.Assert(node.LocalCheck, qt.Equals, test.wantLocal)
		})
	}
}

package compare_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/core/schemamodel"
	"ptah.run/migration/schemadiff/internal/compare"
)

// The two sides qualify a trigger's function differently, so the comparison
// uses the more specific spelling both of them carry (stokaro/ptah#3113).
//
// A declaration names the function the way its author wrote it -- `app.touch`
// in a SQL file, or the qualified name an HCL reference resolves to -- while
// the catalog reports pg_proc.proname, which carries no schema. Compared as
// text, a trigger nobody had touched was planned for replacement on every run.
func TestTriggerDefinitions_AQualifiedFunctionMatchesItsBareName(t *testing.T) {
	tests := []struct {
		name     string
		declared string
		observed string
	}{
		{name: "declared qualified against a bare catalog", declared: "app.touch", observed: "touch"},
		{name: "declared bare against a qualified catalog", declared: "touch", observed: "app.touch"},
		{name: "both bare", declared: "touch", observed: "touch"},
		{name: "both qualified and the same", declared: "app.touch", observed: "app.touch"},
		{name: "case differs", declared: "APP.Touch", observed: "app.touch"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			diff := compare.TriggerDefinitions(
				schemamodel.Trigger{Name: "t", Table: "app.t", ExecuteFunction: test.declared},
				catalog.Trigger{Name: "t", Table: "app.t", ExecuteFunction: test.observed},
			)

			c.Assert(diff.Changes["function"], qt.Equals, "")
		})
	}
}

// TestTriggerDefinitions_TwoSchemasAreTwoFunctions is the control.
//
// Folding the qualifier off both sides would bind a trigger to whichever
// `touch` the search path happened to find, so two qualified names stay two
// functions and a real rebinding is still reported.
func TestTriggerDefinitions_TwoSchemasAreTwoFunctions(t *testing.T) {
	tests := []struct {
		name     string
		declared string
		observed string
	}{
		{name: "a different schema", declared: "app.touch", observed: "other.touch"},
		{name: "a different function", declared: "app.touch", observed: "app.stamp"},
		{name: "a different bare function", declared: "touch", observed: "stamp"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			diff := compare.TriggerDefinitions(
				schemamodel.Trigger{Name: "t", Table: "app.t", ExecuteFunction: test.declared},
				catalog.Trigger{Name: "t", Table: "app.t", ExecuteFunction: test.observed},
			)

			c.Assert(diff.Changes["function"], qt.Not(qt.Equals), "")
		})
	}
}

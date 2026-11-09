package extensionplan_test

import (
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/featureplan"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemavalidation"
	"ptah.run/feature/pgextension"
	"ptah.run/feature/pgextension/extensionplan"
)

func TestPlanFeatures_RefusesDropWithForeignTable(t *testing.T) {
	c := qt.New(t)
	change := &pgextension.Change{Before: &pgextension.ObservedExtension{
		Schema: "public", Version: "1.1", DependentForeignTables: []string{"public.external_events"},
	}}
	result, err := (extensionplan.Service{}).PlanFeatures(t.Context(), featureplan.Request{
		Target: "postgres", Changes: []schemaext.ChangeRecord{{Subject: pgextension.Ref("postgres_fdw"), Value: change}},
	})

	c.Assert(err, qt.IsNil)
	c.Assert(result.Complete, qt.IsTrue)
	c.Assert(result.Contributions, qt.HasLen, 0)
	c.Assert(result.Diagnostics, qt.HasLen, 1)
	c.Assert(result.Diagnostics[0].Problem.Code, qt.Equals, schemavalidation.UnsupportedFeature)
	c.Assert(strings.Contains(result.Diagnostics[0].Problem.Message, "public.external_events"), qt.IsTrue)
}

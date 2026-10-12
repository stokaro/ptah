package generator_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/catalog"
	"ptah.run/core/platform/capability"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/dialect/postgres/pgroutine"
	"ptah.run/engine/builtin"
	"ptah.run/migration/generator"
	"ptah.run/migration/schemadiff"
)

func TestPlanBidirectionalSchemaDiff_PostgresFunctionProperties(t *testing.T) {
	for _, test := range []struct {
		name       string
		properties pgroutine.Properties
		clause     string
	}{
		{"leakproof", pgroutine.Properties{Leakproof: true}, "LEAKPROOF"},
		{"strict", pgroutine.Properties{Strict: true}, "STRICT"},
		{"parallel", pgroutine.Properties{Parallel: "SAFE"}, "PARALLEL SAFE"},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			runtime := must.Must(builtin.New())
			desired := &schemamodel.Database{Functions: []schemamodel.Function{{Name: "public.f", Parameters: "a integer", Returns: "integer", Language: "sql", Volatility: "VOLATILE", Body: "SELECT a"}}, FeatureCoverage: must.Must(pgroutine.PropertiesCoverage(schemaext.Desired, schemaext.Knowledge{State: schemaext.Complete}, nil))}
			current := &catalog.Database{Functions: []catalog.Function{{Schema: "public", Name: "f", Parameters: "a integer", Returns: "integer", Language: "sql", Volatility: "VOLATILE", Body: "SELECT a", Facets: must.Must(pgroutine.WithObservedProperties(schemaext.Facets{}, test.properties))}}, FeatureCoverage: must.Must(pgroutine.PropertiesCoverage(schemaext.Observed, schemaext.Knowledge{State: schemaext.Complete}, nil))}
			diff := must.Must(schemadiff.CompareWithDialect(t.Context(), desired, current, "postgres", runtime))
			plan, err := generator.PlanBidirectionalSchemaDiff(t.Context(), generator.BidirectionalSchemaPlanOptions{Runtime: runtime, Diff: diff, DesiredSchema: desired, CurrentSchema: current, Dialect: "postgres", Capabilities: capability.ForDialect("postgres")})
			c.Assert(err, qt.IsNil)
			forward, err := builtin.RenderSQLWithCapabilities("postgres", capability.ForDialect("postgres"), plan.Forward.Nodes...)
			c.Assert(err, qt.IsNil)
			reverse, err := builtin.RenderSQLWithCapabilities("postgres", capability.ForDialect("postgres"), plan.Reverse.Nodes...)
			c.Assert(err, qt.IsNil)
			c.Assert(forward, qt.Contains, "CREATE OR REPLACE FUNCTION")
			c.Assert(forward, qt.Not(qt.Contains), test.clause)
			c.Assert(reverse, qt.Contains, "CREATE OR REPLACE FUNCTION")
			c.Assert(reverse, qt.Contains, test.clause)
			c.Assert(plan.Reverse.Diff.FunctionsModified[0].FeatureChanges[0].Value, qt.DeepEquals, &pgroutine.PropertiesChange{After: new(pgroutine.DesiredProperties(test.properties))})
			c.Assert(diff.FunctionsModified[0].FeatureChanges[0].Value, qt.DeepEquals, &pgroutine.PropertiesChange{Before: new(pgroutine.ObservedProperties(test.properties))})
		})
	}
}

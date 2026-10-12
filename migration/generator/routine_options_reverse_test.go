package generator_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/catalog"
	"ptah.run/core/platform"
	"ptah.run/core/platform/capability"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/dialect/mssql/mssqldiff"
	"ptah.run/dialect/mssql/mssqlschema"
	"ptah.run/engine/builtin"
	"ptah.run/migration/generator"
	"ptah.run/migration/schemadiff"
)

// A rollback runs a SQL Server routine as the principal the forward change
// replaced: the reverse entry replaces the routine with its prior
// declaration, and the principal is part of it (stokaro/ptah#4490).
func TestPlanBidirectionalSchemaDiff_ARoutinesPrincipalRollsBack(t *testing.T) {
	c := qt.New(t)
	runtime := must.Must(builtin.New())
	desired := &schemamodel.Database{
		Functions: []schemamodel.Function{{
			Name: "dbo.audit_it", Returns: "int", Language: "sql", Security: "DEFINER", Volatility: "VOLATILE",
			Body: "BEGIN RETURN 1; END", Facets: must.Must(mssqlschema.WithRoutineOptions(schemaext.Facets{}, mssqlschema.RoutineOptions{Principal: "reviewer"})),
		}},
		FeatureCoverage: must.Must(mssqlschema.RoutineOptionsCoverage(schemaext.Desired, schemaext.Knowledge{State: schemaext.Complete}, nil)),
	}
	current := &catalog.Database{
		Functions: []catalog.Function{{
			Schema: "dbo", Name: "audit_it", Returns: "int", Language: "sql", Security: "DEFINER", Volatility: "VOLATILE",
			Body: "BEGIN RETURN 1; END", Facets: must.Must(mssqlschema.WithObservedRoutineOptions(schemaext.Facets{}, mssqlschema.RoutineOptions{Principal: "auditor", SchemaBinding: true, ReturnsNullOnNullInput: true})),
		}},
		FeatureCoverage: must.Must(mssqlschema.RoutineOptionsCoverage(schemaext.Observed, schemaext.Knowledge{State: schemaext.Complete}, nil)),
	}
	diff := must.Must(schemadiff.CompareWithDialect(t.Context(), desired, current, platform.SQLServer, runtime))

	plan, err := generator.PlanBidirectionalSchemaDiff(t.Context(),
		generator.BidirectionalSchemaPlanOptions{Runtime: runtime, Diff: diff,
			DesiredSchema: desired,
			CurrentSchema: current,
			Dialect:       platform.SQLServer,
			Capabilities:  capability.ForDialect(platform.SQLServer),
		})
	c.Assert(err, qt.IsNil)
	forward, err := builtin.RenderSQLWithCapabilities(platform.SQLServer, capability.ForDialect(platform.SQLServer), plan.Forward.Nodes...)
	c.Assert(err, qt.IsNil)
	reverse, err := builtin.RenderSQLWithCapabilities(platform.SQLServer, capability.ForDialect(platform.SQLServer), plan.Reverse.Nodes...)

	c.Assert(err, qt.IsNil)
	c.Assert(forward, qt.Contains, "WITH EXECUTE AS N'reviewer'")
	c.Assert(reverse, qt.Contains, "WITH SCHEMABINDING, RETURNS NULL ON NULL INPUT, EXECUTE AS N'auditor'")
	c.Assert(plan.Reverse.Diff.FunctionsModified, qt.HasLen, 1)
	c.Assert(plan.Reverse.Diff.FunctionsModified[0].FeatureChanges, qt.HasLen, 1)
	c.Assert(plan.Reverse.Diff.FunctionsModified[0].FeatureChanges[0].Value, qt.DeepEquals, &mssqldiff.RoutineOptions{
		Before: &mssqlschema.ObservedRoutineOptions{Principal: "reviewer"},
		After:  &mssqlschema.DesiredRoutineOptions{Principal: "auditor", SchemaBinding: true, ReturnsNullOnNullInput: true},
	})
	c.Assert(diff.FunctionsModified[0].FeatureChanges[0].Value, qt.DeepEquals, &mssqldiff.RoutineOptions{
		Before: &mssqlschema.ObservedRoutineOptions{Principal: "auditor", SchemaBinding: true, ReturnsNullOnNullInput: true},
		After:  &mssqlschema.DesiredRoutineOptions{Principal: "reviewer"},
	}, qt.Commentf("the reversal must not write through to the forward diff"))
}

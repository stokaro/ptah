package featuretests_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/catalog"
	"ptah.run/config"
	"ptah.run/core/objectidentity"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/dialect/clickhouse/chschema"
	"ptah.run/engine/builtin"
	"ptah.run/migration/generator"
	"ptah.run/migration/schemadiff"
)

// A removal needs the stored definition when its down migration recreates
// the view. Its query alone cannot restore a sorting key or a TO target.
func TestPlanBidirectionalSchemaDiff_RefusesUnknownRemovedViewStorage(t *testing.T) {
	for _, test := range []struct {
		name, database, schema, view, reason string
		state                                schemaext.KnowledgeState
	}{
		{
			name: "local sorting key", database: "analytics", view: "daily",
			state: schemaext.Unrepresentable, reason: "ENGINE = MergeTree ORDER BY c",
		},
		{
			name: "qualified target table", database: "analytics", schema: "analytics", view: "daily",
			state: schemaext.Unrepresentable, reason: "TO analytics.events",
		},
		{
			name: "another engine", database: "analytics", schema: "archive", view: "daily",
			state: schemaext.Unrepresentable, reason: "ENGINE = SummingMergeTree ORDER BY c",
		},
		{
			name: "database containing a dot", database: "tenant.analytics", view: "daily",
			state: schemaext.Unrepresentable, reason: "ENGINE = MergeTree PARTITION BY c ORDER BY tuple()",
		},
		{
			name: "view containing a dot", database: "analytics", view: "daily.metrics",
			state: schemaext.Unrepresentable, reason: "ENGINE = MergeTree ORDER BY c",
		},
		{
			name: "uninspected storage", database: "analytics", view: "daily",
			state: schemaext.Uninspected, reason: "storage was not inspected",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			runtime := must.Must(builtin.New())
			semantics := identifier.ForDialect("clickhouse")
			semantics.DefaultSchema = test.database
			subject := objectidentity.NewBuilder(identifier.ForDialect("clickhouse")).SchemaScopedParts(
				objectidentity.KindMatView, test.schema, test.view,
			)
			current := &catalog.Database{
				MatViews: []catalog.MaterializedView{{Schema: test.schema, Name: test.view, Body: "SELECT 1 AS c"}},
				FeatureCoverage: removedViewStorageCoverage(c, schemaext.SubjectCoverage{
					Kind: chschema.ViewStorageKind, Subject: subject,
					Knowledge: schemaext.Knowledge{State: test.state, Reason: test.reason},
				}),
			}
			desired := &schemamodel.Database{}
			diff, err := schemadiff.CompareWithDatabaseInfo(t.Context(), desired, current,
				catalog.ServerInfo{Dialect: "clickhouse", Schema: test.database, IdentifierSemantics: semantics}, nil, runtime,
			)
			c.Assert(err, qt.IsNil)
			c.Assert(diff.MaterializedViewsRemoved, qt.HasLen, 1)

			plan, err := generator.PlanBidirectionalSchemaDiff(t.Context(), generator.BidirectionalSchemaPlanOptions{
				Runtime: runtime, Diff: diff, DesiredSchema: desired, CurrentSchema: current, Dialect: "clickhouse",
			})

			c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
			c.Assert(err.Error(), qt.Contains, "cannot plan the rollback of removing materialized view")
			c.Assert(err.Error(), qt.Contains, string(chschema.ViewStorageKind))
			c.Assert(err.Error(), qt.Contains, test.reason)
			c.Assert(plan, qt.IsNil)
			c.Assert(current.FeatureCoverage.SubjectRecords()[0].Subject, qt.Equals, subject)
		})
	}
}

// A limit on a retained sibling must not refuse another view's removal. The
// schema and the case of a view's name are part of its identity.
func TestPlanBidirectionalSchemaDiff_RestoresKnownViewStorageBesideUnknownStorage(t *testing.T) {
	for _, test := range []struct {
		name, schema, view, declared string
	}{
		{name: "another view", view: "keyed", declared: "keyed"},
		{name: "another database", schema: "archive", view: "daily", declared: "archive.daily"},
		{name: "another case", view: "Daily", declared: "Daily"},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			runtime := must.Must(builtin.New())
			semantics := identifier.ForDialect("clickhouse")
			semantics.DefaultSchema = "analytics"
			subject := objectidentity.NewBuilder(identifier.ForDialect("clickhouse")).SchemaScopedParts(
				objectidentity.KindMatView, test.schema, test.view,
			)
			current := &catalog.Database{
				MatViews: []catalog.MaterializedView{
					{Name: "daily", Body: "SELECT 1 AS c"},
					{Schema: test.schema, Name: test.view, Body: "SELECT 2 AS c"},
				},
				FeatureCoverage: removedViewStorageCoverage(c, schemaext.SubjectCoverage{
					Kind: chschema.ViewStorageKind, Subject: subject,
					Knowledge: schemaext.Knowledge{State: schemaext.Unrepresentable, Reason: "ENGINE = MergeTree ORDER BY c"},
				}),
			}
			desired := &schemamodel.Database{MaterializedViews: []schemamodel.MaterializedView{
				{Name: test.declared, Body: "SELECT 2 AS c"},
			}}
			diff, diagnostics, err := schemadiff.CompareReportingUndecidedAdditions(t.Context(), desired, current,
				&config.CompareOptions{Dialect: "clickhouse", IdentifierSemantics: &semantics}, runtime,
			)
			c.Assert(err, qt.IsNil)
			c.Assert(diagnostics.Features, qt.HasLen, 1)
			c.Assert(diagnostics.Features[0].Kind, qt.Equals, chschema.ViewStorageKind)
			c.Assert(diagnostics.Features[0].Subject.Name.Source, qt.Equals, test.view)
			c.Assert(diff.MaterializedViewsRemoved, qt.HasLen, 1)
			c.Assert(diff.MaterializedViewsModified, qt.HasLen, 0)

			plan, err := generator.PlanBidirectionalSchemaDiff(t.Context(), generator.BidirectionalSchemaPlanOptions{
				Runtime: runtime, Diff: diff, DesiredSchema: desired, CurrentSchema: current, Dialect: "clickhouse",
			})

			c.Assert(err, qt.IsNil)
			c.Assert(plan.Reverse.Diff.MaterializedViewsAdded, qt.HasLen, 1)
			down, err := builtin.RenderSQL("clickhouse", plan.Reverse.Nodes...)
			c.Assert(err, qt.IsNil)
			c.Assert(down, qt.Contains, "CREATE MATERIALIZED VIEW `daily` ENGINE = MergeTree ORDER BY tuple() AS")
			c.Assert(down, qt.Contains, "SELECT 1 AS c")
		})
	}
}

func removedViewStorageCoverage(c *qt.C, subjects ...schemaext.SubjectCoverage) schemaext.Coverage {
	c.Helper()
	coverage, err := schemaext.OwnedCoverage("ptah.run/clickhouse", chschema.ViewStorageCodecs(), chschema.ViewStorageKind,
		schemaext.Observed, schemaext.Knowledge{State: schemaext.Complete}, subjects,
	)
	c.Assert(err, qt.IsNil)
	return coverage
}

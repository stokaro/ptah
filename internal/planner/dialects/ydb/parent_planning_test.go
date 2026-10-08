package ydb_test

import (
	"context"
	"errors"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/featureplan"
	"ptah.run/core/objectidentity"
	"ptah.run/core/platform/capability"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/ydb/ydbschema"
	"ptah.run/engine/builtin"
	"ptah.run/internal/planner/dialects/ydb"
	"ptah.run/migration/schemadiff/difftypes"
)

func TestPlannerRefusesDropWithUnknownCapturedChildren(t *testing.T) {
	parent := objectidentity.NewBuilder(identifier.ForDialect("ydb")).TableParts("app", "items")
	for _, test := range []struct {
		name    string
		subject objectidentity.ID
		state   schemaext.KnowledgeState
	}{
		{"uninspected namespace", parent, schemaext.Uninspected},
		{"unrepresentable namespace", parent, schemaext.Unrepresentable},
		{"uninspected child", ydbschema.ChangefeedRef("app", "items", "updates"), schemaext.Uninspected},
		{"unrepresentable child", ydbschema.ChangefeedRef("app", "items", "updates"), schemaext.Unrepresentable},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			current := observedFeeds(t, "app", "items")
			current.FeatureCoverage = feedCoverage(t, schemaext.Observed, schemaext.SubjectCoverage{Kind: ydbschema.ChangefeedKind,
				Subject: test.subject, Knowledge: schemaext.Knowledge{State: test.state, Reason: "source cannot represent stream ownership"}})
			diff := &difftypes.SchemaDiff{TablesRemoved: difftypes.TableRemovals{{Name: "app.items", Current: current}}}
			runtime, err := builtin.New()
			c.Assert(err, qt.IsNil)
			nodes, err := ydb.NewWithCapabilities(capability.YDB262()).GenerateMigrationAST(t.Context(), runtime, diff)
			c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
			c.Assert(err, qt.ErrorMatches, "(?s).*source cannot represent stream ownership.*")
			c.Assert(nodes, qt.IsNil)
		})
	}
}

func TestPlannerDropsKnownChildrenWithTheirParent(t *testing.T) {
	for _, test := range []struct {
		name  string
		feeds []ydbschema.ChangefeedSpec
	}{
		{"empty namespace", nil},
		{"known stream", []ydbschema.ChangefeedSpec{{Name: "updates", Mode: "UPDATES", Format: "JSON"}}},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			current := observedFeeds(t, "app", "items", test.feeds...)
			diff := &difftypes.SchemaDiff{TablesRemoved: difftypes.TableRemovals{{Name: "app.items", Current: current}}}
			c.Assert(render(c, capability.YDB262(), diff), qt.Equals, "DROP TABLE `app/items`;\n")
		})
	}
}

func TestPlannerCallsSelectedOwnerForARebuildWithoutFeatureChanges(t *testing.T) {
	c := qt.New(t)
	runtime, err := builtin.New()
	c.Assert(err, qt.IsNil)
	failure := errors.New("selected parent planner failed")
	var received featureplan.Request
	selected := selectedPlanning{Runtime: runtime, plan: func(_ context.Context, request featureplan.Request) (featureplan.Result, error) {
		received = request
		return featureplan.Result{}, failure
	}}
	current := observedFeeds(t, "app", "items")
	desired := declaredFeeds(t, appItems(field("n", "BIGINT", true)))
	diff := modified(t, difftypes.TableDiff{TableName: "app.items", Desired: desired, Current: current,
		ColumnsModified: []difftypes.ColumnDiff{{ColumnName: "n", Changes: map[string]string{"type": "Int32 -> Int64"}}}})
	nodes, err := ydb.NewWithCapabilities(capability.YDB262()).WithTableRebuild(true).GenerateMigrationAST(t.Context(), selected, diff)
	c.Assert(err, qt.ErrorIs, failure)
	c.Assert(nodes, qt.IsNil)
	c.Assert(received.Changes, qt.HasLen, 0)
	c.Assert(received.Tables, qt.HasLen, 1)
	c.Assert(received.Tables[0].Action, qt.Equals, featureplan.RebuildTable)
}

func TestPlannerRefusesIncompleteParentPlanning(t *testing.T) {
	c := qt.New(t)
	runtime, err := builtin.New()
	c.Assert(err, qt.IsNil)
	selected := selectedPlanning{Runtime: runtime, plan: func(context.Context, featureplan.Request) (featureplan.Result, error) {
		return featureplan.Result{}, nil
	}}
	diff := &difftypes.SchemaDiff{TablesRemoved: difftypes.TableRemovals{{Name: "items", Current: observedFeeds(t, "", "items")}}}
	nodes, err := ydb.NewWithCapabilities(capability.YDB262()).GenerateMigrationAST(t.Context(), selected, diff)
	c.Assert(err, qt.ErrorIs, schemaext.ErrInvalidValue)
	c.Assert(err, qt.ErrorMatches, "(?s).*planning did not complete.*")
	c.Assert(nodes, qt.IsNil)
}

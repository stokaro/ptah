package builtin_test

import (
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/ast"
	"ptah.run/core/objectidentity"
	"ptah.run/core/platform/capability"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/dialect/clickhouse/chschema"
	"ptah.run/engine/builtin"
)

// A materialized view's facets reach its renderer, which refuses a setting no
// owner of the target interprets on a view instead of rendering the view
// without it. A table's storage settings are such a setting everywhere.
func TestRenderSQL_RefusesAMaterializedViewSettingNoOwnerInterprets_FailurePath(t *testing.T) {
	for _, dialect := range []string{"postgres", "clickhouse"} {
		t.Run(dialect, func(t *testing.T) {
			c := qt.New(t)
			storage := &chschema.ObservedTable{Engine: "MergeTree", OrderBy: "id"}
			node := &ast.CreateMaterializedViewNode{Name: "daily", Body: "SELECT 1", Facets: must.Must(schemaext.NewFacets(storage.Desired()))}
			sql, err := builtin.RenderSQL(dialect, node)
			c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
			c.Assert(sql, qt.Equals, "")
		})
	}
}

// A database read whose refresh clause for one view could not be read still
// renders: the view is created without the schedule, and a note in the output
// says so. Refused, `ptah db read`, checkpoint generation and inspection to
// SQL failed for the whole database over one view (stokaro/ptah#4278).
func TestRender_AViewWithAnUncapturedScheduleRendersWithANote(t *testing.T) {
	c := qt.New(t)
	view := objectidentity.NewBuilder(identifier.ForDialect("clickhouse")).SchemaScopedParts(objectidentity.KindMatView, "analytics", "daily")
	database := &schemamodel.Database{
		MaterializedViews: []schemamodel.MaterializedView{{Name: "analytics.daily", Body: "SELECT 1 AS c"}},
		FeatureCoverage: must.Must(chschema.RefreshCoverage(schemaext.Desired, schemaext.Knowledge{State: schemaext.Complete},
			[]schemaext.SubjectCoverage{{Kind: chschema.RefreshKind, Subject: view, Knowledge: schemaext.Knowledge{State: schemaext.Unrepresentable, Reason: "the refresh clause could not be read"}}})),
	}

	statements, err := builtin.GetOrderedCreateStatementsWithCapabilities(database, "clickhouse", capability.ClickHouse2411())

	c.Assert(err, qt.IsNil)
	rendered := strings.Join(statements, "\n")
	c.Assert(rendered, qt.Contains, "-- materialized view analytics.daily: ptah.run/clickhouse/refresh was not captured (the refresh clause could not be read), and its statement does not carry it")
	c.Assert(rendered, qt.Contains, "CREATE MATERIALIZED VIEW")
	c.Assert(rendered, qt.Not(qt.Contains), "REFRESH")
}

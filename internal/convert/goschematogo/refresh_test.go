package goschematogo_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/goschema"
	"ptah.run/core/objectidentity"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/dialect/clickhouse/chschema"
	"ptah.run/internal/builtintest"
	"ptah.run/internal/convert/goschematogo"
)

// A materialized view's refresh schedule is written back as the `refresh`
// attribute the parser reads, so exporting and reparsing keeps the schedule.
func TestRenderWritesTheRefreshScheduleOfAMaterializedView(t *testing.T) {
	c := qt.New(t)
	schedule := &chschema.DesiredRefresh{Schedule: chschema.Schedule{
		Mode: chschema.RefreshEvery, Interval: "1 DAY", Offset: "2 HOUR", DependsOn: []string{"analytics.source"}, Append: true,
	}}
	db := &schemamodel.Database{MaterializedViews: []schemamodel.MaterializedView{{
		Name: "daily", Body: "SELECT 1",
		Facets: must.Must(must.Must(schemaext.NewFacets(schedule)).WithTargetScope(chschema.RefreshKind, "clickhouse")),
	}}}

	files, err := goschematogo.Render(c.Context(), db, goschematogo.Options{SingleFile: true})

	c.Assert(err, qt.IsNil)
	c.Assert(string(files[0].Data), qt.Contains, `refresh="EVERY 1 DAY OFFSET 2 HOUR DEPENDS ON analytics.source APPEND"`)
	reparsed, err := goschema.ParseSource(builtintest.Annotations(), "schema.go", string(files[0].Data))
	c.Assert(err, qt.IsNil)
	c.Assert(reparsed.MaterializedViews, qt.HasLen, 1)
	c.Assert(reparsed.MaterializedViews[0].Facets, qt.DeepEquals, db.MaterializedViews[0].Facets)
}

// A setting the matview annotation has no attribute for is refused rather than
// dropped from the exported source.
func TestRenderRefusesAMaterializedViewSettingItCannotWrite_FailurePath(t *testing.T) {
	c := qt.New(t)
	settings := &chschema.DesiredTable{Engine: chschema.Setting{State: chschema.Explicit, Value: "MergeTree"}}
	db := &schemamodel.Database{MaterializedViews: []schemamodel.MaterializedView{{
		Name: "daily", Body: "SELECT 1", Facets: must.Must(schemaext.NewFacets(settings)),
	}}}

	files, err := goschematogo.Render(c.Context(), db, goschematogo.Options{SingleFile: true})

	c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
	c.Assert(err, qt.ErrorMatches, `.*: materialized view "daily" carries setting "ptah.run/clickhouse/table", which a Go annotation cannot represent`)
	c.Assert(files, qt.IsNil)
}

// A schedule the read could not capture cannot be written: a Go source states
// every view's schedule, so the view would come out plain and applying the
// export would remove the schedule. The export is refused and names the view
// (stokaro/ptah#4278).
func TestRenderRefusesAScheduleItCouldNotCapture_FailurePath(t *testing.T) {
	for _, state := range []schemaext.KnowledgeState{schemaext.Unrepresentable, schemaext.Uninspected} {
		t.Run(string(state), func(t *testing.T) {
			c := qt.New(t)
			view := objectidentity.NewBuilder(identifier.ForDialect("clickhouse")).SchemaScopedParts(objectidentity.KindMatView, "analytics", "daily")
			db := &schemamodel.Database{
				MaterializedViews: []schemamodel.MaterializedView{{Name: "analytics.daily", Body: "SELECT 1"}},
				FeatureCoverage: must.Must(chschema.RefreshCoverage(schemaext.Desired, schemaext.Knowledge{State: schemaext.Complete},
					[]schemaext.SubjectCoverage{{Kind: chschema.RefreshKind, Subject: view, Knowledge: schemaext.Knowledge{State: state, Reason: "not read"}}})),
			}

			files, err := goschematogo.Render(c.Context(), db, goschematogo.Options{SingleFile: true})

			c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
			c.Assert(err, qt.ErrorMatches, `.*materialized view analytics.daily: its refresh schedule was not captured \(not read\).*`)
			c.Assert(files, qt.IsNil)
		})
	}
}

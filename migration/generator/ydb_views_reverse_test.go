package generator_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/core/platform"
	"ptah.run/core/platform/capability"
	"ptah.run/core/renderer"
	"ptah.run/core/schemamodel"
	"ptah.run/migration/generator"
	"ptah.run/migration/schemadiff/difftypes"
)

// A YDB rollback puts back the views the database had: a view the forward
// change added is dropped, one it removed is created again from the query the
// database held, and one whose query it changed is dropped and created with
// the old query, since YDB has no CREATE OR REPLACE VIEW. The drops come first
// and the creates last, as they do going forward.
func TestPlanBidirectionalSchemaDiff_YDBViewsRollBack(t *testing.T) {
	c := qt.New(t)
	diff := &difftypes.SchemaDiff{
		ViewsAdded:   difftypes.ViewChanges{{Name: "app.fresh", Body: "SELECT 1 AS a"}},
		ViewsRemoved: difftypes.ViewChanges{{Name: "app.gone", Body: "SELECT 2 AS a"}},
		ViewsModified: []difftypes.ViewDiff{{
			ViewName:     "app.changed",
			Changes:      map[string]string{"body": "SELECT 3 AS a -> SELECT 4 AS a"},
			Desired:      schemamodel.View{Name: "app.changed", Body: "SELECT 4 AS a"},
			PreviousBody: "SELECT 3 AS a",
		}},
	}
	desired := &schemamodel.Database{Views: []schemamodel.View{
		{Name: "app.fresh", Body: "SELECT 1 AS a"},
		{Name: "app.changed", Body: "SELECT 4 AS a"},
	}}
	current := &catalog.Database{Views: []catalog.View{
		{Schema: "app", Name: "gone", Body: "SELECT 2 AS a"},
		{Schema: "app", Name: "changed", Body: "SELECT 3 AS a"},
	}}

	plan, err := generator.PlanBidirectionalSchemaDiff(generator.BidirectionalSchemaPlanOptions{
		Diff:          diff,
		DesiredSchema: desired,
		CurrentSchema: current,
		Dialect:       platform.YDB,
		Capabilities:  capability.YDB251(),
	})
	c.Assert(err, qt.IsNil)
	up, err := renderer.RenderSQLWithCapabilities(platform.YDB, capability.YDB251(), plan.Forward.Nodes...)
	c.Assert(err, qt.IsNil)
	down, err := renderer.RenderSQLWithCapabilities(platform.YDB, capability.YDB251(), plan.Reverse.Nodes...)
	c.Assert(err, qt.IsNil)

	c.Assert(up, qt.Equals, "DROP VIEW `app/changed`;\n"+
		"DROP VIEW `app/gone`;\n"+
		"CREATE VIEW `app/fresh` WITH (security_invoker = TRUE) AS\nSELECT 1 AS a\n;\n"+
		"CREATE VIEW `app/changed` WITH (security_invoker = TRUE) AS\nSELECT 4 AS a\n;\n")
	c.Assert(down, qt.Equals, "DROP VIEW `app/changed`;\n"+
		"DROP VIEW `app/fresh`;\n"+
		"CREATE VIEW `app/gone` WITH (security_invoker = TRUE) AS\nSELECT 2 AS a\n;\n"+
		"CREATE VIEW `app/changed` WITH (security_invoker = TRUE) AS\nSELECT 3 AS a\n;\n")
}

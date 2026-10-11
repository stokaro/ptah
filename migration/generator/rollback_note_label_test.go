package generator_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/catalog"
	"ptah.run/core/platform"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/engine/builtin"
	"ptah.run/feature/pgextension"
	"ptah.run/migration/generator"
	"ptah.run/migration/schemadiff"
)

// TestPlanBidirectionalSchemaDiff_RollbackNoteNamesTheOwnersModel pins that a
// rollback note names an owner's object as every other report does: the label
// the owner's off-target policy declares, then the object's path. The raised
// version is the case, because its rollback runs no statement and the note is
// all the down migration says about it. An owner that declares no label keeps
// its kind; TestPlanBidirectionalSchemaDiff_ReplicationsRollBack pins that.
func TestPlanBidirectionalSchemaDiff_RollbackNoteNamesTheOwnersModel(t *testing.T) {
	c := qt.New(t)
	runtime := must.Must(builtin.New())
	desired := &schemamodel.Database{
		FeatureObjects: must.Must(schemaext.NewObjects(must.Must(pgextension.DesiredObject(pgextension.Ref("pg_trgm"),
			pgextension.DesiredExtension{Schema: "public", Version: "1.6"})))),
		FeatureCoverage: must.Must(pgextension.CompleteCoverage(schemaext.Desired)),
	}
	current := &catalog.Database{
		FeatureObjects: must.Must(schemaext.NewObjects(must.Must(pgextension.ObservedObject(pgextension.Ref("pg_trgm"),
			pgextension.ObservedExtension{Schema: "public", Version: "1.5", Relocatable: true})))),
		FeatureCoverage: must.Must(pgextension.CompleteCoverage(schemaext.Observed)),
	}
	diff := must.Must(schemadiff.CompareWithDialect(t.Context(), desired, current, platform.Postgres, runtime))

	plan, err := generator.PlanBidirectionalSchemaDiff(t.Context(), generator.BidirectionalSchemaPlanOptions{
		Runtime: runtime, Diff: diff, DesiredSchema: desired, CurrentSchema: current, Dialect: platform.Postgres})

	c.Assert(err, qt.IsNil)
	c.Assert(must.Must(builtin.RenderSQL(platform.Postgres, plan.Reverse.Nodes...)), qt.Equals,
		`-- Rollback of "PostgreSQL extension pg_trgm": "keep the version the migration installed".`+"\n"+
			`-- Recovery limit: "PostgreSQL updates an extension in one direction only: after the rollback, pg_trgm stays at version 1.6 rather than 1.5."`+"\n")
}

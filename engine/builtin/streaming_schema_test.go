package builtin_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/catalog"
	"ptah.run/core/platform"
	"ptah.run/core/platform/capability"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/dialect/ydb/ydbdiff"
	"ptah.run/dialect/ydb/ydbstreaming"
	"ptah.run/engine/builtin"
	"ptah.run/migration/planner"
	"ptah.run/migration/schemadiff"
	"ptah.run/migration/schemadiff/difftypes"
)

func TestStreamingFeatureSchemaUsesSelectedOwnerOnEveryEntryPoint(t *testing.T) {
	c := qt.New(t)
	database := coordinationFeatureSchema(c, "unused")
	database.FeatureObjects = must.Must(schemaext.NewObjects(ydbstreaming.DesiredObject("app", "copy.with.dot", "Copy", ydbstreaming.Spec{Text: "SELECT 1;", Run: new(false)}, true)))
	assertOwnedSchemaEntryPoints(c, database, capability.YDB262().With(capability.StreamingQueries, true), "CREATE OR REPLACE STREAMING QUERY `app/copy.with.dot` WITH (RUN = FALSE, RESOURCE_POOL = `default`) AS DO BEGIN\nSELECT 1;\nEND DO;")
}

// The shared model no longer enumerates this kind. Both public paths must
// still refuse it on every target without a registered, enabled owner.
func TestStreamingFeatureSchemaRefusesUnsupportedTargets(t *testing.T) {
	for _, target := range platform.DialectSpellings() {
		t.Run(target, func(t *testing.T) {
			c := qt.New(t)
			runtime := must.Must(builtin.New())
			desired := &schemamodel.Database{FeatureObjects: must.Must(schemaext.NewObjects(ydbstreaming.DesiredObject("", "copy", "", ydbstreaming.Spec{Text: "SELECT 1;"}, false)))}
			c.Assert(builtin.ValidateSchema(desired, target), qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
			diff, err := schemadiff.CompareWithDatabaseInfo(t.Context(), desired, &catalog.Database{}, catalog.ServerInfo{Dialect: target}, nil, runtime)
			c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
			c.Assert(diff, qt.IsNil)
		})
	}
}

func TestStreamingChangesRefuseUnregisteredPlanner(t *testing.T) {
	for _, test := range []struct {
		name   string
		change *ydbdiff.StreamingQuery
	}{
		{name: "create", change: &ydbdiff.StreamingQuery{After: &ydbstreaming.Desired{Spec: ydbstreaming.Spec{Text: "SELECT 1;"}}}},
		{name: "drop", change: &ydbdiff.StreamingQuery{Before: &ydbstreaming.Observed{Spec: ydbstreaming.Spec{Text: "SELECT 1;"}}}},
		{name: "alter", change: &ydbdiff.StreamingQuery{Before: &ydbstreaming.Observed{Spec: ydbstreaming.Spec{Text: "SELECT 1;"}}, After: &ydbstreaming.Desired{Spec: ydbstreaming.Spec{Text: "SELECT 2;"}, AllowStateReset: true}}},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			diff := &difftypes.SchemaDiff{FeatureChanges: []schemaext.ChangeRecord{{Subject: ydbstreaming.Ref("", "copy"), Value: test.change}}}
			statements, err := planner.GenerateSchemaDiffSQLStatementsWithOptions(t.Context(), must.Must(builtin.New()), diff, "postgres", planner.Options{})
			c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
			c.Assert(statements, qt.HasLen, 0)
		})
	}
}

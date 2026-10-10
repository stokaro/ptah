package postgres_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/objectidentity"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/clickhouse/chdiff"
	"ptah.run/dialect/clickhouse/chschema"
	"ptah.run/engine/builtin"
	"ptah.run/internal/planner/dialects/postgres"
	"ptah.run/migration/schemadiff/difftypes"
)

// No owner on the PostgreSQL family plans a setting attached to a materialized
// view, so a diff carrying one is refused before the first statement rather
// than planned as if the setting had been applied.
func TestMaterializedViewFeatureChange_FailurePath(t *testing.T) {
	c := qt.New(t)
	subject := objectidentity.NewBuilder(identifier.ForDialect("postgres")).SchemaScopedParts(objectidentity.KindMatView, "", "hourly")
	diff := &difftypes.SchemaDiff{MaterializedViewsModified: []difftypes.MaterializedViewDiff{{
		ViewName: "hourly", Changes: map[string]string{},
		FeatureChanges: []schemaext.ChangeRecord{{Subject: subject, Value: &chdiff.Refresh{
			Before: &chschema.ObservedRefresh{Schedule: chschema.Schedule{Mode: chschema.RefreshEvery, Interval: "1 HOUR"}},
			After:  &chschema.DesiredRefresh{Schedule: chschema.Schedule{Mode: chschema.RefreshEvery, Interval: "2 HOUR"}},
		}}},
	}}}

	nodes, err := postgres.New().GenerateMigrationAST(t.Context(), must.Must(builtin.New()), diff)

	c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
	c.Assert(err, qt.ErrorMatches, `.*planner has no feature handler for .*hourly.*`)
	c.Assert(nodes, qt.IsNil)
}

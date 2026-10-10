package mssql_test

import (
	"context"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/featureplan"
	"ptah.run/core/plangraph"
	"ptah.run/core/schemaext"
	"ptah.run/engine/builtin"
	"ptah.run/internal/planner/dialects/mssql"
	"ptah.run/migration/schemadiff/difftypes"
)

// pathRecorder plans as fixtureRuntime does and keeps the database path each
// planning request carried.
type pathRecorder struct {
	fixtureRuntime
	paths *[]string
}

func (r pathRecorder) PlanFeatures(ctx context.Context, request featureplan.Request) (featureplan.Result, error) {
	*r.paths = append(*r.paths, request.DatabasePath)
	return r.fixtureRuntime.PlanFeatures(ctx, request)
}

// TestPlanner_OwnersReceiveTheDatabasePath hands the owners the diff's
// database path. A SQL Server read leaves it empty, so the test sets one to
// see it arrive.
func TestPlanner_OwnersReceiveTheDatabasePath(t *testing.T) {
	c := qt.New(t)
	var paths []string
	runtime := pathRecorder{fixtureRuntime: fixtureRuntime{Runtime: must.Must(builtin.New()), phase: featureplan.PhaseDefault}, paths: &paths}
	diff := &difftypes.SchemaDiff{FeatureChanges: []schemaext.ChangeRecord{fixtureRecord("guarded", fixtureOwner, plangraph.Create)},
		CurrentDatabasePath: "/probe"}

	_, err := mssql.New().GenerateMigrationAST(context.Background(), runtime, diff)

	c.Assert(err, qt.IsNil)
	c.Assert(paths, qt.DeepEquals, []string{"/probe"})
}

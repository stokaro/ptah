package postgres_test

import (
	"context"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/featureplan"
	"ptah.run/core/platform"
	"ptah.run/core/platform/capability"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/cockroachdb/crdbschema"
	"ptah.run/engine/builtin"
	"ptah.run/internal/planner/dialects/postgres"
	"ptah.run/migration/schemadiff/difftypes"
)

// planningPathRecorder passes each planning request to Runtime and keeps the
// database path it carried.
type planningPathRecorder struct {
	featureplan.Runtime
	paths *[]string
}

func (r planningPathRecorder) PlanFeatures(ctx context.Context, request featureplan.Request) (featureplan.Result, error) {
	*r.paths = append(*r.paths, request.DatabasePath)
	return r.Runtime.PlanFeatures(ctx, request)
}

// TestGenerateMigrationAST_OwnersReceiveTheDatabasePath hands the diff's
// database path to the owners of both requests the host builds: the one for
// standalone features on PostgreSQL, and the one for a CockroachDB table's
// settings. Neither target's read sets a path, so the test sets one to see it
// arrive.
func TestGenerateMigrationAST_OwnersReceiveTheDatabasePath(t *testing.T) {
	standalone := &difftypes.SchemaDiff{FeatureChanges: []schemaext.ChangeRecord{{Subject: phaseSubject("guarded"), Value: &phaseChange{}}}}
	tests := []struct {
		name    string
		planner *postgres.Planner
		runtime featureplan.Runtime
		diff    *difftypes.SchemaDiff
	}{
		{name: "standalone features on PostgreSQL", planner: postgres.New(),
			runtime: phaseRuntime{Runtime: must.Must(builtin.New()), phase: featureplan.PhaseDefault}, diff: standalone},
		{name: "a table's settings on CockroachDB", planner: postgres.NewForDialect(platform.CockroachDB, capability.CockroachDB26()),
			runtime: must.Must(builtin.New()), diff: withDeclaredObjects(ttlDiff(&crdbschema.Policy{ExpirationExpression: "expires_at"}, nil), nil)},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			test.diff.CurrentDatabasePath = "/probe"
			var paths []string
			_, err := test.planner.GenerateMigrationAST(t.Context(), planningPathRecorder{Runtime: test.runtime, paths: &paths}, test.diff)
			c.Assert(err, qt.IsNil)
			c.Assert(paths, qt.DeepEquals, []string{"/probe"})
		})
	}
}

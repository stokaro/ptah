package ydb_test

import (
	"context"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/featureplan"
	"ptah.run/core/platform/capability"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/ydb/ydbschema"
	"ptah.run/engine/builtin"
	"ptah.run/internal/planner/dialects/ydb"
)

// planningPathRecorder passes each planning request to the built-in owners
// and keeps the database path it carried.
type planningPathRecorder struct {
	featureplan.Runtime
	paths *[]string
}

func (r planningPathRecorder) PlanFeatures(ctx context.Context, request featureplan.Request) (featureplan.Result, error) {
	*r.paths = append(*r.paths, request.DatabasePath)
	return r.Runtime.PlanFeatures(ctx, request)
}

// TestGenerateMigrationAST_OwnersReceiveTheDatabasePath plans a standalone
// object and a table's TTL against the database /local, and hands both
// owner batches the path: the plan's own request and the one a table's
// facets are planned with.
func TestGenerateMigrationAST_OwnersReceiveTheDatabasePath(t *testing.T) {
	c := qt.New(t)
	diff := ttlChange(t, itemsDeclaration(field("ts", "TIMESTAMP", true)), &ydbschema.TTL{Column: "ts", Interval: "P30D"}, nil)
	diff.FeatureChanges = []schemaext.ChangeRecord{coordinationCreated("", "lock")}
	diff.CurrentDatabasePath = "/local"
	var paths []string

	_, err := ydb.NewWithCapabilities(capability.YDB262()).GenerateMigrationAST(t.Context(),
		planningPathRecorder{Runtime: must.Must(builtin.New()), paths: &paths}, diff)

	c.Assert(err, qt.IsNil)
	c.Assert(paths, qt.DeepEquals, []string{"/local", "/local"})
}

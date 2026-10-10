package schemadiff_test

import (
	"context"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/platform"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/ydb/ydbtopic"
	"ptah.run/engine"
	"ptah.run/engine/builtin"
	"ptah.run/migration/schemadiff"
)

// comparisonPathRecorder passes each feature comparison to the built-in
// owners and keeps the database path it carried.
type comparisonPathRecorder struct {
	*engine.Runtime
	paths *[]string
}

func (r comparisonPathRecorder) CompareFeatures(ctx context.Context, request schemaext.ComparisonRequest) (schemaext.ComparisonResult, error) {
	*r.paths = append(*r.paths, request.DatabasePath)
	return r.Runtime.CompareFeatures(ctx, request)
}

// TestCompare_OwnersReceiveTheDatabasePath compares against a read of the
// database /local and hands the owners that path, the one a plan against
// the read runs in.
func TestCompare_OwnersReceiveTheDatabasePath(t *testing.T) {
	c := qt.New(t)
	current := heldTopics(ydbtopic.ObservedObject("app", "events", readTopicSpec()))
	current.DatabasePath = "/local"
	var paths []string

	diff, err := schemadiff.CompareWithDialect(t.Context(), declaredTopics(), current, platform.YDB,
		comparisonPathRecorder{Runtime: must.Must(builtin.New()), paths: &paths})

	c.Assert(err, qt.IsNil)
	c.Assert(diff.CurrentDatabasePath, qt.Equals, "/local")
	c.Assert(paths, qt.DeepEquals, []string{"/local"})
}

// Package plannerorder_test pins an order dependence on purpose: the YDB
// migration planner is in migration/planner's registry only once a built-in
// runtime exists in the process. The package holds this one test, so nothing
// assembles a runtime before it runs.
package plannerorder_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform"
	"ptah.run/core/ptaherr"
	"ptah.run/engine/builtin"
	"ptah.run/migration/planner"
)

// TestGetPlanner_FindsYDBOnlyOnceABuiltInRuntimeExists refuses ydb as an
// unregistered dialect before builtin.New runs, and finds the YDB planner
// after it. migration/planner links no owner, so the built-in runtime
// registers the planner when it assembles the YDB owner. A runtime-provided
// planner removes the dependence (stokaro/ptah#4204).
func TestGetPlanner_FindsYDBOnlyOnceABuiltInRuntimeExists(t *testing.T) {
	c := qt.New(t)

	before, err := planner.GetPlanner(platform.YDB)

	c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedDialect)
	c.Assert(before, qt.IsNil)

	_, err = builtin.New()
	c.Assert(err, qt.IsNil)

	after, err := planner.GetPlanner(platform.YDB)

	c.Assert(err, qt.IsNil)
	c.Assert(after, qt.IsNotNil)
}

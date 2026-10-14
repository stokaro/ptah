// Package plannerorder_test pins an order dependence on purpose: the migration
// planner of a bundled dialect is in migration/planner's registry only once a
// built-in runtime exists in the process. The package holds this one test, so
// nothing assembles a runtime before it runs.
package plannerorder_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ptaherr"
	"ptah.run/engine/builtin"
	"ptah.run/internal/builtindialects"
	"ptah.run/migration/planner"
)

// TestGetPlanner_FindsABundledDialectOnlyOnceABuiltInRuntimeExists refuses
// every bundled dialect as unregistered before builtin.New runs, and finds
// each one's planner after it. migration/planner links no dialect, so the
// built-in runtime registers the planners when it is assembled. A
// runtime-provided planner removes the dependence (stokaro/ptah#4204).
func TestGetPlanner_FindsABundledDialectOnlyOnceABuiltInRuntimeExists(t *testing.T) {
	c := qt.New(t)
	dialects := builtindialects.Descriptors()

	registered, err := planner.RegisteredDialects()
	c.Assert(err, qt.IsNil)
	c.Assert(registered, qt.HasLen, 0)
	for _, dialect := range dialects {
		before, err := planner.GetPlanner(dialect.Name)
		c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedDialect, qt.Commentf("%s", dialect.Name))
		c.Assert(before, qt.IsNil)
	}

	_, err = builtin.New()
	c.Assert(err, qt.IsNil)

	registered, err = planner.RegisteredDialects()
	c.Assert(err, qt.IsNil)
	c.Assert(registered, qt.HasLen, len(dialects))
	for _, dialect := range dialects {
		after, err := planner.GetPlanner(dialect.Name)
		c.Assert(err, qt.IsNil, qt.Commentf("%s", dialect.Name))
		c.Assert(after, qt.IsNotNil)
	}
}

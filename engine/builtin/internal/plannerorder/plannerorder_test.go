// Package plannerorder_test verifies that constructing a bundled runtime does
// not change the planners available through another runtime.
package plannerorder_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/planext"
	"ptah.run/core/ptaherr"
	"ptah.run/engine"
	"ptah.run/engine/builtin"
	"ptah.run/internal/builtindialects"
)

func TestBundledPlannersBelongOnlyToTheirRuntime(t *testing.T) {
	c := qt.New(t)
	empty, err := engine.New()
	c.Assert(err, qt.IsNil)
	bundled, err := builtin.New()
	c.Assert(err, qt.IsNil)
	dialects := builtindialects.Descriptors()
	c.Assert(bundled.Targets(), qt.HasLen, len(dialects))
	for _, dialect := range dialects {
		absent, err := empty.MigrationPlanner(dialect.Name, planext.Options{})
		c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedDialect)
		c.Assert(absent, qt.IsNil)
		selected, err := bundled.MigrationPlanner(dialect.Name, planext.Options{})
		c.Assert(err, qt.IsNil)
		c.Assert(selected, qt.IsNotNil)
	}
}

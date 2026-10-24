package engineruntime_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ptaherr"
	"ptah.run/engine"
	"ptah.run/internal/cli/internal/engineruntime"
)

func TestExplicitRuntimeDoesNotFallBackToBundledTargets(t *testing.T) {
	c := qt.New(t)
	empty, err := engine.New()
	c.Assert(err, qt.IsNil)
	selected, err := engineruntime.With(c.Context(), empty)
	c.Assert(err, qt.IsNil)
	actual, err := engineruntime.Resolve(selected)
	c.Assert(err, qt.IsNil)
	c.Assert(actual, qt.Equals, empty)
	_, err = actual.ResolveTarget("sqlite")
	c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedDialect)
	plain, err := engineruntime.Resolve(c.Context())
	c.Assert(err, qt.IsNil)
	_, err = plain.ResolveTarget("sqlite")
	c.Assert(err, qt.IsNil)
	invalid, err := engineruntime.With(c.Context(), nil)
	c.Assert(err, qt.IsNotNil)
	c.Assert(invalid, qt.IsNil)
}

// A selection is reported as one, and a context without one reports none.
func TestSelectedReportsOnlyAnExplicitRuntime(t *testing.T) {
	c := qt.New(t)
	empty, err := engine.New()
	c.Assert(err, qt.IsNil)
	selected, err := engineruntime.With(c.Context(), empty)
	c.Assert(err, qt.IsNil)

	actual, found := engineruntime.Selected(selected)
	c.Assert(found, qt.IsTrue)
	c.Assert(actual, qt.Equals, empty)

	none, found := engineruntime.Selected(c.Context())
	c.Assert(found, qt.IsFalse)
	c.Assert(none, qt.IsNil)
}

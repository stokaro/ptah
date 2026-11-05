package engineruntime_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/engine/builtin"
	"ptah.run/internal/cli/internal/engineruntime"
)

// A runtime that serves the dialect, or no dialect at all, is kept, and so is
// one for a dialect nothing serves: the reader refuses that name itself.
func TestServing_HappyPath(t *testing.T) {
	c := qt.New(t)
	bundled, err := builtin.Bundled()
	c.Assert(err, qt.IsNil)
	for _, dialect := range []string{"", "postgres", " PostgreSQL ", "nosuch"} {
		t.Run(dialect, func(t *testing.T) {
			c := qt.New(t)
			runtime, err := engineruntime.Serving(c.Context(), bundled, dialect)
			c.Assert(err, qt.IsNil)
			c.Assert(runtime, qt.Equals, bundled)
		})
	}
}

// A dialect the runtime does not serve starts the provider a session serves
// it with, so a provider that cannot start fails the call.
func TestServing_FailurePath(t *testing.T) {
	c := qt.New(t)
	bundled, err := builtin.Bundled()
	c.Assert(err, qt.IsNil)

	runtime, err := engineruntime.Serving(fixtureSession(c), bundled, "fixture")

	c.Assert(err, qt.ErrorMatches, `(?s).*executable was not found.*`)
	c.Assert(runtime, qt.IsNil)
}

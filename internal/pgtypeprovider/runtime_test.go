package pgtypeprovider_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/engine"
	"ptah.run/internal/pgtypeprovider"
)

// TestProvider_NeedsItsTargets pins that the owner registers only beside a
// provider of every target it serves.
func TestProvider_NeedsItsTargets(t *testing.T) {
	c := qt.New(t)

	runtime, err := engine.New(engine.Provider{ID: "example.org/targets"}, pgtypeprovider.Provider())

	c.Assert(err, qt.ErrorIs, engine.ErrInvalidRegistration)
	c.Assert(runtime, qt.IsNil)
}

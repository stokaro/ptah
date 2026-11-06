package dispatchwitness_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/dispatchwitness"
)

func TestSending_HappyPath(t *testing.T) {
	c := qt.New(t)
	heard := 0
	ctx := dispatchwitness.With(c.Context(), func() { heard++ })

	dispatchwitness.Sending(ctx)
	dispatchwitness.Sending(ctx)

	c.Assert(heard, qt.Equals, 2)
}

// A rehearsal, a migration and every other caller run with no witness, and
// sending under them is not an error.
func TestSending_WithoutAWitness(t *testing.T) {
	c := qt.New(t)
	heard := 0
	_ = dispatchwitness.With(c.Context(), func() { heard++ })

	dispatchwitness.Sending(c.Context())

	c.Assert(heard, qt.Equals, 0)
}

package schemacensus

// White-box testing required: the plan surface the census measures is
// planOne, an unexported function that composes the shipping compare, plan
// and render entry points; measuring it through the exported MeasurePlan would
// take the whole ablation matrix twice to observe one render twice, and would
// still leave the flip to chance. The property under test is the one Measure
// and MeasurePlan rely on and cannot check themselves: one schema on one cell
// renders the same bytes every time.

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/engine/builtin"
	"ptah.run/internal/capabilityprobe"
)

// TestSurfaces_RenderTheSameBytesTwice pins the property the census stands
// on. Measure compares a fixture's render with and without a field and calls
// any difference a field the surface reads, so a render that moves on its own
// reads as a field one surface loses, and the field it lands on is whichever
// ablation ran next to the reordering. Measured on the fixture that flaked
// (stokaro/ptah#2968): the PostgreSQL-family renderer walked the table
// options in map order, and `table-mysql` rendered its AUTO_INCREMENT,
// CHARSET and COLLATE options in a different order on every other run, on
// every PostgreSQL, CockroachDB, YugabyteDB and Spanner cell, through both
// surfaces.
func TestSurfaces_RenderTheSameBytesTwice(t *testing.T) {
	// A selected runtime serves a whole schema session. Rebuilding its codec
	// registry for every cell measures registration thousands of times and
	// exhausts the package timeout before the failure-boundary tests run.
	runtime := must.Must(builtin.New())
	surfaces := []struct {
		name    string
		surface surface
	}{
		{name: "render", surface: renderSurface(t.Context(), runtime)},
		{name: "plan", surface: planSurface(t.Context(), runtime)},
	}

	for _, surface := range surfaces {
		for _, fixture := range Fixtures() {
			t.Run(surface.name+" "+fixture.Name, func(t *testing.T) {
				t.Parallel()
				c := qt.New(t)
				first, err := everyCell(surface.surface, fixture.Schema, fixture.Cells(capabilityprobe.Cells))
				c.Assert(err, qt.IsNil)
				second, err := everyCell(surface.surface, fixture.Schema, fixture.Cells(capabilityprobe.Cells))
				c.Assert(err, qt.IsNil)
				c.Assert(second, qt.DeepEquals, first)
			})
		}
	}
}

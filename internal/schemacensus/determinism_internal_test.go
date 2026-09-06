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

	"ptah.run/core/schemamodel"
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
	surfaces := []struct {
		name    string
		surface func(schemamodel.Database, capabilityprobe.Cell) string
	}{
		{name: "render", surface: renderOne},
		{name: "plan", surface: planOne},
	}

	for _, surface := range surfaces {
		for _, fixture := range Fixtures() {
			t.Run(surface.name+" "+fixture.Name, func(t *testing.T) {
				c := qt.New(t)
				first := everyCell(surface.surface, fixture.Schema, capabilityprobe.Cells)
				second := everyCell(surface.surface, fixture.Schema, capabilityprobe.Cells)
				c.Assert(second, qt.DeepEquals, first)
			})
		}
	}
}

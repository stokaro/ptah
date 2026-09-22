package capabilityprobe

// White-box testing required: Unprobed governs skipReasons, which is
// unexported and reachable from outside only through CIMatrix over the
// package-level Cells. No declared line sets Unprobed, so a black-box test
// would assert over a corpus the rule has no subject in, and would report
// success about a rule nothing exercised.

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform"
	"ptah.run/core/platform/capability"
)

func probedCell() Cell {
	return Cell{
		Dialect: platform.Postgres, Line: "15",
		Preset: capability.Postgres16, PresetName: "Postgres16",
		Refinement: RefinedByVersion, Support: capability.BestEffort, Image: "postgres:15",
	}
}

// A reason makes the line skipped and is what the matrix reports, while the
// image stays: the declaration keeps saying which container the line ran on.
func TestUnprobed_HappyPath(t *testing.T) {
	t.Run("a reason skips the line", func(t *testing.T) {
		c := qt.New(t)
		cell := probedCell()
		cell.Unprobed = "upstream support ended on 2026-08-29"

		converted := toCICell(cell)

		c.Assert(converted.Runnable, qt.IsFalse)
		c.Assert(converted.Skip, qt.Equals, "upstream support ended on 2026-08-29")
		c.Assert(converted.Image, qt.Equals, "postgres:15")
	})

	// The control. Without it, a rule that skipped every line would pass the
	// assertion above and say nothing.
	t.Run("the same line without a reason runs", func(t *testing.T) {
		c := qt.New(t)
		converted := toCICell(probedCell())
		c.Assert(converted.Runnable, qt.IsTrue)
		c.Assert(converted.Skip, qt.Equals, "")
	})
}

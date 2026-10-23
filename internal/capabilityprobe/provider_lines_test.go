package capabilityprobe_test

import (
	"cmp"
	"slices"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform"
	"ptah.run/internal/capabilityprobe"
	"ptah.run/internal/providerreleaselines"
)

// The YDB plugin declares its release lines itself, and the matrix declares
// them here. The two have to agree on the line, the level Ptah promises, the
// image that reproduces it and whether it is exercised, or the support page
// says one thing and the provider another.
func TestYDBCellsMatchTheLinesThePluginDeclares(t *testing.T) {
	c := qt.New(t)

	document, err := providerreleaselines.YDB()
	c.Assert(err, qt.IsNil)
	c.Assert(document.Engines, qt.HasLen, 1)

	type shape struct {
		Line, Support, Image string
		Unprobed             bool
	}
	var declared []shape
	for _, line := range document.Engines[0].Lines {
		level, err := providerreleaselines.SupportLevel(line.Support)
		c.Assert(err, qt.IsNil)
		declared = append(declared, shape{line.Line, level.String(), line.Image, line.Unprobed != ""})
	}
	ydbCells := slices.DeleteFunc(slices.Clone(capabilityprobe.Cells), func(cell capabilityprobe.Cell) bool {
		return cell.Dialect != platform.YDB
	})
	cells := make([]shape, len(ydbCells))
	for i, cell := range ydbCells {
		cells[i] = shape{cell.Line, cell.Support.String(), cell.Image, cell.Unprobed != ""}
	}
	slices.SortFunc(declared, func(a, b shape) int { return cmp.Compare(a.Line, b.Line) })
	slices.SortFunc(cells, func(a, b shape) int { return cmp.Compare(a.Line, b.Line) })

	c.Assert(cells, qt.DeepEquals, declared)
}

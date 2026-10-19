package atlashclrender_test

import (
	"slices"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform"
	"ptah.run/internal/atlashclrender"
)

// TestModeledColumnTypes_AreTheDescriptors pins that the writer models every
// column type a dialect's descriptor states, and no other, in every spelling
// of the dialect: the measured vocabulary lives in the descriptor
// (descriptor.Atlas.HCL.ColumnTypes), and a type the writer dropped from it
// would be wrapped in sql() where the binary reads it bare.
func TestModeledColumnTypes_AreTheDescriptors(t *testing.T) {
	for _, spelling := range platform.DialectSpellings() {
		t.Run(spelling, func(t *testing.T) {
			c := qt.New(t)
			described, found := platform.Lookup(spelling)
			c.Assert(found, qt.IsTrue)

			got := atlashclrender.ModeledColumnTypes(spelling)

			c.Assert(got, qt.DeepEquals, sortedOrNil(described.Atlas.HCL.ColumnTypes))
			for _, name := range described.Atlas.HCL.ColumnTypes {
				c.Assert(atlashclrender.IsModeledColumnType(spelling, strings.ToUpper(name)), qt.IsTrue, qt.Commentf("%s", name))
			}
		})
	}
}

// TestAtlasRefusedBlockTypes_AreTheDescriptors pins the same for the blocks
// the binary refuses (descriptor.Atlas.HCL.RefusedBlocks).
func TestAtlasRefusedBlockTypes_AreTheDescriptors(t *testing.T) {
	for _, spelling := range platform.DialectSpellings() {
		t.Run(spelling, func(t *testing.T) {
			c := qt.New(t)
			described, found := platform.Lookup(spelling)
			c.Assert(found, qt.IsTrue)

			c.Assert(atlashclrender.AtlasRefusedBlockTypes(spelling), qt.DeepEquals, described.Atlas.HCL.RefusedBlocks)
			for _, block := range described.Atlas.HCL.RefusedBlocks {
				c.Assert(atlashclrender.AtlasRefusesBlock(spelling, block), qt.IsTrue, qt.Commentf("%s", block))
			}
		})
	}
}

// sortedOrNil is names sorted, or nil for none, as ModeledColumnTypes
// answers a dialect without a vocabulary.
func sortedOrNil(names []string) []string {
	if len(names) == 0 {
		return nil
	}
	return slices.Sorted(slices.Values(names))
}

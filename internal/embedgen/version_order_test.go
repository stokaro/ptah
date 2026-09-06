package embedgen_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/embedgen"
	"ptah.run/internal/embedrun"
)

// TestVersionOrder_EveryStrategyStatesHowItsVersionsCompare is the ratchet on
// stokaro/ptah#2635.
//
// One comparison served every strategy — opaque strings ordered by length then
// lexicographically — which is right for a counter and exactly backwards for a
// rendered timestamp. A strategy added later inherits `OrderUnknown` unless
// somebody decides, and this walks the enumeration so that "nobody decided" is
// a failure rather than a silent default.
func TestVersionOrder_EveryStrategyStatesHowItsVersionsCompare(t *testing.T) {
	want := map[embedgen.VersionStrategy]embedrun.VersionOrder{
		embedgen.VersionUnset:          embedrun.OrderUnknown,
		embedgen.VersionMonotonic:      embedrun.OrderNumeric,
		embedgen.VersionOutboxSequence: embedrun.OrderNumeric,
		embedgen.VersionUpdatedAt:      embedrun.OrderTimestamp,
		embedgen.VersionInputHash:      embedrun.OrderUnknown,
	}

	strategies := embedgen.VersionStrategies()
	c := qt.New(t)
	c.Assert(strategies, qt.Not(qt.HasLen), 0,
		qt.Commentf("an empty enumeration would make every row below vacuous"))

	for _, strategy := range strategies {
		t.Run(string(strategy), func(t *testing.T) {
			c := qt.New(t)
			expected, stated := want[strategy]

			c.Assert(stated, qt.IsTrue,
				qt.Commentf("strategy %q has no recorded order; decide how its versions "+
					"compare rather than letting it inherit OrderUnknown", strategy))
			c.Assert(strategy.VersionOrder(), qt.Equals, expected)
		})
	}
}

// TestVersionOrder_TheTableNamesNoStrategyThatIsGone is the other direction.
//
// An entry left behind by a rename makes the coverage above look more complete
// than it is.
func TestVersionOrder_TheTableNamesNoStrategyThatIsGone(t *testing.T) {
	c := qt.New(t)
	live := make(map[embedgen.VersionStrategy]bool)
	for _, strategy := range embedgen.VersionStrategies() {
		live[strategy] = true
	}

	for _, strategy := range []embedgen.VersionStrategy{
		embedgen.VersionUnset, embedgen.VersionMonotonic, embedgen.VersionOutboxSequence,
		embedgen.VersionUpdatedAt, embedgen.VersionInputHash,
	} {
		c.Assert(live[strategy], qt.IsTrue,
			qt.Commentf("%q is classified above and is not a strategy any more", strategy))
	}
}

package capability_test

import (
	"sync"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform/capability"
)

// A preset is built once and every caller gets that one set, so nothing a
// caller does with its set may reach the next caller's. A set offers no way
// to change it: With returns a new set and Map returns a map the caller owns.
// The rows cover a set written as a literal, one derived from it, and one at
// the end of a chain of derived sets.
func TestPresets_NoCallerChangesAnotherCallersSet(t *testing.T) {
	tests := []struct {
		name   string
		preset func() capability.Capabilities
	}{
		{name: "Postgres16", preset: capability.Postgres16},
		{name: "CockroachDB23", preset: capability.CockroachDB23},
		{name: "YDB251", preset: capability.YDB251},
		{name: "MySQL8013", preset: capability.MySQL8013},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			c := qt.New(t)
			first := tc.preset()
			unchanged := first.Map()
			c.Assert(len(unchanged) > 10, qt.IsTrue)

			flags := first.Map()
			for key, enabled := range flags {
				flags[key] = !enabled
			}
			flags["test.invented"] = true
			derived := first
			for key, enabled := range first.All() {
				derived = derived.With(key, !enabled)
			}
			derived = derived.With("test.invented", true)

			c.Assert(first.Map(), qt.DeepEquals, unchanged)
			c.Assert(tc.preset().Map(), qt.DeepEquals, unchanged)
			c.Assert(derived.Equal(first), qt.IsFalse)
		})
	}
}

// Concurrent callers share one preset and derive their own sets from it; the
// race detector reports a write two callers can see. Presets are
// process-wide, so this cannot force the first build to be concurrent;
// sync.OnceValue owns that guarantee.
func TestPresets_ConcurrentCallsShareTheSetSafely(t *testing.T) {
	c := qt.New(t)
	want := capability.YDB252().Map()
	results := make([]capability.Capabilities, 8)
	var wg sync.WaitGroup
	for i := range results {
		wg.Go(func() {
			results[i] = capability.YDB252().With("test.invented", true)
		})
	}
	wg.Wait()
	for _, result := range results {
		c.Assert(result.Has("test.invented"), qt.IsTrue)
		c.Assert(capability.YDB252().Map(), qt.DeepEquals, want)
	}
}

// A set made from a map keeps no reference to it, and a map read from a set
// is the caller's: changing either map changes no set.
func TestOf_SharesNoMapWithItsCaller(t *testing.T) {
	c := qt.New(t)
	flags := map[capability.Capability]bool{capability.DropConstraintGeneric: true}

	set := capability.Of(flags)
	flags[capability.DropConstraintGeneric] = false
	flags[capability.DropIndexIfExists] = true
	read := set.Map()
	read[capability.DropConstraintGeneric] = false

	c.Assert(set.Has(capability.DropConstraintGeneric), qt.IsTrue)
	c.Assert(set.Established(capability.DropIndexIfExists), qt.IsFalse)
	c.Assert(set.Len(), qt.Equals, 1)
}

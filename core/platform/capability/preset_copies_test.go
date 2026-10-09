package capability_test

import (
	"maps"
	"sync"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform/capability"
)

// A preset is built once and shared, so every call has to hand back a set its
// caller may change without changing the next caller's. The rows cover a set
// written as a literal, one derived from it, and one at the end of a chain of
// derived sets.
func TestPresets_EveryCallReturnsAnIndependentCopy(t *testing.T) {
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
			c.Assert(len(first) > 10, qt.IsTrue)
			unchanged := maps.Clone(first)

			for key, enabled := range first {
				first[key] = !enabled
			}
			first["test.invented"] = true

			c.Assert(tc.preset(), qt.DeepEquals, unchanged)
		})
	}
}

// Concurrent first use builds the set once and hands each caller its own copy;
// the race detector reports a set two callers share.
func TestPresets_ConcurrentCallsGetTheirOwnSet(t *testing.T) {
	c := qt.New(t)
	want := capability.YDB252()
	results := make([]capability.Capabilities, 8)
	var wg sync.WaitGroup
	for i := range results {
		wg.Go(func() {
			results[i] = capability.YDB252()
			results[i]["test.invented"] = true
		})
	}
	wg.Wait()
	for _, result := range results {
		delete(result, "test.invented")
		c.Assert(result, qt.DeepEquals, want)
	}
}

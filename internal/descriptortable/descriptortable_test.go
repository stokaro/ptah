package descriptortable_test

import (
	"sync"
	"sync/atomic"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform"
	"ptah.run/core/platform/descriptor"
	"ptah.run/internal/descriptortable"
)

// TestTable_Get answers every spelling the process registry accepts, in the
// case and padding a caller may pass, with what derive gives for the
// descriptor it selects, and an empty or unknown dialect with what derive
// gives for the zero descriptor.
func TestTable_Get(t *testing.T) {
	names := descriptortable.New(func(described descriptor.Dialect) string { return "name:" + described.Name })
	for _, spelling := range append(platform.DialectSpellings(), " PGX ") {
		t.Run(spelling, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(names.Get(spelling), qt.Equals, "name:"+platform.NormalizeDialect(spelling))
		})
	}
	for _, dialect := range []string{"", "nonesuch"} {
		t.Run("unknown "+dialect, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(names.Get(dialect), qt.Equals, "name:")
		})
	}
}

// TestTable_DerivesOncePerDescriptor asks many times, from many goroutines,
// and derive runs once for each descriptor of the registry and once for the
// zero descriptor, once the first call has derived them.
func TestTable_DerivesOncePerDescriptor(t *testing.T) {
	var calls atomic.Int64
	table := descriptortable.New(func(descriptor.Dialect) bool {
		calls.Add(1)
		return true
	})
	c := qt.New(t)
	c.Assert(table.Get("postgres"), qt.IsTrue)
	var group sync.WaitGroup
	for range 16 {
		group.Go(func() {
			for _, spelling := range platform.DialectSpellings() {
				table.Get(spelling)
			}
		})
	}
	group.Wait()
	c.Assert(calls.Load(), qt.Equals, int64(len(platform.Dialects().Dialects())+1))
}

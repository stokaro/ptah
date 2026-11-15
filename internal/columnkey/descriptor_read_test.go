package columnkey_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform"
	"ptah.run/internal/columnkey"
)

// TestNamed_CopiesNoDescriptor holds the cost of a question the comparison and
// the planners ask once per column and key. Reading ColumnKeys through
// platform.Lookup copies the whole descriptor, dozens of allocations, every
// time; read from a table kept per registry, it allocates nothing.
func TestNamed_CopiesNoDescriptor(t *testing.T) {
	for _, dialect := range append(platform.DialectSpellings(), "", "nonesuch") {
		t.Run(dialect, func(t *testing.T) {
			c := qt.New(t)
			named := func() { columnkey.Named(dialect) }
			named()

			c.Assert(testing.AllocsPerRun(100, named), qt.Equals, float64(0))
		})
	}
}

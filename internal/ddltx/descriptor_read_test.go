package ddltx_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform"
	"ptah.run/internal/ddltx"
)

// TestClassOf_CopiesNoDescriptor holds the cost of a question the migrator
// asks once per statement it runs. Reading DDLTransactions through
// platform.Lookup copies the whole descriptor, dozens of allocations, every
// time; read from a table kept per registry, it allocates nothing.
func TestClassOf_CopiesNoDescriptor(t *testing.T) {
	for _, dialect := range append(platform.DialectSpellings(), "", "nonesuch") {
		t.Run(dialect, func(t *testing.T) {
			c := qt.New(t)
			classOf := func() { ddltx.ClassOf(dialect) }
			classOf()

			c.Assert(testing.AllocsPerRun(100, classOf), qt.Equals, float64(0))
		})
	}
}

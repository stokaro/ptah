package revisiontable_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform"
	"ptah.run/internal/revisiontable"
)

// TestVersionLiteral_CopiesNoDescriptor holds the cost of the literal the
// migrator renders for every revision it records or reads. Reading the style
// through platform.Lookup copies the whole descriptor, dozens of allocations,
// every time; read from a table kept per registry, the call allocates only the
// literal it returns.
func TestVersionLiteral_CopiesNoDescriptor(t *testing.T) {
	for _, dialect := range platform.DialectSpellings() {
		t.Run(dialect, func(t *testing.T) {
			c := qt.New(t)
			literal := func() { revisiontable.VersionLiteral(dialect, "20260101000000") }
			literal()

			allocs := testing.AllocsPerRun(100, literal)

			c.Assert(allocs <= 1, qt.IsTrue, qt.Commentf("VersionLiteral allocated %v times", allocs))
		})
	}
}

package tableidentity_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform"
	"ptah.run/core/platform/identifier"
	"ptah.run/internal/tableidentity"
)

// TestSubject_CopiesNoDescriptor holds the cost of the call a comparison makes
// once per table and column it pairs. Reading VerbatimTableNames through
// platform.Lookup copies the whole descriptor, dozens of allocations, every
// time; read from a table kept per registry, the identity allocates nothing.
func TestSubject_CopiesNoDescriptor(t *testing.T) {
	for _, dialect := range platform.DialectSpellings() {
		t.Run(dialect, func(t *testing.T) {
			c := qt.New(t)
			semantics := identifier.ForDialect(dialect)
			subject := func() { tableidentity.Subject("public", "items", dialect, semantics) }
			subject()

			c.Assert(testing.AllocsPerRun(100, subject), qt.Equals, float64(0))
		})
	}
}

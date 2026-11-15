package indexbacking_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/core/platform"
	"ptah.run/internal/indexbacking"
)

// sqliteAutoindex is named with the prefix SQLite gives the indexes it builds.
var sqliteAutoindex = catalog.Index{Name: "sqlite_autoindex_users_1", IsUnique: true}

// TestServerBacks_CopiesNoDescriptor holds the cost of a question the
// comparator asks about every index and key it pairs. Reading the answer
// through platform.Lookup copies the whole descriptor, dozens of allocations,
// every time; read from a table kept per registry, it allocates nothing.
func TestServerBacks_CopiesNoDescriptor(t *testing.T) {
	for _, dialect := range platform.DialectSpellings() {
		t.Run(dialect, func(t *testing.T) {
			c := qt.New(t)
			backs := func() {
				indexbacking.ServerBacks(dialect, indexbacking.ForeignKey)
				indexbacking.Unaddressable(sqliteAutoindex, dialect)
			}
			backs()

			c.Assert(testing.AllocsPerRun(100, backs), qt.Equals, float64(0))
		})
	}
}

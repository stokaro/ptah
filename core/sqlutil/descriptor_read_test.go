package sqlutil_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/sqlutil"
)

// TestRebind_CopiesNoDescriptor holds the cost of rebinding a statement whose
// placeholders a dialect keeps as written. Reading the placeholder style
// through platform.Lookup copies the whole descriptor, dozens of allocations,
// on every statement; read from a table kept per registry, a statement that
// needs no rewrite allocates nothing.
func TestRebind_CopiesNoDescriptor(t *testing.T) {
	for _, dialect := range []string{"mysql", "mariadb", "sqlite", "clickhouse", "nonesuch"} {
		t.Run(dialect, func(t *testing.T) {
			c := qt.New(t)
			rebind := func() { sqlutil.Rebind(dialect, "SELECT a FROM t WHERE b = ?") }
			rebind()

			c.Assert(testing.AllocsPerRun(100, rebind), qt.Equals, float64(0))
		})
	}
}

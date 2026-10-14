package sqlutil_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform"
	"ptah.run/core/platform/descriptor"
	"ptah.run/core/sqlutil"
)

// TestRebind_AgreesWithEachDescriptor pins the placeholder style every
// registered descriptor states against what Rebind writes for the dialect,
// so the descriptor is proven before Rebind is changed to read it.
func TestRebind_AgreesWithEachDescriptor(t *testing.T) {
	rebound := map[descriptor.Placeholder]string{
		descriptor.PlaceholderQuestion:    "SELECT a FROM t WHERE b = ? AND c = ?",
		descriptor.PlaceholderDollar:      "SELECT a FROM t WHERE b = $1 AND c = $2",
		descriptor.PlaceholderAtP:         "SELECT a FROM t WHERE b = @p1 AND c = @p2",
		descriptor.PlaceholderColon:       "SELECT a FROM t WHERE b = :1 AND c = :2",
		descriptor.PlaceholderDollarNamed: "SELECT a FROM t WHERE b = $p1 AND c = $p2",
	}
	for _, described := range platform.Dialects().Dialects() {
		t.Run(described.Name, func(t *testing.T) {
			c := qt.New(t)

			got := sqlutil.Rebind(described.Name, "SELECT a FROM t WHERE b = ? AND c = ?")

			c.Assert(got, qt.Equals, rebound[described.Placeholder], qt.Commentf("placeholder %q", described.Placeholder))
		})
	}
}

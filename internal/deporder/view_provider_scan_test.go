package deporder_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/deporder"
	"ptah.run/internal/lexerext"
	"ptah.run/internal/providerscan"
	"ptah.run/internal/providerscan/scantest"
)

// A provider's scanner decides whether a view body names another view. The
// multiline string is not an edge back to the first view and cannot turn the
// actual dependency into a false cycle.
func TestViewLikesForCreateForDialect_UsesProviderScanner(t *testing.T) {
	c := qt.New(t)
	release, err := lexerext.Provide(providerscan.New(scantest.Scan), "scanned-views")
	c.Assert(err, qt.IsNil)
	defer release()

	views := []deporder.ViewLike{
		{Name: "a_report", Body: "SELECT id FROM z_base"},
		{Name: "z_base", Body: "SELECT @@a_report@@ AS label"},
	}
	c.Assert(viewLikeNames(deporder.ViewLikesForCreateForDialect(views, "scanned-views")),
		qt.DeepEquals, []string{"z_base", "a_report"})
	c.Assert(viewLikeNames(deporder.ViewLikesForDropForDialect(views, "scanned-views")),
		qt.DeepEquals, []string{"a_report", "z_base"})
}

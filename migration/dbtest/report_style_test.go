package dbtest_test

import (
	"context"
	"maps"
	"regexp"
	"slices"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/migration/dbtest"
)

// styledReport is a run with one passing and one skipped case, which is enough
// markup for every rule this package adds to the shared appearance.
func styledReport(c *qt.C) string {
	c.Helper()
	report, err := dbtest.RunMigrationTest(context.Background(), dbtest.Options{
		Cases: []dbtest.Case{
			{Name: "skipped", Skip: true, Steps: []dbtest.Step{{Exec: "SELECT 1"}}},
			{Name: "ran", Steps: []dbtest.Step{{Exec: "SELECT 1"}}},
		},
	})
	c.Assert(err, qt.IsNil)
	page, err := report.HTML()
	c.Assert(err, qt.IsNil)
	return page
}

// TestReportHTML_ResolvesEveryCustomPropertyItUses is what keeps this report's
// own rules honest about the shared appearance they are added to.
//
// The page is internal/htmlstyle's tokens plus the case and step list, and only
// the second half lives in this package. A var() naming a token that
// declaration stopped carrying does not fail: the browser discards the whole
// declaration and says nothing, so a retired token leaves a report that
// renders, renders wrongly, and passes every other test here.
func TestReportHTML_ResolvesEveryCustomPropertyItUses(t *testing.T) {
	c := qt.New(t)

	page := styledReport(c)

	styles := regexp.MustCompile(`(?s)<style>(.*?)</style>`).FindStringSubmatch(page)
	c.Assert(styles, qt.HasLen, 2, qt.Commentf("the report carries exactly one stylesheet"))

	declared := make(map[string]bool)
	for _, match := range regexp.MustCompile(`(--[a-z0-9-]+)\s*:`).FindAllStringSubmatch(styles[1], -1) {
		declared[match[1]] = true
	}
	used := make(map[string]bool)
	for _, match := range regexp.MustCompile(`var\((--[a-z0-9-]+)`).FindAllStringSubmatch(styles[1], -1) {
		used[match[1]] = true
	}
	c.Assert(len(used) > 0, qt.IsTrue, qt.Commentf("the stylesheet uses no tokens at all"))
	for _, token := range slices.Sorted(maps.Keys(used)) {
		c.Assert(declared[token], qt.IsTrue,
			qt.Commentf("var(%s) resolves to nothing: no block declares it", token))
	}
}

// TestReportHTML_FetchesNothing keeps the report shareable by copying.
func TestReportHTML_FetchesNothing(t *testing.T) {
	c := qt.New(t)

	page := styledReport(c)

	c.Assert(regexp.MustCompile(`(?i)(https?:)?//[a-z0-9.-]+\.[a-z]{2,}`).FindAllString(page, -1), qt.HasLen, 0)
	for _, element := range []string{"<script", "<link", "<img", "@import"} {
		c.Assert(page, qt.Not(qt.Contains), element)
	}
}

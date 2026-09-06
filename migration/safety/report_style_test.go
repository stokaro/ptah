package safety_test

import (
	"maps"
	"regexp"
	"slices"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/migration/safety"
)

// TestRenderHTML_ResolvesEveryCustomPropertyItUses is what keeps this report's
// own rules honest about the shared appearance they are added to.
//
// The page is internal/htmlstyle's tokens plus what only a safety report has,
// and only the second half lives in this package. A var() naming a token that
// declaration stopped carrying does not fail: the browser discards the whole
// declaration and says nothing, so a retired token leaves a report that
// renders, renders wrongly, and passes every other test here.
func TestRenderHTML_ResolvesEveryCustomPropertyItUses(t *testing.T) {
	c := qt.New(t)

	var page strings.Builder
	err := safety.RenderHTML(&page, []safety.StatementAssessment{
		{Index: 1, Severity: safety.Safe, Subject: "products", Reason: "adds a column", Statement: "ALTER TABLE"},
		{Index: 2, Severity: safety.Warning, Subject: "products", Reason: "tightens", Statement: "ALTER TABLE"},
		{Index: 3, Severity: safety.Destructive, Subject: "products", Reason: "removes", Statement: "DROP TABLE"},
	})

	c.Assert(err, qt.IsNil)
	styles := regexp.MustCompile(`(?s)<style>(.*?)</style>`).FindStringSubmatch(page.String())
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

// TestRenderHTML_AnswersTheQuestionItIsOpenedWith puts the destructive count
// above the table.
//
// A reader opens a safety report to find out whether anything removes data.
// The report they had made them read every row to answer that.
func TestRenderHTML_AnswersTheQuestionItIsOpenedWith(t *testing.T) {
	c := qt.New(t)

	var page strings.Builder
	err := safety.RenderHTML(&page, []safety.StatementAssessment{
		{Index: 1, Severity: safety.Safe, Subject: "products", Reason: "adds a column", Statement: "ALTER TABLE"},
		{Index: 2, Severity: safety.Destructive, Subject: "products", Reason: "removes", Statement: "DROP TABLE"},
	})

	c.Assert(err, qt.IsNil)
	c.Assert(page.String(), qt.Contains,
		`<div class="stat"><div class="stat-n">1</div><div class="stat-l">destructive</div></div>`)
	c.Assert(page.String(), qt.Contains, `<span class="tag destructive">destructive</span>`)
}

// TestRenderHTML_FetchesNothing keeps the report shareable by copying.
func TestRenderHTML_FetchesNothing(t *testing.T) {
	c := qt.New(t)

	var page strings.Builder
	err := safety.RenderHTML(&page, []safety.StatementAssessment{
		{Index: 1, Severity: safety.Safe, Subject: "products", Reason: "adds a column", Statement: "ALTER TABLE"},
	})

	c.Assert(err, qt.IsNil)
	c.Assert(regexp.MustCompile(`(?i)(https?:)?//[a-z0-9.-]+\.[a-z]{2,}`).FindAllString(page.String(), -1), qt.HasLen, 0)
	for _, element := range []string{"<script", "<link", "<img", "@import"} {
		c.Assert(page.String(), qt.Not(qt.Contains), element)
	}
}

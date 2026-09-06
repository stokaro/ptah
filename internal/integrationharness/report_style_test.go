package integrationharness_test

import (
	"maps"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/integrationharness"
)

// styledHTMLReport writes one report with a pass, a failure carrying steps, and
// a skip, which is every branch this package's own rules render.
func styledHTMLReport(c *qt.C) string {
	c.Helper()
	now := time.Now()
	report := &integrationharness.TestReport{
		StartTime:    now,
		EndTime:      now.Add(time.Second),
		TotalTests:   3,
		PassedTests:  1,
		FailedTests:  1,
		SkippedTests: 1,
		Results: []integrationharness.TestResult{
			{Name: "runs_postgres", Database: "postgres", Success: true, Description: "applies and rolls back"},
			{
				Name: "fails_mysql", Database: "mysql", Description: "applies a migration",
				Error: "expected 2 rows, got 0",
				Steps: []integrationharness.TestStep{
					{Name: "apply", Success: true, Description: "migrated to latest"},
					{Name: "verify", Error: "expected 2 rows, got 0"},
				},
			},
			{Name: "skips_clickhouse", Database: "clickhouse", Skipped: true, SkipReason: "not compatible"},
		},
	}
	outputDir := c.TempDir()
	c.Assert(integrationharness.NewReporter(report).GenerateReport(integrationharness.FormatHTML, outputDir), qt.IsNil)
	files, err := filepath.Glob(filepath.Join(outputDir, "*-report.html"))
	c.Assert(err, qt.IsNil)
	c.Assert(files, qt.HasLen, 1)
	content, err := os.ReadFile(files[0])
	c.Assert(err, qt.IsNil)
	return string(content)
}

// TestHTMLReport_ResolvesEveryCustomPropertyItUses is what keeps this report's
// own rules honest about the shared appearance they are added to.
//
// A var() naming a token internal/htmlstyle stopped carrying does not fail: the
// browser discards the whole declaration and says nothing, so a retired token
// leaves a report that renders, renders wrongly, and passes every other test
// here.
func TestHTMLReport_ResolvesEveryCustomPropertyItUses(t *testing.T) {
	c := qt.New(t)

	page := styledHTMLReport(c)

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

// TestHTMLReport_ExpandsStepsWithoutAScript pins the disclosure this report
// uses.
//
// The step list was a table row toggled by an inline onclick handler, which
// interpolated a scenario name into JavaScript. A details element needs no
// script, works with the keyboard and with find-in-page, and leaves nothing to
// escape into a JavaScript context.
func TestHTMLReport_ExpandsStepsWithoutAScript(t *testing.T) {
	c := qt.New(t)

	page := styledHTMLReport(c)

	c.Assert(page, qt.Contains, "<details>")
	c.Assert(page, qt.Contains, "<summary>2 steps</summary>")
	c.Assert(page, qt.Not(qt.Contains), "<script")
	c.Assert(page, qt.Not(qt.Contains), "onclick")
}

// TestHTMLReport_LabelsEveryOutcome keeps a skip from reading as a pass.
func TestHTMLReport_LabelsEveryOutcome(t *testing.T) {
	c := qt.New(t)

	page := styledHTMLReport(c)

	c.Assert(page, qt.Contains, `<span class="tag pass">PASS</span>`)
	c.Assert(page, qt.Contains, `<span class="tag fail">FAIL</span>`)
	c.Assert(page, qt.Contains, `<span class="tag skip">SKIP</span>`)
	c.Assert(page, qt.Contains, "not compatible")
}

package integration

import (
	"bytes"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"
)

func TestGenerateSummaryExcludesSkippedFromSuccessRate(t *testing.T) {
	c := qt.New(t)

	runner := NewTestRunner(nil)
	runner.report.StartTime = time.Now()
	runner.report.EndTime = runner.report.StartTime.Add(time.Second)
	runner.report.TotalTests = 3
	runner.report.PassedTests = 1
	runner.report.FailedTests = 0
	runner.report.SkippedTests = 2

	runner.generateSummary()

	c.Assert(runner.report.Summary, qt.Contains, "Executed 1 tests")
	c.Assert(runner.report.Summary, qt.Contains, "1 passed, 0 failed, 2 skipped")
	c.Assert(runner.report.Summary, qt.Contains, "100.0% success rate")
}

func TestTextReportShowsSkippedAndExecutedSuccessRate(t *testing.T) {
	c := qt.New(t)

	now := time.Now()
	report := &TestReport{
		StartTime:    now,
		EndTime:      now.Add(time.Second),
		TotalTests:   3,
		PassedTests:  1,
		FailedTests:  0,
		SkippedTests: 2,
		Summary:      "Executed 1 tests in 1s. 1 passed, 0 failed, 2 skipped (100.0% success rate)",
		Results: []TestResult{
			{Name: "runs_postgres", Database: "postgres", Success: true},
			{Name: "skips_clickhouse", Database: "clickhouse", Skipped: true, SkipReason: "not compatible"},
		},
	}

	var buf bytes.Buffer
	err := NewReporter(report).generateTextStreamReport(&buf)
	c.Assert(err, qt.IsNil)

	out := buf.String()
	c.Assert(out, qt.Contains, "Total Tests: 3")
	c.Assert(out, qt.Contains, "Passed: 1")
	c.Assert(out, qt.Contains, "Failed: 0")
	c.Assert(out, qt.Contains, "Skipped: 2")
	c.Assert(out, qt.Contains, "Success Rate: 100.0%")
	c.Assert(out, qt.Contains, "SKIP skips_clickhouse")
	c.Assert(out, qt.Contains, "Skip: not compatible")
}

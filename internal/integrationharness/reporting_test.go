package integrationharness_test

import (
	"context"
	"io/fs"
	"os"
	"path/filepath"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/dbschema"
	"go.5x5.cz/ptah/internal/atlasurl"
	"go.5x5.cz/ptah/internal/integrationharness"
)

func TestTestRunnerSummaryUsesExecutedTestsForSuccessRate(t *testing.T) {
	c := qt.New(t)
	runner := integrationharness.NewTestRunner(nil)
	runner.AddDatabase("sqlite", atlasurl.SQLiteURLFromPath(filepath.Join(c.TempDir(), "report.db")))
	runner.AddScenario(integrationharness.TestScenario{
		Name:        "passes",
		Description: "A successful public runner scenario",
		TestFunc: func(context.Context, *dbschema.DatabaseConnection, fs.FS) error {
			return nil
		},
	})
	c.Assert(runner.RunAll(t.Context()), qt.IsNil)
	report := runner.GetReport()
	c.Assert(report.TotalTests, qt.Equals, 1)
	c.Assert(report.PassedTests, qt.Equals, 1)
	c.Assert(report.FailedTests, qt.Equals, 0)
	c.Assert(report.SkippedTests, qt.Equals, 0)
	c.Assert(report.Summary, qt.Contains, "Executed 1 tests")
	c.Assert(report.Summary, qt.Contains, "100.0% success rate")
}

func TestReporterTextOutputShowsSkippedAndExecutedSuccessRate(t *testing.T) {
	c := qt.New(t)
	now := time.Now()
	report := &integrationharness.TestReport{
		StartTime:    now,
		EndTime:      now.Add(time.Second),
		TotalTests:   3,
		PassedTests:  1,
		FailedTests:  0,
		SkippedTests: 2,
		Summary:      "Executed 1 tests in 1s. 1 passed, 0 failed, 2 skipped (100.0% success rate)",
		Results: []integrationharness.TestResult{
			{Name: "runs_postgres", Database: "postgres", Success: true},
			{Name: "skips_clickhouse", Database: "clickhouse", Skipped: true, SkipReason: "not compatible"},
		},
	}
	outputDir := c.TempDir()
	c.Assert(integrationharness.NewReporter(report).GenerateReport(integrationharness.FormatTXT, outputDir), qt.IsNil)
	files, err := filepath.Glob(filepath.Join(outputDir, "*-report.txt"))
	c.Assert(err, qt.IsNil)
	c.Assert(files, qt.HasLen, 1)
	content, err := os.ReadFile(files[0])
	c.Assert(err, qt.IsNil)
	out := string(content)
	c.Assert(out, qt.Contains, "Total Tests: 3")
	c.Assert(out, qt.Contains, "Passed: 1")
	c.Assert(out, qt.Contains, "Failed: 0")
	c.Assert(out, qt.Contains, "Skipped: 2")
	c.Assert(out, qt.Contains, "Success Rate: 100.0%")
	c.Assert(out, qt.Contains, "SKIP skips_clickhouse")
	c.Assert(out, qt.Contains, "Skip: not compatible")
}

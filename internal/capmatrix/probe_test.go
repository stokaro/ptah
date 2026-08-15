package capmatrix_test

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/internal/capmatrix"
	"go.5x5.cz/ptah/internal/integrationharness"
)

// writeSuiteReport writes the JSON report the integration runner produces,
// under the timestamped name the runner gives it.
func writeSuiteReport(c *qt.C, dir string, report integrationharness.TestReport) {
	c.Helper()

	body, err := json.Marshal(report)
	c.Assert(err, qt.IsNil)
	c.Assert(os.MkdirAll(dir, 0o755), qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(dir, "20260812-030000-report.json"), body, 0o600), qt.IsNil)
}

// TestRecordSuite_HappyPath folds a clean suite run into a probed cell.
func TestRecordSuite_HappyPath(t *testing.T) {
	c := qt.New(t)

	dir := c.TempDir()
	writeSuiteReport(c, dir, integrationharness.TestReport{TotalTests: 40, PassedTests: 38, SkippedTests: 2})

	recorded, err := capmatrix.RecordSuite(capmatrix.CellResult{
		Cell: "postgres-17", Probe: capmatrix.ProbeOutcome{OK: true},
	}, 0, dir)

	c.Assert(err, qt.IsNil)
	c.Assert(recorded.Suite, qt.IsNotNil)
	c.Assert(recorded.Suite.OK, qt.IsTrue)
	c.Assert(recorded.Suite.Passed, qt.Equals, 38)
	c.Assert(recorded.Verdict(), qt.Equals, capmatrix.Passed)
}

// TestRecordSuite_FailurePath covers the ways a tier 3 suite half fails,
// including the one an exit code cannot see.
//
// The all-skipped row is that one. The integration runner exits 0 when every
// requested scenario skipped — it prints "All requested tests were skipped" and
// returns success — so a cell whose URL never reached the runner would
// otherwise be recorded as a nightly pass having executed no test at all.
func TestRecordSuite_FailurePath(t *testing.T) {
	c := qt.New(t)

	for _, tc := range []struct {
		name     string
		exitCode int
		report   integrationharness.TestReport
		write    func(c *qt.C, dir string, report integrationharness.TestReport)
		expect   string
	}{{
		name:     "the runner exited non-zero",
		exitCode: 1,
		report:   integrationharness.TestReport{TotalTests: 40, PassedTests: 33, FailedTests: 7},
		write:    writeSuiteReport,
		expect:   "the integration suite exited 1 with 7 failures out of 40 tests",
	}, {
		name:     "the runner exited zero and reported failures anyway",
		exitCode: 0,
		report:   integrationharness.TestReport{TotalTests: 40, PassedTests: 39, FailedTests: 1},
		write:    writeSuiteReport,
		expect:   "the integration suite exited 0 and reported 1 failures out of 40 tests",
	}, {
		name:     "every scenario skipped, which the runner calls success",
		exitCode: 0,
		report:   integrationharness.TestReport{TotalTests: 12, SkippedTests: 12},
		write:    writeSuiteReport,
		expect:   "the integration suite executed no test at all \\(12 skipped of 12\\); .*",
	}, {
		name:     "the runner wrote no report, so nothing counted what ran",
		exitCode: 0,
		report:   integrationharness.TestReport{},
		write: func(c *qt.C, dir string, _ integrationharness.TestReport) {
			c.Assert(os.MkdirAll(dir, 0o755), qt.IsNil)
		},
		expect: "expected exactly one \\*-report.json under .* and found 0, .*",
	}} {
		c.Run(tc.name, func(c *qt.C) {
			dir := filepath.Join(c.TempDir(), "reports")
			tc.write(c, dir, tc.report)

			recorded, err := capmatrix.RecordSuite(capmatrix.CellResult{
				Cell: "postgres-17", Probe: capmatrix.ProbeOutcome{OK: true},
			}, tc.exitCode, dir)

			c.Assert(err, qt.IsNil)
			c.Assert(recorded.Suite.OK, qt.IsFalse)
			c.Assert(recorded.Suite.Error, qt.Matches, tc.expect)
			c.Assert(recorded.Verdict(), qt.Equals, capmatrix.SuiteFailure)
			c.Assert(recorded.Reasons(), qt.Contains, recorded.Suite.Error)
		})
	}
}

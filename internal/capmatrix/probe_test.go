package capmatrix_test

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/internal/capmatrix"
	"go.5x5.cz/ptah/internal/integrationharness"
)

// writeSuiteReports writes the JSON reports the integration runner produces,
// under the timestamped names the runner gives them.
//
// The directory is created whether or not a report lands in it, because a run
// that wrote none still leaves the directory behind -- which is the state the
// missing-report row measures, and it is not the same state as a directory that
// was never created.
func writeSuiteReports(c *qt.C, dir string, reports ...integrationharness.TestReport) {
	c.Helper()

	c.Assert(os.MkdirAll(dir, 0o755), qt.IsNil)
	for i, report := range reports {
		body, err := json.Marshal(report)
		c.Assert(err, qt.IsNil)
		name := fmt.Sprintf("20260812-0300%02d-report.json", i)
		c.Assert(os.WriteFile(filepath.Join(dir, name), body, 0o600), qt.IsNil)
	}
}

// TestRecordSuite_HappyPath folds a clean suite run into a probed cell.
func TestRecordSuite_HappyPath(t *testing.T) {
	c := qt.New(t)

	dir := c.TempDir()
	writeSuiteReports(c, dir, integrationharness.TestReport{TotalTests: 40, PassedTests: 38, SkippedTests: 2})

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
	for _, tc := range []struct {
		name     string
		exitCode int
		// reports is what the runner left in the directory. The last row leaves
		// it empty, which is the one shape no exit code and no report field can
		// stand in for.
		reports []integrationharness.TestReport
		expect  string
	}{{
		name:     "the runner exited non-zero",
		exitCode: 1,
		reports:  []integrationharness.TestReport{{TotalTests: 40, PassedTests: 33, FailedTests: 7}},
		expect:   "the integration suite exited 1 with 7 failures out of 40 tests",
	}, {
		name:     "the runner exited zero and reported failures anyway",
		exitCode: 0,
		reports:  []integrationharness.TestReport{{TotalTests: 40, PassedTests: 39, FailedTests: 1}},
		expect:   "the integration suite exited 0 and reported 1 failures out of 40 tests",
	}, {
		name:     "every scenario skipped, which the runner calls success",
		exitCode: 0,
		reports:  []integrationharness.TestReport{{TotalTests: 12, SkippedTests: 12}},
		expect:   "the integration suite executed no test at all \\(12 skipped of 12\\); .*",
	}, {
		name:     "the runner wrote no report, so nothing counted what ran",
		exitCode: 0,
		expect:   "expected exactly one \\*-report.json under .* and found 0, .*",
	}} {
		t.Run(tc.name, func(t *testing.T) {
			c := qt.New(t)

			dir := filepath.Join(c.TempDir(), "reports")
			writeSuiteReports(c, dir, tc.reports...)

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

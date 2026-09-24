package lint_test

import (
	"encoding/json"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/cli/internal/exitcode"
)

// TestRunLint_AFailingReportStaysOnStdout is the contract a CI step relies
// on: redirect stdout to a file, and the file holds the report whatever the
// outcome, while the exit code carries the verdict.
//
// A failing run is the one that matters. It is the run with findings, so a
// report that moves to another stream on failure leaves the file holding the
// unmet-input notice on exactly the run it exists to publish -- which is what
// the GitHub Action showed as "Lint JSON (unavailable)" (stokaro/ptah#3500).
func TestRunLint_AFailingReportStaysOnStdout(t *testing.T) {
	c := qt.New(t)

	stdout, stderr, err := execute("--dir", "testdata/bad", "--format", "json")

	// The fixture carries DS errors, so this is the failing report.
	c.Assert(exitcode.Code(err, 0), qt.Equals, 1)
	var report map[string]any
	c.Assert(json.Unmarshal([]byte(stdout), &report), qt.IsNil,
		qt.Commentf("stdout is not a decodable document:\n%s", stdout))
	c.Assert(report["failed"], qt.Equals, true)
	c.Assert(stderr, qt.Contains, "DS110P",
		qt.Commentf("the unmet-input notice is not on stderr"))
	c.Assert(stderr, qt.Not(qt.Contains), `"findings"`)
}

// TestRunLint_ThePassingReportKeepsTheNoticeOffStdout is the other half: the
// stream does not depend on the outcome in either direction.
func TestRunLint_ThePassingReportKeepsTheNoticeOffStdout(t *testing.T) {
	c := qt.New(t)
	dir := c.TempDir()
	writeLintTestFile(c, dir, "0000000001_drop.up.sql",
		"ALTER TABLE users DROP COLUMN email;\n")

	// DS102 is the error the same statement raises, and disabling it is what
	// leaves a passing report that still has a rule asking for the baseline.
	stdout, stderr, err := execute("--dir", dir, "--format", "json", "--disable", "DS102")

	c.Assert(err, qt.IsNil)
	var report map[string]any
	c.Assert(json.Unmarshal([]byte(stdout), &report), qt.IsNil,
		qt.Commentf("stdout is not a decodable document:\n%s", stdout))
	c.Assert(stderr, qt.Contains, "DS110P",
		qt.Commentf("the unmet-input notice is not on stderr"))
}

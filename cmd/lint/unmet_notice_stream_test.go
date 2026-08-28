package lint_test

import (
	"encoding/json"
	"testing"

	qt "github.com/frankban/quicktest"
)

// TestRunLint_TheNoticeNeverSharesAStreamWithTheReport is the property the
// notice's placement rests on.
//
// A failing report goes to stderr and a passing one to stdout, so a fixed
// stream for the notice puts prose inside the JSON document half the time. That
// is not hypothetical: it happened the moment a rule common enough to fire on
// an ordinary migration declared the baseline input, and `--format json` on a
// failing directory stopped decoding (stokaro/ptah#1632, stokaro/ptah#2394).
func TestRunLint_TheNoticeNeverSharesAStreamWithTheReport(t *testing.T) {
	c := qt.New(t)

	stdout, stderr, err := execute("--dir", "testdata/bad", "--format", "json")

	// The fixture carries DS errors, so the report is the failing one and takes
	// stderr.
	c.Assert(err, qt.IsNotNil)
	var report map[string]any
	c.Assert(json.Unmarshal([]byte(stderr), &report), qt.IsNil,
		qt.Commentf("stderr is not a decodable document:\n%s", stderr))
	c.Assert(stdout, qt.Contains, "DS110P",
		qt.Commentf("the unmet-input notice is not on the other stream"))
}

// TestRunLint_ThePassingReportKeepsTheNoticeOffStdout is the other half.
//
// A passing report takes stdout, so the notice has to take stderr. Without this
// row a rule that always wrote the notice to stdout would satisfy the test
// above and put prose into the document of every clean run.
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
		qt.Commentf("the unmet-input notice is not on the other stream"))
}

package docs_test

// The CI page publishes recipes readers paste into their own pipelines, and a
// broken one fails in their repository rather than in this one. Two shapes have
// shipped and neither had a gate: a redirect that captures an empty file, and a
// pipe that swallows the exit status (stokaro/ptah#3122).
//
// It reaches out of its own directory for the page the same way readme_test.go
// reaches for `../README.md`: one named repository file that no Go package sits
// beside.

import (
	"os"
	"regexp"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"
)

const ciPagePath = "site/src/content/docs/testing/ci.md"

// lintRedirect matches a lint invocation whose report is redirected to a file.
// The command may be wrapped across lines by YAML, so the whole page is read as
// one string with newlines folded to spaces first.
var lintRedirect = regexp.MustCompile(`ptah migrations lint[^>|\n]*>[^\n]*`)

// lintPipe matches a lint invocation whose output is piped.
var lintPipe = regexp.MustCompile(`ptah migrations lint[^>|\n]*\|[^\n]*`)

// fencedBlock matches one fenced code block's body.
//
// Only the fenced blocks are read: a table cell separator is also a `|`, and a
// command quoted inline in prose is a reference rather than a recipe anyone
// runs, so both would be false findings.
var fencedBlock = regexp.MustCompile("(?s)```[a-z]*\n(.*?)```")

// ciPageCommands returns the page's fenced code blocks with YAML line folding
// undone, so a command split across lines reads as the one command a shell
// runs.
func ciPageCommands(c *qt.C) string {
	body, err := os.ReadFile(ciPagePath)
	c.Assert(err, qt.IsNil)

	blocks := make([]string, 0, 8)
	for _, match := range fencedBlock.FindAllStringSubmatch(string(body), -1) {
		blocks = append(blocks, match[1])
	}
	folded := strings.Join(blocks, "\n")
	folded = strings.ReplaceAll(folded, "\\\n", " ")
	folded = strings.ReplaceAll(folded, "\n      ", " ")
	folded = strings.ReplaceAll(folded, "\n          ", " ")
	return folded
}

// TestCIPage_EveryRedirectedLintSuppressesItsExitCode pins the property that
// makes a redirected report non-empty.
//
// Above the threshold the report goes to stderr and the command exits 1, so a
// recipe that redirects stdout and relies on the exit code publishes a 0-byte
// artifact on exactly the run it exists to report. `--fail-on none` moves the
// report to stdout; a second run at the real threshold is what fails the step.
func TestCIPage_EveryRedirectedLintSuppressesItsExitCode(t *testing.T) {
	c := qt.New(t)

	redirects := lintRedirect.FindAllString(ciPageCommands(c), -1)

	// A floor as well as a property: a page that stopped publishing any recipe
	// would satisfy an empty comparison.
	c.Assert(len(redirects) >= 4, qt.IsTrue,
		qt.Commentf("the page redirects %d lint reports, too few to be its recipes", len(redirects)))
	for _, redirect := range redirects {
		c.Assert(redirect, qt.Contains, "--fail-on none",
			qt.Commentf("this recipe captures an empty file on the run it exists to report:\n%s", redirect))
	}
}

// TestCIPage_NoLintReportIsPiped is the other shape.
//
// A pipeline's exit status is its last stage's, so `ptah ... | tee report.json`
// reports tee and the step passes with findings outstanding.
func TestCIPage_NoLintReportIsPiped(t *testing.T) {
	c := qt.New(t)

	pipes := lintPipe.FindAllString(ciPageCommands(c), -1)

	c.Assert(pipes, qt.HasLen, 0,
		qt.Commentf("a piped lint reports the last stage's status, not ptah's:\n%s",
			strings.Join(pipes, "\n")))
}

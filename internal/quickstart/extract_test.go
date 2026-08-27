package quickstart_test

import (
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/internal/quickstart"
)

// optedInPage is the fixture both this file and the runner tests read. It
// carries two shells, one file, and one expectation on each stream.
const optedInPage = "testdata/pages/start/opted-in.mdx"

// loadPage extracts one fixture page and fails the test if the page is refused
// or turns out not to be opted in.
func loadPage(c *qt.C, path string) *quickstart.Page {
	source, err := os.ReadFile(path)
	c.Assert(err, qt.IsNil)

	page, err := quickstart.Extract(path, source)
	c.Assert(err, qt.IsNil)
	c.Assert(page, qt.IsNotNil)
	return page
}

// program returns one shell's program, having asserted the page publishes one.
func program(c *qt.C, page *quickstart.Page, shell quickstart.Shell) *quickstart.Program {
	found, ok := page.Program(shell)
	c.Assert(ok, qt.IsTrue)
	return found
}

// TestExtract_HappyPath pins what one page yields per shell.
//
// The counts are the point: a tab set assigns its blocks to one shell each, and
// the file and the two output blocks outside any tab belong to both. An
// extractor that ignored tabs would give one shell every step.
func TestExtract_HappyPath(t *testing.T) {
	tests := []struct {
		name             string
		shell            quickstart.Shell
		wantSteps        int
		wantExpectations int
		wantFirstStep    string
	}{
		{
			name:             "bash",
			shell:            quickstart.Bash,
			wantSteps:        3,
			wantExpectations: 2,
			wantFirstStep:    "mkdir work && cd work",
		},
		{
			name:             "powershell",
			shell:            quickstart.PowerShell,
			wantSteps:        3,
			wantExpectations: 2,
			wantFirstStep:    "New-Item -ItemType Directory work | Out-Null\nSet-Location work",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			page := loadPage(c, optedInPage)
			found := program(c, page, test.shell)

			c.Assert(found.Steps(), qt.Equals, test.wantSteps)
			c.Assert(found.Expectations(), qt.Equals, test.wantExpectations)
			c.Assert(found.Actions[0].Kind, qt.Equals, quickstart.ActionStep)
			c.Assert(found.Actions[0].Body, qt.Equals, test.wantFirstStep)
			c.Assert(found.Actions[0].Number, qt.Equals, 1)
		})
	}
}

// TestExtract_FileBlockBecomesAFileWrite pins the one shape that reaches the
// disk without appearing in any command block: the sql block whose introduction
// names a path.
func TestExtract_FileBlockBecomesAFileWrite(t *testing.T) {
	tests := []struct {
		name  string
		shell quickstart.Shell
	}{
		{name: "bash", shell: quickstart.Bash},
		{name: "powershell", shell: quickstart.PowerShell},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			page := loadPage(c, optedInPage)
			found := program(c, page, test.shell)

			c.Assert(found.Actions[1].Kind, qt.Equals, quickstart.ActionFile)
			c.Assert(found.Actions[1].Path, qt.Equals, "schema.sql")
			c.Assert(found.Actions[1].Body, qt.Equals, "CREATE TABLE users (id INTEGER PRIMARY KEY);")
			c.Assert(found.Actions[1].Number, qt.Equals, 0)
		})
	}
}

// TestExtract_StreamComesFromTheIntroducingSentence pins the tie between the
// sentence the reader is shown and the stream the runner asserts against. They
// are one mechanism on purpose: a page that stops saying which stream stops
// being checkable, and says so.
func TestExtract_StreamComesFromTheIntroducingSentence(t *testing.T) {
	tests := []struct {
		name       string
		stepNumber int
		wantStream quickstart.Stream
		wantLine   string
	}{
		{name: "standard output", stepNumber: 2, wantStream: quickstart.Stdout, wantLine: "CREATE TABLE users (id INTEGER PRIMARY KEY);"},
		{name: "standard error", stepNumber: 3, wantStream: quickstart.Stderr, wantLine: "schema.sql"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			page := loadPage(c, optedInPage)
			found := program(c, page, quickstart.Bash)
			action := stepNumbered(c, found, test.stepNumber)

			c.Assert(action.Expectations, qt.HasLen, 1)
			c.Assert(action.Expectations[0].Stream, qt.Equals, test.wantStream)
			c.Assert(action.Expectations[0].Lines, qt.DeepEquals, []string{test.wantLine})
		})
	}
}

// TestExtract_PageWithoutTheOptInKeyIsLeftAlone keeps the runner off every page
// that happens to carry a command block.
func TestExtract_PageWithoutTheOptInKeyIsLeftAlone(t *testing.T) {
	c := qt.New(t)

	path := "testdata/pages/start/not-opted-in.md"
	source, err := os.ReadFile(path)
	c.Assert(err, qt.IsNil)

	page, err := quickstart.Extract(path, source)
	c.Assert(err, qt.IsNil)
	c.Assert(page, qt.IsNil)
}

// TestExtract_FailurePath is the set of page shapes that would otherwise run
// nothing, or check nothing, without saying so.
func TestExtract_FailurePath(t *testing.T) {
	tests := []struct {
		name    string
		file    string
		wantErr string
	}{
		{
			name:    "an output block that names no stream",
			file:    "stream-missing.mdx",
			wantErr: `.*:13: an output block that names no stream cannot be asserted: end its introduction with "on standard output:" or "on standard error:"`,
		},
		{
			name:    "an output block with no command above it",
			file:    "orphan-expectation.mdx",
			wantErr: `.*:9: an output block with no command block above it in the same tab asserts nothing`,
		},
		{
			name:    "an sql block that names no file",
			file:    "sql-without-path.mdx",
			wantErr: `.*:9: an sql block names no file: put the path in a code span in the sentence above it, or label a block that is not written to disk with a language other than "sql"`,
		},
		{
			name:    "a Bash block in the Windows tab",
			file:    "bash-in-windows-tab.mdx",
			wantErr: `.*:12: a bash block sits in the tab labeled "Windows PowerShell", which selects the other shell; move it to the tab for its own shell`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			path := filepath.Join("testdata", "broken", test.file)
			source, err := os.ReadFile(path)
			c.Assert(err, qt.IsNil)

			page, err := quickstart.Extract(path, source)
			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(page, qt.IsNil)
		})
	}
}

// stepNumbered returns the action carrying one step number.
func stepNumbered(c *qt.C, found *quickstart.Program, number int) quickstart.Action {
	for _, action := range found.Actions {
		if action.Kind == quickstart.ActionStep && action.Number == number {
			return action
		}
	}
	c.Fatalf("the program has no step %d", number)
	return quickstart.Action{}
}

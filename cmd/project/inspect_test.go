package project_test

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"slices"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/cmd/project"
)

// projectFile writes an atlas.hcl and returns its path.
func projectFile(c *qt.C, document string) string {
	c.Helper()

	path := filepath.Join(c.TempDir(), "atlas.hcl")
	c.Assert(os.WriteFile(path, []byte(document), 0o600), qt.IsNil)
	return path
}

// runInspect executes the verb and returns stdout, stderr and the error.
func runInspect(c *qt.C, args ...string) (stdout, stderr string, err error) {
	c.Helper()

	cmd := project.NewProjectCommand()
	var out, errOut bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&errOut)
	cmd.SetArgs(args)
	err = cmd.Execute()
	return out.String(), errOut.String(), err
}

// The verb reports what Ptah acts on.
func TestProjectInspect_ReportsWhatIsCarried(t *testing.T) {
	c := qt.New(t)
	path := projectFile(c, `
env "local" {
  url     = "postgres://localhost:5432/app?sslmode=disable"
  dev     = "docker://postgres/17/dev"
  schemas = ["public", "app"]
}
`)

	stdout, _, err := runInspect(c, "inspect", "--atlas-config", path, "--env", "local")

	c.Assert(err, qt.IsNil)
	c.Assert(stdout, qt.Contains, "Environment: local")
	c.Assert(stdout, qt.Contains, "postgres://localhost:5432/app")
	c.Assert(stdout, qt.Contains, "docker://postgres/17/dev")
	c.Assert(stdout, qt.Contains, "public, app")
}

// A setting the file does not carry is named as absent rather than omitted.
//
// A list of only what happens to be present answers "what did I write". The
// operator's question is "what does Ptah do with this project", and an unset
// dev database is part of that answer.
func TestProjectInspect_NamesTheSettingsTheFileDoesNotSet(t *testing.T) {
	c := qt.New(t)
	path := projectFile(c, `
env "local" {
  url = "postgres://localhost:5432/app?sslmode=disable"
}
`)

	stdout, _, err := runInspect(c, "inspect", "--atlas-config", path, "--env", "local")

	c.Assert(err, qt.IsNil)
	c.Assert(stdout, qt.Contains, "dev url          (not set)")
	c.Assert(stdout, qt.Contains, "exclude          (not set)")
}

// A name the file declares and nothing acts on is reported with its position.
//
// This is the half that cannot be obtained any other way: Atlas CE reports
// nothing for a name it does not know, so a setting that silently does nothing
// is indistinguishable from one that works.
func TestProjectInspect_NamesWhatItReadAndIgnored(t *testing.T) {
	c := qt.New(t)
	path := projectFile(c, `
env "local" {
  url        = "postgres://localhost:5432/app?sslmode=disable"
  frobnicate = "this does nothing"
}
`)

	stdout, _, err := runInspect(c, "inspect", "--atlas-config", path, "--env", "local")

	c.Assert(err, qt.IsNil)
	c.Assert(stdout, qt.Contains, `attribute "frobnicate"`)
	c.Assert(stdout, qt.Contains, "atlas.hcl:4")
	c.Assert(stdout, qt.Contains, "Read and ignored (1)")
}

// A file with nothing ignored says so, rather than printing an empty heading.
//
// The control for the test above: a verb that always printed the section would
// pass it while saying nothing about whether the list is real.
func TestProjectInspect_SaysWhenNothingWasIgnored(t *testing.T) {
	c := qt.New(t)
	path := projectFile(c, `
env "local" {
  url = "postgres://localhost:5432/app?sslmode=disable"
}
`)

	stdout, _, err := runInspect(c, "inspect", "--atlas-config", path, "--env", "local")

	c.Assert(err, qt.IsNil)
	c.Assert(stdout, qt.Contains, "Read and ignored: nothing.")
	c.Assert(stdout, qt.Not(qt.Contains), "Read and ignored (")
}

// The JSON form carries the same answer, for a caller that is not a reader.
func TestProjectInspect_JSONCarriesTheSameAnswer(t *testing.T) {
	c := qt.New(t)
	path := projectFile(c, `
env "local" {
  url        = "postgres://localhost:5432/app?sslmode=disable"
  frobnicate = "nothing"
}
`)

	stdout, _, err := runInspect(c, "inspect", "--atlas-config", path, "--env", "local", "--format", "json")

	c.Assert(err, qt.IsNil)
	var report project.Report
	c.Assert(json.Unmarshal([]byte(stdout), &report), qt.IsNil)
	c.Assert(report.Env, qt.Equals, "local")
	c.Assert(report.Ignored, qt.HasLen, 1)
	c.Assert(report.Ignored[0].Name, qt.Equals, "frobnicate")
	c.Assert(report.Ignored[0].Line, qt.Equals, 4)

	carried := make(map[string]string, len(report.Carried))
	for _, setting := range report.Carried {
		carried[setting.Name] = setting.Value
	}
	c.Assert(carried["database url"], qt.Contains, "postgres://")
	c.Assert(carried["dev url"], qt.Equals, "")
}

// The ignored list is reported in ascending position.
//
// A reader follows a file top to bottom, and a list that jumped around would
// make them hunt. The parser happens to yield file order today, so the sort is
// a no-op on this input -- which is exactly why the property is asserted here
// rather than assumed: a reversal of the list passes every other test in this
// file, and would pass review as a refactor.
func TestProjectInspect_TheIgnoredListIsInAscendingPosition(t *testing.T) {
	c := qt.New(t)
	path := projectFile(c, `
env "local" {
  url   = "postgres://localhost:5432/app?sslmode=disable"
  zulu  = "nothing"
  alpha = "nothing"
  mike  = "nothing"
  bravo = "nothing"
}
`)

	stdout, _, err := runInspect(c, "inspect", "--atlas-config", path, "--env", "local", "--format", "json")
	c.Assert(err, qt.IsNil)

	var report project.Report
	c.Assert(json.Unmarshal([]byte(stdout), &report), qt.IsNil)
	c.Assert(len(report.Ignored) > 1, qt.IsTrue,
		qt.Commentf("one entry cannot be out of order, so this would pass vacuously"))

	lines := make([]int, 0, len(report.Ignored))
	for _, ignored := range report.Ignored {
		lines = append(lines, ignored.Line)
	}
	c.Assert(slices.IsSorted(lines), qt.IsTrue, qt.Commentf("lines were %v", lines))
	// The names in that order, so the assertion is about which entry is where
	// rather than only about the numbers being non-decreasing.
	names := make([]string, 0, len(report.Ignored))
	for _, ignored := range report.Ignored {
		names = append(names, ignored.Name)
	}
	c.Assert(names, qt.DeepEquals, []string{"zulu", "alpha", "mike", "bravo"})
}

// Two runs over one file produce the same report.
//
// The ignored list is sorted by position rather than left in read order, so a
// diff between two runs means the file changed rather than that a map was
// walked twice.
func TestProjectInspect_IsStableAcrossRuns(t *testing.T) {
	c := qt.New(t)
	path := projectFile(c, `
env "local" {
  url    = "postgres://localhost:5432/app?sslmode=disable"
  zulu   = "nothing"
  alpha  = "nothing"
  mike   = "nothing"
  bravo  = "nothing"
}
`)

	first, _, err := runInspect(c, "inspect", "--atlas-config", path, "--env", "local", "--format", "json")
	c.Assert(err, qt.IsNil)
	for range 8 {
		next, _, err := runInspect(c, "inspect", "--atlas-config", path, "--env", "local", "--format", "json")
		c.Assert(err, qt.IsNil)
		c.Assert(next, qt.Equals, first)
	}
}

// An unsupported format is refused rather than silently defaulting.
func TestProjectInspect_RefusesAnUnknownFormat(t *testing.T) {
	c := qt.New(t)
	path := projectFile(c, `
env "local" {
  url = "postgres://localhost:5432/app?sslmode=disable"
}
`)

	_, _, err := runInspect(c, "inspect", "--atlas-config", path, "--env", "local", "--format", "yaml")

	c.Assert(err, qt.ErrorMatches, `.*unsupported --format "yaml".*`)
}

package quickstart_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/quickstart"
)

// TestSequences_HappyPath pins the shapes a journey can take.
//
// A page nobody continues to, and that continues nowhere, is a journey of one:
// the counts and the path are the page's own, so every existing quick start
// keeps running exactly as it ran. A declared continuation joins the pages and
// renumbers the steps, because a sentinel repeated between pages would make the
// transcript unreadable to the checker.
func TestSequences_HappyPath(t *testing.T) {
	tests := []struct {
		name       string
		pages      map[string]string
		wantNames  []string
		wantSteps  []int
		wantShells int
	}{
		{
			name:       "a page that declares nothing is a journey of one",
			pages:      map[string]string{"a.mdx": page("", "one")},
			wantNames:  []string{"a.mdx"},
			wantSteps:  []int{1},
			wantShells: 2,
		},
		{
			name: "a declared continuation joins the pages",
			pages: map[string]string{
				"a.mdx": page("b.mdx", "one"),
				"b.mdx": page("", "two"),
			},
			wantNames:  []string{"a.mdx -> b.mdx"},
			wantSteps:  []int{2},
			wantShells: 2,
		},
		{
			name: "a branch becomes one journey per path",
			pages: map[string]string{
				"a.mdx": page("b.mdx, c.mdx", "one"),
				"b.mdx": page("", "two"),
				"c.mdx": page("", "three"),
			},
			wantNames:  []string{"a.mdx -> b.mdx", "a.mdx -> c.mdx"},
			wantSteps:  []int{2, 2},
			wantShells: 2,
		},
		{
			name: "a chain of three runs in order",
			pages: map[string]string{
				"a.mdx": page("b.mdx", "one"),
				"b.mdx": page("c.mdx", "two"),
				"c.mdx": page("", "three"),
			},
			wantNames:  []string{"a.mdx -> b.mdx -> c.mdx"},
			wantSteps:  []int{3},
			wantShells: 2,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			journeys, err := quickstart.DiscoverJourneys(writePages(c, test.pages))

			c.Assert(err, qt.IsNil)
			c.Assert(names(c, journeys), qt.DeepEquals, test.wantNames)
			c.Assert(steps(c, journeys), qt.DeepEquals, test.wantSteps)
			c.Assert(journeys[0].ShellsPresent(), qt.HasLen, test.wantShells)
		})
	}
}

// TestSequences_FailurePath refuses a declaration nothing can act on.
//
// Each of these would otherwise remove pages from the run rather than report:
// an unresolved target drops the continuation, a cycle has no head so every
// page in it disappears, and two predecessors make the journey depend on which
// one the walk reached first.
func TestSequences_FailurePath(t *testing.T) {
	tests := []struct {
		name    string
		pages   map[string]string
		wantErr string
	}{
		{
			name:    "a target this runner does not read",
			pages:   map[string]string{"a.mdx": page("missing.mdx", "one")},
			wantErr: `.*a\.mdx: quickstartContinues names "missing\.mdx", which is not a page this runner reads.*`,
		},
		{
			name:    "a page naming itself",
			pages:   map[string]string{"a.mdx": page("a.mdx", "one")},
			wantErr: `.*a\.mdx: quickstartContinues names the page itself`,
		},
		{
			name: "a cycle",
			pages: map[string]string{
				"a.mdx": page("b.mdx", "one"),
				"b.mdx": page("a.mdx", "two"),
			},
			wantErr: `.*a\.mdx, .*b\.mdx: no journey reaches these pages, so nothing would run them.*`,
		},
		{
			// The shape that looks like a second kind of cycle and is not: an
			// edge into a loop is a second predecessor, so the rule below
			// refuses it before any walk happens. Recorded because it is the
			// obvious next test to write, and writing it measures the
			// predecessor rule rather than a loop.
			name: "an edge into a loop is a second predecessor",
			pages: map[string]string{
				"a.mdx": page("b.mdx", "one"),
				"b.mdx": page("c.mdx", "two"),
				"c.mdx": page("b.mdx", "three"),
			},
			wantErr: `.*both continue to b\.mdx; a page has one predecessor`,
		},
		{
			name: "two pages continuing to one",
			pages: map[string]string{
				"a.mdx": page("c.mdx", "one"),
				"b.mdx": page("c.mdx", "two"),
				"c.mdx": page("", "three"),
			},
			wantErr: `.*both continue to c\.mdx; a page has one predecessor`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			journeys, err := quickstart.DiscoverJourneys(writePages(c, test.pages))

			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(journeys, qt.IsNil)
		})
	}
}

// page renders a minimal opted-in page with one shell-neutral step.
func page(continues, echo string) string {
	front := "---\ntitle: A page\nquickstart: true\n"
	if continues != "" {
		front += "quickstartContinues: " + continues + "\n"
	}
	return front + "---\n\nRun it:\n\n```console\necho " + echo + "\n```\n"
}

// writePages lays the pages out under a scratch documentation root and returns
// it.
func writePages(c *qt.C, pages map[string]string) string {
	c.Helper()
	root := filepath.Join(c.TempDir(), "docs")
	c.Assert(os.MkdirAll(root, 0o750), qt.IsNil)
	for name, source := range pages {
		c.Assert(os.WriteFile(filepath.Join(root, name), []byte(source), 0o600), qt.IsNil)
	}
	return root
}

// names lists the journeys as a report would, with the scratch root trimmed so
// a row carries the page names rather than a temporary directory.
func names(c *qt.C, journeys []*quickstart.Page) []string {
	c.Helper()
	out := make([]string, 0, len(journeys))
	for _, journey := range journeys {
		out = append(out, trimRoot(journey.Path))
	}
	return out
}

func steps(c *qt.C, journeys []*quickstart.Page) []int {
	c.Helper()
	out := make([]int, 0, len(journeys))
	for _, journey := range journeys {
		program, ok := journey.Program(quickstart.Bash)
		c.Assert(ok, qt.IsTrue)
		out = append(out, program.Steps())
	}
	return out
}

// trimRoot reduces every path in a journey name to its file name, so a row
// carries the pages rather than the scratch directory they were written to.
func trimRoot(name string) string {
	parts := strings.Split(name, " -> ")
	for i, part := range parts {
		parts[i] = filepath.Base(part)
	}
	return strings.Join(parts, " -> ")
}

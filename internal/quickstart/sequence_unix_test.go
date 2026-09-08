//go:build !windows

package quickstart_test

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/quickstart"
)

// journeyFixture holds the pair that stands in for a quick start and its
// continuation: the first page builds a working directory and stays in it, the
// second opens by reading what the first wrote.
const journeyFixture = "testdata/journey"

// finishStep is the last command of the first page, and the seam the failure
// case replaces. Keeping it a constant means the happy path and the broken
// transition read the same fixture rather than two copies of it.
const finishStep = "echo finished"

// TestRun_AJourneyCrossesThePageBoundary_HappyPath is the capability
// stokaro/ptah#3018 asks for: the pages of a journey run in ONE working
// directory, in order.
//
// The second page's only command reads a file the first page created and never
// names the directory, so it can only pass if the run carried the first page's
// state and its working directory across the boundary. Three steps numbered
// straight through is the other half: a sentinel repeated between pages would
// make the transcript unreadable to the checker.
func TestRun_AJourneyCrossesThePageBoundary_HappyPath(t *testing.T) {
	c := qt.New(t)

	page := journey(c, finishStep)

	result, err := quickstart.Run(context.Background(), page, quickstart.Bash, quickstart.Options{})

	c.Assert(err, qt.IsNil)
	c.Assert(result.Failures, qt.HasLen, 0)
	c.Assert(result.Steps, qt.Equals, 3)
	c.Assert(result.Asserted, qt.Equals, 3)
}

// TestRun_AJourneyCatchesABrokenTransition_FailurePath is the defect this gate
// exists for, in the shape stokaro/ptah#2995 measured on the real pages: the
// first page ends by deleting the working directory its continuation opens by
// assuming.
//
// Every command on each page is still correct, and a link checker still passes.
// What fails is the move between them, and it fails on the continuation's own
// step -- which is why the failure has to be attributed to the second page
// rather than to the head of the journey.
func TestRun_AJourneyCatchesABrokenTransition_FailurePath(t *testing.T) {
	c := qt.New(t)

	page := journey(c, "cd ..\nrm -rf work\n"+finishStep)

	result, err := quickstart.Run(context.Background(), page, quickstart.Bash, quickstart.Options{})

	c.Assert(err, qt.IsNil)
	c.Assert(result.Failures, qt.HasLen, 1)
	c.Assert(result.Failures[0].Page, qt.Contains, "second.mdx")
	c.Assert(result.Failures[0].Step, qt.Equals, 3)
	c.Assert(result.Failures[0].Problem, qt.Contains, "the step did not finish")
}

// journey copies the fixture pair into a scratch documentation root, with the
// first page's last command replaced by finish, and returns the one journey the
// pair assembles into.
//
// Copying rather than templating keeps one source for the fixture: the pages a
// reader of this package opens are the pages these tests run.
func journey(c *qt.C, finish string) *quickstart.Page {
	c.Helper()
	root := filepath.Join(c.TempDir(), "docs")
	c.Assert(os.MkdirAll(filepath.Join(root, "start"), 0o750), qt.IsNil)

	for _, name := range []string{"first.mdx", "second.mdx"} {
		source, err := os.ReadFile(filepath.Join(journeyFixture, "start", name))
		c.Assert(err, qt.IsNil)
		// A no-op on the second page and on the happy path, which is what lets
		// both cases read the same fixture without a branch here.
		edited := strings.Replace(string(source), finishStep, finish, 1)
		// #nosec G703 -- the destination is filepath.Join of this subtest's own
		// TempDir and a name from the constant list above; nothing from the
		// fixture's contents reaches the path.
		written := os.WriteFile(filepath.Join(root, "start", name), []byte(edited), 0o600)
		c.Assert(written, qt.IsNil)
	}

	journeys, err := quickstart.DiscoverJourneys(root)
	c.Assert(err, qt.IsNil)
	c.Assert(journeys, qt.HasLen, 1)
	return journeys[0]
}

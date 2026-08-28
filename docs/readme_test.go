package docs_test

// "Which databases does it support" is the first question a reader arrives
// with, and a hand-written answer is a claim that was true when it was written.
// So nothing here is written twice: the support matrix is held to the dialect
// set the code declares, and the README is held to reaching the matrix.
//
// The chain was longer. The README used to name every engine above the fold and
// was held to the matrix name by name, because its prose had come to name nine
// of the ten and no gate saw it -- the sentence read as a partition of the
// supported set and Oracle was in neither half. The README was later rewritten
// to link the matrix instead, so that half of the chain has no subject; a test
// still asserting it would measure nothing while staying green. What replaced
// it is the property the new shape rests on: with the names gone, the link is
// the reader's only route, so losing it costs more than it used to.
//
// It reaches out of its own directory for `../README.md`, which the embed test
// beside it deliberately does not do. That test walks the tree the patterns are
// meant to cover and can do its work from anywhere; this one is about one named
// repository file that no Go package sits beside. `internal/capabilityprobe`
// reads `../../renovate.json` and the integration workflow for the same reason.

import (
	"os"
	"regexp"
	"slices"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/platform/capability"
	"go.5x5.cz/ptah/docs"
)

const (
	readmePath       = "../README.md"
	supportMatrixDoc = "site/src/content/docs/databases/support-matrix.md"
	glanceHeading    = "## Engines at a glance"
)

// htmlLink and markdownLink read a link's visible label, which is what a reader
// scans the engine row for.
var (
	backticked = regexp.MustCompile("`([^`]*)`")
)

// supportMatrixRoute is the published page the README sends a reader to for the
// engine list. Matched as a route rather than a whole URL so the documentation
// channel in front of it can change without touching this test.
const supportMatrixRoute = "/databases/support-matrix/"

// TestReadmeRoutesToTheSupportMatrix holds what the README now claims.
//
// It used to name every engine above the fold, and this test held that list to
// the matrix. The README was rewritten to link the matrix instead, so the list
// has no subject and an assertion about it would be a tautology dressed as a
// guarantee -- the shape AGENTS.md warns about, where a check quietly stops
// having anything to measure.
//
// What survives is the property the new shape depends on. A reader asking
// "does it support my database" has exactly one route now, so the link going
// missing costs more than it did when the names were on the page. The matrix
// itself is still held to the code by the test below, which is the half of the
// chain that never depended on the README.
func TestReadmeRoutesToTheSupportMatrix(t *testing.T) {
	c := qt.New(t)

	body, err := os.ReadFile(readmePath)
	c.Assert(err, qt.IsNil)

	c.Assert(string(body), qt.Contains, supportMatrixRoute,
		qt.Commentf("the README links %s nowhere; it is the only route a reader has"+
			" to the engine list since the names left the page", supportMatrixRoute))
}

func TestSupportMatrixGlanceCoversEveryDeclaredDialect(t *testing.T) {
	c := qt.New(t)

	c.Assert(glanceDialects(c), qt.DeepEquals, capability.DefaultDialects())
}

// glanceDialects returns the normalized dialect name each glance row declares,
// sorted. The first backticked token in the dialect cell is the canonical name;
// the ones after it are aliases.
func glanceDialects(c *qt.C) []string {
	var dialects []string
	for _, cells := range glanceRows(c) {
		spellings := backticked.FindAllStringSubmatch(cells[1], -1)
		c.Assert(len(spellings) > 0, qt.IsTrue)
		dialects = append(dialects, spellings[0][1])
	}
	return sorted(dialects)
}

// glanceRows returns the body rows of the "Engines at a glance" table, read out
// of the embedded documentation rather than off disk, so the test sees what the
// binary carries.
func glanceRows(c *qt.C) [][]string {
	body, err := docs.FS.ReadFile(supportMatrixDoc)
	c.Assert(err, qt.IsNil)

	_, after, found := strings.Cut(string(body), glanceHeading)
	c.Assert(found, qt.IsTrue)
	section, _, _ := strings.Cut(after, "\n## ")

	var rows [][]string
	for line := range strings.SplitSeq(section, "\n") {
		rows = appendTableRow(rows, line)
	}

	// A section that yielded no rows would make both tests above compare an
	// empty list with an empty list and pass at the moment they stopped
	// reading anything.
	c.Assert(len(rows) > 0, qt.IsTrue)
	return rows
}

func appendTableRow(rows [][]string, line string) [][]string {
	trimmed := strings.TrimSpace(line)
	if !strings.HasPrefix(trimmed, "|") {
		return rows
	}
	cells := tableCells(trimmed)
	if len(cells) < 2 || isDelimiter(cells[0]) || cells[0] == "Engine" {
		return rows
	}
	return append(rows, cells)
}

func tableCells(row string) []string {
	parts := strings.Split(strings.Trim(row, "|"), "|")
	cells := make([]string, 0, len(parts))
	for _, part := range parts {
		cells = append(cells, strings.TrimSpace(part))
	}
	return cells
}

func isDelimiter(cell string) bool {
	return strings.Trim(cell, "-: ") == ""
}

func sorted(values []string) []string {
	out := slices.Clone(values)
	slices.Sort(out)
	return out
}

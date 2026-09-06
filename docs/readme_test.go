package docs_test

// The README names every engine Ptah supports, above the fold, because that is
// the first question a reader arrives with. A hand-written list of engines is a
// claim that was true when it was written, so nothing here is written twice:
// the README is held to the support matrix, and the support matrix is held to
// the dialect set the code declares.
//
// The chain matters in that order. Adding an engine touches the code and the
// matrix; the README is the file nobody thinks of, and it is the one everybody
// reads. This test exists because the README's own prose had come to name nine
// of the ten engines, and no gate saw it: the sentence read as a partition of
// the supported set and Oracle was in neither half.
//
// The chain was broken once by two repairs that disagreed. A rewrite dropped
// the engine row; #2447 put it back and #2448 removed the test instead, and
// both landed, leaving the row on the page with nothing holding it to the
// matrix. The link assertion below is what #2448 put in its place, and it is
// kept: it is a weaker property than the name-by-name pin, and the two do not
// overlap -- a README could link the matrix and still name nine engines.
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

	"ptah.run/core/platform/capability"
	"ptah.run/docs"
)

const (
	readmePath       = "../README.md"
	supportMatrixDoc = "site/src/content/docs/databases/support-matrix.md"
	glanceHeading    = "## Engines at a glance"
)

// centeredBlock matches one `<p align="center">` element of the README header.
var centeredBlock = regexp.MustCompile(`(?s)<p align="center">.*?</p>`)

// htmlLink and markdownLink read a link's visible label, which is what a reader
// scans the engine row for.
var (
	htmlLink     = regexp.MustCompile(`<a\s+href="([^"]*)"[^>]*>([^<]*)</a>`)
	markdownLink = regexp.MustCompile(`\[([^\]]*)\]\(([^)]*)\)`)
	backticked   = regexp.MustCompile("`([^`]*)`")
)

// databasesPrefix is the documentation area every engine link points into.
const databasesPrefix = "https://docs.ptah.run/edge/databases/"

// minimumEngineLinks separates the engine row from the other centered blocks in
// the header, which carry badges and section links. It is a floor rather than
// the exact count on purpose: this test must not have to be edited when an
// engine is added, only when the row stops being a row.
const minimumEngineLinks = 5

func TestReadmeNamesEveryEngineTheSupportMatrixLists(t *testing.T) {
	c := qt.New(t)

	c.Assert(readmeEngines(c), qt.DeepEquals, glanceEngines(c))
}

func TestSupportMatrixGlanceCoversEveryDeclaredDialect(t *testing.T) {
	c := qt.New(t)

	c.Assert(glanceDialects(c), qt.DeepEquals, capability.DefaultDialects())
}

// readmeEngines returns the engine names the README's centered engine row
// links, sorted.
func readmeEngines(c *qt.C) []string {
	body, err := os.ReadFile(readmePath)
	c.Assert(err, qt.IsNil)

	var rows [][]string
	for _, block := range centeredBlock.FindAllString(string(body), -1) {
		labels := engineLabels(block)
		rows = appendWhenRow(rows, labels)
	}

	// Exactly one row, or the rule this test states has stopped being readable
	// off the file: no row means the engine list is gone, and two mean a reader
	// meets two answers.
	c.Assert(rows, qt.HasLen, 1)
	return sorted(rows[0])
}

func engineLabels(block string) []string {
	var labels []string
	for _, match := range htmlLink.FindAllStringSubmatch(block, -1) {
		labels = appendEngineLabel(labels, match[1], match[2])
	}
	return labels
}

func appendEngineLabel(labels []string, href, label string) []string {
	if !strings.HasPrefix(href, databasesPrefix) {
		return labels
	}
	return append(labels, strings.TrimSpace(label))
}

func appendWhenRow(rows [][]string, labels []string) [][]string {
	if len(labels) < minimumEngineLinks {
		return rows
	}
	return append(rows, labels)
}

// glanceEngines returns the engine names the support matrix's "Engines at a
// glance" table lists, sorted, with any parenthetical qualifier dropped so that
// "Spanner (PostgreSQL interface)" and "Spanner" are the same engine.
func glanceEngines(c *qt.C) []string {
	var names []string
	for _, cells := range glanceRows(c) {
		names = append(names, engineName(cells[0]))
	}
	return sorted(names)
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

// engineName reads the display name out of a glance cell, which may be a link
// to the engine's own page, and drops a parenthetical qualifier.
func engineName(cell string) string {
	name := cell
	if match := markdownLink.FindStringSubmatch(cell); match != nil {
		name = match[1]
	}
	name, _, _ = strings.Cut(name, " (")
	return strings.TrimSpace(name)
}

func sorted(values []string) []string {
	out := slices.Clone(values)
	slices.Sort(out)
	return out
}

// supportMatrixRoute is the documentation route the README has to reach.
const supportMatrixRoute = "/databases/support-matrix/"

// TestReadmeRoutesToTheSupportMatrix holds the weaker half of the chain.
//
// The engine row above is name-by-name; this is the link a reader follows when
// the row does not answer their question. The two are separate properties: a
// README could link the matrix and name nine of ten engines, or name all ten
// and link nothing.
func TestReadmeRoutesToTheSupportMatrix(t *testing.T) {
	c := qt.New(t)

	body, err := os.ReadFile(readmePath)
	c.Assert(err, qt.IsNil)

	c.Assert(string(body), qt.Contains, supportMatrixRoute,
		qt.Commentf("the README links %s nowhere, so a reader whose engine is not"+
			" in the row above has no route to the answer", supportMatrixRoute))
}

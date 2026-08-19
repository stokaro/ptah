package atlasfilter

// White-box testing required: the documented table is checked against
// excludeFieldSelectors, which is unexported and has no accessor. Exporting one
// for a test would be a public API surface existing only to be read here.

import (
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"
)

// referencePage is the page whose table this test holds to the registry.
const referencePage = "../../docs/site/src/content/docs/reference/atlas-commands.md"

// TestExcludeFieldSelectors_MatchTheDocumentedSet holds the reference table and
// the registry to each other.
//
// The set is small and the refusal message names it, so a user who tries the
// second field learns the first. What they could not learn anywhere was which
// fields exist at all, or why these and not others -- the criterion was in
// nobody's head but the author's (stokaro/ptah#1710).
//
// A documented set that drifts from the code is worse than none: it answers the
// question wrongly and with authority. So adding a field to excludeFieldSelectors
// without adding the row reddens here, and so does the reverse.
func TestExcludeFieldSelectors_MatchTheDocumentedSet(t *testing.T) {
	c := qt.New(t)

	documented := documentedExcludeFields(c)
	registered := make(map[string][]string, len(excludeFieldSelectors))
	for resourceType, fields := range excludeFieldSelectors {
		names := make([]string, 0, len(fields))
		for field := range fields {
			names = append(names, field)
		}
		slices.Sort(names)
		registered[resourceType] = names
	}

	c.Assert(documented, qt.DeepEquals, registered,
		qt.Commentf("the reference table and excludeFieldSelectors disagree; update %s", referencePage))
}

// TestExcludeFieldSelectors_TheDocumentedTableIsFound is the control on the
// test above.
//
// A parser that stopped finding the table would compare two empty maps and
// pass, which is the way a documentation check quietly stops checking. This
// asserts the table was read at all, and that it carries the one row whose
// field is not `comment` -- the row that proves the parser read values rather
// than filling a default.
func TestExcludeFieldSelectors_TheDocumentedTableIsFound(t *testing.T) {
	c := qt.New(t)

	documented := documentedExcludeFields(c)

	c.Assert(len(documented) > 0, qt.IsTrue)
	c.Assert(documented["extension"], qt.DeepEquals, []string{"version"})
}

// documentedExcludeFields reads the reference table into the shape the registry
// holds.
func documentedExcludeFields(c *qt.C) map[string][]string {
	c.Helper()

	raw, err := os.ReadFile(filepath.Clean(referencePage))
	c.Assert(err, qt.IsNil)
	page := string(raw)

	const begin = "<!-- BEGIN GENERATED EXCLUDE FIELD SELECTORS -->"
	const end = "<!-- END GENERATED EXCLUDE FIELD SELECTORS -->"
	from := strings.Index(page, begin)
	to := strings.Index(page, end)
	c.Assert(from >= 0 && to > from, qt.IsTrue,
		qt.Commentf("the marked section is missing from %s", referencePage))

	documented := make(map[string][]string)
	for line := range strings.SplitSeq(page[from:to], "\n") {
		cells := tableRowCells(line)
		if len(cells) != 2 {
			continue
		}
		resourceType, ok := backtickedName(cells[0])
		if !ok {
			continue
		}
		fields := make([]string, 0, 2)
		for field := range strings.SplitSeq(cells[1], ",") {
			if name, named := backtickedName(field); named {
				fields = append(fields, name)
			}
		}
		slices.Sort(fields)
		documented[resourceType] = fields
	}
	return documented
}

// tableRowCells splits one Markdown table row into its cells, and returns
// nothing for a line that is not one.
func tableRowCells(line string) []string {
	trimmed := strings.TrimSpace(line)
	if !strings.HasPrefix(trimmed, "|") || !strings.HasSuffix(trimmed, "|") {
		return nil
	}
	cells := strings.Split(strings.Trim(trimmed, "|"), "|")
	for i, cell := range cells {
		cells[i] = strings.TrimSpace(cell)
	}
	return cells
}

// backtickedName reads a `name` cell, reporting whether the cell held one. The
// header and separator rows hold none, which is what keeps them out.
func backtickedName(cell string) (string, bool) {
	trimmed := strings.TrimSpace(cell)
	if !strings.HasPrefix(trimmed, "`") || !strings.HasSuffix(trimmed, "`") || len(trimmed) < 3 {
		return "", false
	}
	return trimmed[1 : len(trimmed)-1], true
}

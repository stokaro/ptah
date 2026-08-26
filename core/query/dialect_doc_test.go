package query_test

import (
	"os"
	"regexp"
	"slices"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/renderer"
)

// queryBuilderPage is the page whose dialect table this test holds to
// SupportedDialects.
const queryBuilderPage = "../../docs/site/src/content/docs/extend/query-builder.md"

// dialectTableHeading is the section whose first table is the one read here.
const dialectTableHeading = "## Dialect coverage"

// TestDialectCoverageDoc_ListsEverySupportedDialect holds the documented table
// and the renderer to each other.
//
// The page said SQL Server and ClickHouse were refused for months after they
// rendered, and quoted the refusal to prove it. Nothing caught that, because a
// page is only read by people and the two people who could have noticed were
// the ones who had just taught the query. So a name added to
// SupportedDialects without a row written for it reddens here, and so does the
// reverse (stokaro/ptah#941).
func TestDialectCoverageDoc_ListsEverySupportedDialect(t *testing.T) {
	c := qt.New(t)

	documented := documentedDialects(c)

	c.Assert(documented, qt.DeepEquals, slices.Sorted(slices.Values(renderer.SupportedDialects())),
		qt.Commentf("the dialect table and SupportedDialects disagree; update %s", queryBuilderPage))
}

// TestDialectCoverageDoc_TableIsFound is the control on the test above.
//
// A parser that stopped finding the table would compare two empty lists and
// report success, which is how a documentation check stops checking without
// saying so. This one asserts a name was read at all, and that it is a name no
// default could have produced.
func TestDialectCoverageDoc_TableIsFound(t *testing.T) {
	c := qt.New(t)

	documented := documentedDialects(c)

	c.Assert(len(documented) > 1, qt.IsTrue)
	c.Assert(documented, qt.Contains, "yugabytedb")
}

// dialectCell matches one backticked token in the table's first column.
var dialectCell = regexp.MustCompile("`([a-z0-9]+)`")

// documentedDialects reads the first column of the first table under the
// dialect-coverage heading.
func documentedDialects(c *qt.C) []string {
	c.Helper()

	page, err := os.ReadFile(queryBuilderPage)
	c.Assert(err, qt.IsNil)
	_, after, found := strings.Cut(string(page), dialectTableHeading)
	c.Assert(found, qt.IsTrue, qt.Commentf("heading %q not found in %s", dialectTableHeading, queryBuilderPage))
	// Bounded at the next heading, so a later table on the same page cannot
	// contribute names -- and so a table moved out of this section is a
	// failure here rather than a silent pass on rows read from elsewhere.
	section, _, _ := strings.Cut(after, "\n## ")

	names := make([]string, 0, len(renderer.SupportedDialects()))
	for line := range strings.SplitSeq(section, "\n") {
		row := strings.TrimSpace(line)
		if !strings.HasPrefix(row, "|") {
			continue
		}
		first := strings.TrimSpace(strings.Split(strings.Trim(row, "|"), "|")[0])
		for _, match := range dialectCell.FindAllStringSubmatch(first, -1) {
			names = append(names, match[1])
		}
	}
	slices.Sort(names)
	return names
}

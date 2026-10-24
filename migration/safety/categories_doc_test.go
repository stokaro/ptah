package safety_test

import (
	"os"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/difftypes"
	"ptah.run/core/schemamodel"
	"ptah.run/migration/safety"
)

// driftPage is the page whose category tables this file holds to
// ClassifySchemaDiff.
const driftPage = "../../docs/site/src/content/docs/reference/drift-findings.md"

// undocumentedCategories are the categories the page's tables leave out, each
// with the reason.
var undocumentedCategories = map[string]string{
	"table_constraints_added":   "a table's own constraint lists, which only a diff assembled by hand fills",
	"table_constraints_removed": "a table's own constraint lists, which only a diff assembled by hand fills",
	"feature_changes":           "named with the owner's change kind, which the page describes in prose",
}

// TestDriftPage_NamesEveryCategory holds the page's tables and the classifier
// to each other: every category a change can be counted under is a row with
// the severity the classifier gives it, and every row is a category the
// classifier reports. A category added without a row, or a row left behind by
// a renamed category, fails here.
func TestDriftPage_NamesEveryCategory(t *testing.T) {
	c := qt.New(t)

	documented := documentedCategories(c)

	c.Assert(documented, qt.DeepEquals, reportedCategories())
}

// TestDriftPage_TheTablesAreFound is the control on the test above: a parser
// that stopped finding a table would compare two maps the same lost rows left
// out of, so it has to read a category from each table.
func TestDriftPage_TheTablesAreFound(t *testing.T) {
	c := qt.New(t)

	documented := documentedCategories(c)

	c.Assert(len(documented) > 40, qt.IsTrue, qt.Commentf("read %d categories from %s", len(documented), driftPage))
	c.Assert(documented["sequences_removed"], qt.Equals, safety.Destructive)
	c.Assert(documented["comments_changed"], qt.Equals, safety.Safe)
}

// reportedCategories is every category ClassifySchemaDiff reports for a diff
// holding one change in one place, with its severity, and the two categories
// a zero entry cannot reach: a FORCE turned on, and a vector dimension
// changed. A feature category is reported as its generic name.
func reportedCategories() map[string]safety.Severity {
	diffs := []*difftypes.SchemaDiff{
		{RLSForceChanged: difftypes.RLSForceChanges{schemamodel.RLSEnabledTable{Forced: true}}},
		{TablesModified: []difftypes.TableDiff{{ColumnsModified: []difftypes.ColumnDiff{
			{Changes: map[string]string{"type": "vector(3) -> vector(4)"}},
		}}}},
	}
	for _, slot := range changeSlots() {
		diffs = append(diffs, slot.diff)
	}
	reported := make(map[string]safety.Severity)
	for _, diff := range diffs {
		for _, finding := range safety.ClassifySchemaDiff(diff) {
			category, _, _ := strings.Cut(finding.Category, ":")
			if _, left := undocumentedCategories[category]; left || strings.HasPrefix(category, "feature_") {
				continue
			}
			reported[category] = finding.Severity
		}
	}
	return reported
}

// documentedCategories reads the page's two tables: the kinds, with a
// severity under each direction a kind has, and the categories that count a
// setting or a part of an object.
func documentedCategories(c *qt.C) map[string]safety.Severity {
	c.Helper()
	page, err := os.ReadFile(driftPage)
	c.Assert(err, qt.IsNil)

	documented := make(map[string]safety.Severity)
	var directions []string
	for line := range strings.SplitSeq(string(page), "\n") {
		cells := tableCells(line)
		switch {
		case len(cells) == 0 || strings.HasPrefix(cells[0], "---"):
		case cells[0] == "Kind":
			directions = unquoted(cells[1:])
		case cells[0] == "Category":
			directions = nil
		case directions != nil:
			for i, direction := range directions {
				if i+1 < len(cells) && cells[i+1] != "" {
					documented[unquote(cells[0])+direction] = safety.Severity(cells[i+1])
				}
			}
		default:
			documented[unquote(cells[0])] = safety.Severity(cells[1])
		}
	}
	return documented
}

// tableCells splits a Markdown table row into its trimmed cells, and returns
// nothing for a line that is not a row.
func tableCells(line string) []string {
	line = strings.TrimSpace(line)
	if !strings.HasPrefix(line, "|") || !strings.HasSuffix(line, "|") {
		return nil
	}
	cells := strings.Split(strings.Trim(line, "|"), "|")
	for i := range cells {
		cells[i] = strings.TrimSpace(cells[i])
	}
	return cells
}

func unquoted(cells []string) []string {
	out := make([]string, 0, len(cells))
	for _, cell := range cells {
		out = append(out, unquote(cell))
	}
	return out
}

func unquote(cell string) string { return strings.Trim(cell, "`") }

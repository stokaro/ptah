package lineage_test

import (
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/internal/lineage"
)

// renderRow is one format and what its output must carry.
type renderRow struct {
	name        string
	format      string
	wantLines   []string
	wantMissing []string
}

// sampleResult is one resolved dependency and one column whose source is not
// established, which is the pair every assertion below is about.
func sampleResult() lineage.Result {
	return lineage.Result{
		Edges: []lineage.Edge{{
			FromTable: "authors", FromColumn: "name",
			ToView: "book_titles", ToColumn: "name",
		}},
		Unresolved: []lineage.Unresolved{{
			View: "shouty", Column: "loud", Reason: lineage.ReasonComputed,
		}},
	}
}

// TestRender_DrawsTheUnresolvedColumnsToo is the property that matters: a
// column whose source was not established is on the picture, with the reason.
//
// A lineage graph that draws only the resolved half looks like a schema that is
// fully understood, and the reader has no way to tell (stokaro/ptah#1712).
func TestRender_DrawsTheUnresolvedColumnsToo(t *testing.T) {
	rows := []renderRow{{
		name:   "dot carries both halves",
		format: lineage.FormatDOT,
		wantLines: []string{
			`"authors.name" -> "book_titles.name";`,
			`style=dashed`,
			lineage.ReasonComputed,
			`shouty.loud`,
		},
	}, {
		name:   "mermaid carries both halves",
		format: lineage.FormatMermaid,
		wantLines: []string{
			"flowchart LR",
			"-->",
			lineage.ReasonComputed,
			"shouty.loud",
		},
		// Mermaid breaks a line on <br/>; an escaped newline prints literally,
		// which is what this row exists to catch.
		wantMissing: []string{`\n` + lineage.ReasonComputed},
	}}

	for _, row := range rows {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)
			rendered, err := lineage.Render(sampleResult(), row.format)
			c.Assert(err, qt.IsNil)
			for _, want := range row.wantLines {
				c.Assert(string(rendered), qt.Contains, want)
			}
			for _, missing := range row.wantMissing {
				c.Assert(string(rendered), qt.Not(qt.Contains), missing)
			}
		})
	}
}

// TestRender_RefusesAFormatItCannotDraw pins that an unknown format is an error
// rather than an empty graph, which would read as "nothing depends on
// anything".
func TestRender_RefusesAFormatItCannotDraw(t *testing.T) {
	c := qt.New(t)

	_, err := lineage.Render(sampleResult(), "svg")

	c.Assert(err, qt.IsNotNil)
	c.Assert(err.Error(), qt.Contains, "unsupported lineage format")
}

// TestRender_NamesEveryNodeOnce pins that a column feeding two view columns is
// declared once and referenced twice, because a repeated node declaration is a
// second node in both formats and the graph would show the column twice.
func TestRender_NamesEveryNodeOnce(t *testing.T) {
	c := qt.New(t)
	result := lineage.Result{Edges: []lineage.Edge{
		{FromTable: "authors", FromColumn: "id", ToView: "v1", ToColumn: "id"},
		{FromTable: "authors", FromColumn: "id", ToView: "v2", ToColumn: "id"},
	}}

	rendered, err := lineage.Render(result, lineage.FormatDOT)

	c.Assert(err, qt.IsNil)
	c.Assert(strings.Count(string(rendered), `"authors.id" [label=`), qt.Equals, 1)
}

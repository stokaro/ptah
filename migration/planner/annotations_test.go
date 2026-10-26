package planner_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ast"
	"ptah.run/core/renderer"
	"ptah.run/migration/planner"
)

func TestPlannedAnnotationsNeverBecomeExecutionUnits(t *testing.T) {
	for _, test := range []struct {
		name        string
		exact, only bool
		wantNodes   []int
		wantBefore  []int
	}{
		{"exact", true, false, []int{1, 3}, []int{0, 1, 2}},
		{"script", false, false, []int{1, 3}, []int{0, 1, 2}},
		{"exact notes only", true, true, make([]int, 0), []int{0, 0, 0}},
		{"script notes only", false, true, make([]int, 0), []int{0, 0, 0}},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			first, middle, last := "before", "", "trailing\n*/ DROP TABLE users;"
			fragments := []renderer.Fragment{{Annotation: &first}, {}, {Annotation: &middle}, {}, {Annotation: &last}}
			if !test.only {
				fragments[1].SQL, fragments[3].SQL = "SELECT 1;", "SELECT 2;"
			}
			plan := planner.RenderedPlan{Request: renderer.Request{Nodes: make([]ast.Node, len(fragments))}, Result: renderer.Result{Complete: true, ExecutableUnits: test.exact, Fragments: fragments}, Dialect: "sqlite"}
			units, err := plan.Units()
			c.Assert(err, qt.IsNil)
			nodes := make([]int, 0, len(units))
			for _, unit := range units {
				nodes = append(nodes, unit.Node)
			}
			c.Assert(nodes, qt.DeepEquals, test.wantNodes)
			annotations, err := plan.Annotations()
			c.Assert(err, qt.IsNil)
			positions := make([]int, 0, len(annotations))
			for _, note := range annotations {
				positions = append(positions, note.Before)
			}
			c.Assert(positions, qt.DeepEquals, test.wantBefore)
			text, err := plan.SQL()
			c.Assert(err, qt.IsNil)
			c.Assert(text, qt.Contains, "-- trailing\n-- */ DROP TABLE users;\n")
		})
	}
}

func TestPlannedMixedUnitsRetainAnnotationAndNodeOrder(t *testing.T) {
	c := qt.New(t)
	note := "native follows"
	payload := &nativeUnit{Value: "native"}
	plan := planner.RenderedPlan{Request: renderer.Request{Nodes: make([]ast.Node, 3)}, Result: renderer.Result{Complete: true, ExecutableUnits: true, Fragments: []renderer.Fragment{{SQL: "OPAQUE 'syntax;"}, {Annotation: &note}, {Native: payload}}}}
	units, err := plan.Units()
	c.Assert(err, qt.IsNil)
	c.Assert(units, qt.HasLen, 2)
	c.Assert(units[0].Fragment.SQL, qt.Equals, "OPAQUE 'syntax;")
	c.Assert(units[1].Node, qt.Equals, 2)
	notes, err := plan.Annotations()
	c.Assert(err, qt.IsNil)
	c.Assert(notes, qt.DeepEquals, []renderer.Annotation{{Before: 1, Text: note}})
	text, err := plan.SQL()
	c.Assert(err, qt.ErrorIs, renderer.ErrNativeSQL)
	c.Assert(text, qt.Equals, "")
}

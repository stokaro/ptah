package featurehost_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ast"
	"ptah.run/core/featureplan"
	"ptah.run/core/objectidentity"
	"ptah.run/core/plangraph"
	"ptah.run/core/schemaext"
	"ptah.run/internal/planner/featurehost"
)

func TestLifecycleSpanOrdersReplacementAroundCommonSteps(t *testing.T) {
	for _, test := range []struct {
		name    string
		span    *featurehost.Span
		phase   featureplan.Phase
		edges   []plangraph.Dependency
		wantErr error
		want    []string
	}{
		{name: "lifecycle preserves common boundaries and resource dependencies", span: &featurehost.Span{Start: 0, End: 3}, phase: featureplan.PhaseLifecycle,
			want: []string{"common before", "stop reader", "drop table", "create queue", "restart reader", "common after"}},
		{name: "dependent phase keeps its point even with lifecycle enabled", span: &featurehost.Span{Start: 0, End: 3}, phase: featureplan.PhaseDependent, wantErr: plangraph.ErrCycle, want: make([]string, 0)},
		{name: "host without lifecycle refuses the phase", phase: featureplan.PhaseLifecycle, wantErr: schemaext.ErrInvalidValue, want: make([]string, 0)},
		{name: "lifecycle does not override a conflicting dependency", span: &featurehost.Span{Start: 0, End: 3}, phase: featureplan.PhaseLifecycle,
			edges:   []plangraph.Dependency{{Before: plangraph.StepID{Owner: "example.org/owner", Name: "create queue"}, After: plangraph.StepID{Owner: "example.org/owner", Name: "drop table"}}},
			wantErr: plangraph.ErrCycle, want: make([]string, 0)},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			slot := objectidentity.ID{Kind: "example.org/path", Name: objectidentity.Part{Source: "shared", Normalized: "shared"}}
			table := slot
			table.Kind = objectidentity.KindTable
			queue := slot
			queue.Kind = "example.org/queue"
			drop := ownerStep("drop table", plangraph.Drop)
			drop.Effects = []plangraph.Effect{{Subject: table, Action: plangraph.Drop}, {Subject: slot, Action: plangraph.Drop}}
			create := ownerStep("create queue", plangraph.Create)
			create.Effects = []plangraph.Effect{{Subject: queue, Action: plangraph.Create}, {Subject: slot, Action: plangraph.Create}}
			stop, restart := ownerStep("stop reader", plangraph.Read), ownerStep("restart reader", plangraph.Read)
			stop.ID.Owner, restart.ID.Owner = "example.org/reader", "example.org/reader"
			stop.Effects = nil
			restart.Effects = []plangraph.Effect{{Subject: queue, Action: plangraph.Read}}
			stop.Placement = plangraph.PlacementEarly
			graph, err := featurehost.NewGraph("example.org/host",
				[]ast.Node{ast.NewComment("common before"), ast.NewComment("common drop"), ast.NewComment("common after")},
				[]featureplan.CommonStep{{}, {Effects: drop.Effects}, {}},
				featurehost.Windows{Creation: 1, Dependent: 1, Removal: 1, Lifecycle: test.span})
			c.Assert(err, qt.IsNil)
			common := func(name string) plangraph.StepID { return plangraph.StepID{Owner: "example.org/host", Name: name} }
			result := featurehost.Result{
				Contributions: []plangraph.Contribution[[]ast.Node]{
					{Owner: drop.ID.Owner, Steps: []plangraph.Step[[]ast.Node]{drop}},
					{Owner: create.ID.Owner, Steps: []plangraph.Step[[]ast.Node]{create}},
					{Owner: stop.ID.Owner, Steps: []plangraph.Step[[]ast.Node]{stop, restart}, Dependencies: []plangraph.Dependency{
						{Before: common("common/000000"), After: stop.ID},
						{Before: stop.ID, After: common("common/000001")},
						{Before: common("common/000001"), After: restart.ID},
						{Before: restart.ID, After: common("common/000002")},
					}},
				},
				Phases: map[plangraph.StepID]featureplan.Phase{
					drop.ID: test.phase, create.ID: test.phase,
					stop.ID: test.phase, restart.ID: test.phase,
				},
				Rewrites: []plangraph.Rewrite{{Sources: []plangraph.StepID{common("common/000001")}, Replacement: drop.ID}},
			}
			result.Contributions[0].Dependencies = test.edges
			nodes, err := graph.Schedule(c.Context(), result)
			c.Assert(err, qt.ErrorIs, test.wantErr)
			c.Assert(comments(nodes), qt.DeepEquals, test.want)
		})
	}
}

func TestLifecycleSpanRefusesInvalidBounds(t *testing.T) {
	for _, test := range []struct {
		name string
		span featurehost.Span
	}{
		{"negative start", featurehost.Span{Start: -1, End: 3}},
		{"outside sequence", featurehost.Span{Start: 0, End: 5}},
		{"reversed", featurehost.Span{Start: 3, End: 1}},
		{"negative end", featurehost.Span{Start: 0, End: -1}},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			graph, err := featurehost.NewGraph("example.org/host", commonNodes(), nil,
				featurehost.Windows{Creation: 2, Dependent: 2, Removal: 2, Lifecycle: &test.span})
			c.Assert(err, qt.ErrorIs, schemaext.ErrInvalidValue)
			c.Assert(graph, qt.IsNil)
		})
	}
}

func TestLifecycleSpanEnclosesFeatureWindowsAtItsBoundaries(t *testing.T) {
	commonID := plangraph.StepID{Owner: "example.org/host", Name: "common/000000"}
	before, middle, after := ownerStep("before", plangraph.Read), ownerStep("middle", plangraph.Create), ownerStep("after", plangraph.Read)
	before.Effects, middle.Effects, after.Effects = nil, nil, nil
	for _, test := range []struct {
		name  string
		nodes []ast.Node
		point int
		edges []plangraph.Dependency
		want  []string
	}{
		{name: "empty common sequence", want: []string{"before", "middle", "after"}},
		{name: "start boundary", nodes: []ast.Node{ast.NewComment("common")},
			edges: []plangraph.Dependency{{Before: after.ID, After: commonID}}, want: []string{"before", "middle", "after", "common"}},
		{name: "end boundary", nodes: []ast.Node{ast.NewComment("common")}, point: 1,
			edges: []plangraph.Dependency{{Before: commonID, After: before.ID}}, want: []string{"common", "before", "middle", "after"}},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			graph, err := featurehost.NewGraph("example.org/host", test.nodes, nil,
				featurehost.Windows{Creation: test.point, Dependent: test.point, Removal: test.point,
					Lifecycle: &featurehost.Span{Start: 0, End: len(test.nodes)}})
			c.Assert(err, qt.IsNil)
			edges := append([]plangraph.Dependency{{Before: before.ID, After: middle.ID}, {Before: middle.ID, After: after.ID}}, test.edges...)
			nodes, err := graph.Schedule(c.Context(), featurehost.Result{
				Contributions: []plangraph.Contribution[[]ast.Node]{{Owner: before.ID.Owner,
					Steps: []plangraph.Step[[]ast.Node]{before, middle, after}, Dependencies: edges}},
				Phases: map[plangraph.StepID]featureplan.Phase{before.ID: featureplan.PhaseLifecycle, after.ID: featureplan.PhaseLifecycle},
			})
			c.Assert(err, qt.IsNil)
			c.Assert(comments(nodes), qt.DeepEquals, test.want)
		})
	}
}

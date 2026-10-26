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

func TestLifecycleCommonAccessFollowsReplacement(t *testing.T) {
	for _, movable := range []bool{false, true} {
		t.Run(map[bool]string{false: "fixed sequence refuses cycle", true: "access follows replacement"}[movable], func(t *testing.T) {
			c := qt.New(t)
			subject := objectidentity.ID{Kind: "example.org/query", Name: objectidentity.Part{Source: "reader", Normalized: "reader"}}
			access := subject
			access.Kind = "object_access"
			table := objectidentity.ID{Kind: "table", Name: objectidentity.Part{Source: "input", Normalized: "input"}}
			slot := table
			slot.Kind = "example.org/path"
			grantEffects := []plangraph.Effect{{Subject: subject, Action: plangraph.Read}, {Subject: access, Action: plangraph.Alter}}
			drop := ownerStep("drop table", plangraph.Drop)
			drop.Effects = []plangraph.Effect{{Subject: table, Action: plangraph.Drop}, {Subject: slot, Action: plangraph.Drop}}
			create := ownerStep("create input", plangraph.Create)
			create.Effects = []plangraph.Effect{{Subject: slot, Action: plangraph.Create}}
			restart := ownerStep("restart query", plangraph.Alter)
			restart.ID.Owner = "example.org/reader"
			restart.Effects = []plangraph.Effect{{Subject: subject, Action: plangraph.Alter}, {Subject: slot, Action: plangraph.Read}, {Subject: access, Action: plangraph.Read}}
			windows := featurehost.Windows{Creation: 1, Dependent: 2, Removal: 2, Lifecycle: &featurehost.Span{Start: 0, End: 3}}
			if movable {
				windows.LifecycleCommon = []int{1}
				windows.CommonDependencies = []featurehost.CommonDependency{{Before: 0, After: 1}}
			}
			graph, err := featurehost.NewGraph("example.org/host", []ast.Node{ast.NewComment("create role"), ast.NewComment("grant"), ast.NewComment("common drop")},
				[]featureplan.CommonStep{{}, {Effects: grantEffects}, {Effects: drop.Effects}}, windows)
			c.Assert(err, qt.IsNil)
			common := func(name string) plangraph.StepID { return plangraph.StepID{Owner: "example.org/host", Name: name} }
			var ids []string
			for _, step := range graph.CommonSteps() {
				if len(step.Effects) > 0 { ids = append(ids, step.ID.Name) }
			}
			c.Assert(ids, qt.DeepEquals, []string{"common/000001", "common/000002"})
			nodes, err := graph.Schedule(c.Context(), featurehost.Result{
				Contributions: []plangraph.Contribution[[]ast.Node]{
					{Owner: drop.ID.Owner, Steps: []plangraph.Step[[]ast.Node]{drop, create}, Dependencies: []plangraph.Dependency{{Before: drop.ID, After: create.ID}}},
					{Owner: restart.ID.Owner, Steps: []plangraph.Step[[]ast.Node]{restart}, Dependencies: []plangraph.Dependency{{Before: restart.ID, After: common("common/000001")}}},
				},
				Phases: map[plangraph.StepID]featureplan.Phase{drop.ID: featureplan.PhaseLifecycle, create.ID: featureplan.PhaseLifecycle, restart.ID: featureplan.PhaseLifecycle},
				Rewrites: []plangraph.Rewrite{{Sources: []plangraph.StepID{common("common/000002")}, Replacement: drop.ID}},
			})
			if !movable { c.Assert(err, qt.ErrorIs, plangraph.ErrCycle); c.Assert(nodes, qt.IsNil); return }
			c.Assert(err, qt.IsNil)
			c.Assert(comments(nodes), qt.DeepEquals, []string{"create role", "drop table", "create input", "restart query", "grant"})
		})
	}
}

func TestLifecycleCommonRefusesUnknownAndUnorderedFootprints(t *testing.T) {
	for _, test := range []struct{name string; indexes []int; metadata []featureplan.CommonStep; edges []featurehost.CommonDependency; want error}{
		{name:"unknown footprint", indexes:[]int{0}, metadata:[]featureplan.CommonStep{{},{}}, want:schemaext.ErrInvalidValue},
		{name:"outside span", indexes:[]int{2}, want:schemaext.ErrInvalidValue},
		{name:"duplicate index", indexes:[]int{0,0}, want:schemaext.ErrInvalidValue},
		{name:"unordered writes", indexes:[]int{0,1}, want:plangraph.ErrConflict},
		{name:"ordered writes", indexes:[]int{0,1}, edges:[]featurehost.CommonDependency{{Before:0,After:1}}},
		{name:"invalid dependency", indexes:[]int{0}, edges:[]featurehost.CommonDependency{{Before:0,After:2}},want:schemaext.ErrInvalidValue},
	} {
		t.Run(test.name,func(t *testing.T){
			c:=qt.New(t)
			metadata:=test.metadata
			if metadata==nil {
				effects:=[]plangraph.Effect{{Subject:objectidentity.ID{Kind:"object_access",Name:objectidentity.Part{Source:"query",Normalized:"query"}},Action:plangraph.Alter}}
				metadata=[]featureplan.CommonStep{{Effects:effects},{Effects:effects}}
			}
			graph,err:=featurehost.NewGraph("example.org/host",[]ast.Node{ast.NewComment("first"),ast.NewComment("second")},metadata,
				featurehost.Windows{Lifecycle:&featurehost.Span{Start:0,End:2},LifecycleCommon:test.indexes,CommonDependencies:test.edges})
			if err==nil { _,err=graph.Schedule(c.Context(),featurehost.Result{}) }
			c.Assert(err,qt.ErrorIs,test.want)
		})
	}
}

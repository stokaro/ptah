package featurehost_test

import (
	"fmt"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ast"
	"ptah.run/core/featureplan"
	"ptah.run/core/objectidentity"
	"ptah.run/core/plangraph"
	"ptah.run/internal/planner/featurehost"
)

func TestRewriteBoundaryProjectionPreservesClaimValidation(t *testing.T) {
	for _, test := range []struct {
		name   string
		mutate func(*featurehost.Result)
		want   error
	}{
		{name: "multiple independent replacements"},
		{name: "unknown source", mutate: func(r *featurehost.Result) { r.Rewrites[0].Sources[0].Name = "missing" }, want: plangraph.ErrInvalid},
		{name: "duplicate source", mutate: func(r *featurehost.Result) { r.Rewrites[1].Sources = r.Rewrites[0].Sources }, want: plangraph.ErrInvalid},
		{name: "foreign source", mutate: func(r *featurehost.Result) { r.Rewrites[0].Sources[0] = r.Contributions[0].Steps[1].ID }, want: plangraph.ErrInvalid},
		{name: "unclaimed write", mutate: func(r *featurehost.Result) {
			r.Contributions[0].Steps[0].Effects = r.Contributions[0].Steps[0].Effects[1:]
		}, want: plangraph.ErrInvalid},
		{name: "cyclic replacement", mutate: func(r *featurehost.Result) {
			steps := r.Contributions[0].Steps
			r.Contributions[0].Dependencies = append(r.Contributions[0].Dependencies, plangraph.Dependency{Before: steps[1].ID, After: steps[0].ID})
		}, want: plangraph.ErrCycle},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			result := featurehost.Result{Phases: make(map[plangraph.StepID]featureplan.Phase)}
			writers := plangraph.Contribution[[]ast.Node]{Owner: "example.org/owner"}
			readers := plangraph.Contribution[[]ast.Node]{Owner: "example.org/reader"}
			var nodes []ast.Node
			var metadata []featureplan.CommonStep
			for i := range 2 {
				name := fmt.Sprintf("input%d", i)
				table := objectidentity.ID{Kind: "table", Name: objectidentity.Part{Source: name, Normalized: name}}
				slot := table
				slot.Kind = "example.org/path"
				drop := ownerStep("drop "+name, plangraph.Drop)
				drop.Effects = []plangraph.Effect{{Subject: table, Action: plangraph.Drop}, {Subject: slot, Action: plangraph.Drop}}
				create := ownerStep("create "+name, plangraph.Create)
				create.Effects = []plangraph.Effect{{Subject: slot, Action: plangraph.Create}}
				read := ownerStep("read "+name, plangraph.Read)
				read.ID.Owner = readers.Owner
				read.Effects = []plangraph.Effect{{Subject: slot, Action: plangraph.Read}}
				common := plangraph.StepID{Owner: "example.org/host", Name: fmt.Sprintf("common/%06d", i)}
				nodes = append(nodes, ast.NewComment("common "+name))
				metadata = append(metadata, featureplan.CommonStep{Effects: drop.Effects[:1]})
				writers.Steps = append(writers.Steps, drop, create)
				writers.Dependencies = append(writers.Dependencies, plangraph.Dependency{Before: drop.ID, After: create.ID})
				readers.Steps = append(readers.Steps, read)
				readers.Dependencies = append(readers.Dependencies, plangraph.Dependency{Before: common, After: read.ID})
				result.Rewrites = append(result.Rewrites, plangraph.Rewrite{Sources: []plangraph.StepID{common}, Replacement: drop.ID})
				result.Phases[drop.ID], result.Phases[create.ID], result.Phases[read.ID] = featureplan.PhaseLifecycle, featureplan.PhaseLifecycle, featureplan.PhaseLifecycle
			}
			result.Contributions = []plangraph.Contribution[[]ast.Node]{writers, readers}
			if test.mutate != nil {
				test.mutate(&result)
			}
			graph, err := featurehost.NewGraph("example.org/host", nodes, metadata, featurehost.Windows{Lifecycle: &featurehost.Span{Start: 0, End: 2}})
			c.Assert(err, qt.IsNil)
			scheduled, err := graph.Schedule(c.Context(), result)
			c.Assert(err, qt.ErrorIs, test.want)
			if test.want != nil {
				c.Assert(scheduled, qt.IsNil)
				return
			}
			got := comments(scheduled)
			c.Assert(got, qt.HasLen, 6)
			positions := make(map[string]int)
			for index, name := range got {
				positions[name] = index
			}
			for i := range 2 {
				name := fmt.Sprintf("input%d", i)
				c.Assert(positions["drop "+name] < positions["create "+name], qt.IsTrue)
				c.Assert(positions["create "+name] < positions["read "+name], qt.IsTrue)
			}
		})
	}
}

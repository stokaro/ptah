package ydb

import (
	"context"

	"ptah.run/core/ast"
	"ptah.run/core/plangraph"
	"ptah.run/internal/planner/featurehost"
)

// scheduleChangefeeds joins feature contributions before flattening any nodes.
// The surrounding planner phases retain their existing order. Their missing
// object footprints remain unknown until those families contribute effects.
func scheduleChangefeeds(ctx context.Context, before, after []ast.Node, features featurehost.Result) ([]ast.Node, error) {
	common := plangraph.Contribution[[]ast.Node]{Owner: "ptah.run/ydb"}
	var first, last, featureIDs []plangraph.StepID
	if len(before) > 0 {
		id := plangraph.StepID{Owner: common.Owner, Name: "relations/before-changefeeds"}
		first = append(first, id)
		common.Steps = append(common.Steps, plangraph.Step[[]ast.Node]{ID: id, Payload: before})
	}
	if len(after) > 0 {
		id := plangraph.StepID{Owner: common.Owner, Name: "relations/after-changefeeds"}
		last = append(last, id)
		common.Steps = append(common.Steps, plangraph.Step[[]ast.Node]{ID: id, Payload: after})
	}
	for _, contribution := range features.Contributions {
		for _, step := range contribution.Steps {
			featureIDs = append(featureIDs, step.ID)
		}
	}
	common.Dependencies = append(common.Dependencies, dependencies(first, last)...)
	common.Dependencies = append(common.Dependencies, dependencies(first, featureIDs)...)
	common.Dependencies = append(common.Dependencies, dependencies(featureIDs, last)...)
	plan, err := plangraph.ScheduleRewritten(ctx, common, features.Rewrites, features.Contributions...)
	if err != nil {
		return nil, err
	}
	var nodes []ast.Node
	for _, step := range plan.Steps {
		nodes = append(nodes, step.Payload...)
	}
	return nodes, nil
}

func dependencies(before, after []plangraph.StepID) []plangraph.Dependency {
	var edges []plangraph.Dependency
	for _, first := range before {
		for _, last := range after {
			edges = append(edges, plangraph.Dependency{Before: first, After: last})
		}
	}
	return edges
}

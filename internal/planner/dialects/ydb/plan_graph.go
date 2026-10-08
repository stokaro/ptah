package ydb

import (
	"context"
	"fmt"

	"ptah.run/core/ast"
	"ptah.run/core/featureplan"
	"ptah.run/core/objectidentity"
	"ptah.run/core/plangraph"
	"ptah.run/core/schemaext"
	"ptah.run/migration/schemadiff/difftypes"
)

// scheduleChangefeeds joins feature contributions before flattening any nodes.
// The surrounding planner phases retain their existing order. Their missing
// object footprints remain unknown until those families contribute effects.
func scheduleChangefeeds(ctx context.Context, before, after []ast.Node, result featureplan.Result, diff *difftypes.SchemaDiff) ([]ast.Node, error) {
	features, err := featureNodes(result, diff)
	if err != nil {
		return nil, err
	}
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
	for _, contribution := range features {
		for _, step := range contribution.Steps {
			featureIDs = append(featureIDs, step.ID)
		}
	}
	common.Dependencies = append(common.Dependencies, dependencies(first, last)...)
	common.Dependencies = append(common.Dependencies, dependencies(first, featureIDs)...)
	common.Dependencies = append(common.Dependencies, dependencies(featureIDs, last)...)
	plan, err := plangraph.Schedule(ctx, append(features, common)...)
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

func featureNodes(result featureplan.Result, diff *difftypes.SchemaDiff) ([]plangraph.Contribution[[]ast.Node], error) {
	names := make(map[objectidentity.Key]string)
	for _, table := range diff.TablesModified {
		for _, change := range table.FeatureChanges {
			ref := change.Subject
			parent := objectidentity.ID{Kind: objectidentity.KindTable, Catalog: ref.Catalog, Schema: ref.Schema, Name: ref.Parent}
			names[parent.Key()] = table.TableName
		}
	}
	contributions := make([]plangraph.Contribution[[]ast.Node], len(result.Contributions))
	for i, feature := range result.Contributions {
		contribution := plangraph.Contribution[[]ast.Node]{Owner: feature.Owner, Dependencies: feature.Dependencies}
		for _, step := range feature.Steps {
			var nodes []ast.Node
			for _, note := range step.Payload.Notes {
				nodes = append(nodes, ast.NewComment(note))
			}
			switch step.Payload.Role {
			case ast.StatementExtension:
				nodes = append(nodes, &ast.ExtensionStatement{Payload: step.Payload.Payload})
			case ast.AlterExtension:
				name, found := names[step.Payload.Parent.Key()]
				if !found {
					return nil, fmt.Errorf("%w: feature operation has no table name", schemaext.ErrInvalidValue)
				}
				nodes = append(nodes, &ast.AlterTableNode{Name: name, Operations: []ast.AlterOperation{&ast.ExtensionAlterOperation{Payload: step.Payload.Payload}}})
			default:
				return nil, fmt.Errorf("%w: unsupported feature operation role", schemaext.ErrInvalidValue)
			}
			contribution.Steps = append(contribution.Steps, plangraph.Step[[]ast.Node]{ID: step.ID, Payload: nodes, Effects: step.Effects, Transaction: step.Transaction, Impact: step.Impact})
		}
		contributions[i] = contribution
	}
	return contributions, nil
}

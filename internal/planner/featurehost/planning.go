// Package featurehost lowers selected feature plans into contributions to a
// host's common operation graph. Target-specific phases remain with the host.
package featurehost

import (
	"context"
	"fmt"
	"maps"
	"slices"

	"ptah.run/core/ast"
	"ptah.run/core/featureplan"
	"ptah.run/core/objectidentity"
	"ptah.run/core/plangraph"
	"ptah.run/core/schemaext"
)

// Plan dispatches one contextual batch and converts complete operation replies
// into AST contributions without scheduling them independently. Names binds
// captured table identities to the host's source-spelled emission names. A
// parent without a binding cannot receive an ALTER operation. The caller must
// join these contributions to its common graph before returning any nodes.
func Plan(ctx context.Context, runtime featureplan.Runtime, request featureplan.Request, names map[objectidentity.Key]string) ([]plangraph.Contribution[[]ast.Node], error) {
	if err := schemaext.RequireRuntime(ctx, runtime); err != nil {
		return nil, err
	}
	names = maps.Clone(names)
	if err := validateNames(request, names); err != nil {
		return nil, err
	}
	result, err := runtime.PlanFeatures(ctx, request)
	if err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if !result.Complete {
		return nil, fmt.Errorf("%w: planning did not complete", schemaext.ErrInvalidValue)
	}
	contributions := make([]plangraph.Contribution[[]ast.Node], len(result.Contributions))
	for i, feature := range result.Contributions {
		contribution := plangraph.Contribution[[]ast.Node]{Owner: feature.Owner, Dependencies: slices.Clone(feature.Dependencies)}
		for _, step := range feature.Steps {
			nodes, err := operationNodes(step.Payload, names)
			if err != nil {
				return nil, err
			}
			contribution.Steps = append(contribution.Steps, plangraph.Step[[]ast.Node]{
				ID: step.ID, Payload: nodes, Effects: slices.Clone(step.Effects), Transaction: step.Transaction, Impact: step.Impact,
			})
		}
		contributions[i] = contribution
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return contributions, nil
}

func validateNames(request featureplan.Request, names map[objectidentity.Key]string) error {
	captured := make(map[objectidentity.Key]bool, len(request.Tables))
	for _, table := range request.Tables {
		captured[table.Subject.Key()] = true
	}
	builder := objectidentity.NewBuilder(request.Identifiers)
	for key, name := range names {
		if name == "" || !captured[key] || builder.Table(name).Key() != key {
			return fmt.Errorf("%w: feature table name disagrees with its captured identity", schemaext.ErrInvalidValue)
		}
	}
	return nil
}

func operationNodes(operation featureplan.Operation, names map[objectidentity.Key]string) ([]ast.Node, error) {
	payload, err := ast.CloneExtensionPayload(operation.Payload)
	if err != nil {
		return nil, err
	}
	var nodes []ast.Node
	for _, note := range operation.Notes {
		nodes = append(nodes, ast.NewComment(note))
	}
	switch operation.Role {
	case ast.StatementExtension:
		nodes = append(nodes, &ast.ExtensionStatement{Payload: payload})
	case ast.AlterExtension:
		name, found := names[operation.Parent.Key()]
		if !found {
			return nil, fmt.Errorf("%w: feature operation has no table name", schemaext.ErrInvalidValue)
		}
		nodes = append(nodes, &ast.AlterTableNode{Name: name, Operations: []ast.AlterOperation{&ast.ExtensionAlterOperation{Payload: payload}}})
	default:
		return nil, fmt.Errorf("%w: unsupported feature operation role", schemaext.ErrInvalidValue)
	}
	return nodes, nil
}

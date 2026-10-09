// Package featureops lowers typed owner operations into local AST envelopes.
// It does not select providers, schedule contributions, or render SQL.
package featureops

import (
	"fmt"

	"ptah.run/core/ast"
	"ptah.run/core/featureplan"
	"ptah.run/core/objectidentity"
	"ptah.run/core/schemaext"
)

// Nodes clones one operation into its grammatical placement. Names binds
// captured table identities to source-spelled emission names for ALTER payloads.
func Nodes(operation featureplan.Operation, names map[objectidentity.Key]string) ([]ast.Node, error) {
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

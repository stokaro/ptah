package ydb

import (
	"fmt"

	"ptah.run/core/ast"
	"ptah.run/core/ptaherr"
	"ptah.run/core/renderer"
	"ptah.run/internal/ydbextensions"
)

func (r *Renderer) renderExtension(role ast.ExtensionRole, payload ast.ExtensionPayload) error {
	registry, err := ydbextensions.Registry()
	if err != nil {
		return err
	}
	statements, err := registry.Render(renderer.ExtensionContext{Target: DialectName, Capabilities: r.caps}, role, payload)
	if err != nil {
		return err
	}
	for _, statement := range statements {
		r.w.WriteLine(statement)
	}
	return nil
}

func (r *Renderer) renderExtensionNode(node ast.Node) error {
	switch typed := node.(type) {
	case *ast.ExtensionStatement:
		return r.renderExtension(ast.StatementExtension, typed.Payload)
	case *ast.ExtensionAlterOperation:
		return r.renderExtension(ast.AlterExtension, typed.Payload)
	default:
		return fmt.Errorf("%w: expected an extension node, got %T", ptaherr.ErrInvalidSchemaDiff, node)
	}
}

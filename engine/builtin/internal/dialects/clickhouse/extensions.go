package clickhouse

import (
	"fmt"

	"ptah.run/core/ast"
	"ptah.run/core/ptaherr"
	"ptah.run/core/renderer"
	"ptah.run/dialect/clickhouse/chrender"
)

func (r *Renderer) renderExtensionNode(node ast.Node) error {
	var role ast.ExtensionRole
	var payload ast.ExtensionPayload
	switch typed := node.(type) {
	case *ast.ExtensionStatement:
		role, payload = ast.StatementExtension, typed.Payload
	case *ast.ExtensionAlterOperation:
		role, payload = ast.AlterExtension, typed.Payload
	default:
		return fmt.Errorf("%w: expected an extension node, got %T", ptaherr.ErrInvalidSchemaDiff, node)
	}
	return r.renderOwnedExtension(nil, role, payload)
}

func (r *Renderer) renderOwnedExtension(parent *ast.AlterTableNode, role ast.ExtensionRole, payload ast.ExtensionPayload) error {
	registry, err := chrender.Registry()
	if err != nil {
		return err
	}
	statements, err := registry.Render(renderer.ExtensionContext{Target: DialectName, Capabilities: r.capabilities(), Parent: parent}, role, payload)
	if err != nil {
		return err
	}
	for _, statement := range statements {
		r.w.WriteLine(statement)
	}
	return nil
}

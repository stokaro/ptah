package mysqllike

import (
	"fmt"

	"ptah.run/core/ast"
	"ptah.run/core/ptaherr"
	"ptah.run/core/renderer"
	"ptah.run/dialect/mysql/mysqlrender"
)

// renderExtensionNode renders an owned payload that reaches the renderer on
// its own. The MySQL owner's operations all belong to an ALTER TABLE, so one
// without its parent is refused by the registry, and a payload of another
// owner is refused as unsupported.
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

// prepareAlterExtensions checks every owned operation of an ALTER before any
// of its operations is written, so a refused payload leaves no partial SQL.
func (r *Renderer) prepareAlterExtensions(node *ast.AlterTableNode) error {
	registry, err := mysqlrender.Registry()
	if err != nil {
		return err
	}
	for _, operation := range node.Operations {
		extension, ok := operation.(*ast.ExtensionAlterOperation)
		if !ok {
			continue
		}
		if extension == nil {
			return fmt.Errorf("%w: extension alter-table operation is nil", ptaherr.ErrInvalidSchemaDiff)
		}
		if _, err := registry.Prepare(r.extensionContext(node), ast.AlterExtension, extension.Payload); err != nil {
			return err
		}
	}
	return nil
}

func (r *Renderer) renderOwnedExtension(parent *ast.AlterTableNode, role ast.ExtensionRole, payload ast.ExtensionPayload) error {
	registry, err := mysqlrender.Registry()
	if err != nil {
		return err
	}
	statements, err := registry.Render(r.extensionContext(parent), role, payload)
	if err != nil {
		return err
	}
	for _, statement := range statements {
		r.w.WriteLine(statement)
	}
	return nil
}

func (r *Renderer) extensionContext(parent *ast.AlterTableNode) renderer.ExtensionContext {
	return renderer.ExtensionContext{Target: r.dialect, Capabilities: r.caps, Parent: parent}
}

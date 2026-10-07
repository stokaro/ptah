package nodedispatch

import (
	"fmt"

	"ptah.run/core/ast"
	"ptah.run/core/ptaherr"
	"ptah.run/core/renderer"
)

// RefuseExtension rejects an extension on a backend with no owner handler.
// Malformed payloads are invalid input; valid payloads are unsupported even
// when the caller claims capabilities that belong to another backend.
func RefuseExtension(dialect string, node ast.Node) error {
	var payload ast.ExtensionPayload
	var role ast.ExtensionRole
	switch typed := node.(type) {
	case *ast.ExtensionStatement:
		payload, role = typed.Payload, ast.StatementExtension
	case *ast.ExtensionAlterOperation:
		payload, role = typed.Payload, ast.AlterExtension
	default:
		return fmt.Errorf("%w: expected an extension node, got %T", ptaherr.ErrInvalidSchemaDiff, node)
	}
	_, err := (renderer.Extensions{}).Prepare(renderer.ExtensionContext{Target: dialect}, role, payload)
	return err
}

// RefuseAlterExtensions checks a complete ALTER before a non-owning backend
// writes any of its common operations. Claimed capabilities cannot supply a
// handler, and a later extension must not leave partial SQL in the buffer.
func RefuseAlterExtensions(dialect string, node *ast.AlterTableNode) error {
	for _, operation := range node.Operations {
		if extension, ok := operation.(*ast.ExtensionAlterOperation); ok {
			if extension == nil {
				return fmt.Errorf("%w: extension alter-table operation is nil", ptaherr.ErrInvalidSchemaDiff)
			}
			return RefuseExtension(dialect, extension)
		}
	}
	return nil
}

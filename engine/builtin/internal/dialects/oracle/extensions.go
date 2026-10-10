package oracle

import (
	"fmt"

	"ptah.run/core/ast"
	"ptah.run/core/ptaherr"
	"ptah.run/core/renderer"
	"ptah.run/feature/synonym"
)

// renderExtensionNode renders an owner statement through the handlers of the
// Oracle owners, which are the synonym owner's alone. A payload no Oracle
// owner renders is refused.
func (r *Renderer) renderExtensionNode(node ast.Node) error {
	statement, ok := node.(*ast.ExtensionStatement)
	if !ok {
		return fmt.Errorf("%w: expected an extension statement, got %T", ptaherr.ErrInvalidSchemaDiff, node)
	}
	registry, err := renderer.NewExtensions(synonym.Handlers()...)
	if err != nil {
		return err
	}
	context := renderer.ExtensionContext{Target: r.GetDialect(), Capabilities: r.capabilities()}
	statements, err := registry.Render(context, ast.StatementExtension, statement.Payload)
	if err != nil {
		return err
	}
	for _, line := range statements {
		r.w.WriteLine(line)
	}
	return nil
}

package mssql

import (
	"fmt"

	"ptah.run/core/ast"
	"ptah.run/core/platform/capability"
	"ptah.run/core/ptaherr"
	"ptah.run/core/renderer"
	"ptah.run/dialect/mssql/mssqlrender"
)

// gatedPayload is an owner statement that needs one capability key. A target
// without the key writes the skip line and records the omission, as this
// renderer does for its own capability-gated objects.
type gatedPayload interface {
	RequiredCapability() capability.Capability
	OmissionSubject() (kind, name string)
}

// renderExtensionNode renders an owner statement through the SQL Server
// owners' registry. A payload no SQL Server owner renders is refused, and so
// is an owned operation that names a table, since no SQL Server owner writes
// one inside ALTER TABLE.
func (r *Renderer) renderExtensionNode(node ast.Node) error {
	statement, ok := node.(*ast.ExtensionStatement)
	if !ok {
		return fmt.Errorf("%w: expected an extension statement, got %T", ptaherr.ErrInvalidSchemaDiff, node)
	}
	registry, err := mssqlrender.Registry()
	if err != nil {
		return err
	}
	context := renderer.ExtensionContext{Target: r.GetDialect(), Capabilities: r.capabilities()}
	if gated, ok := statement.Payload.(gatedPayload); ok {
		// The payload is checked first, so a payload no owner renders is
		// refused rather than given a skip line.
		if _, err := registry.Prepare(context, ast.StatementExtension, statement.Payload); err != nil {
			return err
		}
		kind, name := gated.OmissionSubject()
		if r.refuses(gated.RequiredCapability(), kind, name) {
			return nil
		}
	}
	statements, err := registry.Render(context, ast.StatementExtension, statement.Payload)
	if err != nil {
		return err
	}
	for _, line := range statements {
		r.w.WriteLine(line)
	}
	return nil
}

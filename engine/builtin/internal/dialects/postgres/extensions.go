package postgres

import (
	"fmt"

	"ptah.run/core/ast"
	"ptah.run/core/platform"
	"ptah.run/core/ptaherr"
	"ptah.run/core/renderer"
	"ptah.run/dialect/cockroachdb/crdbrender"
	"ptah.run/engine/builtin/internal/dialects/internal/nodedispatch"
)

// ownedExtensions returns the owner handlers this target renders. CockroachDB
// is the only PostgreSQL-wire target with owned operations; every other one
// has none and refuses each payload through the common boundary.
func (r *Renderer) ownedExtensions() (renderer.Extensions, error) {
	if r.dialect != platform.CockroachDB {
		return renderer.Extensions{}, nil
	}
	return crdbrender.Registry()
}

func (r *Renderer) extensionContext(parent *ast.AlterTableNode) renderer.ExtensionContext {
	return renderer.ExtensionContext{Target: r.dialect, Capabilities: r.capabilities(), Parent: parent}
}

// prepareAlterExtensions checks every owned operation of an ALTER before any of
// its common operations is written, so a refused payload leaves no partial SQL.
func (r *Renderer) prepareAlterExtensions(node *ast.AlterTableNode) error {
	if r.dialect != platform.CockroachDB {
		return nodedispatch.RefuseAlterExtensions(r.GetDialect(), node)
	}
	registry, err := r.ownedExtensions()
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

// writeExtensionOperation renders one owned operation inside its ALTER TABLE.
func (r *Renderer) writeExtensionOperation(node *ast.AlterTableNode, operation *ast.ExtensionAlterOperation) error {
	registry, err := r.ownedExtensions()
	if err != nil {
		return err
	}
	statements, err := registry.Render(r.extensionContext(node), ast.AlterExtension, operation.Payload)
	if err != nil {
		return err
	}
	for _, statement := range statements {
		r.w.WriteLine(statement)
	}
	return nil
}

// renderExtensionNode answers a standalone extension the way the same payload
// is answered inside a parent: an unowned payload is refused, and an owned
// ALTER operation is validated and then reported as needing its ALTER TABLE.
func (r *Renderer) renderExtensionNode(node ast.Node) error {
	if r.dialect != platform.CockroachDB {
		return nodedispatch.RefuseExtension(r.GetDialect(), node)
	}
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
	registry, err := r.ownedExtensions()
	if err != nil {
		return err
	}
	statements, err := registry.Render(r.extensionContext(nil), role, payload)
	if err != nil {
		return err
	}
	for _, statement := range statements {
		r.w.WriteLine(statement)
	}
	return nil
}

// renderOwnedTableStorage returns the ` WITH (...)` clause an owner renders for
// a CREATE TABLE's facets, and the empty string for a table without any. A
// target whose renderer consumes no facet refuses one rather than dropping it:
// the builtin preparation refuses first, and this keeps a direct render from
// emitting a table without the setting it declared.
func (r *Renderer) renderOwnedTableStorage(node *ast.CreateTableNode) (string, error) {
	if r.dialect == platform.CockroachDB {
		return crdbrender.CreateTableClause(r.dialect, r.capabilities(), node.Name, node.Facets)
	}
	if kinds := node.Facets.Kinds(); len(kinds) > 0 {
		return "", &ptaherr.CapabilityError{Dialect: r.dialect, Feature: string(kinds[0]), Err: ptaherr.ErrUnsupportedFeature,
			Message: fmt.Sprintf("%s: table %q declares feature facet %q, which this target does not render", r.dialect, node.Name, kinds[0])}
	}
	return "", nil
}

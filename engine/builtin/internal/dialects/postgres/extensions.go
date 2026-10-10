package postgres

import (
	"fmt"

	"ptah.run/core/ast"
	"ptah.run/core/platform/capability"
	"ptah.run/core/ptaherr"
	"ptah.run/core/renderer"
	"ptah.run/core/schemaext"
)

// Owners is what a target's feature owners contribute to rendering it: the
// handlers for owned operations, and the clause owned facets add to a CREATE
// TABLE. The composition that selects a target's owners supplies them, so this
// renderer names no owner and serves every PostgreSQL-wire target alike.
//
// The zero value contributes nothing: every owned payload is refused through
// the common boundary, and a CREATE TABLE carrying a facet is refused rather
// than rendered without it.
type Owners struct {
	// Extensions renders owned payloads. The zero value has no handler.
	Extensions renderer.Extensions
	// TableStorage returns the ` WITH (...)` clause for a CREATE TABLE's
	// facets, or the empty string for none. Nil means no owner renders one.
	TableStorage func(target string, caps capability.Capabilities, table string, facets schemaext.Facets) (string, error)
}

// WithOwners sets what the target's feature owners render and returns the
// renderer. Owners are fixed for the renderer's lifetime.
func (r *Renderer) WithOwners(owners Owners) *Renderer {
	r.owners = owners
	return r
}

func (r *Renderer) extensionContext(parent *ast.AlterTableNode) renderer.ExtensionContext {
	return renderer.ExtensionContext{Target: r.dialect, Capabilities: r.capabilities(), Parent: parent}
}

// prepareAlterExtensions checks every owned operation of an ALTER before any of
// its common operations is written, so a refused payload leaves no partial SQL.
func (r *Renderer) prepareAlterExtensions(node *ast.AlterTableNode) error {
	for _, operation := range node.Operations {
		extension, ok := operation.(*ast.ExtensionAlterOperation)
		if !ok {
			continue
		}
		if extension == nil {
			return fmt.Errorf("%w: extension alter-table operation is nil", ptaherr.ErrInvalidSchemaDiff)
		}
		if _, err := r.owners.Extensions.Prepare(r.extensionContext(node), ast.AlterExtension, extension.Payload); err != nil {
			return err
		}
	}
	return nil
}

// writeExtensionOperation renders one owned operation inside its ALTER TABLE.
func (r *Renderer) writeExtensionOperation(node *ast.AlterTableNode, operation *ast.ExtensionAlterOperation) error {
	statements, err := r.owners.Extensions.Render(r.extensionContext(node), ast.AlterExtension, operation.Payload)
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
	statements, err := r.owners.Extensions.Render(r.extensionContext(nil), role, payload)
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
// target whose owners render no facet refuses one rather than dropping it:
// the builtin preparation refuses first, and this keeps a direct render from
// emitting a table without the setting it declared.
func (r *Renderer) renderOwnedTableStorage(node *ast.CreateTableNode) (string, error) {
	if r.owners.TableStorage != nil {
		return r.owners.TableStorage(r.dialect, r.capabilities(), node.Name, node.Facets)
	}
	if kinds := node.Facets.Kinds(); len(kinds) > 0 {
		return "", &ptaherr.CapabilityError{Dialect: r.dialect, Feature: string(kinds[0]), Err: ptaherr.ErrUnsupportedFeature,
			Message: fmt.Sprintf("%s: table %q declares feature facet %q, which this target does not render", r.dialect, node.Name, kinds[0])}
	}
	return "", nil
}

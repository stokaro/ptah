// Package renderer defines rendering contracts shared by providers and the
// application pipeline. Built-in construction lives in engine/builtin.
package renderer

import "ptah.run/core/ast"

// RenderVisitor defines the interface for rendering AST nodes to SQL statements.
//
// This interface extends ast.Visitor with methods for managing renderer state
// and retrieving the generated SQL output.
//
// A RenderVisitor carries mutable output state, so one instance is not safe
// for concurrent use. Render clears that state before it visits its node, so
// each call returns only the SQL for the node it was handed and one renderer
// may be reused across many nodes sequentially.
//
// Implementations must answer a node the same way through every
// entry point: Render returns what Reset, VisitNode and Output return together,
// the same SQL or the same error. A nil node,
// or a nil pointer of a node type, is refused with
// [ptah.run/core/ptaherr.ErrInvalidSchemaDiff] on every target.
type RenderVisitor interface {
	ast.Visitor

	// Dialect returns the database dialect this renderer targets.
	Dialect() string

	// Reset clears the internal output buffer.
	Reset()

	// Output returns the current generated SQL output.
	Output() string

	// Render renders an AST node to SQL and returns the result.
	Render(node ast.Node) (string, error)

	// GetDialect returns the database dialect.
	GetDialect() string

	// GetOutput returns the current generated SQL output.
	GetOutput() string
}

package txrequire

import (
	"go.5x5.cz/ptah/core/ast"
	"go.5x5.cz/ptah/core/platform"
)

// NodeRequiresAutocommit reports whether a PLANNED node must be routed away
// from the transactional migration file.
//
// This is the generator's question, and its answer for `ALTER TYPE ... ADD
// VALUE` is deliberately stricter than [Analyze]'s. The two are not in
// conflict; they are asked about different inputs.
//
// A generated ADD VALUE always targets a type the database already has -- a
// diff that introduces an enum emits CREATE TYPE, and CREATE TYPE carries its
// values -- so the exception [Analyze] relies on, a type created in the same
// transaction, cannot arise here. The added value is therefore unusable until
// commit, and the node has to leave the transactional file.
//
// Relaxing this to match PostgreSQL 12's rule that the ALTER itself is
// transactional would put the ADD VALUE back with the statements that use the
// value, which is the shape that fails at apply with 55P04. The generator
// refuses such a mix rather than splitting it (migration/generator: "cannot be
// split automatically"), and that refusal depends on this answer.
func NodeRequiresAutocommit(dialect string, node ast.Node) bool {
	if !platform.IsPostgresFamily(dialect) {
		return false
	}
	switch typed := node.(type) {
	case *ast.IndexNode:
		return typed.Concurrently
	case *ast.DropIndexNode:
		// DROP INDEX CONCURRENTLY is refused inside a transaction block exactly
		// as CREATE INDEX CONCURRENTLY is.
		return typed.Concurrently
	case *ast.AlterTypeNode:
		return addsEnumValue(typed)
	default:
		return false
	}
}

func addsEnumValue(node *ast.AlterTypeNode) bool {
	for _, operation := range node.Operations {
		if _, ok := operation.(*ast.AddEnumValueOperation); ok {
			return true
		}
	}
	return false
}

package txrequire

import (
	"fmt"
	"strings"

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

// AutocommitKind classifies a node that [NodeRequiresAutocommit] routes out of
// the transactional file, by what a generator can do about it.
//
// The two known kinds move in OPPOSITE directions, which is why this is a
// classification rather than a boolean. `ALTER TYPE ... ADD VALUE` has to be
// committed BEFORE any statement that uses the value -- PostgreSQL answers
// 55P04 otherwise -- so its file leads. A concurrent index is built after the
// table it indexes, so its file follows.
//
// [KindUnsplittable] is not reachable from a plan today: every node
// [NodeRequiresAutocommit] answers true for is one of the other two. It exists
// so that a third kind added later is refused by name rather than dropped into
// whichever file happens to come first.
type AutocommitKind int

const (
	// KindUnsplittable has no ordered place. It is refused, by name.
	KindUnsplittable AutocommitKind = iota
	// KindEnumValue is `ALTER TYPE ... ADD VALUE`, which leads.
	KindEnumValue
	// KindConcurrentIndex is CREATE/DROP INDEX CONCURRENTLY, which follows.
	KindConcurrentIndex
)

// Kind classifies one node. It answers for the node alone and does not ask
// whether the dialect routes it out at all; callers reach it having already
// asked [NodeRequiresAutocommit].
func Kind(node ast.Node) AutocommitKind {
	switch typed := node.(type) {
	case *ast.IndexNode:
		if typed.Concurrently {
			return KindConcurrentIndex
		}
	case *ast.DropIndexNode:
		if typed.Concurrently {
			return KindConcurrentIndex
		}
	case *ast.AlterTypeNode:
		if addsEnumValue(typed) {
			return KindEnumValue
		}
	}
	return KindUnsplittable
}

// Describe names one node in a refusal. A statement kind alone -- "an ALTER
// TYPE" -- sends the reader looking through every type in the schema.
func Describe(node ast.Node) string {
	switch typed := node.(type) {
	case *ast.IndexNode:
		return "CREATE INDEX " + typed.Name
	case *ast.DropIndexNode:
		return "DROP INDEX " + typed.Name
	case *ast.AlterTypeNode:
		return "ALTER TYPE " + typed.Name
	default:
		return fmt.Sprintf("%T", node)
	}
}

// UnsplittableMixError refuses a plan whose non-transactional statements a
// generator has no ordered place for, naming them.
//
// It takes the whole non-transactional group and reports only the unsplittable
// members: a message listing the concurrent index beside the offender would
// send the operator after a statement the generator handles perfectly well.
//
// The message this replaced said only that the plan "mixes transactional
// statements with non-transactional statements that cannot be split
// automatically", which named neither the statement nor a way forward. A user
// who did not choose the transactionality and cannot see it in their schema was
// left with no migration and nothing to act on (stokaro/ptah#1714).
func UnsplittableMixError(nodes []ast.Node) error {
	names := make([]string, 0, len(nodes))
	for _, node := range nodes {
		if Kind(node) == KindUnsplittable {
			names = append(names, Describe(node))
		}
	}
	return fmt.Errorf(
		"generated migration mixes transactional statements with %s, which must run outside a transaction "+
			"and which this generator has no ordered place for. Apply that change in its own migration first, "+
			"then generate the rest",
		strings.Join(names, ", "))
}

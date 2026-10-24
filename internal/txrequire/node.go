package txrequire

import (
	"fmt"
	"slices"
	"strings"

	"ptah.run/core/ast"
	"ptah.run/core/plangraph"
	"ptah.run/core/platform/descriptor"
	"ptah.run/internal/ddltx"
	"ptah.run/internal/descriptortable"
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
//
// Owner operations carry explicit transaction requirements, independent of
// built-in dialect descriptors. A forbidden transaction routes the operation
// out of the transactional file. This does not establish an ordering for
// splitting a mixed plan; Kind keeps such operations unsplittable. On a
// dialect whose migrator opens no transaction (ddltx.NoTransaction) there is
// no transactional file to leave, so the requirement holds where the operation
// already is.
//
// For common operations, the engine's descriptor states: a
// concurrent index build or drop where its grammar has one
// (ConcurrentIndexes), an enum value where it commits before a use
// (Migration.EnumValuesCommitFirst), and a constraint validation where the
// online grammar is NOT VALID (OnlineAlter).
func NodeRequiresAutocommit(dialect string, node ast.Node) bool {
	rules := autocommitRules.Get(dialect)
	if !rules.noTransaction && slices.Contains(ast.OwnerTransactions(node), plangraph.TransactionForbidden) {
		return true
	}
	switch typed := node.(type) {
	case *ast.IndexNode:
		return rules.concurrentIndexes && typed.Concurrently
	case *ast.DropIndexNode:
		// DROP INDEX CONCURRENTLY is refused inside a transaction block exactly
		// as CREATE INDEX CONCURRENTLY is.
		return rules.concurrentIndexes && typed.Concurrently
	case *ast.AlterTypeNode:
		return rules.enumValues && addsEnumValue(typed)
	case *ast.AlterTableNode:
		// A constraint added NOT VALID and validated in the same transaction
		// gains nothing: the ACCESS EXCLUSIVE lock the addition takes is held
		// to commit, so the weaker lock the validation asks for is held behind
		// it and writers wait either way. The pair is worth writing only when
		// the two statements commit separately.
		return rules.constraintValidation && validatesConstraint(typed)
	default:
		return false
	}
}

// autocommitNodes are the statements a dialect's descriptor sends out of the
// transactional migration file.
type autocommitNodes struct {
	concurrentIndexes    bool
	enumValues           bool
	constraintValidation bool
	// noTransaction says the migrator runs every statement unwrapped, so no
	// statement can be inside a transaction it would have to leave.
	noTransaction bool
}

// autocommitRules holds the answer of every dialect. A generator asks once
// per planned node, so the answers are derived once per registry rather than
// copied out of a descriptor per node.
var autocommitRules = descriptortable.New(func(described descriptor.Dialect) autocommitNodes {
	return autocommitNodes{
		concurrentIndexes:    described.ConcurrentIndexes,
		enumValues:           described.Migration.EnumValuesCommitFirst,
		constraintValidation: described.OnlineAlter == descriptor.OnlineAlterNotValid,
		noTransaction:        ddltx.Class(described.DDLTransactions) == ddltx.NoTransaction,
	}
})

// validatesConstraint reports whether the ALTER completes a constraint added
// NOT VALID.
func validatesConstraint(node *ast.AlterTableNode) bool {
	for _, operation := range node.Operations {
		if _, ok := operation.(*ast.ValidateConstraintOperation); ok {
			return true
		}
	}
	return false
}

// onlyValidatesConstraints reports whether every operation of the ALTER is a
// validation. An ALTER that also adds the constraint it validates is the pair
// that has to stay together, and is not this.
func onlyValidatesConstraints(node *ast.AlterTableNode) bool {
	return len(node.Operations) > 0 && !slices.ContainsFunc(node.Operations, func(operation ast.AlterOperation) bool {
		_, ok := operation.(*ast.ValidateConstraintOperation)
		return !ok
	})
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
// A statement that only validates a constraint the database already holds NOT
// VALID is [KindConstraintValidation]. It scans rows under a weaker lock and
// depends on nothing else in the plan, so it follows every other statement in a
// file of its own.
//
// [KindUnsplittable] is what a constraint pair asking for its own transaction
// lands on, and the refusal is the answer: a NOT VALID constraint and the
// statement that validates it have to stay together in one ALTER TABLE and
// have to commit apart from the statements around them, which is one migration
// of their own rather than a place in this one.
type AutocommitKind int

const (
	// KindUnsplittable has no ordered place. It is refused, by name.
	KindUnsplittable AutocommitKind = iota
	// KindEnumValue is `ALTER TYPE ... ADD VALUE`, which leads.
	KindEnumValue
	// KindConcurrentIndex is CREATE/DROP INDEX CONCURRENTLY, which follows.
	KindConcurrentIndex
	// KindConstraintValidation is an ALTER TABLE that only validates
	// constraints already in the database, which follows.
	KindConstraintValidation
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
	case *ast.AlterTableNode:
		if onlyValidatesConstraints(typed) {
			return KindConstraintValidation
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
	case *ast.AlterTableNode:
		return "ALTER TABLE " + typed.Name
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

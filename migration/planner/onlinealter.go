package planner

import (
	"ptah.run/core/ast"
	"ptah.run/core/platform"
	"ptah.run/core/platform/capability"
)

// requestOnlineAlter rewrites a plan so the server is asked to apply it
// without blocking the writes already running against the tables.
//
// The two engine families ask in different grammars, and the difference is who
// decides. A MySQL-family statement carries `ALGORITHM=INPLACE, LOCK=NONE`,
// which the server refuses when it cannot honor it, so the plan either runs
// the way it said or does not run. PostgreSQL has no such clause, so the safe
// form is generated instead: a constraint is added `NOT VALID` and validated
// by a second statement whose scan takes a weaker lock.
//
// Deliberately, the choice of clause does not consult the cost model in
// migration/lint. That model says what a measured release line did with a
// measured statement, and the clause exists to stop Ptah from having to be
// right about that: a model consulted here can be wrong, can go stale on the
// next patch release, and answers for a table whose shape it has not seen. The
// server answers for the statement in front of it. What the measurement is for
// is knowing that the request is meaningful and what its refusal looks like --
// see [capability.AlterTableAlgorithmLock].
//
// A target without the grammar is left alone here rather than rewritten and
// dropped in the renderer, so an offline plan for such a target reads exactly
// as it did before anything was asked for.
func requestOnlineAlter(nodes []ast.Node, dialect string, caps capability.Capabilities) []ast.Node {
	if isMySQLFamily(dialect) && caps.Has(capability.AlterTableAlgorithmLock) {
		return requestInPlaceAlgorithm(nodes)
	}
	if platform.IsPostgresFamily(dialect) && caps.Has(capability.AddConstraintNotValid) {
		return requestUnvalidatedConstraints(nodes)
	}
	return nodes
}

// isMySQLFamily reports whether the dialect takes the MySQL grammar. The
// capability decides whether the clause is written; this decides which of the
// two rewrites below is the one for the target.
func isMySQLFamily(dialect string) bool {
	switch platform.NormalizeDialect(dialect) {
	case platform.MySQL, platform.MariaDB:
		return true
	default:
		return false
	}
}

// requestInPlaceAlgorithm asks every ALTER TABLE for the in-place algorithm
// with no lock.
//
// INPLACE with LOCK=NONE rather than INSTANT: an INSTANT change is accepted
// under this request too -- measured on MySQL 8.4.6 and MariaDB 12.3.3, every
// form either engine applies instantly also accepts `ALGORITHM=INPLACE,
// LOCK=NONE` -- so asking for the weaker of the two refuses exactly the
// statements that would block writes, and no others. Asking for INSTANT would
// refuse a change that runs online and blocks nothing.
func requestInPlaceAlgorithm(nodes []ast.Node) []ast.Node {
	for _, node := range nodes {
		alter, ok := node.(*ast.AlterTableNode)
		if !ok {
			continue
		}
		alter.Algorithm = "INPLACE"
		alter.Lock = "NONE"
	}
	return nodes
}

// requestUnvalidatedConstraints splits every constraint addition PostgreSQL
// can add without a scan into the pair that does it: the constraint arrives
// NOT VALID, and a second statement validates it.
//
// CHECK and FOREIGN KEY only. They are the constraints whose cost is the scan
// of the rows already in the table; a primary key or a unique constraint
// builds an index, which NOT VALID says nothing about, and PostgreSQL refuses
// the clause there.
//
// An unnamed constraint is left alone: the validation names what it completes,
// and a plan that emitted VALIDATE CONSTRAINT "" would be a statement the
// server refuses for a reason that has nothing to do with the table.
func requestUnvalidatedConstraints(nodes []ast.Node) []ast.Node {
	for _, node := range nodes {
		alter, ok := node.(*ast.AlterTableNode)
		if !ok {
			continue
		}
		alter.Operations = withValidations(alter.Operations)
	}
	return nodes
}

func withValidations(operations []ast.AlterOperation) []ast.AlterOperation {
	rewritten := make([]ast.AlterOperation, 0, len(operations))
	for _, operation := range operations {
		add, ok := operation.(*ast.AddConstraintOperation)
		if !ok || !validatesSeparately(add.Constraint) {
			rewritten = append(rewritten, operation)
			continue
		}
		add.Constraint.NotValid = true
		rewritten = append(rewritten, add,
			&ast.ValidateConstraintOperation{ConstraintName: add.Constraint.Name})
	}
	return rewritten
}

func validatesSeparately(constraint *ast.ConstraintNode) bool {
	if constraint == nil || constraint.Name == "" {
		return false
	}
	return constraint.Type == ast.CheckConstraint || constraint.Type == ast.ForeignKeyConstraint
}

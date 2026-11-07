package mysql

import (
	"ptah.run/core/ast"
	"ptah.run/core/difftypes"
	"ptah.run/core/platform/identifier"
	"ptah.run/internal/indexscope"
)

// takeoverLedger is the removals the plan's additions take over from that a statement
// of the plan has dropped so far; see [difftypes.IndexChange.Replaces]. Several additions
// may name one index, since each of them would make the server drop it, and the first
// statement the plan writes for one of them drops it. A later one leaves it out: the
// index is gone by then, and DROP INDEX answers Error 1091. The planner writes the
// additions in this order: the takeovers that go ahead of the column changes, the
// column changes, the constraint additions, and the index takeovers that follow the
// constraint removals.
type takeoverLedger struct {
	semantics identifier.Semantics
	dropped   map[difftypes.IndexRef]struct{}
}

func newTakeoverLedger(semantics identifier.Semantics) *takeoverLedger {
	return &takeoverLedger{semantics: semantics, dropped: make(map[difftypes.IndexRef]struct{})}
}

// claim answers the removals of refs no statement has dropped yet, in order, and
// records them as dropped by the statement the caller writes.
func (t *takeoverLedger) claim(refs []difftypes.IndexRef) []difftypes.IndexRef {
	var claimed []difftypes.IndexRef
	for _, ref := range refs {
		identity := indexscope.IdentityKeyWithSemantics(t.semantics, ref)
		if _, done := t.dropped[identity]; done {
			continue
		}
		t.dropped[identity] = struct{}{}
		claimed = append(claimed, ref)
	}
	return claimed
}

// drops answers the clauses that drop what claim answers for refs, for the statement
// of the addition that takes over from them. DROP INDEX drops a UNIQUE constraint too,
// as the key is an index of its name on MySQL and MariaDB.
func (t *takeoverLedger) drops(refs []difftypes.IndexRef) []ast.AlterOperation {
	claimed := t.claim(refs)
	operations := make([]ast.AlterOperation, 0, len(claimed))
	for _, ref := range claimed {
		operations = append(operations, &ast.DropConstraintOperation{ConstraintName: ref.Name, Unique: true})
	}
	return operations
}

// withTakeovers is the ALTER TABLE that drops what drops holds and adds addition in one
// statement, or adds it alone. The drops are what an addition takes over from, and the
// server checks the foreign keys against the table the whole statement leaves:
// `ALTER TABLE child DROP INDEX c_fk, ADD CONSTRAINT u UNIQUE (pid)` leaves a key over
// pid an index at every moment, where DROP INDEX alone answers Error 1553. Measured on
// MySQL 8.4 and 26.10 and MariaDB 12.3.
func withTakeovers(table string, drops []ast.AlterOperation, addition ast.AlterOperation) *ast.AlterTableNode {
	if len(drops) == 0 {
		return &ast.AlterTableNode{Name: table, Operations: []ast.AlterOperation{addition}}
	}
	return oneStatement(table, append(drops, addition)...)
}

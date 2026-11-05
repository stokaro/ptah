package postgres

import (
	"strings"

	"ptah.run/core/ast"
	"ptah.run/core/difftypes"
	"ptah.run/core/objectidentity"
	"ptah.run/core/planext"
	"ptah.run/core/schemamodel"
	"ptah.run/dialect/postgres/pgtable"
	"ptah.run/feature/pgpolicy"
	"ptah.run/internal/planner/guards"
	"ptah.run/internal/tableref"
)

// The statement style decides how a plan's statements spell their comments,
// existence guards and CASCADE. A feature owner may make an implicit drop
// explicit early enough to release a dependency before a common drop.
//
// Comments. A comment that only describes the statements beside it is an
// [ast.NewDescriptiveComment] node, which migration/planner leaves out of an
// exact plan, or a line comment a drop node carries, which this planner sets
// only when [Planner.describes]: the renderer writes a node's line comment and
// knows nothing of styles. A comment that reports what the plan left out or
// cannot do is an [ast.NewComment] and is written in every style.
//
// CASCADE. The annotated style drops with CASCADE wherever the target takes it.
// The exact style drops with CASCADE only where the plan relies on it: a
// dependent the plan knows of is still in the database when the drop runs,
// because the plan drops it later or not at all. Each call site of
// [Planner.cascades] says what it relies on. A dependent the plan does not know
// of, such as a view in a schema the run leaves out, is not a reason: without
// CASCADE the server refuses the drop and names it, where CASCADE would remove
// an object nobody declared.
//
// Guards. The annotated style writes IF EXISTS on a drop and IF NOT EXISTS on a
// new index. The exact style keeps a guard only where an earlier statement of
// the same plan may already have removed the object, or created it; see
// [settleGuards].

// exact reports whether the plan is spelled in [planext.StatementStyleExact].
func (p *Planner) exact() bool {
	return p.statements == planext.StatementStyleExact
}

// describes reports whether the plan writes the comments that only describe
// its statements.
func (p *Planner) describes() bool {
	return !p.exact()
}

// cascades reports whether a drop carries CASCADE, given whether the plan
// relies on it to remove a dependent it does not drop itself before the drop.
func (p *Planner) cascades(relied bool) bool {
	if !p.dropsWithCascade() {
		return false
	}
	return relied || !p.exact()
}

// columnDropRelies reports whether dropping column of tableDiff relies on
// CASCADE.
//
// The objects a column drop needs CASCADE for are the ones outside the
// table's own indexes and constraints, which PostgreSQL drops with the column
// anyway. This plan removes such an object before the column when it changes
// or removes it in an earlier step: a policy, a view, a trigger, an index, a
// foreign key, and a generated column of the same table, which goes before
// the columns it reads, and a routine that reads the column; see
// [routinesAhead]. It removes these only after the column, so a drop relies on
// CASCADE when the plan holds one:
//
//   - a captured object of a table it removes that reads the table, such as a
//     row-level security policy;
//   - an owner operation that may drop a standalone object in a later window,
//     whose object reads the table's columns or cannot say; see
//     [removedFeatureReadsColumns].
func columnDropRelies(diff *difftypes.SchemaDiff, tableDiff difftypes.TableDiff, droppedPolicies map[objectidentity.Key]bool) bool {
	return removedTableReadsExcept(diff, tableDiff.TableName, nil, droppedPolicies) ||
		removedFeatureReadsColumns(diff, tableDiff.TableName)
}

// tableDropRelies reports whether dropping the table name relies on CASCADE,
// given the removed tables dropped after it.
//
// The table's indexes, constraints, triggers and policies go with it, its
// foreign keys and the ones that point at it are dropped by name first, and
// the views that read it and the routines that depend on it are removed before
// it; see [routinesAhead]. What the plan removes only after the table is:
//
//   - a captured object of a removed table dropped after this one, such as a
//     policy that reads this table. The tables are ordered so the one holding
//     the policy goes first, and only a cycle leaves one after;
//   - an owner operation that may drop a standalone object that reads the
//     table, or one whose object cannot say; see [removedFeatureReads]. A
//     removed type whose attribute is the table's row type is decided on the
//     scheduled plan by [Planner.cascadeForLaterTypeDrops].
func tableDropRelies(diff *difftypes.SchemaDiff, name string, droppedBefore map[string]bool, droppedPolicies map[objectidentity.Key]bool) bool {
	return removedTableReadsExcept(diff, name, droppedBefore, droppedPolicies) ||
		removedFeatureReads(diff, name)
}

func isGenerated(column schemamodel.Field) bool {
	return strings.TrimSpace(column.GeneratedExpression) != ""
}

// settleGuards removes from an exact plan every IF EXISTS and IF NOT EXISTS no
// earlier statement of the plan makes necessary; see [guards.Settle]. An owner's
// drop of a policy or an exclusion constraint removes nothing a later drop names,
// so a drop after it needs no guard for its sake.
func settleGuards(nodes []ast.Node) {
	guards.Settle(nodes, guards.Options{HarmlessOwner: isolatedOwnerDrop})
}

// isolatedOwnerDrop is an owner statement whose only effect on object existence
// is to remove the policy or the exclusion constraint it names, with the index
// that enforces the constraint. No later statement drops either, so it cannot
// make a later view, column or table drop need an existence guard.
func isolatedOwnerDrop(node ast.Node) bool {
	if ast.PlacementOf(node) != ast.IsolatedExtension {
		return false
	}
	operations := ast.OwnerOperations(node)
	if len(operations) != 1 {
		return false
	}
	switch operation := operations[0].(type) {
	case *pgpolicy.PolicyOperation:
		return operation != nil && operation.Change.Before != nil && operation.Change.After == nil
	case *pgtable.AlterExclusion:
		return operation != nil && operation.Change.Before != nil && operation.Change.After == nil
	default:
		return false
	}
}

// bareName is name without its schema, folded to lower case. Two spellings of
// one object compare equal; two objects in different schemas may too, which
// only keeps a guard that might be needed.
func bareName(name string) string {
	if ref, ok := tableref.Parse(name); ok {
		return strings.ToLower(ref.Name)
	}
	return strings.ToLower(strings.TrimSpace(name))
}

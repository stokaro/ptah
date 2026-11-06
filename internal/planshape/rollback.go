package planshape

import (
	"math"
	"slices"
	"strings"

	"ptah.run/core/ast"
	"ptah.run/core/difftypes"
	"ptah.run/internal/planner/notnullname"
)

// AlignRollback returns rollback, the statements that undo forward, in the catalog's
// shape for dialect and as close to forward read backwards as it can be shown to run: a
// rollback is planned as a diff of its own, so it does not come in that order. Steps that
// depend on each other keep the order rollback planned for them, whatever forward's order
// says, because a forward file read backwards is not what makes a rollback run.
//
// Only a run of table statements moves, as in [Apply]: a statement of another kind keeps
// its place, and a statement that undoes nothing in forward stays ahead of the run. The
// drop of a constraint forward wrote inside a new table goes with the table, unless
// something between them needs the constraint gone.
//
// diff is what the rollback compared. A statement whose access it and forward do not settle
// keeps its place.
func AlignRollback(dialect string, forward, rollback []ast.Node, diff *difftypes.SchemaDiff) []ast.Node {
	form, spelling, ok := catalogRules(dialect)
	if !ok {
		return rollback
	}
	// The planner freed the names its own order needs, and this order differs.
	rollback = notnullname.Unfree(rollback, diff)
	facts := newPlanFacts()
	facts.readDiff(diff)
	facts.readApplied(forward)
	facts.readPlan(rollback)
	how := arrangementFor(form, facts)
	how.redundant = inlinedConstraints(forward)
	index := how.indexForward(forward)
	how.reorder = index.namesFreedFirst(how)
	how.places = index.places(how)
	out := notnullname.Free(arrange(inlineConstraints(rollback), how), diff)
	for _, node := range out {
		respell(node, spelling)
	}
	return out
}

// forwardIndex says where forward holds what a rollback step undoes.
type forwardIndex map[string]int

// indexForward records the position of each statement and each operation of a plan,
// under the key of the step that undoes it.
func (how arrangement) indexForward(forward []ast.Node) forwardIndex {
	index := make(forwardIndex)
	position := 0
	record := func(key string) {
		if key != "" {
			index[key] = position
		}
		position++
	}
	for _, node := range forward {
		switch n := node.(type) {
		case *ast.CreateTableNode:
			record("drop table " + tableKey(n.Name))
		case *ast.DropTableNode:
			for _, name := range n.TableNames() {
				record("create table " + tableKey(name))
			}
		case *ast.IndexNode:
			record("drop index " + indexKey(n.Table, n.Name))
		case *ast.DropIndexNode:
			record("create index " + indexKey(n.Table, n.Name))
		case *ast.AlterTableNode:
			for _, operation := range writtenOperations(n) {
				record(how.undoingKey(n.Name, operation))
			}
		}
	}
	return index
}

func indexKey(table, name string) string { return tableKey(table) + " " + name }

// writtenOperations is an ALTER TABLE's operations in the order a reader of the file
// sees them where they fit one statement.
func writtenOperations(alter *ast.AlterTableNode) []ast.AlterOperation {
	if len(alter.OneStatement) > 0 {
		return alter.OneStatement
	}
	return alter.Operations
}

// places gives each step of a rollback the reverse of the place of what it undoes. A step
// that undoes nothing there goes ahead of the ones that do.
func (i forwardIndex) places(how arrangement) func(run []*step) {
	return func(run []*step) {
		for _, s := range run {
			at, found := i[how.ownKey(s)]
			if !found {
				s.place = place{column: math.MinInt}
				continue
			}
			s.place = place{column: -at}
		}
	}
}

// namesFreedFirst moves a step that takes a name behind the step that frees it, where the
// rollback plans them the other way round. A rollback adds what it restores ahead of what
// it drops, and forward ran the two the other way: it freed the name, then took it. Forward
// read backwards frees it in turn, so the server takes the name only once it is free. A step
// moves only over steps it shares nothing else with.
func (i forwardIndex) namesFreedFirst(how arrangement) func(run []*step) []*step {
	return func(run []*step) []*step {
		out := slices.Clone(run)
		for at := len(out) - 2; at >= 0; at-- {
			takes := out[at]
			frees := slices.IndexFunc(out[at+1:], func(later *step) bool { return i.freesANameFor(how, later, takes) })
			if frees < 0 {
				continue
			}
			over := out[at+1 : at+1+frees+1]
			if !slices.ContainsFunc(over, func(later *step) bool { return takes.access.conflictsBesidesNames(later.access) }) {
				moved := append(slices.Clone(over), takes)
				copy(out[at:], moved)
			}
		}
		return out
	}
}

// freesANameFor reports two steps that share a name and that undo steps of forward in
// the order frees then takes.
func (i forwardIndex) freesANameFor(how arrangement, frees, takes *step) bool {
	undoneFree, found := i[how.ownKey(frees)]
	if !found {
		return false
	}
	undoneTake, found := i[how.ownKey(takes)]
	return found && undoneFree > undoneTake && frees.access.sharesAName(takes.access)
}

// ownKey names a step of a rollback. undoingKey names, for a step of forward, the
// rollback step that undoes it, so the two meet in one map.
func (how arrangement) ownKey(s *step) string {
	switch n := s.node.(type) {
	case *ast.CreateTableNode:
		return "create table " + tableKey(n.Name)
	case *ast.DropTableNode:
		return "drop table " + tableKey(s.table)
	case *ast.IndexNode:
		return "create index " + indexKey(n.Table, n.Name)
	case *ast.DropIndexNode:
		return "drop index " + indexKey(n.Table, n.Name)
	}
	switch op := s.operation.(type) {
	case *ast.AddColumnOperation:
		return tableKey(s.table) + " add column " + op.Column.Name
	case *ast.DropColumnOperation:
		return tableKey(s.table) + " drop column " + op.ColumnName
	case *ast.ModifyColumnOperation:
		return tableKey(s.table) + " modify column " + op.Column.Name
	case *ast.AddConstraintOperation:
		if op.Constraint == nil {
			return ""
		}
		return tableKey(s.table) + " add constraint " + addedConstraintKey(op.Constraint)
	case *ast.DropConstraintOperation:
		return tableKey(s.table) + " drop constraint " + how.droppedConstraintKey(s.table, op)
	default:
		return ""
	}
}

func (how arrangement) undoingKey(table string, operation ast.AlterOperation) string {
	switch op := operation.(type) {
	case *ast.AddColumnOperation:
		return tableKey(table) + " drop column " + op.Column.Name
	case *ast.DropColumnOperation:
		return tableKey(table) + " add column " + op.ColumnName
	case *ast.ModifyColumnOperation:
		return tableKey(table) + " modify column " + op.Column.Name
	case *ast.AddConstraintOperation:
		if op.Constraint == nil {
			return ""
		}
		return tableKey(table) + " drop constraint " + addedConstraintKey(op.Constraint)
	case *ast.DropConstraintOperation:
		return tableKey(table) + " add constraint " + how.droppedConstraintKey(table, op)
	default:
		return ""
	}
}

// primaryKeyKey stands for a table's primary key, which one direction may name and the
// other not: the statement that adds one names none, and the server gives it a name.
const primaryKeyKey = "\x00primary key"

func addedConstraintKey(constraint *ast.ConstraintNode) string {
	if constraint.Type == ast.PrimaryKeyConstraint {
		return primaryKeyKey
	}
	return strings.ToLower(constraint.Name)
}

func (how arrangement) droppedConstraintKey(table string, drop *ast.DropConstraintOperation) string {
	if how.droppedKind(table, drop) == ast.PrimaryKeyConstraint {
		return primaryKeyKey
	}
	return strings.ToLower(drop.ConstraintName)
}

// inlinedConstraints is the constraints forward wrote inside each table it creates.
func inlinedConstraints(forward []ast.Node) map[string]map[string]bool {
	inlined := make(map[string]map[string]bool)
	for _, node := range forward {
		table, ok := node.(*ast.CreateTableNode)
		if !ok {
			continue
		}
		names := make(map[string]bool, len(table.Constraints))
		for _, constraint := range table.Constraints {
			names[constraint.Name] = true
		}
		inlined[tableKey(table.Name)] = names
	}
	return inlined
}

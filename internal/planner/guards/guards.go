// Package guards settles the existence guards of a plan spelled in
// [ptah.run/core/planext.StatementStyleExact]. It reads the statements a dialect
// planner produced, in their order, and every planner calls it the same way: the
// guard a statement needs follows from what the statements before it may have
// removed, which does not depend on the engine.
package guards

import (
	"strings"

	"ptah.run/core/ast"
	"ptah.run/core/schemamodel"
	"ptah.run/internal/tableref"
)

// Settle removes from an exact plan every IF EXISTS and IF NOT EXISTS no earlier
// statement of the plan makes necessary, and keeps the rest.
//
// A drop keeps IF EXISTS when an earlier statement may already have removed
// its object:
//
//   - a drop with CASCADE, a statement written as raw SQL, or an owner
//     operation, any of which may remove objects this pass cannot name; every
//     drop after one keeps its guard;
//   - DROP TABLE, which takes the table's indexes, constraints, triggers and
//     policies, and the sequences its columns own;
//   - ALTER TABLE ... DROP COLUMN, which takes the indexes and constraints of
//     the table that use the column, and the sequence the column owns;
//   - DROP VIEW and DROP MATERIALIZED VIEW, which take the indexes and
//     triggers on the view;
//   - DROP TRIGGER, whose statement also drops the function Ptah wrote for
//     the trigger;
//   - an earlier drop of the same object.
//
// A new index never keeps IF NOT EXISTS: the plan creates each index once.
// A column drop carries no guard in either style, so it has none to settle.
// An index that backs a constraint is dropped as the constraint, and every
// other index drop comes before the constraint step, so no DROP INDEX follows
// a DROP CONSTRAINT that took its index.
//
// Names are compared without their schema and without case, so an object that
// might be the one an earlier statement removed keeps its guard. The pass
// leaves alone CREATE SCHEMA IF NOT EXISTS, which the plan writes for every
// schema an added object names other than the connection's own, whether or
// not it exists, and every guard the declaration spells itself, such as IF
// NOT EXISTS on a sequence.
func Settle(nodes []ast.Node, opts Options) {
	seen := guardState{opts: opts}
	seen.init()
	for _, node := range nodes {
		seen.settle(node)
	}
}

// Options is what a planner knows about the statements only it writes.
type Options struct {
	// HarmlessOwner reports an owner statement that removes no object a later
	// drop names. It cannot make a later drop need a guard. Nil treats every
	// owner statement as one that may remove objects the pass cannot name.
	HarmlessOwner func(ast.Node) bool
	// HarmlessRaw reports a raw statement that removes no object a later drop
	// names, such as a pragma, a copy of a table's rows or an enum rebuild. Nil
	// treats every raw statement as one that may.
	HarmlessRaw func(*ast.RawSQLNode) bool
}

// guardState is what [Settle] knows the statements before the current one
// removed or created.
type guardState struct {
	opts Options
	// anything is set once a statement may have removed objects the pass
	// cannot name.
	anything bool
	// tables, relations and triggerFunctions hold the bare names of the
	// tables, the views and materialized views, and the trigger functions
	// dropped.
	tables, relations, triggerFunctions map[string]bool
	// shrunk holds the tables that lost a column.
	shrunk map[string]bool
	// dropped holds every object dropped by name, keyed by kind and name.
	dropped map[string]bool
	// sequences is set once a table or a column was dropped, which may take a
	// sequence it owns.
	sequences bool
}

func (g *guardState) init() {
	g.tables = make(map[string]bool)
	g.relations = make(map[string]bool)
	g.triggerFunctions = make(map[string]bool)
	g.shrunk = make(map[string]bool)
	g.dropped = make(map[string]bool)
}

func (g *guardState) settle(node ast.Node) {
	if ast.PlacementOf(node) != ast.NoExtension {
		if g.opts.HarmlessOwner != nil && g.opts.HarmlessOwner(node) {
			return
		}
		g.anything = true
		return
	}
	switch n := node.(type) {
	case *ast.DropTableNode:
		g.settleDropTable(n)
	case *ast.AlterTableNode:
		g.settleAlterTable(n)
	case *ast.DropIndexNode:
		g.settleDropIndex(n)
	case *ast.IndexNode:
		// The plan creates each index once, so nothing before it can have.
		n.IfNotExists = false
	case *ast.DropViewNode:
		g.dropRelation(n.Name, &n.IfExists, n.Cascade)
	case *ast.DropMaterializedViewNode:
		g.dropRelation(n.Name, &n.IfExists, n.Cascade)
	case *ast.DropTriggerNode:
		g.settleDropTrigger(n)
	case *ast.DropFunctionNode:
		g.settleDropFunction(n)
	case *ast.DropSequenceNode:
		g.settleDropSequence(n)
	case *ast.DropTypeNode:
		g.dropNamed("type", n.Name, &n.IfExists, n.Cascade)
	case *ast.DropRoleNode:
		g.dropNamed("role", n.Name, &n.IfExists, false)
	case *ast.RawSQLNode:
		g.anything = g.anything || g.opts.HarmlessRaw == nil || !g.opts.HarmlessRaw(n)
	case *ast.DoBlockNode, *ast.OpaqueRoutineNode:
		g.anything = true
	}
}

func (g *guardState) settleDropIndex(n *ast.DropIndexNode) {
	n.IfExists = n.IfExists && (g.mayBeGone("index", n.Name) || g.onRemoved(n.Table))
	g.drop("index:"+bareName(n.Name), n.Cascade)
}

func (g *guardState) settleDropFunction(n *ast.DropFunctionNode) {
	relied := false
	for _, target := range n.Targets() {
		relied = relied || g.mayBeGone("function", target.Name) || g.triggerFunctions[bareName(target.Name)]
	}
	n.IfExists = n.IfExists && relied
	for _, target := range n.Targets() {
		g.drop("function:"+bareName(target.Name), n.Cascade)
	}
}

func (g *guardState) settleDropSequence(n *ast.DropSequenceNode) {
	n.IfExists = n.IfExists && (g.mayBeGone("sequence", n.Name) || g.sequences)
	g.drop("sequence:"+bareName(n.Name), n.Cascade)
}

func (g *guardState) settleDropTable(n *ast.DropTableNode) {
	relied := false
	for _, name := range n.TableNames() {
		relied = relied || g.mayBeGone("table", name)
	}
	n.IfExists = n.IfExists && relied
	for _, name := range n.TableNames() {
		g.tables[bareName(name)] = true
		g.drop("table:"+bareName(name), n.Cascade)
	}
	g.sequences = true
}

func (g *guardState) settleDropTrigger(n *ast.DropTriggerNode) {
	table := bareName(n.Table)
	n.IfExists = n.IfExists && (g.mayBeGone("trigger", scopedName(n.Table, n.Name)) || g.tables[table] || g.relations[table])
	g.drop("trigger:"+scopedName(n.Table, n.Name), n.Cascade)
	if n.ExternalFunction {
		return
	}
	function := n.FunctionName
	if function == "" {
		function = schemamodel.Trigger{Table: n.Table, Name: n.Name}.FunctionName()
	}
	g.triggerFunctions[bareName(function)] = true
}

// drop records the drop of the object keyed key, and whether the drop
// cascades.
func (g *guardState) drop(key string, cascade bool) {
	g.dropped[key] = true
	g.anything = g.anything || cascade
}

func (g *guardState) settleAlterTable(n *ast.AlterTableNode) {
	table := bareName(n.Name)
	cascaded := false
	for _, operation := range n.Operations {
		switch op := operation.(type) {
		case *ast.DropColumnOperation:
			// The planner writes no guard on a column drop, so there is none
			// to settle; the drop is recorded for the statements after it.
			g.shrunk[table] = true
			g.sequences = true
			cascaded = cascaded || op.Cascade
		case *ast.DropConstraintOperation:
			key := "constraint:" + table + "." + strings.ToLower(op.ConstraintName)
			op.IfExists = op.IfExists && (g.anything || g.tables[table] || g.shrunk[table] || g.dropped[key])
			g.dropped[key] = true
		}
	}
	g.anything = g.anything || cascaded
}

// dropRelation settles the guard of a view or materialized view drop and
// records the drop.
func (g *guardState) dropRelation(name string, ifExists *bool, cascade bool) {
	g.dropNamed("relation", name, ifExists, cascade)
	g.relations[bareName(name)] = true
}

// dropNamed settles the guard of a drop no other statement of the plan takes
// implicitly, which only an earlier drop of the same object, or a statement
// that may remove anything, makes necessary.
func (g *guardState) dropNamed(kind, name string, ifExists *bool, cascade bool) {
	*ifExists = *ifExists && g.mayBeGone(kind, name)
	g.drop(kind+":"+bareName(name), cascade)
}

// mayBeGone reports whether an earlier statement may have removed the object
// of kind keyed key: a bare name, or a [scopedName] for an object named within
// its table.
func (g *guardState) mayBeGone(kind, key string) bool {
	if g.anything {
		return true
	}
	if kind == "trigger" {
		return g.dropped[kind+":"+key]
	}
	return g.dropped[kind+":"+bareName(key)]
}

// scopedName keys an object whose name is unique within its table, such as a
// trigger.
func scopedName(table, name string) string {
	return bareName(table) + "/" + strings.ToLower(name)
}

// onRemoved reports whether an earlier statement removed the relation an index
// is on, or a column of it, either of which may have taken the index.
func (g *guardState) onRemoved(table string) bool {
	name := bareName(table)
	return g.tables[name] || g.shrunk[name] || g.relations[name]
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

package postgres

import (
	"strings"

	"ptah.run/core/ast"
	"ptah.run/core/difftypes"
	"ptah.run/core/planext"
	"ptah.run/core/schemamodel"
	"ptah.run/internal/deporder"
	"ptah.run/internal/tableref"
)

// The statement style decides how a plan's statements spell their comments,
// existence guards and CASCADE, and never which objects the plan creates,
// changes or drops.
//
// Comments. A comment that only describes the statements beside it is an
// [ast.NewDescriptiveComment] node, which migration/planner leaves out of an
// exact plan, or a line comment a drop node carries, which this planner sets
// only when [Planner.describes]: the renderer writes a node's line comment and
// knows nothing of styles. A comment that reports what the plan left out or
// cannot do is an [ast.NewComment] and is written in every style.
//
// CASCADE. Ptah's own style drops with CASCADE wherever the target takes it.
// The exact style drops with CASCADE only where the plan relies on it: a
// dependent the plan knows of is still in the database when the drop runs,
// because the plan drops it later or not at all. Each call site of
// [Planner.cascades] says what it relies on. A dependent the plan does not know
// of, such as a view in a schema the run leaves out, is not a reason: without
// CASCADE the server refuses the drop and names it, where CASCADE would remove
// an object nobody declared.
//
// Guards. Ptah's own style writes IF EXISTS on a drop and IF NOT EXISTS on a
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

// columnDropRelies reports whether dropping a column of tableDiff relies on
// CASCADE.
//
// The objects a column drop needs CASCADE for are the ones outside the
// table's own indexes and constraints, which PostgreSQL drops with the column
// anyway. This plan removes such an object before the column when it changes
// or removes it in an earlier step: a policy, a view it replaces, an index, a
// foreign key. It removes these only after the column, so a drop relies on
// CASCADE when the plan holds one:
//
//   - a trigger it removes from the table, which may fire on UPDATE OF the
//     column;
//   - a view or materialized view it removes whose definition reads the
//     table, unless the step before the column changes dropped it already;
//   - another removed column of the table that is generated, which may be
//     computed from this one;
//   - a routine it removes, whose body may read the column;
//   - an owner operation on a standalone object, which may be dropped in a
//     later window and may read the column.
func columnDropRelies(diff *difftypes.SchemaDiff, tableDiff difftypes.TableDiff) bool {
	table := bareName(tableDiff.TableName)
	for _, trigger := range diff.TriggersRemoved {
		if bareName(trigger.TableName) == table {
			return true
		}
	}
	owned := planViewRebuild(diff).owns()
	for _, object := range removedViewLikes(diff) {
		if !owned.has(object) && deporder.ReferencesIdentifier(object.Body, table) {
			return true
		}
	}
	for _, column := range tableDiff.ColumnsRemoved {
		if isGenerated(column) && len(tableDiff.ColumnsRemoved) > 1 {
			return true
		}
	}
	return removesRoutines(diff) || len(diff.FeatureChanges) > 0
}

// tableDropRelies reports whether dropping a table relies on CASCADE.
//
// The table's indexes, constraints, triggers and policies go with it, its
// foreign keys and the ones that point at it are dropped by name first, and
// the views that read it are removed before it. What the plan removes only
// after the table is a routine, whose signature or body may name the table's
// row type, and an owner operation on a standalone object, such as a type
// whose attribute is that row type.
func tableDropRelies(diff *difftypes.SchemaDiff) bool {
	return removesRoutines(diff) || len(diff.FeatureChanges) > 0
}

func removesRoutines(diff *difftypes.SchemaDiff) bool {
	return len(diff.FunctionsRemoved) > 0 || len(diff.ProceduresRemoved) > 0
}

func isGenerated(column schemamodel.Field) bool {
	return strings.TrimSpace(column.GeneratedExpression) != ""
}

// settleGuards removes from an exact plan every IF EXISTS and IF NOT EXISTS no
// earlier statement of the plan makes necessary, and keeps the rest.
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
//   - ALTER TABLE ... DROP CONSTRAINT, which takes the index that backs the
//     constraint;
//   - an earlier drop of the same object.
//
// A new index keeps IF NOT EXISTS only after an earlier statement created an
// index or a constraint of that name.
//
// Names are compared without their schema and without case, so an object that
// might be the one an earlier statement removed keeps its guard. The pass
// leaves alone CREATE SCHEMA IF NOT EXISTS, which the plan writes for every
// schema an added object names whether or not it exists, and every guard the
// declaration spells itself, such as IF NOT EXISTS on a sequence.
func settleGuards(nodes []ast.Node) {
	var seen guardState
	seen.init()
	for _, node := range nodes {
		seen.settle(node)
	}
}

// guardState is what [settleGuards] knows the statements before the current
// one removed or created.
type guardState struct {
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
	// indexNames holds the names of indexes and constraints created.
	indexNames map[string]bool
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
	g.indexNames = make(map[string]bool)
}

func (g *guardState) settle(node ast.Node) {
	if ast.PlacementOf(node) != ast.NoExtension {
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
		n.IfNotExists = n.IfNotExists && g.indexNames[bareName(n.Name)]
		g.indexNames[bareName(n.Name)] = true
	case *ast.CreateTableNode:
		g.createTable(n)
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
	case *ast.DropPolicyNode:
		g.settleDropPolicy(n)
	case *ast.RawSQLNode, *ast.DoBlockNode, *ast.OpaqueRoutineNode:
		g.anything = true
	}
}

func (g *guardState) settleDropIndex(n *ast.DropIndexNode) {
	n.IfExists = n.IfExists && (g.mayBeGone("index", n.Name) || g.onRemoved(n.Table))
	g.drop("index:"+bareName(n.Name), n.Cascade)
}

func (g *guardState) settleDropFunction(n *ast.DropFunctionNode) {
	n.IfExists = n.IfExists && (g.mayBeGone("function", n.Name) || g.triggerFunctions[bareName(n.Name)])
	g.drop("function:"+bareName(n.Name), n.Cascade)
}

func (g *guardState) settleDropSequence(n *ast.DropSequenceNode) {
	n.IfExists = n.IfExists && (g.mayBeGone("sequence", n.Name) || g.sequences)
	g.drop("sequence:"+bareName(n.Name), n.Cascade)
}

func (g *guardState) settleDropPolicy(n *ast.DropPolicyNode) {
	n.IfExists = n.IfExists && (g.mayBeGone("policy", scopedName(n.Table, n.Name)) || g.tables[bareName(n.Table)])
	g.drop("policy:"+scopedName(n.Table, n.Name), false)
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

func (g *guardState) createTable(n *ast.CreateTableNode) {
	for _, constraint := range n.Constraints {
		g.indexNames[bareName(constraint.Name)] = true
	}
	for _, index := range n.Indexes {
		g.indexNames[bareName(index.Name)] = true
	}
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
			op.IfExists = op.IfExists && (g.anything || g.tables[table] ||
				g.dropped["column:"+table+"."+strings.ToLower(op.ColumnName)])
			g.dropped["column:"+table+"."+strings.ToLower(op.ColumnName)] = true
			g.shrunk[table] = true
			g.sequences = true
			cascaded = cascaded || op.Cascade
		case *ast.DropConstraintOperation:
			key := "constraint:" + table + "." + strings.ToLower(op.ConstraintName)
			op.IfExists = op.IfExists && (g.anything || g.tables[table] || g.shrunk[table] || g.dropped[key])
			g.dropped[key] = true
			// The index that backs a key goes with it.
			g.dropped["index:"+strings.ToLower(op.ConstraintName)] = true
		case *ast.AddConstraintOperation:
			if op.Constraint != nil {
				g.indexNames[bareName(op.Constraint.Name)] = true
			}
		case *ast.ReplaceIndexOperation:
			g.anything = true
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
	if kind == "trigger" || kind == "policy" {
		return g.dropped[kind+":"+key]
	}
	return g.dropped[kind+":"+bareName(key)]
}

// scopedName keys an object whose name is unique within its table, such as a
// trigger or a policy.
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

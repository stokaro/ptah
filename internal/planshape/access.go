package planshape

import (
	"slices"
	"strings"

	"ptah.run/core/ast"
)

// access is what a step of a plan depends on. Two steps keep the order the planner
// gave them when one writes what the other reads or writes, and may change places
// only when neither does. An unknown is never read as independent: where a step's
// access cannot be stated, the step is a barrier.
//
// The resources are named below. Tables go by their bare name, so two tables that
// share one count as the same table here: that can only hold a statement back.
type access struct {
	reads, writes []string
}

func (a *access) read(resources ...string)  { a.reads = append(a.reads, resources...) }
func (a *access) write(resources ...string) { a.writes = append(a.writes, resources...) }

func (a access) conflicts(other access) bool {
	overlap := func(left, right []string) bool {
		return slices.ContainsFunc(left, func(resource string) bool { return slices.Contains(right, resource) })
	}
	return overlap(a.writes, other.writes) || overlap(a.writes, other.reads) || overlap(a.reads, other.writes)
}

// sharesAName reports that both give or free one name.
func (a access) sharesAName(other access) bool {
	return slices.ContainsFunc(a.writes, func(resource string) bool {
		return strings.HasPrefix(resource, namePrefix) && slices.Contains(other.writes, resource)
	})
}

// conflictsBesidesNames is conflicts, leaving out the names both give or free.
func (a access) conflictsBesidesNames(other access) bool {
	withoutNames := func(use access) access {
		isName := func(resource string) bool { return strings.HasPrefix(resource, namePrefix) }
		return access{reads: use.reads, writes: slices.DeleteFunc(slices.Clone(use.writes), isName)}
	}
	return withoutNames(a).conflicts(withoutNames(other))
}

// tableResource is the table itself: creating or dropping it writes it, and every
// other statement on it reads it.
func tableResource(table string) string { return "table " + bare(table) }

// columnResource is one column: what adds, drops or changes it writes it, and what
// is defined over it reads it.
func columnResource(table, column string) string {
	return "column " + memberKey(table, column)
}

// keyedResource is one column as a part of a key or an index. It keeps the keys and
// indexes over one column in order, such as a new index and the old one it replaces
// under a foreign key, and it ties a foreign key to the key it references.
func keyedResource(table, column string) string {
	return "keyed " + memberKey(table, column)
}

// nameResource is a name a statement gives or frees. PostgreSQL holds tables and
// indexes in one namespace of a schema and MySQL a foreign key's name across a
// database, so two statements naming one thing keep their order wherever it stands.
func nameResource(name string) string { return namePrefix + strings.ToLower(name) }

const namePrefix = "name "

// primaryKeyResource is the one primary key a table holds, whatever its columns.
func primaryKeyResource(table string) string { return "primary key " + bare(table) }

// autoIncrementResource is the one counter a table holds: a column gives it up in one
// statement before another takes it in the next.
func autoIncrementResource(table string) string { return "auto increment " + bare(table) }

// columnOrderResource keeps a table's added columns in order. The order they are
// added in is the order the table holds them in, which `SELECT *` shows.
func columnOrderResource(table string) string { return "column order " + bare(table) }

// linkResource is a foreign key as a tie between two tables. A change to the type of a
// column at either end of it writes it, so changes at the two ends keep their order even
// where the key itself stays.
func linkResource(key constraintFact) string { return "link " + memberKey(key.table, key.name) }

// stepsOf splits node into the steps of a plan. It answers false for a node the pass
// has no rule for, and for one whose access it cannot state.
func (how arrangement) stepsOf(node ast.Node) ([]*step, bool) {
	if !movable(node) {
		return nil, false
	}
	if alter, ok := node.(*ast.AlterTableNode); ok {
		steps := make([]*step, 0, len(alter.Operations))
		for _, operation := range alter.Operations {
			use, ok := how.operationAccess(alter.Name, operation)
			if !ok {
				return nil, false
			}
			steps = append(steps, &step{table: alter.Name, operation: operation, access: use})
		}
		return steps, true
	}
	use, ok := how.statementAccess(node)
	if !ok {
		return nil, false
	}
	return []*step{{table: tableOf(node), node: node, access: use}}, true
}

// tableOf names the table a statement other than an ALTER TABLE is on.
func tableOf(node ast.Node) string {
	switch n := node.(type) {
	case *ast.CreateTableNode:
		return n.Name
	case *ast.IndexNode:
		return n.Table
	case *ast.DropIndexNode:
		return n.Table
	case *ast.DropTableNode:
		if names := n.TableNames(); len(names) > 0 {
			return names[0]
		}
	}
	return ""
}

func (how arrangement) statementAccess(node ast.Node) (access, bool) {
	var use access
	switch n := node.(type) {
	case *ast.CreateTableNode:
		return how.createTableAccess(n)
	case *ast.DropTableNode:
		for _, name := range n.TableNames() {
			if !how.dropTableAccess(&use, name) {
				return access{}, false
			}
		}
		return use, true
	case *ast.IndexNode:
		how.indexAccess(&use, n.Table, n.Name, indexFactOf(n))
		return use, !how.facts.callsRoutine(strings.Join(indexFactOf(n).words, " "))
	case *ast.DropIndexNode:
		return how.dropIndexAccess(n)
	default:
		return access{}, false
	}
}

func (how arrangement) createTableAccess(table *ast.CreateTableNode) (access, bool) {
	var use access
	use.write(tableResource(table.Name), nameResource(unqualified(table.Name)))
	for _, column := range table.Columns {
		if !how.columnAccess(&use, table.Name, column) {
			return access{}, false
		}
	}
	for _, constraint := range table.Constraints {
		fact, ok := constraintFactOf(table.Name, constraint)
		if !ok || how.facts.callsRoutine(fact.expression) {
			return access{}, false
		}
		how.constraintAccess(&use, fact)
	}
	for _, index := range table.Indexes {
		use.write(nameResource(index.Name))
	}
	return use, true
}

// dropTableAccess is a table's drop: everything the table holds goes with it, so it
// touches what each of its keys ties it to.
func (how arrangement) dropTableAccess(use *access, table string) bool {
	if !how.facts.observed[bare(table)] {
		return false
	}
	use.write(tableResource(table), nameResource(unqualified(table)))
	for _, key := range how.facts.foreignKeys[bare(table)] {
		if bare(key.table) == bare(table) {
			how.constraintAccess(use, key)
			continue
		}
		use.write(linkResource(key))
		use.read(tableResource(key.table))
	}
	for key, indexes := range how.facts.indexes {
		owner, name, _ := strings.Cut(key, "\x00")
		if owner == bare(table) && len(indexes) > 0 {
			use.write(nameResource(name))
		}
	}
	for key, constraints := range how.facts.constraints {
		owner, name, _ := strings.Cut(key, "\x00")
		if owner == bare(table) && len(constraints) > 0 && name != "" {
			use.write(nameResource(name))
		}
	}
	slices.Sort(use.writes)
	use.writes = slices.Compact(use.writes)
	return true
}

func (how arrangement) indexAccess(use *access, table, name string, fact indexFact) {
	how.keyedIndexAccess(use, table, name, fact)
	for _, column := range fact.columns {
		use.read(columnResource(table, column))
	}
	for _, word := range fact.words {
		use.read(columnResource(table, word))
	}
}

// keyedIndexAccess is what an index is to the foreign keys around it, and all its drop
// touches: the index goes whatever happens to its columns, and a column that goes first
// takes the index with it, which leaves the drop nothing to find.
func (how arrangement) keyedIndexAccess(use *access, table, name string, fact indexFact) {
	use.read(tableResource(table))
	use.write(nameResource(name))
	for _, column := range fact.columns {
		if fact.unique || how.indexesBackKeys {
			use.write(keyedResource(table, column))
		}
	}
}

// dropIndexAccess is an index's drop. A table the plan changes in no other way is not
// observed, so what such an index covers is unknown; it can matter only to a foreign
// key at that table.
func (how arrangement) dropIndexAccess(drop *ast.DropIndexNode) (access, bool) {
	var use access
	facts := slices.Clone(how.facts.indexes[memberKey(drop.Table, drop.Name)])
	for _, constraint := range how.facts.constraints[memberKey(drop.Table, drop.Name)] {
		facts = append(facts, indexFact{columns: constraint.columns, unique: true})
	}
	if len(facts) == 0 {
		if how.facts.observed[bare(drop.Table)] || len(how.facts.foreignKeys[bare(drop.Table)]) > 0 {
			return access{}, false
		}
		use.read(tableResource(drop.Table))
		use.write(nameResource(drop.Name))
		return use, true
	}
	for _, fact := range facts {
		how.keyedIndexAccess(&use, drop.Table, drop.Name, fact)
	}
	return use, true
}

func (how arrangement) operationAccess(table string, operation ast.AlterOperation) (access, bool) {
	var use access
	switch op := operation.(type) {
	case *ast.AddColumnOperation:
		use.write(columnOrderResource(table))
		return use, how.columnAccess(&use, table, op.Column)
	case *ast.ModifyColumnOperation:
		use.read(autoIncrementResource(table))
		if op.Column != nil {
			how.linksOfColumn(&use, table, op.Column.Name)
		}
		return use, how.columnAccess(&use, table, op.Column)
	case *ast.DropColumnOperation:
		use.read(tableResource(table))
		use.write(columnResource(table, op.ColumnName))
		words := identifiers(how.facts.expressions[memberKey(table, op.ColumnName)])
		for _, word := range words {
			use.read(columnResource(table, word))
		}
		return use, true
	case *ast.AddConstraintOperation:
		if op.Constraint == nil {
			return access{}, false
		}
		fact, ok := constraintFactOf(table, op.Constraint)
		if !ok || op.Constraint.UsingIndex != "" || how.facts.callsRoutine(fact.expression) {
			return access{}, false
		}
		how.constraintAccess(&use, fact)
		return use, true
	case *ast.DropConstraintOperation:
		facts := how.droppedConstraint(table, op)
		for _, fact := range facts {
			how.constraintAccess(&use, fact)
		}
		return use, len(facts) > 0
	case *ast.ReplaceIndexOperation:
		if op.Index == nil {
			return access{}, false
		}
		how.indexAccess(&use, table, op.Index.Name, indexFactOf(op.Index))
		for _, column := range indexFactOf(op.Index).columns {
			use.write(keyedResource(table, column))
		}
		// The statement frees these names too, which a foreign key may take next.
		for _, name := range op.Drops {
			use.write(nameResource(name))
		}
		return use, true
	default:
		return access{}, false
	}
}

// droppedConstraint is what a drop removes. A primary key counts under every column
// list the table holds one over, on either side of the plan: the statement that adds
// one names none, so its name says nothing about which it is.
func (how arrangement) droppedConstraint(table string, drop *ast.DropConstraintOperation) []constraintFact {
	facts := how.facts.constraints[memberKey(table, drop.ConstraintName)]
	if drop.PrimaryKey || slices.ContainsFunc(facts, func(fact constraintFact) bool { return fact.kind == ast.PrimaryKeyConstraint }) {
		return how.facts.primaryKeys[bare(table)]
	}
	return facts
}

func (how arrangement) columnAccess(use *access, table string, column *ast.ColumnNode) bool {
	if column == nil {
		return false
	}
	use.read(tableResource(table))
	use.write(columnResource(table, column.Name))
	expression := column.Check + " " + column.GeneratedExpression
	if column.Default != nil {
		expression += " " + column.Default.Expression
	}
	if how.facts.callsRoutine(expression) {
		return false
	}
	for _, word := range identifiers(expression) {
		use.read(columnResource(table, word))
	}
	for _, word := range identifiers(column.Type) {
		// A column typed by a table's row type needs that table.
		if how.facts.tables[word] && word != bare(table) {
			use.write(tableResource(word))
		}
	}
	if column.CheckName != "" {
		use.write(nameResource(column.CheckName))
	}
	if column.AutoInc || column.IdentityGeneration != "" {
		use.write(autoIncrementResource(table))
		use.write(keyedResource(table, column.Name))
	}
	if column.Primary {
		use.write(primaryKeyResource(table), keyedResource(table, column.Name))
	}
	if column.Unique {
		use.write(keyedResource(table, column.Name))
	}
	if column.ForeignKey != nil {
		columns := referencedColumns(column.ForeignKey.Column, column.ForeignKey.Columns)
		if len(columns) == 0 {
			return false
		}
		how.constraintAccess(use, constraintFact{
			kind: ast.ForeignKeyConstraint, table: table, name: column.ForeignKey.Name, columns: []string{column.Name},
			refTable: column.ForeignKey.Table, refColumns: columns,
		})
	}
	return true
}

// linksOfColumn is the foreign keys a change to a column touches, at either end. A
// column added or dropped needs no link: a key over one is added or dropped in the same
// plan, and that step reads the column.
func (how arrangement) linksOfColumn(use *access, table, column string) {
	for _, key := range how.facts.foreignKeys[bare(table)] {
		own := bare(key.table) == bare(table) && containsFold(key.columns, column)
		referenced := bare(key.refTable) == bare(table) && containsFold(key.refColumns, column)
		if own || referenced {
			use.write(linkResource(key))
		}
	}
}

func containsFold(names []string, name string) bool {
	return slices.ContainsFunc(names, func(candidate string) bool { return strings.EqualFold(candidate, name) })
}

// constraintAccess is a constraint added or dropped. Both touch the same things: a
// drop has to keep its place against what needs the constraint gone, as an addition
// does against what it needs there.
func (how arrangement) constraintAccess(use *access, fact constraintFact) {
	use.read(tableResource(fact.table))
	if fact.name != "" {
		use.write(nameResource(fact.name))
	}
	for _, column := range fact.columns {
		use.read(columnResource(fact.table, column))
	}
	for _, word := range identifiers(fact.expression) {
		use.read(columnResource(fact.table, word))
	}
	switch fact.kind {
	case ast.PrimaryKeyConstraint:
		use.write(primaryKeyResource(fact.table))
		for _, column := range fact.columns {
			use.write(keyedResource(fact.table, column))
		}
	case ast.UniqueConstraint:
		for _, column := range fact.columns {
			use.write(keyedResource(fact.table, column))
		}
	case ast.ForeignKeyConstraint:
		use.write(linkResource(fact))
		for _, column := range fact.columns {
			use.write(keyedResource(fact.table, column))
		}
		use.read(tableResource(fact.refTable))
		for _, column := range fact.refColumns {
			use.read(columnResource(fact.refTable, column), keyedResource(fact.refTable, column))
		}
	default:
	}
}

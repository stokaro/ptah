package planshape

import (
	"slices"
	"strings"

	"ptah.run/catalog"
	"ptah.run/core/ast"
	"ptah.run/core/difftypes"
	"ptah.run/core/schemacapture"
)

// constraintFact is what a constraint covers. A statement that drops one names it
// and says nothing else, so the plan's diff is where this comes from.
type constraintFact struct {
	kind       ast.ConstraintType
	table      string
	name       string
	columns    []string
	refTable   string
	refColumns []string
	expression string
}

// indexFact is what an index covers. words is what its expressions and its condition read,
// which only a new index needs.
type indexFact struct {
	columns []string
	words   []string
	unique  bool
}

// planFacts is what a plan's diffs say about the tables its statements name, on
// either side of the plan. Every entry can only hold a statement back, so a fact
// that is true of one side only, or of another table of the same bare name, costs
// an order and never a refusal.
type planFacts struct {
	// observed is the tables whose constraints and indexes are known.
	observed map[string]bool
	// constraints holds each constraint by table and name, and primaryKeys by table.
	constraints map[string][]constraintFact
	primaryKeys map[string][]constraintFact
	// foreignKeys holds each foreign key under both of the tables it ties.
	foreignKeys map[string][]constraintFact
	indexes     map[string][]indexFact
	// expressions is what a generated column reads, by table and column.
	expressions map[string]string
	// routines is the functions the plan's schema declares. A body is not read, so an
	// expression calling one may need any table.
	routines map[string]bool
	// tables is every table a plan's statements name.
	tables map[string]bool
}

func newPlanFacts() *planFacts {
	return &planFacts{
		observed:    make(map[string]bool),
		constraints: make(map[string][]constraintFact),
		primaryKeys: make(map[string][]constraintFact),
		foreignKeys: make(map[string][]constraintFact),
		indexes:     make(map[string][]indexFact),
		expressions: make(map[string]string),
		routines:    make(map[string]bool),
		tables:      make(map[string]bool),
	}
}

func memberKey(table, member string) string {
	return bare(table) + "\x00" + strings.ToLower(member)
}

// readDiff records what diff says. A nil diff says nothing, and a statement that
// depends on what it would have said stays where the planner put it.
func (f *planFacts) readDiff(diff *difftypes.SchemaDiff) {
	if diff == nil {
		return
	}
	for i := range diff.TablesModified {
		table := &diff.TablesModified[i]
		f.readObservation(table.Current)
		for _, column := range table.ColumnsRemoved {
			if strings.TrimSpace(column.GeneratedExpression) != "" {
				f.expressions[memberKey(table.TableName, column.Name)] = column.GeneratedExpression
			}
		}
	}
	for i := range diff.TablesRemoved {
		f.readObservation(diff.TablesRemoved[i].Current)
	}
	for i := range diff.ObservedConstraintHosts {
		f.readObservation(diff.ObservedConstraintHosts[i])
	}
	for _, key := range diff.DeclaredForeignKeys {
		// The schema this plan moves towards. It ties tables together, and what a drop
		// finds is the other side's to say.
		f.addForeignKey(constraintFact{
			kind: ast.ForeignKeyConstraint, table: key.TableName, name: key.Name, columns: key.Columns,
			refTable: key.ForeignTable, refColumns: referencedColumns(key.ForeignColumn, key.ForeignColumns),
		})
	}
	for _, key := range diff.ForeignKeysRemovedWithTables {
		f.addConstraint(constraintFact{
			kind: ast.ForeignKeyConstraint, table: key.TableName, name: key.Name, columns: key.Columns,
			refTable: key.ForeignTable, refColumns: key.ForeignColumns,
		})
	}
	for _, name := range diff.DeclaredFunctions.Order {
		f.routines[bare(name)] = true
	}
	for _, routines := range []difftypes.FunctionChanges{diff.RoutinesKept, diff.FunctionsAdded, diff.FunctionsRemoved} {
		for i := range routines {
			f.routines[bare(routines[i].Name)] = true
		}
	}
}

func (f *planFacts) readObservation(observation schemacapture.TableObservation) {
	if !observation.HasTable() {
		return
	}
	table := observation.Table.QualifiedName()
	f.observed[bare(table)] = true
	for i := range observation.Constraints {
		f.addCatalogConstraint(table, &observation.Constraints[i])
	}
	for i := range observation.Indexes {
		index := &observation.Indexes[i]
		fact := indexFact{columns: index.Columns, unique: index.IsUnique || index.IsPrimary}
		for _, part := range index.Parts {
			if part.Name != "" {
				fact.columns = append(fact.columns, part.Name)
			}
		}
		f.indexes[memberKey(table, index.Name)] = append(f.indexes[memberKey(table, index.Name)], fact)
	}
}

func (f *planFacts) addCatalogConstraint(table string, constraint *catalog.Constraint) {
	fact := constraintFact{table: table, name: constraint.Name, columns: constraint.ColumnNames}
	if len(fact.columns) == 0 && constraint.ColumnName != "" {
		fact.columns = []string{constraint.ColumnName}
	}
	switch strings.ToUpper(strings.TrimSpace(constraint.Type)) {
	case "PRIMARY KEY":
		fact.kind = ast.PrimaryKeyConstraint
	case "UNIQUE":
		fact.kind = ast.UniqueConstraint
	case "CHECK":
		fact.kind = ast.CheckConstraint
		if constraint.CheckClause != nil {
			fact.expression = *constraint.CheckClause
		}
	case "FOREIGN KEY":
		fact.kind = ast.ForeignKeyConstraint
		if constraint.ForeignTable != nil {
			fact.refTable = *constraint.ForeignTable
		}
		fact.refColumns = constraint.ForeignColumns
		if len(fact.refColumns) == 0 && constraint.ForeignColumn != nil {
			fact.refColumns = []string{*constraint.ForeignColumn}
		}
	default:
		// A kind this pass has no rule for reads as unknown to whoever drops it.
		return
	}
	f.addConstraint(fact)
}

func (f *planFacts) addConstraint(fact constraintFact) {
	if fact.kind == ast.PrimaryKeyConstraint && fact.name == "" {
		// The statement that adds one names none, and the rollback drops it under the
		// name the server gave it.
		fact.name = unqualified(fact.table) + "_pkey"
	}
	key := memberKey(fact.table, fact.name)
	f.constraints[key] = append(f.constraints[key], fact)
	switch fact.kind {
	case ast.PrimaryKeyConstraint:
		f.primaryKeys[bare(fact.table)] = append(f.primaryKeys[bare(fact.table)], fact)
	case ast.ForeignKeyConstraint:
		f.addForeignKey(fact)
	default:
	}
}

func (f *planFacts) addForeignKey(fact constraintFact) {
	f.foreignKeys[bare(fact.table)] = append(f.foreignKeys[bare(fact.table)], fact)
	if bare(fact.refTable) != bare(fact.table) {
		f.foreignKeys[bare(fact.refTable)] = append(f.foreignKeys[bare(fact.refTable)], fact)
	}
}

// statedFacts says what a reading of statements records.
type statedFacts struct {
	constraint func(constraintFact)
	index      func(*ast.IndexNode)
	created    func(table string)
}

// readPlan records what the statements being arranged say. They describe the database
// after they run, so they tie tables together and say nothing about what a drop removes:
// a drop removes what the database holds when it runs, which may be another thing of the
// same name.
func (f *planFacts) readPlan(nodes []ast.Node) {
	f.readNodes(nodes, statedFacts{
		constraint: func(fact constraintFact) {
			if fact.kind == ast.ForeignKeyConstraint {
				f.addForeignKey(fact)
			}
		},
		index:   func(*ast.IndexNode) {},
		created: func(string) {},
	})
}

// readApplied records what statements that will have run say: for a rollback, its forward
// plan. What they add is what a drop finds. A rollback's diff is projected from the forward
// diff and may not say it: a key moved through a column's own change is not in its lists.
func (f *planFacts) readApplied(nodes []ast.Node) {
	f.readNodes(nodes, statedFacts{
		constraint: f.addConstraint,
		index: func(n *ast.IndexNode) {
			f.indexes[memberKey(n.Table, n.Name)] = append(f.indexes[memberKey(n.Table, n.Name)], indexFactOf(n))
		},
		created: func(table string) { f.observed[bare(table)] = true },
	})
}

func (f *planFacts) readNodes(nodes []ast.Node, stated statedFacts) {
	for _, node := range nodes {
		switch n := node.(type) {
		case *ast.CreateTableNode:
			f.tables[bare(n.Name)] = true
			stated.created(n.Name)
			f.readCreatedTable(n, stated)
		case *ast.DropTableNode:
			for _, name := range n.TableNames() {
				f.tables[bare(name)] = true
			}
		case *ast.AlterTableNode:
			f.tables[bare(n.Name)] = true
			f.readOperations(n, stated)
		case *ast.IndexNode:
			f.tables[bare(n.Table)] = true
			stated.index(n)
		case *ast.DropIndexNode:
			f.tables[bare(n.Table)] = true
		}
	}
}

func (f *planFacts) readCreatedTable(table *ast.CreateTableNode, stated statedFacts) {
	for _, constraint := range table.Constraints {
		if fact, ok := constraintFactOf(table.Name, constraint); ok {
			stated.constraint(fact)
		}
	}
	for _, column := range table.Columns {
		f.readColumn(table.Name, column, stated)
	}
}

func (f *planFacts) readOperations(alter *ast.AlterTableNode, stated statedFacts) {
	for _, operation := range alter.Operations {
		switch op := operation.(type) {
		case *ast.AddColumnOperation:
			f.readColumn(alter.Name, op.Column, stated)
		case *ast.ModifyColumnOperation:
			f.readColumn(alter.Name, op.Column, stated)
		case *ast.AddConstraintOperation:
			if op.Constraint == nil {
				continue
			}
			if fact, ok := constraintFactOf(alter.Name, op.Constraint); ok {
				stated.constraint(fact)
			}
		}
	}
}

func (f *planFacts) readColumn(table string, column *ast.ColumnNode, stated statedFacts) {
	if column == nil {
		return
	}
	if strings.TrimSpace(column.GeneratedExpression) != "" {
		f.expressions[memberKey(table, column.Name)] = column.GeneratedExpression
	}
	if column.ForeignKey != nil {
		stated.constraint(constraintFact{
			kind: ast.ForeignKeyConstraint, table: table, name: column.ForeignKey.Name, columns: []string{column.Name},
			refTable: column.ForeignKey.Table, refColumns: referencedColumns(column.ForeignKey.Column, column.ForeignKey.Columns),
		})
	}
	if column.Primary {
		stated.constraint(constraintFact{kind: ast.PrimaryKeyConstraint, table: table, columns: []string{column.Name}})
	}
	if column.Unique {
		// The server names it: PostgreSQL after the table and the column, the MySQL family
		// after the column.
		for _, name := range []string{unqualified(table) + "_" + column.Name + "_key", column.Name} {
			stated.constraint(constraintFact{kind: ast.UniqueConstraint, table: table, name: name, columns: []string{column.Name}})
		}
	}
}

// constraintFactOf reads a constraint a statement writes. A kind this pass has no
// rule for is not a fact.
func constraintFactOf(table string, constraint *ast.ConstraintNode) (constraintFact, bool) {
	fact := constraintFact{kind: constraint.Type, table: table, name: constraint.Name, columns: constraint.Columns, expression: constraint.Expression}
	for _, part := range constraint.ColumnParts {
		if part.Name != "" {
			fact.columns = append(fact.columns, part.Name)
		}
		fact.expression += " " + part.Expr
	}
	fact.columns = append(slices.Clip(fact.columns), constraint.IncludeColumns...)
	switch constraint.Type {
	case ast.PrimaryKeyConstraint, ast.UniqueConstraint, ast.CheckConstraint:
		return fact, true
	case ast.ForeignKeyConstraint:
		if constraint.Reference == nil {
			return constraintFact{}, false
		}
		fact.refTable = constraint.Reference.Table
		fact.refColumns = referencedColumns(constraint.Reference.Column, constraint.Reference.Columns)
		return fact, len(fact.refColumns) > 0
	default:
		return constraintFact{}, false
	}
}

func indexFactOf(index *ast.IndexNode) indexFact {
	fact := indexFact{columns: index.Columns, unique: index.Unique}
	fact.columns = append(slices.Clip(fact.columns), index.IncludeColumns...)
	for _, part := range index.Parts {
		if part.Name != "" {
			fact.columns = append(fact.columns, part.Name)
		}
		fact.words = append(fact.words, identifiers(part.Expr)...)
	}
	fact.words = append(fact.words, identifiers(index.Condition)...)
	return fact
}

func referencedColumns(column string, columns []string) []string {
	if len(columns) > 0 {
		return columns
	}
	if column != "" {
		return []string{column}
	}
	return nil
}

// identifiers is the words of an expression, in lower case, and each name in quotes as a
// whole, since a space or a dot inside one does not end it. It reads a string literal as
// words too, which can only hold a statement back.
func identifiers(expression string) []string {
	expression = strings.ToLower(expression)
	words := strings.FieldsFunc(expression, func(r rune) bool {
		return r != '_' && r != '$' && (r < '0' || r > '9') && (r < 'a' || r > 'z') && r < 0x80
	})
	return append(words, quotedNames(expression)...)
}

// quotedNames is what stands between each two quotes of an expression, double or backtick,
// where a doubled quote is one quote of the name. A literal's quote can stand among them, and
// how a literal ends depends on the engine: `E'it\'s'` and `$$it's$$` on PostgreSQL, `'it\'s'`
// on MySQL. So it reads no literal and takes every span, which holds the name whichever way
// the text reads. A span that names nothing only reads a column no table holds.
func quotedNames(expression string) []string {
	var names []string
	for _, quote := range []string{`"`, "`"} {
		spans := strings.Split(strings.ReplaceAll(expression, quote+quote, "\x00"), quote)
		for _, span := range spans[1:max(len(spans)-1, 1)] {
			names = append(names, strings.ReplaceAll(span, "\x00", quote))
		}
	}
	return names
}

// callsRoutine reports an expression naming a function the plan's schema declares.
func (f *planFacts) callsRoutine(expression string) bool {
	for _, word := range identifiers(expression) {
		if f.routines[word] {
			return true
		}
	}
	return false
}

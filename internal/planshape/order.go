package planshape

import (
	"cmp"
	"maps"
	"slices"
	"strings"

	"ptah.run/core/ast"
	"ptah.run/core/difftypes"
	"ptah.run/core/platform/descriptor"
)

// arrangement is what the arranging of a plan reads.
type arrangement struct {
	// merge says one ALTER TABLE can hold several changes.
	merge bool
	// restatesColumns says a statement restates a whole column when it changes one, so
	// the last change of a column says all of them.
	restatesColumns bool
	// unorderedStatement says the server reads one ALTER TABLE as a whole, so the order
	// inside it is free but for the columns it adds. PostgreSQL runs the drops of one
	// statement in the order written.
	unorderedStatement bool
	// indexesBackKeys says a plain index can be what a foreign key needs, on either
	// side of it. On PostgreSQL only a unique one can, and only on the referenced side.
	indexesBackKeys bool
	// sameNamedKeysApart says the server refuses a statement that drops a foreign key
	// and adds one of the same name: MySQL answers 1826.
	sameNamedKeysApart bool
	// selfKeysApart says the server refuses a statement adding a foreign key to its own
	// table beside the change it needs there: MySQL answers 1215 or 1826 and MariaDB 1005.
	selfKeysApart bool
	// absorbs says a foreign key into a table the plan changes moves into its new table
	// once what it needs is in place.
	absorbs bool
	facts   *planFacts
	// reorder, when set, changes the order a run's steps depend on each other in.
	reorder func(run []*step) []*step
	// places gives the steps of a run the place they want.
	places func(run []*step)
	// redundant names, by table, the constraints a rollback need not drop on its own
	// when it drops the table.
	redundant map[string]map[string]bool
}

// arrangementFor is the rules of an engine.
func arrangementFor(form descriptor.CatalogForm, facts *planFacts) arrangement {
	family := form == descriptor.MySQLCatalogForm || form == descriptor.MariaDBCatalogForm
	return arrangement{
		merge:              form != descriptor.SQLiteCatalogForm,
		restatesColumns:    family,
		unorderedStatement: family,
		indexesBackKeys:    family,
		sameNamedKeysApart: form == descriptor.MySQLCatalogForm,
		selfKeysApart:      family,
		facts:              facts,
	}
}

// step is one statement of a plan, or one operation of its ALTER TABLE: the unit this
// pass moves. Exactly one of node and operation is set.
type step struct {
	table     string
	node      ast.Node
	operation ast.AlterOperation
	access    access
	// at is the step's place in the planner's order, and after the steps that have to
	// stay ahead of it.
	at    int
	after []*step
	place place
}

// place is where a step goes when nothing holds it back, as the community binary writes
// it. Its fields compare in the order they are declared.
type place struct {
	phase     int
	depth     int
	bareTable string
	table     string
	part      int
	class     int
	column    int
	name      string
}

func comparePlaces(a, b *step) int {
	return cmp.Or(
		cmp.Compare(a.place.phase, b.place.phase),
		cmp.Compare(a.place.depth, b.place.depth),
		cmp.Compare(a.place.bareTable, b.place.bareTable),
		cmp.Compare(a.place.table, b.place.table),
		cmp.Compare(a.place.part, b.place.part),
		cmp.Compare(a.place.class, b.place.class),
		cmp.Compare(a.place.column, b.place.column),
		cmp.Compare(a.place.name, b.place.name),
		cmp.Compare(a.at, b.at),
	)
}

// The phases of a forward plan.
const (
	phaseLeading = iota
	phaseChanged
	phaseNew
	phaseTrailing
	phaseDropped
)

// The parts of what a plan does to one table it changes.
const (
	partIndexDrops = iota
	partOperations
	partIndexes
)

// arrange writes each run of table statements in the order the community binary
// writes them, as far as that can be shown to change nothing. A statement of another
// kind, such as a view, a routine or a statement a feature owner renders, ends a run:
// the plan's order around it is the planner's, and what depends on it stays on its
// side. So does a table statement whose access cannot be stated.
//
// Inside a run, steps that depend on each other keep the planner's order. Any order
// that does so runs to the same result, so the pass takes the one closest to the
// binary's: of the steps nothing holds back, the one the binary writes first goes next.
func arrange(nodes []ast.Node, how arrangement) []ast.Node {
	out := make([]ast.Node, 0, len(nodes))
	var run []*step
	flush := func() {
		out = append(out, how.arrangeRun(run)...)
		run = nil
	}
	for _, node := range nodes {
		steps, ok := how.stepsOf(node)
		if !ok {
			flush()
			out = append(out, node)
			continue
		}
		run = append(run, steps...)
	}
	flush()
	return out
}

// movable reports a table statement this pass may move within its run.
func movable(node ast.Node) bool {
	switch n := node.(type) {
	case *ast.CreateTableNode:
		return n.SelectBody == "" && n.CustomSQL == "" && n.OwnedObjects.IsZero() && len(n.OwnedConstraints) == 0
	case *ast.IndexNode:
		return !n.Concurrently
	case *ast.DropTableNode:
		return !n.Cascade
	case *ast.DropIndexNode:
		return n.Table != "" && !n.Concurrently && !n.Cascade
	case *ast.AlterTableNode:
		return n.Algorithm == "" && n.Lock == ""
	default:
		return false
	}
}

func (how arrangement) arrangeRun(run []*step) []ast.Node {
	run = how.withoutRedundantDrops(run)
	run = addedColumnsAheadOfGeneratedOnes(run)
	if how.reorder != nil {
		run = how.reorder(run)
	}
	linkSteps(run)
	how.places(run)
	order := linearize(run, func(s *step) []*step { return s.after })
	if how.absorbs {
		order = absorbForeignKeys(order)
	}
	return how.statements(order)
}

// addedColumnsAheadOfGeneratedOnes moves a column a run adds ahead of a generated column
// the run adds and that reads it. A diff lists a rollback's columns in the order the
// forward plan dropped them, generated ones first, and PostgreSQL refuses the other way
// round, so this is the one order the planner gives that no server takes. It moves a
// column only over other added columns of its table.
func addedColumnsAheadOfGeneratedOnes(run []*step) []*step {
	out := slices.Clone(run)
	// Generated columns reading each other are the server's to refuse. The bound keeps
	// them from moving each other forever.
	for range len(out) * len(out) {
		at, later := nextColumnToMove(out)
		if later == 0 {
			break
		}
		column := out[at+later]
		out = slices.Insert(slices.Delete(out, at+later, at+later+1), at, column)
	}
	return out
}

// nextColumnToMove is where a generated column stands and how far behind it a column it
// reads does, or zeros where none does.
func nextColumnToMove(run []*step) (at, later int) {
	for at, s := range run {
		if later := columnGeneratedFrom(s, run[at+1:]); later > 0 {
			return at, later
		}
	}
	return 0, 0
}

// columnGeneratedFrom is the one-based position in rest of a column generated reads, or
// zero where none stands behind only the added columns of its table.
func columnGeneratedFrom(generated *step, rest []*step) int {
	add, ok := generated.operation.(*ast.AddColumnOperation)
	if !ok || add.Column == nil || strings.TrimSpace(add.Column.GeneratedExpression) == "" {
		return 0
	}
	reads := identifiers(add.Column.GeneratedExpression)
	for at, s := range rest {
		other, ok := s.operation.(*ast.AddColumnOperation)
		if !ok || other.Column == nil || tableKey(s.table) != tableKey(generated.table) {
			return 0
		}
		if slices.Contains(reads, strings.ToLower(other.Column.Name)) {
			return at + 1
		}
	}
	return 0
}

// linkSteps numbers the steps of a run and gives each the earlier steps it depends on.
func linkSteps(run []*step) {
	readers := make(map[string][]*step)
	writers := make(map[string][]*step)
	for at, current := range run {
		current.at = at
		current.after = nil
		for _, resource := range current.access.reads {
			current.after = append(current.after, writers[resource]...)
		}
		for _, resource := range current.access.writes {
			current.after = append(current.after, writers[resource]...)
			current.after = append(current.after, readers[resource]...)
		}
		slices.SortFunc(current.after, func(a, b *step) int { return cmp.Compare(a.at, b.at) })
		current.after = slices.Compact(current.after)
		for _, resource := range current.access.reads {
			readers[resource] = append(readers[resource], current)
		}
		for _, resource := range current.access.writes {
			writers[resource] = append(writers[resource], current)
		}
	}
}

// linearize orders steps so that each follows what before names for it, taking the
// step with the earliest place among those nothing holds back.
func linearize(steps []*step, before func(*step) []*step) []*step {
	waiting := make(map[*step]int, len(steps))
	blocked := make(map[*step][]*step, len(steps))
	ready := make([]*step, 0, len(steps))
	enqueue := func(s *step) {
		at, _ := slices.BinarySearchFunc(ready, s, comparePlaces)
		ready = slices.Insert(ready, at, s)
	}
	for _, s := range steps {
		ahead := before(s)
		waiting[s] = len(ahead)
		for _, earlier := range ahead {
			blocked[earlier] = append(blocked[earlier], s)
		}
		if len(ahead) == 0 {
			enqueue(s)
		}
	}
	out := make([]*step, 0, len(steps))
	for len(ready) > 0 {
		next := ready[0]
		ready = ready[1:]
		out = append(out, next)
		for _, later := range blocked[next] {
			waiting[later]--
			if waiting[later] == 0 {
				enqueue(later)
			}
		}
	}
	return out
}

// forwardPlaces gives the steps of a forward run the place the community binary
// writes them in: the tables the plan changes by name, each as its index drops, one
// ALTER TABLE and its new indexes; then the tables it creates; then the foreign keys
// that had to wait for them; then the tables it drops.
func (how arrangement) forwardPlaces(diff *difftypes.SchemaDiff) func(run []*step) {
	columns := columnOrderOf(diff)
	return func(run []*step) {
		created := newTablesOf(run)
		needed := neededByAnotherTable(run)
		for _, s := range run {
			s.place = how.forwardPlace(s, created, needed, columns)
		}
		changed := changedTableDepths(run)
		for _, s := range run {
			if s.place.phase == phaseChanged {
				s.place.depth = changed[tableKey(s.table)]
			}
		}
	}
}

// neededByAnotherTable is the steps a step on another table has to follow.
func neededByAnotherTable(run []*step) map[*step]bool {
	needed := make(map[*step]bool)
	for _, s := range run {
		for _, earlier := range s.after {
			if tableKey(earlier.table) != tableKey(s.table) {
				needed[earlier] = true
			}
		}
	}
	return needed
}

// changedTableDepths is how long a chain of changed tables each changed table needs
// ahead of it. The community binary writes a table after the one it needs, whatever
// their names; dependencies between steps hold in any case, and this keeps the steps
// of one table together where they allow it.
func changedTableDepths(run []*step) map[string]int {
	changed := make(map[string]string)
	for _, s := range run {
		if s.place.phase == phaseChanged {
			changed[bare(s.table)] = tableKey(s.table)
		}
	}
	references := make(map[string][]string)
	for _, s := range run {
		if s.place.phase != phaseChanged {
			continue
		}
		own := tableKey(s.table)
		for _, earlier := range s.after {
			if earlier.place.phase == phaseChanged && tableKey(earlier.table) != own {
				references[own] = append(references[own], tableKey(earlier.table))
			}
		}
		// A key this table gains names the table it needs, whatever else that table's
		// change does.
		if add, ok := s.operation.(*ast.AddConstraintOperation); ok && add.Constraint != nil && add.Constraint.Reference != nil {
			if target, ok := changed[bare(add.Constraint.Reference.Table)]; ok && target != own {
				references[own] = append(references[own], target)
			}
		}
	}
	return chainDepths(references)
}

// chainDepths is the length of the longest chain ahead of each key of references. A
// cycle is cut where the walk meets it again, so its depths are finite and hold no
// promise; what has to precede what is decided by the steps, not by these.
func chainDepths(references map[string][]string) map[string]int {
	depths := make(map[string]int, len(references))
	visiting := make(map[string]bool, len(references))
	var walk func(key string) int
	walk = func(key string) int {
		if depth, known := depths[key]; known {
			return depth
		}
		if visiting[key] {
			return 0
		}
		visiting[key] = true
		depth := 0
		for _, name := range references[key] {
			depth = max(depth, walk(name)+1)
		}
		visiting[key] = false
		depths[key] = depth
		return depth
	}
	keys := slices.Sorted(maps.Keys(references))
	for _, key := range keys {
		walk(key)
	}
	return depths
}

// newTables is the tables a run creates: under their exact name, under the bare name a
// foreign key may name one by, and how long a chain of them each bare name needs ahead
// of it. Two new tables of one bare name share a depth, which can only hold one back.
type newTables struct {
	exact, loose map[string]bool
	depth        map[string]int
}

func newTablesOf(run []*step) newTables {
	tables := newTables{exact: make(map[string]bool), loose: make(map[string]bool)}
	references := make(map[string][]string)
	for _, s := range run {
		if table, ok := s.node.(*ast.CreateTableNode); ok {
			tables.exact[tableKey(table.Name)] = true
			tables.loose[bare(table.Name)] = true
		}
	}
	for _, s := range run {
		if table, ok := s.node.(*ast.CreateTableNode); ok {
			references[bare(table.Name)] = append(references[bare(table.Name)], referencedNewTables(table, tables.loose)...)
		}
	}
	tables.depth = chainDepths(references)
	return tables
}

func (how arrangement) forwardPlace(s *step, created newTables, needed map[*step]bool, columns columnOrder) place {
	own := place{bareTable: bare(s.table), table: tableKey(s.table)}
	switch n := s.node.(type) {
	case *ast.CreateTableNode:
		return how.newTablePlace(own, created)
	case *ast.DropTableNode:
		return place{phase: phaseDropped}
	case *ast.IndexNode:
		// An index of a new table waits for it and then has nothing ahead of it, so it
		// follows its table here too.
		own.phase, own.part, own.name = phaseChanged, partIndexes, n.Name
		return own
	case *ast.DropIndexNode:
		own.phase, own.part, own.name = phaseChanged, partIndexDrops, n.Name
		return own
	}
	if created.exact[tableKey(s.table)] {
		own.phase = phaseTrailing
		return own
	}
	if drop, dropped := s.operation.(*ast.DropConstraintOperation); dropped && needed[s] && how.droppedKind(s.table, drop) == ast.ForeignKeyConstraint {
		// Another table's change needs this key gone first, and the rest of this table's
		// changes may need that change: the community binary lets the key go ahead of all.
		own.phase = phaseLeading
		return own
	}
	if add, ok := s.operation.(*ast.AddConstraintOperation); ok && add.Constraint != nil &&
		add.Constraint.Reference != nil && created.loose[bare(add.Constraint.Reference.Table)] {
		own.phase = phaseTrailing
		return own
	}
	own.phase, own.part = phaseChanged, partOperations
	own.class, own.column = how.operationRank(s, columns[bare(s.table)])
	return own
}

// newTablePlace is a new table, the tables no other new table has to precede first and by
// name, then the ones that need only those, and so on. It is the community binary's order
// on PostgreSQL and the MySQL family; on SQLite it writes them in the order it read them,
// which is the planner's here.
func (how arrangement) newTablePlace(own place, created newTables) place {
	if !how.merge {
		return place{phase: phaseNew}
	}
	own.phase, own.depth = phaseNew, created.depth[own.bareTable]
	return own
}

// droppedKind is the kind of constraint a drop removes. Facts about two tables of one
// bare name may disagree; the planner's own flag decides first.
func (how arrangement) droppedKind(table string, drop *ast.DropConstraintOperation) ast.ConstraintType {
	switch {
	case drop.ForeignKey:
		return ast.ForeignKeyConstraint
	case drop.PrimaryKey:
		return ast.PrimaryKeyConstraint
	case drop.Unique:
		return ast.UniqueConstraint
	case drop.Check:
		return ast.CheckConstraint
	}
	facts := how.droppedConstraint(table, drop)
	if len(facts) == 0 {
		return ast.CheckConstraint
	}
	return facts[0].kind
}

// operationRank is an operation's place in a table's ALTER TABLE: the primary key
// the statement drops, the checks, the columns by position, the primary key it
// adds, the unique constraints, then the foreign keys.
func (how arrangement) operationRank(s *step, positions map[string]int) (class, column int) {
	switch op := s.operation.(type) {
	case *ast.DropConstraintOperation:
		switch how.droppedKind(s.table, op) {
		case ast.ForeignKeyConstraint:
			return 5, 0
		case ast.PrimaryKeyConstraint:
			return 0, 0
		default:
			return 1, 0
		}
	case *ast.AddConstraintOperation:
		if op.Constraint == nil {
			return 7, 0
		}
		switch op.Constraint.Type {
		case ast.PrimaryKeyConstraint:
			return 3, 0
		case ast.UniqueConstraint:
			return 4, 0
		case ast.ForeignKeyConstraint:
			return 6, 0
		default:
			return 1, 0
		}
	case *ast.AddColumnOperation:
		return 2, position(positions, op.Column.Name)
	case *ast.DropColumnOperation:
		return 2, position(positions, op.ColumnName)
	case *ast.ModifyColumnOperation:
		return 2, position(positions, op.Column.Name)
	default:
		return 7, 0
	}
}

func position(positions map[string]int, column string) int {
	if at, ok := positions[column]; ok {
		return at
	}
	return len(positions)
}

// columnOrder is the position of each column of each table a plan changes: the
// columns the table holds, then the ones the plan adds.
type columnOrder map[string]map[string]int

func columnOrderOf(diff *difftypes.SchemaDiff) columnOrder {
	order := make(columnOrder)
	if diff == nil {
		return order
	}
	for _, table := range diff.TablesModified {
		positions := make(map[string]int)
		next := func(name string) {
			if _, known := positions[name]; !known {
				positions[name] = len(positions)
			}
		}
		for _, column := range table.Current.Table.Columns {
			next(column.Name)
		}
		for _, column := range table.ColumnsRemoved {
			next(column.Name)
		}
		for _, column := range table.ColumnsAdded {
			next(column.Name)
		}
		order[bare(table.TableName)] = positions
	}
	return order
}

// referencedNewTables names the other new tables table's foreign keys point at.
func referencedNewTables(table *ast.CreateTableNode, created map[string]bool) []string {
	var names []string
	for _, constraint := range table.Constraints {
		if constraint.Type != ast.ForeignKeyConstraint || constraint.Reference == nil {
			continue
		}
		if name := bare(constraint.Reference.Table); created[name] && name != bare(table.Name) {
			names = append(names, name)
		}
	}
	return names
}

// absorbForeignKeys moves a foreign key of a new table into its CREATE TABLE where
// everything the key needs already stands ahead of that statement. The steps it moves
// over do not depend on it, or it would have had to follow them.
func absorbForeignKeys(order []*step) []*step {
	positions := make(map[*step]int, len(order))
	tables := make(map[string]*ast.CreateTableNode)
	statements := make(map[string]*step)
	created := make(map[string]bool)
	for at, s := range order {
		positions[s] = at
		if table, ok := s.node.(*ast.CreateTableNode); ok {
			tables[tableKey(table.Name)], statements[tableKey(table.Name)] = table, s
			created[bare(table.Name)] = true
		}
	}
	return slices.DeleteFunc(slices.Clone(order), func(s *step) bool {
		add, ok := s.operation.(*ast.AddConstraintOperation)
		if !ok || add.Constraint == nil || add.Constraint.Type != ast.ForeignKeyConstraint || add.Constraint.Reference == nil {
			return false
		}
		table := tables[tableKey(s.table)]
		if table == nil || created[bare(add.Constraint.Reference.Table)] {
			return false
		}
		statement := statements[tableKey(s.table)]
		for _, earlier := range s.after {
			if earlier != statement && positions[earlier] > positions[statement] {
				return false
			}
		}
		table.Constraints = append(table.Constraints, add.Constraint)
		return true
	})
}

// statements writes an ordered run: each statement as it is, and the operations next to
// each other on one table as one ALTER TABLE where the server takes them together.
func (how arrangement) statements(order []*step) []ast.Node {
	out := make([]ast.Node, 0, len(order))
	for at := 0; at < len(order); {
		if order[at].operation == nil {
			out = append(out, order[at].node)
			at++
			continue
		}
		end := at + 1
		for end < len(order) && how.together(order[at:end], order[end]) {
			end++
		}
		out = append(out, how.alterStatements(order[at:end])...)
		at = end
	}
	return out
}

func (how arrangement) together(group []*step, next *step) bool {
	if next.operation == nil || tableKey(next.table) != tableKey(group[0].table) {
		return false
	}
	return !slices.ContainsFunc(group, func(member *step) bool { return how.keptApart(member, next) })
}

// keptApart reports two operations of one table the server refuses in one statement.
func (how arrangement) keptApart(a, b *step) bool {
	if how.sameNamedKeysApart && (swapsForeignKey(how, a, b) || swapsForeignKey(how, b, a)) {
		return true
	}
	return how.selfKeysApart && (how.needsOfItsOwnTable(a, b) || how.needsOfItsOwnTable(b, a))
}

func swapsForeignKey(how arrangement, dropped, added *step) bool {
	drop, isDrop := dropped.operation.(*ast.DropConstraintOperation)
	add, isAdd := added.operation.(*ast.AddConstraintOperation)
	return isDrop && isAdd && add.Constraint != nil && add.Constraint.Type == ast.ForeignKeyConstraint &&
		how.droppedKind(dropped.table, drop) == ast.ForeignKeyConstraint &&
		strings.EqualFold(drop.ConstraintName, add.Constraint.Name)
}

// needsOfItsOwnTable reports a foreign key of a table to itself, added or dropped, and
// an operation that changes what it references there. Adding the two in one statement
// MySQL answers with 1215 or 1826 and MariaDB with 1005; dropping them, both answer 1553.
func (how arrangement) needsOfItsOwnTable(key, other *step) bool {
	for _, fact := range how.constraintsOf(key) {
		// Only a foreign key names a table it references.
		if bare(fact.refTable) != bare(fact.table) {
			continue
		}
		for _, column := range fact.refColumns {
			if slices.Contains(other.access.writes, columnResource(key.table, column)) ||
				slices.Contains(other.access.writes, keyedResource(key.table, column)) {
				return true
			}
		}
	}
	return false
}

// constraintsOf is the constraints a step adds or drops.
func (how arrangement) constraintsOf(s *step) []constraintFact {
	switch op := s.operation.(type) {
	case *ast.AddConstraintOperation:
		if op.Constraint != nil {
			if fact, ok := constraintFactOf(s.table, op.Constraint); ok {
				return []constraintFact{fact}
			}
		}
	case *ast.DropConstraintOperation:
		return how.droppedConstraint(s.table, op)
	}
	return nil
}

// alterStatements writes a group of one table's operations. The steps stand in an order
// that runs one by one, so the node holds that order, and one statement's own order
// beside it for the renderer to use where the whole group fits one statement.
func (how arrangement) alterStatements(group []*step) []ast.Node {
	if how.merge {
		statement := group
		if how.restatesColumns {
			statement = lastChangeOfEachColumn(statement)
		}
		return []ast.Node{&ast.AlterTableNode{
			Name: group[0].table, Operations: operationsOf(group), OneStatement: operationsOf(how.statementOrder(statement)),
		}}
	}
	nodes := make([]ast.Node, 0, len(group))
	for _, s := range group {
		nodes = append(nodes, &ast.AlterTableNode{Name: s.table, Operations: []ast.AlterOperation{s.operation}})
	}
	return nodes
}

func operationsOf(group []*step) []ast.AlterOperation {
	operations := make([]ast.AlterOperation, 0, len(group))
	for _, s := range group {
		operations = append(operations, s.operation)
	}
	return operations
}

// statementOrder writes the operations of one statement, in the place each wants where
// the order written does not matter.
func (how arrangement) statementOrder(group []*step) []*step {
	members := make(map[*step]bool, len(group))
	for _, s := range group {
		members[s] = true
	}
	return linearize(group, func(s *step) []*step {
		return slices.DeleteFunc(slices.Clone(s.after), func(earlier *step) bool {
			return !members[earlier] || how.freeInOneStatement(earlier, s)
		})
	})
}

// freeInOneStatement reports two dependent operations whose order inside one statement
// changes nothing. On every engine added columns take the order written, which is the
// order the table holds them in. Otherwise MySQL and MariaDB read the statement as a
// whole. PostgreSQL runs a statement's drops in the order written and its additions in
// passes of its own, columns ahead of constraints whatever the text says, so only an
// added constraint is free of the column it reads.
func (how arrangement) freeInOneStatement(earlier, later *step) bool {
	_, earlierAdds := earlier.operation.(*ast.AddColumnOperation)
	_, laterAdds := later.operation.(*ast.AddColumnOperation)
	if earlierAdds && laterAdds {
		return false
	}
	if how.unorderedStatement {
		return true
	}
	_, constrained := later.operation.(*ast.AddConstraintOperation)
	return earlierAdds && constrained
}

// lastChangeOfEachColumn leaves out a MODIFY COLUMN a later one of the same column
// replaces. A plan may step a column through a state it needs between statements, as
// when it drops a key under an AUTO_INCREMENT column. One statement has no between, and
// MySQL and MariaDB answer `Unknown column` to a column named twice.
func lastChangeOfEachColumn(group []*step) []*step {
	last := make(map[string]*step)
	for _, s := range group {
		if modify, ok := s.operation.(*ast.ModifyColumnOperation); ok {
			last[modify.Column.Name] = s
		}
	}
	return slices.DeleteFunc(slices.Clone(group), func(s *step) bool {
		modify, ok := s.operation.(*ast.ModifyColumnOperation)
		return ok && last[modify.Column.Name] != s
	})
}

// withoutRedundantDrops leaves out the drop of a constraint forward wrote inside a table
// the rollback then drops, when nothing between the two needs the constraint gone.
func (how arrangement) withoutRedundantDrops(run []*step) []*step {
	if how.redundant == nil {
		return run
	}
	drops := make(map[string]int)
	for at, s := range run {
		if table, ok := s.node.(*ast.DropTableNode); ok {
			for _, name := range table.TableNames() {
				drops[tableKey(name)] = at
			}
		}
	}
	kept := make([]*step, 0, len(run))
	for at, s := range run {
		if how.isRedundantDrop(s, at, run, drops) {
			continue
		}
		kept = append(kept, s)
	}
	return kept
}

func (how arrangement) isRedundantDrop(s *step, at int, run []*step, drops map[string]int) bool {
	drop, ok := s.operation.(*ast.DropConstraintOperation)
	if !ok || !how.redundant[tableKey(s.table)][drop.ConstraintName] {
		return false
	}
	end, dropped := drops[tableKey(s.table)]
	if !dropped || end < at {
		return false
	}
	return !slices.ContainsFunc(run[at+1:end], func(between *step) bool {
		return s.access.conflicts(between.access)
	})
}

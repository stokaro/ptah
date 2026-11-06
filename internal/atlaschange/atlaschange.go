// Package atlaschange names the change a planned statement makes, in the words
// the Atlas CLI writes above each statement of a migration file:
// `create "users" table`, `modify "users" table`, `drop index "i" from table:
// "t"`.
//
// The community binary's planners are the source of the wording, and it differs
// per engine. [For] selects it: the PostgreSQL planner's words are the default,
// the MySQL family writes its schema changes in lower case, `add new schema
// named "s"`, and SQLite names a column it adds, `add column "c" to table: "t"`,
// and each step of a table rebuild. The community binary has no driver for the
// other engines, so they take the PostgreSQL words. The objects it does not
// plan, such as views, routines, triggers, sequences and roles, take the same
// pattern: `create "v" view`, `modify "v" view` for an in-place replace, and
// `drop "v" view`.
//
// A rollback file writes each statement under `reverse: ` and the change it
// undoes, which is the forward change's comment. A rollback here is planned on
// its own rather than by inverting the forward plan, so [Words.Rollback] reads
// that comment off the rollback statement itself: a DROP TABLE undoes
// `create "users" table`. A rollback may rebuild an object the forward plan
// replaced in place, such as a view the forward plan changed with CREATE OR
// REPLACE VIEW and the rollback drops and creates; both of its statements are
// named after the forward `modify "v" view`. A table rebuild undoes nothing
// statement by statement, so each of its steps keeps its own words.
//
// An operation of a built-in feature owner takes the same pattern, `create
// "hstore" extension`, `drop "p" policy`, `create "posint" domain`, because the
// community binary plans none of those objects either.
//
// An operation a provider plans is named from the object it is planned for
// and the effect its plan step has on that object, in the wording of the same
// object's statement: `create "orders" table` for a step that creates the
// table, `drop "orders" table` for one that drops it, and `modify "orders"
// table` for any other step on it, such as one that adds a column or a
// changefeed. A step that only reads its table and creates or drops one index
// of it is that index's change, `create index "orders_by_total" to table:
// "orders"`, as the built-in YDB engine named its `ALTER TABLE ... ADD INDEX`.
// An operation on an object of a kind of the provider's own, or one that
// carries no effects, is not named. Neither is a step of a table rebuild a
// provider planned that only fills the new table or moves a table to another
// name; the rebuild's other steps keep their words (see rebuildMoves).
package atlaschange

import (
	"fmt"
	"slices"
	"strings"

	"ptah.run/core/ast"
	"ptah.run/core/objectidentity"
	"ptah.run/core/plangraph"
	"ptah.run/core/platform"
	"ptah.run/core/platform/descriptor"
	"ptah.run/feature/pgpolicy"
	"ptah.run/feature/pgtype"
	"ptah.run/internal/planner/sqliterebuild"
	"ptah.run/internal/sqliteforeignkeys"
	"ptah.run/internal/tableref"
)

// Words names the changes of a plan in one engine's wording. The zero value is
// the PostgreSQL planner's.
type Words struct {
	family wordFamily
}

// wordFamily is the set of words one engine's changes are named in.
type wordFamily int

const (
	postgresWords wordFamily = iota
	mysqlWords
	sqliteWords
)

// For returns the wording of dialect.
func For(dialect string) Words {
	target, ok := platform.Lookup(dialect)
	if !ok {
		return Words{}
	}
	switch target.Atlas.ChangeWords {
	case descriptor.MySQLDriverChangeWords:
		return Words{family: mysqlWords}
	case descriptor.SQLiteDriverChangeWords:
		return Words{family: sqliteWords}
	default:
		return Words{}
	}
}

// Forward returns the comment naming the change each node of plan makes, in
// plan's order. The comment is empty for a node that makes no change Atlas
// would name, such as a comment of the plan's own, raw SQL, or an owner
// operation that reports no object it changes.
func (w Words) Forward(plan []ast.Node) []string {
	steps := w.rebuildSteps(plan)
	moves := rebuildMoves(plan)
	names := make([]string, len(plan))
	for i, node := range plan {
		if step, ok := steps[i]; ok {
			names[i] = step
			continue
		}
		if moves[i] {
			continue
		}
		if c, ok := w.describe(node); ok {
			names[i] = c.String()
		}
	}
	return names
}

// Rollback returns the comment of the forward change each node of plan, the
// statements of a rollback planned against forward, undoes: the inverse of the
// node's own change, except that a statement that drops or creates an object
// forward only modifies undoes that modification, and a step of a table rebuild
// keeps its own words.
//
// Objects compare by kind and name without case, and by the schema, or for an
// object named within its table, by the table, where both statements name
// one, so a view of one schema is not taken for a view of the same name in
// another.
func (w Words) Rollback(forward, plan []ast.Node) []string {
	type object struct{ kind, name string }
	type seen struct {
		change change
		scope  scope
	}
	modified := make(map[object][]seen)
	rebuilt := make(map[object][]scope)
	for _, node := range forward {
		c, ok := w.describe(node)
		if !ok {
			continue
		}
		key := object{c.kind, strings.ToLower(c.name)}
		switch c.verb {
		case modify:
			modified[key] = append(modified[key], seen{c, scopeOf(node)})
		case create, drop:
			rebuilt[key] = append(rebuilt[key], scopeOf(node))
		}
	}
	undone := func(node ast.Node) string {
		c, ok := w.describe(node)
		if !ok {
			return ""
		}
		if c.verb != create && c.verb != drop {
			return c.inverse().String()
		}
		key := object{c.kind, strings.ToLower(c.name)}
		at := scopeOf(node)
		if slices.ContainsFunc(rebuilt[key], at.overlaps) {
			return c.inverse().String()
		}
		for _, forward := range modified[key] {
			if forward.scope.overlaps(at) {
				return forward.change.String()
			}
		}
		return c.inverse().String()
	}
	steps := w.rebuildSteps(plan)
	moves := rebuildMoves(plan)
	names := make([]string, len(plan))
	for i, node := range plan {
		if step, ok := steps[i]; ok {
			names[i] = step
			continue
		}
		if moves[i] {
			continue
		}
		names[i] = undone(node)
	}
	return names
}

// rebuildMoves finds the steps of the table rebuilds a provider planned that
// take no name of their own, by their index in plan. A provider rebuilds a
// table as the built-in YDB engine did: it creates a new table, fills it from
// the old one, moves the old table to a spare name, moves the new table onto
// the old one's name, and drops the old table. A move alters one table and
// creates or drops a second one. The steps between the creation of a new
// table and its move that only read or alter it or its indexes, or read the
// table it replaces, fill it.
//
// The built-in YDB engine named the creation, any change to the replaced
// table and the drop, and none of these steps: its rebuild wrote them as raw
// statements. A name from a move's subject would also misstate it, since the
// move of the new table drops that table's name and keeps the table, which
// would read `drop "__ptah_rebuild_t" table`.
func rebuildMoves(plan []ast.Node) map[int]bool {
	moves := make(map[int]bool)
	for i, node := range plan {
		effects, ok := nativeEffects(node)
		if !ok {
			continue
		}
		moved, onto, ok := tableMove(effects)
		if !ok {
			continue
		}
		moves[i] = true
		if moved.drop {
			for _, j := range fillers(plan[:i], moved.table, onto) {
				moves[j] = true
			}
		}
	}
	return moves
}

// fillers returns the positions of the steps of plan that fill table, the new
// table of a rebuild that the last step of plan moves onto replaced: those
// after the step that creates table. It returns none when no step creates it.
func fillers(plan []ast.Node, table, replaced objectidentity.ID) []int {
	var found []int
	for j, node := range slices.Backward(plan) {
		effects, ok := nativeEffects(node)
		if !ok {
			continue
		}
		if slices.ContainsFunc(effects, func(effect plangraph.Effect) bool {
			return effect.Subject.Key() == table.Key() && effect.Action == plangraph.Create
		}) {
			return found
		}
		if fills(effects, table, replaced) {
			found = append(found, j)
		}
	}
	return nil
}

// movedTable is a table a move creates or drops.
type movedTable struct {
	table objectidentity.ID
	drop  bool
}

// tableMove reports whether effects are those of a move: the step alters one
// table and creates or drops one other table, and changes no third one. It
// returns the table the step creates or drops, and the one it alters.
func tableMove(effects []plangraph.Effect) (movedTable, objectidentity.ID, bool) {
	var moved []movedTable
	var altered []objectidentity.ID
	for _, effect := range effects {
		if effect.Subject.Kind != objectidentity.KindTable {
			continue
		}
		switch effect.Action {
		case plangraph.Create, plangraph.Drop:
			moved = append(moved, movedTable{table: effect.Subject, drop: effect.Action == plangraph.Drop})
		case plangraph.Alter:
			altered = append(altered, effect.Subject)
		}
	}
	if len(moved) != 1 || len(altered) != 1 || moved[0].table.Key() == altered[0].Key() {
		return movedTable{}, objectidentity.ID{}, false
	}
	return moved[0], altered[0], true
}

// fills reports whether effects only read or alter table or its indexes, or
// read replaced: those of a step that fills the new table of a rebuild before
// it moves onto replaced.
func fills(effects []plangraph.Effect, table, replaced objectidentity.ID) bool {
	for _, effect := range effects {
		switch {
		case effect.Action != plangraph.Read && effect.Action != plangraph.Alter:
			return false
		case effect.Subject.Key() == table.Key():
		case effect.Subject.Kind == objectidentity.KindIndex && indexOf(effect.Subject, table):
		case effect.Action == plangraph.Read && effect.Subject.Key() == replaced.Key():
		default:
			return false
		}
	}
	return len(effects) > 0
}

// nativeEffects returns the effects of the plan step of an operation a
// provider planned, and false for any other node.
func nativeEffects(node ast.Node) ([]plangraph.Effect, bool) {
	statement, ok := node.(*ast.ExtensionStatement)
	if !ok {
		return nil, false
	}
	operation, ok := statement.Payload.(nativeOperation)
	if !ok {
		return nil, false
	}
	return operation.Effects(), true
}

// rebuildSteps names the statements of the table rebuilds a SQLite plan makes,
// by their index in plan: the pair of pragmas around them, and for each table
// the new table, the copy of its rows, the drop of the old table and the rename.
// The copy is what says a table is being rebuilt, so the statements around it
// are read from it rather than from their own shape: DROP TABLE is the drop of
// an old table only here.
func (w Words) rebuildSteps(plan []ast.Node) map[int]string {
	if w.family != sqliteWords {
		return nil
	}
	steps := make(map[int]string)
	for i, node := range plan {
		raw, ok := node.(*ast.RawSQLNode)
		if !ok {
			continue
		}
		switch raw.SQL {
		case sqliteforeignkeys.DisableStatement:
			steps[i] = "disable the enforcement of foreign-keys constraints"
		case sqliteforeignkeys.EnableStatement:
			steps[i] = "enable back the enforcement of foreign-keys constraints"
		default:
			if copied, ok := sqliterebuild.ReadCopyStatement(raw.SQL); ok {
				steps[i] = fmt.Sprintf("copy rows from old table %q to new temporary table %q", bare(copied.Old), bare(copied.New))
				nameRebuildNeighbors(steps, plan, i, copied)
			}
		}
	}
	return steps
}

// nameRebuildNeighbors names the statements a rebuild writes around the copy of
// its rows at position at: the new table before it, the old table's drop and the
// rename after it.
func nameRebuildNeighbors(steps map[int]string, plan []ast.Node, at int, copied sqliterebuild.CopyStatement) {
	if at > 0 {
		if table, ok := plan[at-1].(*ast.CreateTableNode); ok && table.Name == copied.New {
			steps[at-1] = fmt.Sprintf("create %q table", bare(copied.New))
		}
	}
	if at+1 < len(plan) {
		if dropped, ok := plan[at+1].(*ast.DropTableNode); ok && slices.Equal(dropped.TableNames(), []string{copied.Old}) {
			steps[at+1] = fmt.Sprintf("drop %q table after copying rows", bare(copied.Old))
		}
	}
	if at+2 < len(plan) {
		if raw, ok := plan[at+2].(*ast.RawSQLNode); ok && raw.SQL == copied.Rename() {
			steps[at+2] = fmt.Sprintf("rename temporary table %q to %q", bare(copied.New), bare(copied.Old))
		}
	}
}

// describe names the change node makes in this wording.
func (w Words) describe(node ast.Node) (change, bool) {
	if w.family == sqliteWords {
		if c, ok := describeSQLiteColumn(node); ok {
			return c, true
		}
	}
	if raw, ok := node.(*ast.RawSQLNode); ok && w.family == mysqlWords {
		if name, ok := droppedDatabase(raw.SQL); ok {
			return change{verb: drop, kind: kindSchema, name: name, words: w.family}, true
		}
	}
	c, ok := describe(node)
	c.words = w.family
	return c, ok
}

// describeSQLiteColumn names the one column an ALTER TABLE adds or drops, which
// is what SQLite's ALTER TABLE changes at a time.
func describeSQLiteColumn(node ast.Node) (change, bool) {
	alter, ok := node.(*ast.AlterTableNode)
	if !ok || len(alter.Operations) != 1 {
		return change{}, false
	}
	switch op := alter.Operations[0].(type) {
	case *ast.AddColumnOperation:
		return change{verb: addColumn, kind: kindColumn, name: op.Column.Name, table: bare(alter.Name)}, true
	case *ast.DropColumnOperation:
		return change{verb: dropColumn, kind: kindColumn, name: op.ColumnName, table: bare(alter.Name)}, true
	}
	return change{}, false
}

// droppedDatabase is the database a MySQL-family plan's DROP DATABASE names. The
// planner writes it as raw SQL with no other form to read.
func droppedDatabase(statement string) (string, bool) {
	quoted, ok := strings.CutPrefix(statement, "DROP DATABASE ")
	if !ok {
		return "", false
	}
	name, ok := strings.CutPrefix(quoted, "`")
	if !ok {
		return "", false
	}
	name, ok = strings.CutSuffix(name, "`")
	if !ok {
		return "", false
	}
	return strings.ReplaceAll(name, "``", "`"), true
}

// scope is what qualifies the name of an object a statement changes: the
// schema of an object named within a schema, and the table, with its schema,
// of one named within its table. An empty part is one the statement does not
// name.
type scope struct{ schema, table string }

// overlaps reports whether s and other may name the same place: every part
// both name is equal.
func (s scope) overlaps(other scope) bool {
	return (s.schema == "" || other.schema == "" || s.schema == other.schema) &&
		(s.table == "" || other.table == "" || s.table == other.table)
}

// scopeOf is the scope of the object node changes.
func scopeOf(node ast.Node) scope {
	switch n := node.(type) {
	case *ast.CreateViewNode:
		return schemaScope(n.Name)
	case *ast.DropViewNode:
		return schemaScope(n.Name)
	case *ast.CreateMaterializedViewNode:
		return schemaScope(n.Name)
	case *ast.DropMaterializedViewNode:
		return schemaScope(n.Name)
	case *ast.CreateFunctionNode:
		return schemaScope(n.Name)
	case *ast.DropFunctionNode:
		return schemaScope(n.Name)
	case *ast.CreateTableNode:
		return schemaScope(n.Name)
	case *ast.DropTableNode:
		if names := n.TableNames(); len(names) > 0 {
			return schemaScope(names[0])
		}
	case *ast.AlterTableNode:
		return schemaScope(n.Name)
	case *ast.CreateTriggerNode:
		return tableScope(n.Table)
	case *ast.DropTriggerNode:
		return tableScope(n.Table)
	case *ast.CreatePolicyNode:
		return tableScope(n.Table)
	case *ast.DropPolicyNode:
		return tableScope(n.Table)
	case *ast.ExtensionStatement:
		if operation, ok := n.Payload.(nativeOperation); ok {
			return nativeScope(operation.Subject())
		}
		return ownerScope(n.Payload)
	}
	return scope{}
}

// ownerScope is the scope of the object an operation of a built-in feature owner
// changes: a type's schema, and a policy's table with its schema. An extension
// is one object of the database, so it has no scope.
func ownerScope(payload ast.ExtensionPayload) scope {
	switch op := payload.(type) {
	case *pgpolicy.PolicyOperation:
		return scope{schema: strings.ToLower(op.Schema), table: strings.ToLower(op.Table)}
	case *pgpolicy.PolicyCommentOperation:
		return scope{schema: strings.ToLower(op.Schema), table: strings.ToLower(op.Table)}
	case *pgtype.CreateDomain:
		return scope{schema: strings.ToLower(op.Target.Schema)}
	case *pgtype.AlterDomain:
		return scope{schema: strings.ToLower(op.Target.Schema)}
	case *pgtype.CreateComposite:
		return scope{schema: strings.ToLower(op.Target.Schema)}
	case *pgtype.AlterComposite:
		return scope{schema: strings.ToLower(op.Target.Schema)}
	case *pgtype.CreateRange:
		return scope{schema: strings.ToLower(op.Target.Schema)}
	case *pgtype.Drop:
		return scope{schema: strings.ToLower(op.Target.Schema)}
	case *pgtype.Comment:
		return scope{schema: strings.ToLower(op.Target.Schema)}
	}
	return scope{}
}

// nativeScope is the scope of subject: its schema and the directories below
// it, and for an object named within its table, that table.
func nativeScope(subject objectidentity.ID) scope {
	path := []string{subject.Schema.Normalized}
	for _, part := range subject.Namespace.Values() {
		path = append(path, part.Normalized)
	}
	return scope{schema: strings.ToLower(strings.Join(path, "/")), table: strings.ToLower(subject.Parent.Normalized)}
}

// schemaScope is the scope of an object named name within a schema.
func schemaScope(name string) scope {
	if ref, ok := tableref.Parse(name); ok && ref.Qualified {
		return scope{schema: strings.ToLower(ref.Schema)}
	}
	return scope{}
}

// tableScope is the scope of an object named within table.
func tableScope(table string) scope {
	ref, ok := tableref.Parse(table)
	if !ok {
		return scope{}
	}
	at := scope{table: strings.ToLower(ref.Name)}
	if ref.Qualified {
		at.schema = strings.ToLower(ref.Schema)
	}
	return at
}

type verb int

const (
	create verb = iota
	modify
	drop
	rename
	comment
	addValue
	refresh
	grant
	revoke
	addColumn
	dropColumn
)

// object kinds whose wording differs from the default pattern.
const (
	kindTable  = "table"
	kindColumn = "column"
	kindIndex  = "index"
	kindEnum   = "enum type"
	kindSchema = "schema"
)

// change is one named change: what is done, to which kind of object, and the
// names the wording needs.
type change struct {
	verb verb
	kind string
	name string
	// words is the wording the change is named in.
	words wordFamily
	// table is the table an index or a commented column belongs to.
	table string
	// from and to are the two names of a rename; to is also the grantee of a
	// grant and the member of a membership.
	from, to string
	// others are the other tables one DROP TABLE names, which the wording
	// names after name, one change each.
	others []string
}

func (c change) inverse() change {
	switch c.verb {
	case create:
		c.verb = drop
	case drop:
		c.verb = create
	case rename:
		c.from, c.to = c.to, c.from
	case grant:
		c.verb = revoke
	case revoke:
		c.verb = grant
	case addColumn:
		c.verb = dropColumn
	case dropColumn:
		c.verb = addColumn
	}
	return c
}

func (c change) String() string {
	if len(c.others) > 0 {
		names := append([]string{c.name}, c.others...)
		parts := make([]string, 0, len(names))
		for _, name := range names {
			one := c
			one.name, one.others = name, nil
			parts = append(parts, one.String())
		}
		return strings.Join(parts, ", ")
	}
	switch c.kind {
	case kindTable:
		return c.tableWording()
	case kindColumn:
		return c.columnWording()
	case kindIndex:
		return c.indexWording()
	case kindEnum:
		return c.enumWording()
	case kindSchema:
		return c.schemaWording()
	}
	switch c.verb {
	case create:
		return fmt.Sprintf("create %q %s", c.name, c.kind)
	case drop:
		return fmt.Sprintf("drop %q %s", c.name, c.kind)
	case rename:
		return fmt.Sprintf("rename a %s from %q to %q", c.kind, c.from, c.to)
	case comment:
		return fmt.Sprintf("set comment to %s: %q", c.kind, c.name)
	case refresh:
		return fmt.Sprintf("refresh %q %s", c.name, c.kind)
	case grant:
		return fmt.Sprintf("grant %s to %q", c.name, c.to)
	case revoke:
		return fmt.Sprintf("revoke %s from %q", c.name, c.to)
	default:
		return fmt.Sprintf("modify %q %s", c.name, c.kind)
	}
}

func (c change) columnWording() string {
	switch c.verb {
	case rename:
		return fmt.Sprintf("rename a column from %q to %q", c.from, c.to)
	case addColumn:
		return fmt.Sprintf("add column %q to table: %q", c.name, c.table)
	case dropColumn:
		return fmt.Sprintf("drop column %q from table: %q", c.name, c.table)
	default:
		return fmt.Sprintf("set comment to column: %q on table: %q", c.name, c.table)
	}
}

func (c change) tableWording() string {
	switch c.verb {
	case create:
		return fmt.Sprintf("create %q table", c.name)
	case drop:
		return fmt.Sprintf("drop %q table", c.name)
	case rename:
		return fmt.Sprintf("rename a table from %q to %q", c.from, c.to)
	case comment:
		return fmt.Sprintf("set comment to table: %q", c.name)
	default:
		return fmt.Sprintf("modify %q table", c.name)
	}
}

func (c change) indexWording() string {
	switch c.verb {
	case create:
		return fmt.Sprintf("create index %q to table: %q", c.name, c.table)
	case drop:
		return fmt.Sprintf("drop index %q from table: %q", c.name, c.table)
	case rename:
		return fmt.Sprintf("rename an index from %q to %q", c.from, c.to)
	case comment:
		return fmt.Sprintf("set comment to index: %q on table: %q", c.name, c.table)
	default:
		return fmt.Sprintf("modify index %q on table: %q", c.name, c.table)
	}
}

func (c change) enumWording() string {
	switch c.verb {
	case create:
		return fmt.Sprintf("create enum type %q", c.name)
	case drop:
		return fmt.Sprintf("drop enum type %q", c.name)
	case rename:
		return fmt.Sprintf("rename an enum from %q to %q", c.from, c.to)
	case comment:
		return fmt.Sprintf("set comment to enum type: %q", c.name)
	case addValue:
		return fmt.Sprintf("add value to enum type: %q", c.name)
	default:
		return fmt.Sprintf("modify enum type %q", c.name)
	}
}

func (c change) schemaWording() string {
	add, remove := "Add new schema named %q", "Drop schema named %q"
	if c.words == mysqlWords {
		add, remove = "add new schema named %q", "drop schema named %q"
	}
	switch c.verb {
	case create:
		return fmt.Sprintf(add, c.name)
	case drop:
		return fmt.Sprintf(remove, c.name)
	default:
		return fmt.Sprintf("set comment to schema: %q", c.name)
	}
}

func describe(node ast.Node) (change, bool) {
	switch n := node.(type) {
	case *ast.CreateTableNode:
		return change{verb: create, kind: kindTable, name: bare(n.Name)}, true
	case *ast.DropTableNode:
		names := n.TableNames()
		if len(names) == 0 {
			return change{}, false
		}
		others := make([]string, 0, len(names)-1)
		for _, name := range names[1:] {
			others = append(others, bare(name))
		}
		return change{verb: drop, kind: kindTable, name: bare(names[0]), others: others}, true
	case *ast.AlterTableNode:
		return describeAlterTable(n), true
	case *ast.AlterTableEnableRLSNode:
		return change{verb: modify, kind: kindTable, name: bare(n.Table)}, true
	case *ast.AlterTableDisableRLSNode:
		return change{verb: modify, kind: kindTable, name: bare(n.Table)}, true
	case *ast.AlterTableForceRLSNode:
		return change{verb: modify, kind: kindTable, name: bare(n.Table)}, true
	case *ast.IndexNode:
		return change{verb: create, kind: kindIndex, name: bare(n.Name), table: bare(n.Table)}, true
	case *ast.DropIndexNode:
		return change{verb: drop, kind: kindIndex, name: bare(n.Name), table: bare(n.Table)}, true
	case *ast.AlterIndexNode:
		if n.NewName == "" {
			return change{}, false
		}
		return change{verb: rename, kind: kindIndex, from: bare(n.Name), to: bare(n.NewName)}, true
	case *ast.EnumNode:
		return change{verb: create, kind: kindEnum, name: bare(n.Name)}, true
	case *ast.DropTypeNode:
		return change{verb: drop, kind: kindEnum, name: bare(n.Name)}, true
	case *ast.AlterTypeNode:
		return describeAlterType(n), true
	case *ast.CreateTypeNode:
		return change{verb: create, kind: "type", name: bare(n.Name)}, true
	case *ast.CreateSchemaNode:
		return change{verb: create, kind: kindSchema, name: n.Name}, true
	case *ast.ExtensionStatement:
		if c, ok := describeOwner(n.Payload); ok {
			return c, true
		}
		return describeNative(n.Payload)
	default:
		return describeObject(node)
	}
}

// nativeOperation is what an operation a provider planned reports: the object
// it is planned for, and the effect of its plan step on each object the step
// touches.
type nativeOperation interface {
	Subject() objectidentity.ID
	Effects() []plangraph.Effect
}

// nativeKinds are the kinds of object an operation a provider planned is named
// for, with the kind its wording uses. The provider's own kinds are left out:
// the community binary names nothing of theirs.
var nativeKinds = map[objectidentity.Kind]string{
	objectidentity.KindTable:   kindTable,
	objectidentity.KindView:    "view",
	objectidentity.KindIndex:   kindIndex,
	objectidentity.KindMatView: "materialized view",
}

// describeNative names an operation a provider planned by its subject. The
// step's effect on the subject itself decides the verb: a creation creates it,
// a removal drops it, and anything else, such as an alteration or a change to
// a column or a changefeed of the subject, modifies it. A step on a table that
// only reads it and creates or drops one of its indexes is named for that
// index (see indexOfTable).
func describeNative(payload ast.ExtensionPayload) (change, bool) {
	operation, ok := payload.(nativeOperation)
	if !ok {
		return change{}, false
	}
	subject, effects := operation.Subject(), operation.Effects()
	kind, ok := nativeKinds[subject.Kind]
	if !ok || len(effects) == 0 {
		return change{}, false
	}
	if index, ok := indexOfTable(subject, effects); ok {
		return index, true
	}
	c := change{verb: modify, kind: kind, name: partName(subject.Name)}
	if kind == kindIndex {
		c.table = partName(subject.Parent)
	}
	key := subject.Key()
	for _, effect := range effects {
		if effect.Subject.Key() != key {
			continue
		}
		switch effect.Action {
		case plangraph.Create:
			c.verb = create
		case plangraph.Drop:
			c.verb = drop
		}
	}
	return c, true
}

// indexOfTable names the change a step on table makes when its effects only
// read the table and create or drop one index of it: the index's creation or
// removal, the change the built-in YDB engine named above its `ALTER TABLE
// ... ADD INDEX` and `ALTER TABLE ... DROP INDEX`. Any other footprint, such
// as one that also alters the table or touches a second object, is not one.
func indexOfTable(table objectidentity.ID, effects []plangraph.Effect) (change, bool) {
	if table.Kind != objectidentity.KindTable {
		return change{}, false
	}
	var found *plangraph.Effect
	for i, effect := range effects {
		switch {
		case effect.Subject.Key() == table.Key() && effect.Action == plangraph.Read:
		case found == nil && effect.Subject.Kind == objectidentity.KindIndex && indexOf(effect.Subject, table):
			found = &effects[i]
		default:
			return change{}, false
		}
	}
	if found == nil {
		return change{}, false
	}
	c := change{kind: kindIndex, name: partName(found.Subject.Name), table: partName(table.Name)}
	switch found.Action {
	case plangraph.Create:
		c.verb = create
	case plangraph.Drop:
		c.verb = drop
	default:
		return change{}, false
	}
	return c, true
}

// indexOf reports whether the index identity names an index of table: one in
// the table's schema whose parent is the table.
func indexOf(index, table objectidentity.ID) bool {
	parent := objectidentity.ID{Kind: objectidentity.KindTable, Database: index.Database, Catalog: index.Catalog, Schema: index.Schema,
		Namespace: index.Namespace, Name: index.Parent}
	return parent.Key() == table.Key()
}

// partName is the name of one component of an identity as its source spelled
// it, without quotes.
func partName(part objectidentity.Part) string {
	if part.Source == "" {
		return bare(part.Normalized)
	}
	return bare(part.Source)
}

// describeObject names the changes to the objects the community binary does
// not plan.
func describeObject(node ast.Node) (change, bool) {
	if c, ok := describeRelative(node); ok {
		return c, true
	}
	switch n := node.(type) {
	case *ast.CreateFunctionNode:
		return change{verb: createdVerb[n.Replace], kind: routineKinds[n.IsProcedure()], name: bare(n.Name)}, true
	case *ast.DropFunctionNode:
		others := make([]string, 0, len(n.Others))
		for _, other := range n.Others {
			others = append(others, bare(other.Name))
		}
		return change{verb: drop, kind: routineKinds[n.IsProcedure()], name: bare(n.Name), others: others}, true
	case *ast.CreateSequenceNode:
		return change{verb: create, kind: "sequence", name: n.Name}, true
	case *ast.AlterSequenceNode:
		return change{verb: modify, kind: "sequence", name: n.Name}, true
	case *ast.AlterSerialSequenceNode:
		return change{verb: modify, kind: kindTable, name: bare(n.Table)}, true
	case *ast.DropSequenceNode:
		return change{verb: drop, kind: "sequence", name: n.Name}, true
	case *ast.CreateRoleNode:
		return change{verb: create, kind: "role", name: n.Name}, true
	case *ast.AlterRoleNode:
		return change{verb: modify, kind: "role", name: n.Name}, true
	case *ast.DropRoleNode:
		return change{verb: drop, kind: "role", name: n.Name}, true
	default:
		return describeAccess(node)
	}
}

// describeRelative names the changes to the views and to the objects that
// belong to a table or a view.
func describeRelative(node ast.Node) (change, bool) {
	switch n := node.(type) {
	case *ast.CreateViewNode:
		return change{verb: createdVerb[n.Replace], kind: "view", name: bare(n.Name)}, true
	case *ast.DropViewNode:
		return change{verb: drop, kind: "view", name: bare(n.Name)}, true
	case *ast.CreateMaterializedViewNode:
		return change{verb: create, kind: "materialized view", name: bare(n.Name)}, true
	case *ast.DropMaterializedViewNode:
		return change{verb: drop, kind: "materialized view", name: bare(n.Name)}, true
	case *ast.RefreshMaterializedViewNode:
		return change{verb: refresh, kind: "materialized view", name: bare(n.Name)}, true
	case *ast.CreateTriggerNode:
		return change{verb: createdVerb[n.Replace], kind: "trigger", name: n.Name}, true
	case *ast.DropTriggerNode:
		return change{verb: drop, kind: "trigger", name: n.Name}, true
	case *ast.CreatePolicyNode:
		return change{verb: createdVerb[n.Replace], kind: "policy", name: n.Name}, true
	case *ast.DropPolicyNode:
		return change{verb: drop, kind: "policy", name: n.Name}, true
	default:
		return change{}, false
	}
}

// describeAccess names grants, revocations and comments.
func describeAccess(node ast.Node) (change, bool) {
	switch n := node.(type) {
	case *ast.GrantPrivilegeNode:
		return change{verb: grant, name: privileges(n.Privileges, n.ObjectName), to: n.Role}, true
	case *ast.RevokePrivilegeNode:
		return change{verb: revoke, name: privileges(n.Privileges, n.ObjectName), to: n.Role}, true
	case *ast.GrantRoleMembershipNode:
		return change{verb: grant, name: fmt.Sprintf("role %q", n.Role), to: n.Member}, true
	case *ast.RevokeRoleMembershipNode:
		return change{verb: revoke, name: fmt.Sprintf("role %q", n.Role), to: n.Member}, true
	case *ast.ObjectCommentNode:
		return describeObjectComment(n), true
	default:
		return change{}, false
	}
}

// createdVerb is the verb of a CREATE: a modification when the statement
// replaces the object in place.
var createdVerb = map[bool]verb{false: create, true: modify}

// routineKinds names a routine by whether it is a procedure.
var routineKinds = map[bool]string{false: "function", true: "procedure"}

// describeAlterTable names an ALTER TABLE: a rename or a comment alone is
// named as such, and anything else modifies the table.
func describeAlterTable(n *ast.AlterTableNode) change {
	table := bare(n.Name)
	if len(n.Operations) == 1 {
		switch op := n.Operations[0].(type) {
		case *ast.RenameColumnOperation:
			return change{verb: rename, kind: kindColumn, from: op.OldName, to: op.NewName, table: table}
		case *ast.RenameTableOperation:
			return change{verb: rename, kind: kindTable, from: table, to: bare(op.NewName)}
		case *ast.RenameIndexOperation:
			return change{verb: rename, kind: kindIndex, from: bare(op.From), to: bare(op.To)}
		case *ast.SetCommentOperation:
			if op.Column != "" {
				return change{verb: comment, kind: kindColumn, name: op.Column, table: table}
			}
			return change{verb: comment, kind: kindTable, name: table}
		}
	}
	return change{verb: modify, kind: kindTable, name: table}
}

func describeAlterType(n *ast.AlterTypeNode) change {
	name := bare(n.Name)
	if len(n.Operations) == 1 {
		switch op := n.Operations[0].(type) {
		case *ast.AddEnumValueOperation:
			return change{verb: addValue, kind: kindEnum, name: name}
		case *ast.RenameTypeOperation:
			return change{verb: rename, kind: kindEnum, from: name, to: bare(op.NewName)}
		}
	}
	return change{verb: modify, kind: kindEnum, name: name}
}

func describeObjectComment(n *ast.ObjectCommentNode) change {
	switch n.Object {
	case ast.CommentedIndex:
		return change{verb: comment, kind: kindIndex, name: bare(n.Name), table: bare(n.Table)}
	case ast.CommentedSchema:
		return change{verb: comment, kind: kindSchema, name: n.Name}
	case ast.CommentedType:
		return change{verb: comment, kind: "type", name: bare(n.Name)}
	}
	return change{verb: comment, kind: strings.ToLower(string(n.Object)), name: bare(n.Name)}
}

// privileges names what a grant gives: its privileges, and the object they are
// on where it names one.
func privileges(names []string, object string) string {
	list := strings.Join(names, ", ")
	if object == "" {
		return list
	}
	return fmt.Sprintf("%s on %q", list, bare(object))
}

// bare is a name without its schema, which is how the community binary names
// an object in a comment.
func bare(name string) string {
	if ref, ok := tableref.Parse(name); ok {
		return ref.Name
	}
	return name
}

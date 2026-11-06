// Package planshape rewrites a planned migration into the statement shape the
// Atlas CLI writes, for [ptah.run/core/planext.StatementShapeCatalog]: the keys
// and checks of a new table inside its CREATE TABLE, a type spelled the way the
// catalog reports it, an explicit NULL on a nullable column, and the statements in
// the order and the grouping the binary writes.
//
// It reads and rewrites the planner's nodes, so what a plan changes stays the
// planner's decision. A file that runs comes ahead of a file that reads like the
// binary's: the rewrite moves or merges a statement only where it can show that
// nothing depends on the order, and otherwise leaves the planner's order standing.
// It covers the engines the community binary has a driver for, and leaves any other
// plan as it is.
package planshape

import (
	"cmp"
	"slices"
	"strings"

	"ptah.run/core/ast"
	"ptah.run/core/difftypes"
	"ptah.run/core/platform"
	"ptah.run/core/platform/descriptor"
	"ptah.run/internal/pgtypealias"
)

// Apply returns nodes in the catalog's shape for dialect. Nodes it does not
// rewrite are returned as they are.
//
// A rollback goes through [AlignRollback] instead: what a rollback's diff says about
// the database it runs on is projected from the forward diff, and only the forward
// plan's statements say all of it.
func Apply(dialect string, nodes []ast.Node, diff *difftypes.SchemaDiff) []ast.Node {
	form, spelling, ok := catalogRules(dialect)
	if !ok {
		return nodes
	}
	facts := newPlanFacts()
	facts.readDiff(diff)
	facts.readPlan(nodes)
	how := arrangementFor(form, facts)
	how.places = how.forwardPlaces(diff)
	how.absorbs = how.merge && !foreignKeysDeferred(nodes, tablesCreatedBy(nodes))
	nodes = arrange(inlineConstraints(nodes), how)
	for _, node := range nodes {
		respell(node, spelling)
	}
	return nodes
}

// catalogRules is the form an engine's catalog prints and how it spells a column. It
// answers false for an engine the community binary has no driver for.
func catalogRules(dialect string) (descriptor.CatalogForm, columnSpelling, bool) {
	target, ok := platform.Lookup(dialect)
	if !ok {
		return descriptor.NoCatalogForm, nil, false
	}
	switch target.Atlas.CatalogForm {
	case descriptor.PostgresCatalogForm:
		return target.Atlas.CatalogForm, postgresSpelling, true
	case descriptor.MySQLCatalogForm:
		return target.Atlas.CatalogForm, mysqlSpelling{foreignKeyAction: "NO ACTION"}, true
	case descriptor.MariaDBCatalogForm:
		return target.Atlas.CatalogForm, mysqlSpelling{foreignKeyAction: "RESTRICT"}, true
	case descriptor.SQLiteCatalogForm:
		return target.Atlas.CatalogForm, sqliteSpelling, true
	default:
		return descriptor.NoCatalogForm, nil, false
	}
}

// columnSpelling is what an engine's catalog says about a column.
type columnSpelling interface {
	// typeName is the type as the catalog reports it.
	typeName(column *ast.ColumnNode) string
	// foreignKeyDefault is the action a foreign key names where it declares none.
	foreignKeyDefault() string
	// quotesEnumTypes reports whether a column's enum type is written quoted.
	quotesEnumTypes() bool
}

// inlineConstraints moves into each new table the constraints the plan adds to
// it right after creating it, and each index of the table to right after the
// table. A foreign key whose table the plan creates later stays where it is, and
// with it every other foreign key of the plan: tables that reference each other
// are created first and tied together after, as the community binary writes them.
//
// A statement between the table and its constraint that the constraint may need,
// such as a routine a check calls, keeps the constraint where the planner put it.
func inlineConstraints(nodes []ast.Node) []ast.Node {
	created := tablesCreatedBy(nodes)
	if len(created) == 0 {
		return nodes
	}
	plan := inlining{
		created:  created,
		altered:  alteredTables(nodes, created),
		deferred: foreignKeysDeferred(nodes, created),
	}
	out := make([]ast.Node, 0, len(nodes))
	tables := make(map[string]*ast.CreateTableNode, len(created))
	barrier := make(map[string]bool, len(created))
	reachable := func(name string) *ast.CreateTableNode {
		if barrier[tableKey(name)] {
			return nil
		}
		return tables[tableKey(name)]
	}
	for _, node := range nodes {
		switch n := node.(type) {
		case *ast.CreateTableNode:
			tables[tableKey(n.Name)] = n
		case *ast.AlterTableNode:
			if table := reachable(n.Name); table != nil {
				n.Operations = plan.absorb(table, n.Operations)
				if len(n.Operations) == 0 {
					continue
				}
			}
		case *ast.IndexNode:
			if table := reachable(n.Table); table != nil {
				out = insertIndex(out, table, n)
				continue
			}
		default:
			if !planLeavesConstraintsAlone(node) {
				for name := range tables {
					barrier[name] = true
				}
			}
		}
		out = append(out, node)
	}
	return out
}

// tablesCreatedBy is the position in nodes of each table they create.
func tablesCreatedBy(nodes []ast.Node) map[string]int {
	created := make(map[string]int, len(nodes))
	for i, node := range nodes {
		if table, ok := node.(*ast.CreateTableNode); ok {
			created[tableKey(table.Name)] = i
		}
	}
	return created
}

// insertIndex places index after table and the indexes already behind it.
func insertIndex(out []ast.Node, table *ast.CreateTableNode, index *ast.IndexNode) []ast.Node {
	at := slices.Index(out, ast.Node(table)) + 1
	for at < len(out) && isIndexOf(out[at], index.Table) {
		at++
	}
	return slices.Insert(out, at, ast.Node(index))
}

// planLeavesConstraintsAlone reports a node no constraint or index of an earlier
// table can need, so moving one over it changes nothing.
func planLeavesConstraintsAlone(node ast.Node) bool {
	switch node.(type) {
	case *ast.CommentNode, *ast.DropIndexNode, *ast.DropTableNode, *ast.EnumNode, *ast.CreateSchemaNode:
		return true
	default:
		return false
	}
}

func isIndexOf(node ast.Node, table string) bool {
	index, ok := node.(*ast.IndexNode)
	return ok && tableKey(index.Table) == tableKey(table)
}

// createdAt is where the plan creates the table a foreign key names. A key may
// name its table without the schema the table was created under, so a name the
// plan does not create exactly takes the latest table of that bare name: a key
// that waits costs nothing, and a key moved ahead of its table stops at the server.
func createdAt(created map[string]int, reference string) (int, bool) {
	if at, ok := created[tableKey(reference)]; ok {
		return at, true
	}
	at, found := 0, false
	for table, position := range created {
		if strings.ToLower(table[strings.LastIndexByte(table, 0)+1:]) == bare(reference) && (!found || position > at) {
			at, found = position, true
		}
	}
	return at, found
}

// foreignKeysDeferred reports a foreign key on a new table that names a table the
// plan creates after it.
func foreignKeysDeferred(nodes []ast.Node, created map[string]int) bool {
	for _, node := range nodes {
		alter, ok := node.(*ast.AlterTableNode)
		if !ok {
			continue
		}
		own, isNew := created[tableKey(alter.Name)]
		if !isNew {
			continue
		}
		for _, operation := range alter.Operations {
			add, ok := operation.(*ast.AddConstraintOperation)
			if !ok || add.Constraint == nil || add.Constraint.Type != ast.ForeignKeyConstraint || add.Constraint.Reference == nil {
				continue
			}
			if at, isNew := createdAt(created, add.Constraint.Reference.Table); isNew && at > own {
				return true
			}
		}
	}
	return false
}

// alteredTables is the bare names of the existing tables the plan changes. A
// foreign key into one of them may need what the plan adds to it, so it stays after
// those statements.
func alteredTables(nodes []ast.Node, created map[string]int) map[string]bool {
	altered := make(map[string]bool)
	for _, node := range nodes {
		switch n := node.(type) {
		case *ast.AlterTableNode:
			if _, isNew := created[tableKey(n.Name)]; !isNew {
				altered[bare(n.Name)] = true
			}
		case *ast.IndexNode:
			if _, isNew := created[tableKey(n.Table)]; !isNew {
				altered[bare(n.Table)] = true
			}
		}
	}
	return altered
}

// inlining is what decides whether a constraint can move into its new table.
type inlining struct {
	// created is the position in the plan of each table it creates.
	created map[string]int
	// altered is the bare names of the existing tables the plan changes.
	altered map[string]bool
	// deferred says a foreign key of the plan has to wait for a later table, so
	// every foreign key waits.
	deferred bool
}

// absorb moves the operations it can into table and returns the others.
func (i inlining) absorb(table *ast.CreateTableNode, operations []ast.AlterOperation) []ast.AlterOperation {
	kept := make([]ast.AlterOperation, 0, len(operations))
	for _, operation := range operations {
		add, ok := operation.(*ast.AddConstraintOperation)
		if !ok || add.Constraint == nil || !i.inlinable(table, add.Constraint) {
			kept = append(kept, operation)
			continue
		}
		table.Constraints = append(table.Constraints, add.Constraint)
	}
	return kept
}

func (i inlining) inlinable(table *ast.CreateTableNode, constraint *ast.ConstraintNode) bool {
	if constraint.Type != ast.ForeignKeyConstraint {
		return true
	}
	if constraint.Reference == nil {
		return false
	}
	target := constraint.Reference.Table
	if tableKey(target) == tableKey(table.Name) {
		return true
	}
	if i.deferred || i.altered[bare(target)] {
		return false
	}
	at, _ := createdAt(i.created, target)
	return at <= i.created[tableKey(table.Name)]
}

// sortConstraints orders a table's constraints as a catalog read lists them: the
// primary key, the unique constraints, the foreign keys, then the checks.
func sortConstraints(table *ast.CreateTableNode) {
	table.Elements = nil
	slices.SortStableFunc(table.Constraints, func(a, b *ast.ConstraintNode) int {
		return cmp.Or(cmp.Compare(constraintRank(a), constraintRank(b)), cmp.Compare(a.Name, b.Name))
	})
}

func constraintRank(constraint *ast.ConstraintNode) int {
	switch constraint.Type {
	case ast.PrimaryKeyConstraint:
		return 0
	case ast.UniqueConstraint:
		return 1
	case ast.ForeignKeyConstraint:
		return 2
	default:
		return 3
	}
}

// respell writes node's columns and foreign keys in the catalog's spelling.
func respell(node ast.Node, spelling columnSpelling) {
	switch n := node.(type) {
	case *ast.CreateTableNode:
		respellTable(n, spelling)
	case *ast.AlterTableNode:
		for _, operation := range n.Operations {
			switch op := operation.(type) {
			case *ast.AddColumnOperation:
				respellColumn(n.Name, op.Column, spelling)
			case *ast.ModifyColumnOperation:
				respellColumn(n.Name, op.Column, spelling)
			case *ast.AddConstraintOperation:
				respellForeignKey(op.Constraint, spelling)
			}
		}
	}
}

func respellTable(table *ast.CreateTableNode, spelling columnSpelling) {
	var key []string
	for _, column := range table.Columns {
		if spelling == postgresSpelling && column.Unique {
			// A catalog read prints a column's UNIQUE as the constraint it is, under
			// the name the server gives it.
			column.Unique = false
			table.Constraints = append(table.Constraints, ast.NewUniqueConstraint(unqualified(table.Name)+"_"+column.Name+"_key", column.Name))
		}
		if column.Primary && !keepsKeyInline(column) {
			key = append(key, column.Name)
			column.Primary = false
			if spelling != sqliteSpelling {
				column.Nullable = false
			}
		}
		respellColumn(table.Name, column, spelling)
	}
	if len(key) > 0 {
		table.Constraints = append([]*ast.ConstraintNode{ast.NewPrimaryKeyConstraint(key...)}, table.Constraints...)
	}
	markKeyColumns(table, spelling)
	sortConstraints(table)
	for _, constraint := range table.Constraints {
		respellForeignKey(constraint, spelling)
	}
}

// markKeyColumns makes the columns of a table's primary key NOT NULL, which is what
// a catalog read prints. A key declared apart from its columns leaves them nullable
// in the declaration, and PostgreSQL refuses NULL beside a key with `conflicting
// NULL/NOT NULL declarations`. SQLite keeps what was declared: a key does not make
// its column NOT NULL there.
func markKeyColumns(table *ast.CreateTableNode, spelling columnSpelling) {
	if spelling == sqliteSpelling {
		return
	}
	for _, constraint := range table.Constraints {
		if constraint.Type != ast.PrimaryKeyConstraint {
			continue
		}
		for _, column := range table.Columns {
			if slices.Contains(constraint.Columns, column.Name) {
				column.Nullable = false
				column.ExplicitNull = false
			}
		}
	}
}

// keepsKeyInline reports a key SQLite accepts only on its column: AUTOINCREMENT
// is a clause of the column's own PRIMARY KEY.
func keepsKeyInline(column *ast.ColumnNode) bool {
	return column.AutoInc && column.Type != "" && strings.EqualFold(strings.TrimSpace(column.Type), "integer")
}

func respellColumn(table string, column *ast.ColumnNode, spelling columnSpelling) {
	if column == nil {
		return
	}
	if column.NotNullConstraintName == unqualified(table)+"_"+column.Name+"_not_null" {
		// The name PostgreSQL gives a NOT NULL nobody named, which a catalog read
		// reports and the community binary writes nowhere.
		column.NotNullConstraintName = ""
	}
	column.Type = spelling.typeName(column)
	column.TypeIsDeclaredText = true
	if isSerial(column.Type) {
		column.Nullable = false
	}
	if column.Nullable && !column.Primary {
		column.ExplicitNull = true
	}
}

func isSerial(typeName string) bool {
	switch strings.ToLower(typeName) {
	case "serial", "bigserial", "smallserial":
		return true
	default:
		return false
	}
}

func respellForeignKey(constraint *ast.ConstraintNode, spelling columnSpelling) {
	if constraint == nil || constraint.Type != ast.ForeignKeyConstraint || constraint.Reference == nil {
		return
	}
	reference := constraint.Reference
	action := spelling.foreignKeyDefault()
	if reference.OnDelete == "" {
		reference.OnDelete = action
	}
	if reference.OnUpdate == "" {
		reference.OnUpdate = action
	}
	reference.CatalogOrder = true
}

// spellPostgres is the catalog's name for a PostgreSQL type alias. A timestamp
// keeps its short name, which is what the community binary writes.
var postgresAliases = map[string]string{
	"int": "integer", "int4": "integer", "int8": "bigint", "int2": "smallint",
	"varchar": "character varying", "char": "character", "bool": "boolean",
	"float8": "double precision", "float4": "real", "decimal": "numeric",
	"varbit": "bit varying", "serial4": "serial", "serial8": "bigserial", "serial2": "smallserial",
}

type postgresCatalog struct{}

var postgresSpelling columnSpelling = postgresCatalog{}

func (postgresCatalog) foreignKeyDefault() string { return "NO ACTION" }
func (postgresCatalog) quotesEnumTypes() bool     { return true }
func (s postgresCatalog) typeName(column *ast.ColumnNode) string {
	base, tail := splitType(strings.TrimSpace(column.Type))
	if column.EnumType {
		return quotedTypeName(base) + tail
	}
	lower := strings.ToLower(base)
	lower, tail = zoneIntoName(lower, compactArguments(tail))
	if strings.HasSuffix(tail, "[]") && !strings.Contains(tail, "time zone") {
		if words, ok := timeArrayWords[lower]; ok {
			return words.name + strings.TrimSuffix(tail, "[]") + " " + words.zone + "[]"
		}
	}
	if alias, ok := postgresAliases[lower]; ok {
		return alias + tail
	}
	if canonical, ok := pgtypealias.Alias(lower); ok && lower == "bool" {
		return canonical + tail
	}
	if isBuiltinWord(lower) {
		return lower + tail
	}
	return strings.TrimSpace(column.Type)
}

// timeArrayWords spell the element of a temporal array as the catalog prints an array:
// by the type's full name, where a column of the type alone reads `timestamptz`.
var timeArrayWords = map[string]struct{ name, zone string }{
	"timestamp":   {name: "timestamp", zone: "without time zone"},
	"timestamptz": {name: "timestamp", zone: "with time zone"},
	"time":        {name: "time", zone: "without time zone"},
	"timetz":      {name: "time", zone: "with time zone"},
}

// zoneNames are the names a catalog prints for a temporal column of a type read in full.
var zoneNames = map[string]string{
	"timestamp without time zone": "timestamp",
	"timestamp with time zone":    "timestamptz",
	"time without time zone":      "time",
	"time with time zone":         "timetz",
}

// compactArguments takes the spaces out of the argument list that opens tail, as a
// catalog prints it, and leaves what follows the list alone.
func compactArguments(tail string) string {
	end := strings.IndexByte(tail, ')')
	if !strings.HasPrefix(tail, "(") || end < 0 {
		return tail
	}
	return strings.ReplaceAll(tail[:end+1], " ", "") + tail[end+1:]
}

// zoneIntoName reads the time zone words behind a precision into the type's name, as
// a catalog prints it: `timestamp(3) with time zone` is `timestamptz(3)`.
func zoneIntoName(spelled, spelledTail string) (name, tail string) {
	if short, ok := zoneNames[spelled]; ok {
		return short, spelledTail
	}
	end := strings.IndexByte(spelledTail, ')')
	if end < 0 || (spelled != "timestamp" && spelled != "time") {
		return spelled, spelledTail
	}
	arguments, words := spelledTail[:end+1], strings.ToLower(strings.TrimSpace(spelledTail[end+1:]))
	switch words {
	case "without time zone":
		return spelled, arguments
	case "with time zone":
		return spelled + "tz", arguments
	default:
		return spelled, spelledTail
	}
}

// isBuiltinWord reports a built-in PostgreSQL type the catalog prints in lower
// case. Any other name may be one the declaration quotes, and stays as written.
func isBuiltinWord(name string) bool {
	switch name {
	case "integer", "bigint", "smallint", "text", "boolean", "bytea", "date", "time", "timetz",
		"timestamp", "timestamptz", "uuid", "json", "jsonb", "inet", "cidr", "macaddr", "money",
		"xml", "real", "numeric", "interval", "bit", "serial", "bigserial", "smallserial",
		"character varying", "character", "double precision", "bit varying":
		return true
	default:
		return false
	}
}

// quotedTypeName quotes a type's name part by part, as the catalog prints an enum.
func quotedTypeName(name string) string {
	parts := strings.Split(name, ".")
	for i, part := range parts {
		parts[i] = `"` + strings.ReplaceAll(strings.Trim(part, `"`), `"`, `""`) + `"`
	}
	return strings.Join(parts, ".")
}

// splitType cuts a type into its name and what follows it: a length and an array
// marker.
func splitType(spelled string) (name, tail string) {
	if at := strings.IndexAny(spelled, "(["); at >= 0 {
		return strings.TrimSpace(spelled[:at]), spelled[at:]
	}
	return spelled, ""
}

type mysqlSpelling struct{ foreignKeyAction string }

// foreignKeyDefault is what a read of the server reports for an undeclared
// action: MariaDB says RESTRICT and MySQL says NO ACTION.
func (s mysqlSpelling) foreignKeyDefault() string { return s.foreignKeyAction }
func (mysqlSpelling) quotesEnumTypes() bool       { return false }
func (mysqlSpelling) typeName(column *ast.ColumnNode) string {
	spelled := strings.TrimSpace(column.Type)
	lower := strings.ToLower(spelled)
	switch lower {
	case "tinyint(1)", "boolean":
		return "bool"
	case "integer":
		return "int"
	default:
		if strings.HasPrefix(lower, "enum(") || strings.HasPrefix(lower, "set(") {
			return spelled
		}
		return lower
	}
}

type sqliteCatalog struct{}

var sqliteSpelling columnSpelling = sqliteCatalog{}

func (sqliteCatalog) foreignKeyDefault() string { return "NO ACTION" }
func (sqliteCatalog) quotesEnumTypes() bool     { return false }
func (sqliteCatalog) typeName(column *ast.ColumnNode) string {
	return strings.ToLower(strings.TrimSpace(column.Type))
}

// tableKey is a table as the plan's statements name it: each part of its name in
// its case, without quotes. A statement moves onto a table only under this key,
// because a.t and b.t, "Users" and users, "a.b" and a.b, or "a""b" and ab are two
// tables. The parts stay apart under a byte no name part can hold.
func tableKey(name string) string {
	return strings.Join(nameParts(name), "\x00")
}

// nameParts splits a name at the dots outside its quotes and takes the quotes off
// each part. A quote opens only at the start of a part, so a name a reader has
// already unquoted keeps the quote it holds, and a doubled quote inside quotes is
// one quote of the name.
func nameParts(name string) []string {
	var parts []string
	var part strings.Builder
	var quote byte
	name = strings.TrimSpace(name)
	for i := 0; i < len(name); i++ {
		char := name[i]
		switch {
		case quote != 0 && char == quote && i+1 < len(name) && name[i+1] == quote:
			part.WriteByte(char)
			i++
		case quote != 0 && char == quote:
			quote = 0
		case quote == 0 && part.Len() == 0 && (char == '"' || char == '`'):
			quote = char
		case quote == 0 && char == '.':
			parts = append(parts, part.String())
			part.Reset()
		default:
			part.WriteByte(char)
		}
	}
	return append(parts, part.String())
}

// unqualified is a table's name without its schema or its quotes, in its case, as
// the server starts a name it gives the table's constraints.
func unqualified(name string) string {
	parts := nameParts(name)
	return parts[len(parts)-1]
}

// bare is a name that may be the table: without schema, quotes or case. Two tables
// can share it, so it may hold a statement back and never moves one.
func bare(name string) string {
	return strings.ToLower(unqualified(name))
}

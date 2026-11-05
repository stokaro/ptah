// Package planshape rewrites a planned migration into the statement shape the
// Atlas CLI writes, for [ptah.run/core/planext.StatementShapeCatalog]: the keys
// and checks of a new table inside its CREATE TABLE, a type spelled the way the
// catalog reports it, and an explicit NULL on a nullable column.
//
// It reads and rewrites the planner's nodes, so what a plan changes stays the
// planner's decision: the rewrite only moves a constraint into the CREATE TABLE
// that precedes it and respells a column. It covers the engines the community
// binary has a driver for, and leaves any other plan as it is.
package planshape

import (
	"cmp"
	"slices"
	"strings"

	"ptah.run/core/ast"
	"ptah.run/core/platform"
	"ptah.run/core/platform/descriptor"
	"ptah.run/internal/pgtypealias"
)

// Apply returns nodes in the catalog's shape for dialect. Nodes it does not
// rewrite are returned as they are.
func Apply(dialect string, nodes []ast.Node) []ast.Node {
	target, ok := platform.Lookup(dialect)
	if !ok {
		return nodes
	}
	var spelling columnSpelling
	switch target.Atlas.CatalogForm {
	case descriptor.PostgresCatalogForm:
		spelling = postgresSpelling
	case descriptor.MySQLCatalogForm:
		spelling = mysqlSpelling{foreignKeyAction: "NO ACTION"}
	case descriptor.MariaDBCatalogForm:
		spelling = mysqlSpelling{foreignKeyAction: "RESTRICT"}
	case descriptor.SQLiteCatalogForm:
		spelling = sqliteSpelling
	default:
		return nodes
	}
	nodes = inlineConstraints(nodes)
	for _, node := range nodes {
		respell(node, spelling)
	}
	return nodes
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
	created := make(map[string]int, len(nodes))
	for i, node := range nodes {
		if table, ok := node.(*ast.CreateTableNode); ok {
			created[tableKey(table.Name)] = i
		}
	}
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
		if bare(table) == bare(reference) && (!found || position > at) {
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
	if alias, ok := postgresAliases[lower]; ok {
		return alias + strings.ReplaceAll(tail, " ", "")
	}
	if canonical, ok := pgtypealias.Alias(lower); ok && lower == "bool" {
		return canonical + tail
	}
	if isBuiltinWord(lower) {
		return lower + strings.ReplaceAll(tail, " ", "")
	}
	return strings.TrimSpace(column.Type)
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

// tableKey is a table as the plan's statements name it: its schema and its name in
// their case, without quotes. A statement moves onto a table only under this key,
// because a.t and b.t, or "Users" and users, are two tables.
func tableKey(name string) string {
	return strings.NewReplacer("`", "", `"`, "").Replace(strings.TrimSpace(name))
}

// unqualified is a table's name without its schema or its quotes, in its case, as
// the server starts a name it gives the table's constraints.
func unqualified(name string) string {
	name = tableKey(name)
	if at := strings.LastIndex(name, "."); at >= 0 {
		name = name[at+1:]
	}
	return name
}

// bare is a name that may be the table: without schema, quotes or case. Two tables
// can share it, so it may hold a statement back and never moves one.
func bare(name string) string {
	return strings.ToLower(unqualified(name))
}

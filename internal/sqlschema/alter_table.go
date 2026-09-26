package sqlschema

import (
	"fmt"
	"slices"
	"strings"

	"ptah.run/core/ast"
	"ptah.run/core/platform"
	"ptah.run/core/schemamodel"
	"ptah.run/internal/mysqlname"
)

// alterTarget is the table an ALTER TABLE names, found in this file or in an
// earlier file of the same document, together with the databases whose
// objects the statement may change.
//
// A schema directory, and a file with its imports, is one script run in order.
// So a later file may change what an earlier one declared: base is that
// earlier state, and a change to one of its objects is made to base in place.
// The objects this file adds stay in database, which is what the caller
// merges.
type alterTarget struct {
	written    string
	structName string
	qualified  string
	table      *schemamodel.Table
	databases  []*schemamodel.Database
	// sourcePlatform decides how a name the statement writes is read; see
	// [identifierPart]. A name already in the model was read that way once and
	// is compared as it is.
	sourcePlatform string
	// statement is what the operations of the statement share. It is set by
	// [appendAlterTable] and never nil there.
	statement *alterStatement
}

// alterStatement is what the operations of one ALTER TABLE share.
//
// A server decides some names once per statement rather than once per
// operation. The first unnamed foreign key takes its number from the keys the
// table held before the statement ran; see [mysqlname.NextForeignKeyNumber].
// The operations change the model one at a time, so by the second operation
// the model no longer says what the table held: a key the statement dropped is
// gone from it, and a key the statement named is in it, and the server counts
// the first and not the second.
type alterStatement struct {
	// nextForeignKey is the number the next unnamed foreign key of the
	// statement takes, on an engine [mysqlname.NamesForeignKeys] speaks for.
	nextForeignKey int
}

// newAlterStatement reads what the statement's operations share from the
// table before the first of them runs.
func newAlterStatement(target alterTarget) *alterStatement {
	statement := &alterStatement{nextForeignKey: 1}
	if target.table == nil {
		return statement
	}
	statement.nextForeignKey = mysqlname.NextForeignKeyNumber(target.table.Name, target.foreignKeyNames())
	return statement
}

// foreignKeyNames is every foreign key name the table holds: its table-level
// keys and the keys its columns declare.
func (t alterTarget) foreignKeyNames() []string {
	var names []string
	for _, database := range t.databases {
		for _, constraint := range database.Constraints {
			if isForeignKey(constraint) && constraint.Table == t.qualified {
				names = append(names, constraint.Name)
			}
		}
		for _, field := range database.Fields {
			if field.Foreign != "" && field.StructName == t.structName {
				names = append(names, field.ForeignKeyName)
			}
		}
	}
	return names
}

// findAlterTarget looks the table up in database, then in base.
//
// The table is found by the name the statement resolves to, and its columns and
// constraints through the struct name that table carries. A struct name derived
// from the statement's own spelling is not an identity: it camel-cases and
// singularizes, so matched by it, `ALTER TABLE docs` changes a table created
// as "Docs", a statement PostgreSQL refuses (stokaro/ptah#3642).
func findAlterTarget(database, base *schemamodel.Database, written, sourcePlatform string) (alterTarget, bool) {
	target := alterTarget{
		written:        written,
		qualified:      normalizeSQLTableReference(sourcePlatform, written),
		databases:      []*schemamodel.Database{database},
		sourcePlatform: sourcePlatform,
	}
	if base != nil {
		target.databases = append(target.databases, base)
	}
	if table := resolveTable(target.databases, target.qualified, sourcePlatform); table != nil {
		target.table = table
		target.structName = table.StructName
		target.qualified = table.QualifiedName()
		return target, true
	}
	return target, false
}

func undeclaredTableError(written, clause string) error {
	return fmt.Errorf("%w: ALTER TABLE %s %s names a table this schema does not declare",
		ErrUnmodeledStatement, written, clause)
}

// field returns the table's column that name reaches, or nil. See
// [resolveColumn].
func (t alterTarget) field(name string) *schemamodel.Field {
	return resolveColumn(t.databases, t.structName, name, t.sourcePlatform)
}

// reachesColumn reports whether a column name another declaration of the
// table records reaches column, by the rule [alterTarget.field] used to reach
// column itself.
func (t alterTarget) reachesColumn(name, column string) bool {
	_, names := tableColumns(t.databases, t.structName)
	index := resolveDeclaredName(t.sourcePlatform, name, names)
	return index >= 0 && names[index] == column
}

// reachesTable reports whether a table name another declaration records
// reaches the target table, by the rule [findAlterTarget] used to reach it.
func (t alterTarget) reachesTable(name string) bool {
	if name == "" {
		return false
	}
	table := resolveTable(t.databases, normalizeSQLTableReference("", name), t.sourcePlatform)
	return table != nil && table.QualifiedName() == t.table.QualifiedName()
}

// ownsIndex reports whether index belongs to the table. Every index the reader
// records names its table; one that does not is matched by the struct name an
// ALTER TABLE ... ADD INDEX gives it, or by its bare table name.
func (t alterTarget) ownsIndex(index schemamodel.Index) bool {
	if index.TableName != "" {
		return t.reachesTable(index.TableName)
	}
	return index.StructName == t.structName || t.reachesTable(index.StructName)
}

// isPrimaryKeyColumn reports whether the column belongs to the table's primary
// key, declared on the column or on the table.
func (t alterTarget) isPrimaryKeyColumn(field *schemamodel.Field) bool {
	return field.Primary || slices.Contains(t.table.PrimaryKey, field.Name)
}

// applyAlterColumn applies one ALTER COLUMN action to the column it names.
func applyAlterColumn(target alterTarget, operation *ast.AlterColumnOperation) error {
	field := target.field(operation.ColumnName)
	if field == nil {
		return fmt.Errorf("ALTER TABLE %s ALTER COLUMN %s names a column the table does not declare",
			target.written, operation.ColumnName)
	}
	switch operation.Action {
	case ast.AlterColumnSetDefault:
		field.Default, field.DefaultSet, field.DefaultExpr = "", false, ""
		if operation.Default != nil && operation.Default.HasLiteral() {
			field.Default, field.DefaultSet = operation.Default.Value, true
		} else if operation.Default != nil {
			field.DefaultExpr = operation.Default.Expression
		}
	case ast.AlterColumnDropDefault:
		field.Default, field.DefaultSet, field.DefaultExpr = "", false, ""
	case ast.AlterColumnSetNotNull:
		field.Nullable = false
	case ast.AlterColumnDropNotNull:
		// PostgreSQL 18.6: `column "id" is in a primary key`.
		if target.isPrimaryKeyColumn(field) {
			return fmt.Errorf("ALTER TABLE %s ALTER COLUMN %s DROP NOT NULL: the column is in the primary key",
				target.written, operation.ColumnName)
		}
		field.Nullable = true
		field.NotNullConstraintName = ""
	case ast.AlterColumnSetType:
		field.Type = operation.Type
	default:
		return fmt.Errorf("%w: ALTER TABLE %s ALTER COLUMN %s %s",
			ErrUnmodeledStatement, target.written, operation.ColumnName, operation.Action)
	}
	return nil
}

// applyModifyColumn applies a whole new column definition: MySQL's MODIFY, or
// SQL Server's ALTER COLUMN.
//
// The two mean different things. MODIFY replaces the definition, so what it
// does not restate is gone, as on the server. SQL Server's ALTER COLUMN states
// the type, nullability and collation and leaves the default alone, because a
// default there is a constraint of its own.
func applyModifyColumn(target alterTarget, operation *ast.ModifyColumnOperation, sourcePlatform string) error {
	if operation.Column == nil {
		return fmt.Errorf("ALTER TABLE %s MODIFY carries no column", target.written)
	}
	field := target.field(operation.Column.Name)
	if field == nil {
		return fmt.Errorf("ALTER TABLE %s MODIFY %s names a column the table does not declare",
			target.written, operation.Column.Name)
	}
	replacement := fieldFromColumn(operation.Column, target.structName, sourcePlatform)
	if platform.NormalizeDialect(sourcePlatform) == platform.SQLServer {
		field.Type = replacement.Type
		field.Nullable = replacement.Nullable
		field.Collate = replacement.Collate
		return nil
	}
	replacement.Primary = replacement.Primary || field.Primary
	*field = replacement
	return nil
}

// applyDropColumn removes a column nothing else in the schema refers to.
//
// PostgreSQL drops an index or a table constraint on the column with it, and
// refuses a column another table's foreign key references. A schema file that
// keeps those objects declared would render them over a column it no longer
// has, so a column something still refers to is refused, and the message names
// the object to drop first.
func applyDropColumn(target alterTarget, operation *ast.DropColumnOperation) error {
	if operation.Cascade {
		return fmt.Errorf(
			"ALTER TABLE %s DROP COLUMN %s CASCADE also drops the objects that depend on the column, "+
				"which a schema file does not list; drop them by name",
			target.written, operation.ColumnName)
	}
	field := target.field(operation.ColumnName)
	if field == nil {
		if operation.IfExists {
			return nil
		}
		return fmt.Errorf("ALTER TABLE %s DROP COLUMN %s names a column the table does not declare",
			target.written, operation.ColumnName)
	}
	if target.isPrimaryKeyColumn(field) {
		return fmt.Errorf("ALTER TABLE %s DROP COLUMN %s: the column is in the primary key",
			target.written, operation.ColumnName)
	}
	if reference := columnReference(target, field.Name); reference != "" {
		return fmt.Errorf("ALTER TABLE %s DROP COLUMN %s: %s still refers to the column",
			target.written, operation.ColumnName, reference)
	}
	name := field.Name
	for _, database := range target.databases {
		database.Fields = slices.DeleteFunc(database.Fields, func(candidate schemamodel.Field) bool {
			return candidate.StructName == target.structName && candidate.Name == name
		})
	}
	return nil
}

// applyRenameColumn renames a column, and the primary key's list with it.
//
// Anything else that names the column -- an index, a constraint, an expression,
// another table's foreign key -- refuses the rename. PostgreSQL follows the
// rename in each of them; a schema file keeps their text, which would then name
// a column that no longer exists.
func applyRenameColumn(target alterTarget, operation *ast.RenameColumnOperation) error {
	field := target.field(operation.OldName)
	if field == nil {
		return fmt.Errorf("ALTER TABLE %s RENAME COLUMN %s names a column the table does not declare",
			target.written, operation.OldName)
	}
	if target.field(operation.NewName) != nil {
		return fmt.Errorf("ALTER TABLE %s RENAME COLUMN %s TO %s: the table already declares %s",
			target.written, operation.OldName, operation.NewName, operation.NewName)
	}
	if reference := columnReference(target, field.Name); reference != "" {
		return fmt.Errorf(
			"ALTER TABLE %s RENAME COLUMN %s: %s names the column, and a schema file keeps its text; "+
				"declare the column under its new name",
			target.written, operation.OldName, reference)
	}
	oldName, newName := field.Name, normalizeSQLIdentifier(target.sourcePlatform, operation.NewName)
	field.Name = newName
	for i, column := range target.table.PrimaryKey {
		if column == oldName {
			target.table.PrimaryKey[i] = newName
		}
	}
	for i := range target.table.PrimaryKeyParts {
		if target.table.PrimaryKeyParts[i].Name == oldName {
			target.table.PrimaryKeyParts[i].Name = newName
		}
	}
	return nil
}

// columnReference names the first object other than the column itself and the
// table's primary key that refers to column, or returns "".
//
// A name in another declaration refers to column when it reaches column by the
// rule the ALTER TABLE used to reach it, [resolveDeclaredName]. Compared
// exactly, SQLite's `CHECK (length(note) > 0)` lets `DROP COLUMN Note`
// through, a statement SQLite refuses, and the render keeps a CHECK over a
// column the document no longer declares.
//
// Expressions are searched by identifier, so a CHECK, an index expression or a
// generated column that mentions the name counts even inside a string literal.
// That errs toward refusing. Views, policies, triggers and routine bodies are
// not searched: PostgreSQL refuses to drop a column a view depends on, and a
// rendered view over a dropped column fails the same way.
func columnReference(target alterTarget, column string) string {
	for _, database := range target.databases {
		for _, constraint := range database.Constraints {
			if reference := constraintColumnReference(target, constraint, column); reference != "" {
				return reference
			}
		}
		for _, index := range database.Indexes {
			if target.ownsIndex(index) && indexMentions(target, index, column) {
				return fmt.Sprintf("index %s", index.Name)
			}
		}
		for _, field := range database.Fields {
			if reference := fieldColumnReference(target, field, column); reference != "" {
				return reference
			}
		}
	}
	return ""
}

func constraintColumnReference(target alterTarget, constraint schemamodel.Constraint, column string) string {
	name := constraint.Name
	if name == "" {
		name = "(unnamed)"
	}
	reaches := func(name string) bool { return target.reachesColumn(name, column) }
	if constraint.StructName == target.structName &&
		(slices.ContainsFunc(constraint.Columns, reaches) || slices.ContainsFunc(constraint.IncludeColumns, reaches) ||
			expressionMentions(target, constraint.CheckExpression, column) ||
			expressionMentions(target, constraint.ExcludeElements, column) ||
			expressionMentions(target, constraint.WhereCondition, column)) {
		return fmt.Sprintf("%s constraint %s", strings.ToLower(constraint.Type), name)
	}
	if strings.EqualFold(constraint.Type, "FOREIGN KEY") &&
		target.reachesTable(constraint.ForeignTable) &&
		slices.ContainsFunc(constraint.ForeignColumnsOrDefault(), reaches) {
		return fmt.Sprintf("the foreign key %s of %s", name, constraint.Table)
	}
	return ""
}

func indexMentions(target alterTarget, index schemamodel.Index, column string) bool {
	for _, element := range index.Fields {
		if target.reachesColumn(element, column) || expressionMentions(target, element, column) {
			return true
		}
	}
	for _, part := range index.Parts {
		if target.reachesColumn(part.Name, column) || expressionMentions(target, part.Expr, column) {
			return true
		}
	}
	return slices.ContainsFunc(index.IncludeColumns, func(name string) bool { return target.reachesColumn(name, column) }) ||
		expressionMentions(target, index.Condition, column)
}

func fieldColumnReference(target alterTarget, field schemamodel.Field, column string) string {
	if field.StructName == target.structName && field.Name != column &&
		(expressionMentions(target, field.Check, column) ||
			expressionMentions(target, field.GeneratedExpression, column) ||
			expressionMentions(target, field.DefaultExpr, column)) {
		return fmt.Sprintf("column %s", field.Name)
	}
	if field.Foreign == "" {
		return ""
	}
	open := strings.Index(field.Foreign, "(")
	if open < 0 || !strings.HasSuffix(field.Foreign, ")") {
		return ""
	}
	// Foreign holds names the read already folded, so they are not folded
	// again; they are resolved as any other name the table's declarations
	// record.
	if !target.reachesTable(field.Foreign[:open]) {
		return ""
	}
	for referenced := range strings.SplitSeq(field.Foreign[open+1:len(field.Foreign)-1], ",") {
		if target.reachesColumn(normalizeSQLIdentifier("", strings.TrimSpace(referenced)), column) {
			return fmt.Sprintf("the foreign key on column %s", field.Name)
		}
	}
	return ""
}

// expressionMentions reports whether expression contains, quoted or not, an
// identifier that reaches column. An expression is SQL as written, so each
// token is read the way the source dialect reads a name.
func expressionMentions(target alterTarget, expression, column string) bool {
	if expression == "" || column == "" {
		return false
	}
	isIdentifierByte := func(r rune) bool {
		return r == '_' || r == '$' || r == '"' || r == '`' ||
			(r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9')
	}
	for _, token := range strings.FieldsFunc(expression, func(r rune) bool { return !isIdentifierByte(r) }) {
		if target.reachesColumn(normalizeSQLIdentifier(target.sourcePlatform, token), column) {
			return true
		}
	}
	return false
}

// applyDropConstraint removes the constraint an ALTER TABLE ... DROP names.
//
// A name is looked for wherever the model keeps one: a table constraint or
// index, the primary key, and the foreign key and CHECK a column carries. A
// name no declaration gave -- the one a server generates for an unnamed
// constraint -- is not in the model, so dropping it is refused rather than
// guessed at, unless the statement says IF EXISTS.
func applyDropConstraint(target alterTarget, operation *ast.DropConstraintOperation) error {
	if operation.PrimaryKey {
		if len(target.table.PrimaryKey) == 0 && !target.hasPrimaryField() {
			return fmt.Errorf("ALTER TABLE %s DROP PRIMARY KEY: the table declares no primary key", target.written)
		}
		target.clearPrimaryKey()
		return nil
	}
	name := normalizeSQLIdentifier(target.sourcePlatform, operation.ConstraintName)
	if target.removeNamedConstraint(name) {
		return nil
	}
	if operation.IfExists {
		return nil
	}
	return fmt.Errorf(
		"ALTER TABLE %s DROP CONSTRAINT %s names a constraint this schema does not declare by that name",
		target.written, operation.ConstraintName)
}

// applyRenameConstraint renames a constraint the schema declares by name.
func applyRenameConstraint(target alterTarget, operation *ast.RenameConstraintOperation) error {
	from, to := normalizeSQLIdentifier(target.sourcePlatform, operation.From),
		normalizeSQLIdentifier(target.sourcePlatform, operation.To)
	if target.renameNamedConstraint(from, to) {
		return nil
	}
	return fmt.Errorf(
		"ALTER TABLE %s RENAME CONSTRAINT %s names a constraint this schema does not declare by that name",
		target.written, operation.From)
}

func (t alterTarget) hasPrimaryField() bool {
	for _, database := range t.databases {
		for _, field := range database.Fields {
			if field.StructName == t.structName && field.Primary {
				return true
			}
		}
	}
	return false
}

func (t alterTarget) clearPrimaryKey() {
	t.table.PrimaryKey, t.table.PrimaryKeyName = nil, ""
	t.table.PrimaryKeyParts, t.table.PrimaryKeyInclude = nil, nil
	for _, database := range t.databases {
		for i := range database.Fields {
			if database.Fields[i].StructName == t.structName {
				database.Fields[i].Primary = false
			}
		}
	}
}

// removeNamedConstraint removes the object that carries name, and reports
// whether there was one.
func (t alterTarget) removeNamedConstraint(name string) bool {
	if t.table.PrimaryKeyName == name {
		t.clearPrimaryKey()
		return true
	}
	for _, database := range t.databases {
		before := len(database.Constraints) + len(database.Indexes)
		database.Constraints = slices.DeleteFunc(database.Constraints, func(constraint schemamodel.Constraint) bool {
			return constraint.StructName == t.structName && constraint.Name == name
		})
		database.Indexes = slices.DeleteFunc(database.Indexes, func(index schemamodel.Index) bool {
			return t.ownsIndex(index) && index.Name == name
		})
		if len(database.Constraints)+len(database.Indexes) != before {
			return true
		}
		for i := range database.Fields {
			field := &database.Fields[i]
			if field.StructName != t.structName {
				continue
			}
			if field.Foreign != "" && field.ForeignKeyName == name {
				field.Foreign, field.ForeignKeyName, field.OnDelete, field.OnUpdate = "", "", "", ""
				field.Deferrable, field.Initially = false, ""
				return true
			}
			if field.Check != "" && field.CheckName == name {
				field.Check, field.CheckName = "", ""
				return true
			}
		}
	}
	return false
}

// renameNamedConstraint renames the object that carries from, and reports
// whether there was one.
func (t alterTarget) renameNamedConstraint(from, to string) bool {
	if t.table.PrimaryKeyName == from {
		t.table.PrimaryKeyName = to
		return true
	}
	for _, database := range t.databases {
		for i := range database.Constraints {
			if database.Constraints[i].StructName == t.structName && database.Constraints[i].Name == from {
				database.Constraints[i].Name = to
				return true
			}
		}
		for i := range database.Fields {
			field := &database.Fields[i]
			if field.StructName != t.structName {
				continue
			}
			if field.Foreign != "" && field.ForeignKeyName == from {
				field.ForeignKeyName = to
				return true
			}
			if field.Check != "" && field.CheckName == from {
				field.CheckName = to
				return true
			}
		}
	}
	return false
}

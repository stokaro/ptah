package sqlschema

import (
	"fmt"
	"slices"
	"strings"

	"ptah.run/core/ast"
	"ptah.run/core/platform"
	"ptah.run/core/schemamodel"
	"ptah.run/internal/columnkey"
	"ptah.run/internal/mysqlcheck"
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
	// keys are the indexes the document's server built for foreign keys; see
	// [keyIndex]. Set by [appendAlterTable] and never nil there.
	keys *keyIndexes
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
	// nextCheck is the number the next unnamed CHECK of the statement takes
	// on MySQL. MySQL counts the CHECKs the table holds once the statement's
	// drops are made, and not the ones the statement adds under a name:
	// measured on 8.4.11 and 26.7.0, `ALTER TABLE s1 ADD CONSTRAINT s1_chk_5
	// CHECK (...), ADD CHECK (...)` names the second `s1_chk_1`, and `ALTER TABLE
	// s2 DROP CHECK s2_chk_3, ADD CHECK (...)` names it `s2_chk_1`.
	nextCheck uint32
	// checkNames are the CHECK names MariaDB's `CONSTRAINT_<n>` must avoid:
	// the table's once the statement's drops are made, and every name the
	// statement writes, before an unnamed CHECK or after it. Measured on
	// 11.8.9, `ALTER TABLE s1 ADD CHECK (...), ADD CONSTRAINT CONSTRAINT_1
	// CHECK (...)` names the first `CONSTRAINT_2`.
	checkNames []string
	// laterDrops are the constraint names the operations after the current
	// one drop. See [refuseCheckNameDroppedLater].
	laterDrops []string
	// clause is the index a MySQL `ADD FOREIGN KEY name (columns)` clause
	// names, which the parser hands over as an operation of its own just
	// before the key's, until that key takes it.
	clause *schemamodel.Index
}

// newAlterStatement reads what the statement's operations share from the
// table before the first of them runs.
func newAlterStatement(target alterTarget, operations []ast.AlterOperation) *alterStatement {
	statement := &alterStatement{nextForeignKey: 1, nextCheck: 1}
	if target.table == nil {
		return statement
	}
	statement.nextForeignKey = mysqlname.NextForeignKeyNumber(target.table.Name, target.foreignKeyNames())
	held := remainingCheckNames(tableCheckNames(target.databases, *target.table), operations, target.sourcePlatform)
	statement.nextCheck = mysqlname.NextCheckNumber(target.table.Name, held)
	statement.checkNames = slices.Concat(held, addedCheckNames(operations, target.sourcePlatform))
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
//
// A key is not part of the definition MODIFY replaces; see
// [alterTarget.keepColumnKey].
func applyModifyColumn(
	database *schemamodel.Database, target alterTarget, operation *ast.ModifyColumnOperation, sourcePlatform string,
) error {
	if operation.Column == nil {
		return fmt.Errorf("ALTER TABLE %s MODIFY carries no column", target.written)
	}
	field := target.field(operation.Column.Name)
	if field == nil {
		return fmt.Errorf("ALTER TABLE %s MODIFY %s names a column the table does not declare",
			target.written, operation.Column.Name)
	}
	replacement := fieldFromColumn(operation.Column, target.structName, sourcePlatform)
	_, columns := tableColumns(target.databases, target.structName)
	if err := refuseMySQLColumnCheckReference(replacement, columns, target); err != nil {
		return err
	}
	if platform.NormalizeDialect(sourcePlatform) == platform.SQLServer {
		field.Type = replacement.Type
		field.Nullable = replacement.Nullable
		field.Collate = replacement.Collate
		return nil
	}
	replacement.Primary = replacement.Primary || field.Primary
	target.keepColumnKey(database, field, &replacement)
	*field = replacement
	return nil
}

// keepColumnKey carries the own UNIQUE of a column that MODIFY rewrites onto
// the definition that replaces it, on MySQL and MariaDB.
//
// Measured on MySQL 8.4.11 and 26.7.0 and MariaDB 11.8.9 and 12.3.3 against
// Atlas CE v1.3.0, the server reads UNIQUE in MODIFY as a request for a new
// key, not as part of the column (stokaro/ptah#3875):
//
//   - `x int UNIQUE`, then `MODIFY x bigint`, keeps the key `x`.
//   - `x int UNIQUE`, then `MODIFY x bigint UNIQUE`, adds a second key over x,
//     `x_2`, named as [columnkey.Name] names a key whose name is held.
//
// So a column keeps its own key when the definition omits UNIQUE, and a
// definition that restates it on a column that has one adds a UNIQUE
// constraint under the name the server gives the new key. A column without a
// key of its own takes one.
func (t alterTarget) keepColumnKey(database *schemamodel.Database, field, replacement *schemamodel.Field) {
	switch platform.NormalizeDialect(t.sourcePlatform) {
	case platform.MySQL, platform.MariaDB:
	default:
		return
	}
	if !field.Unique {
		return
	}
	if replacement.Unique {
		// The column's own key still holds its name here, so the new key
		// takes the next one.
		name, _ := columnkey.Name(t.sourcePlatform, t.table.Name, field.Name, func(name string) bool {
			return t.keyNameHeld(name, nil)
		})
		database.Constraints = append(database.Constraints, schemamodel.Constraint{
			StructName: t.structName,
			Name:       name,
			Type:       "UNIQUE",
			Table:      normalizeSQLTableReference("", t.qualified),
			Columns:    []string{field.Name},
		})
	}
	replacement.Unique = true
}

// refuseMySQLColumnCheckReference refuses, for MySQL, the CHECK on a column
// ALTER TABLE adds or modifies when it names another column of the table, as
// MySQL refuses it; see [mysqlcheck.OtherColumn]. columns are the table's
// columns, the one written here included. The parser refuses the same CHECK in
// CREATE TABLE, where the whole table is in the statement.
func refuseMySQLColumnCheckReference(field schemamodel.Field, columns []string, target alterTarget) error {
	if platform.NormalizeDialect(target.sourcePlatform) != platform.MySQL || field.Check == "" {
		return nil
	}
	if other, found := mysqlcheck.OtherColumn(field.Name, field.Check, columns); found {
		return fmt.Errorf("ALTER TABLE %s: %w", target.written, mysqlcheck.Refusal(field.Name, other))
	}
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
// column's own UNIQUE is found by the name the server gives it, as the
// comparison finds it; see [alterTarget.columnWithKey]. `DROP INDEX b` on
// MySQL and MariaDB and `DROP CONSTRAINT c_b_key` on PostgreSQL drop the key
// of `b int UNIQUE`, and the column is no longer UNIQUE. Any other name no
// declaration gave -- the one a server generates for an unnamed constraint --
// is not in the model, so dropping it is refused rather than guessed at,
// unless the statement says IF EXISTS.
func applyDropConstraint(target alterTarget, operation *ast.DropConstraintOperation) error {
	if operation.PrimaryKey {
		if len(target.table.PrimaryKey) == 0 && !target.hasPrimaryField() {
			return fmt.Errorf("ALTER TABLE %s DROP PRIMARY KEY: the table declares no primary key", target.written)
		}
		target.clearPrimaryKey()
		return nil
	}
	name := normalizeSQLIdentifier(target.sourcePlatform, operation.ConstraintName)
	if operation.ForeignKey {
		if target.removeForeignKey(name) {
			target.keepKeyIndexes(name)
			return nil
		}
	} else if key := target.holdsForeignKey(name); target.removeNamedConstraint(name) {
		if key {
			target.keepKeyIndexes(name)
		}
		if operation.Unique {
			target.keys.forget(target.qualified, name)
		}
		return nil
	}
	// DROP FOREIGN KEY and DROP CHECK name no unique key; the servers answer
	// that the key does not exist.
	if !operation.ForeignKey && !operation.Check {
		if field := target.columnWithKey(name); field != nil {
			field.Unique = false
			return nil
		}
	}
	if operation.IfExists {
		return nil
	}
	return fmt.Errorf(
		"ALTER TABLE %s DROP CONSTRAINT %s names a constraint this schema does not declare by that name",
		target.written, operation.ConstraintName)
}

// applyRenameConstraint renames a constraint the schema declares by name, or a
// column's own UNIQUE by the name the server gives it; see
// [alterTarget.nameColumnKey].
func applyRenameConstraint(database *schemamodel.Database, target alterTarget, operation *ast.RenameConstraintOperation) error {
	from, to := normalizeSQLIdentifier(target.sourcePlatform, operation.From),
		normalizeSQLIdentifier(target.sourcePlatform, operation.To)
	if target.renameNamedConstraint(from, to) {
		return nil
	}
	if field := target.columnWithKey(from); field != nil {
		return target.nameColumnKey(database, field, operation.To, "RENAME CONSTRAINT "+operation.From)
	}
	return fmt.Errorf(
		"ALTER TABLE %s RENAME CONSTRAINT %s names a constraint this schema does not declare by that name",
		target.written, operation.From)
}

// applyRenameIndex renames an index of the table, as MySQL and MariaDB read
// `RENAME {INDEX | KEY} old TO new`: an index the table declares, a UNIQUE
// constraint, which is an index there, or a column's own UNIQUE by the name
// the server gives it; see [alterTarget.nameColumnKey].
//
// A name the table's indexes already hold is refused, as the servers refuse it
// with `ERROR 1061 (42000): Duplicate key name`, and so is a name the table
// has no index under.
func applyRenameIndex(database *schemamodel.Database, target alterTarget, operation *ast.RenameIndexOperation) error {
	from, to := normalizeSQLIdentifier(target.sourcePlatform, operation.From),
		normalizeSQLIdentifier(target.sourcePlatform, operation.To)
	clause := "RENAME INDEX " + operation.From
	field := target.columnWithKey(from)
	if !target.renamesIndex(from) && field == nil {
		return fmt.Errorf("ALTER TABLE %s %s names an index this schema does not declare by that name",
			target.written, clause)
	}
	if target.keyNameHeld(to, field) {
		return fmt.Errorf("ALTER TABLE %s %s TO %s: the table already holds a key named %s",
			target.written, clause, operation.To, to)
	}
	if field != nil {
		return target.nameColumnKey(database, field, operation.To, clause)
	}
	target.renameIndex(from, to)
	return nil
}

// columnWithKey answers the column of the table whose own UNIQUE the server
// names name, or nil.
//
// The model keeps no name for a column's own key, so the name is derived the
// way the comparison derives it, from the names the schema read so far holds;
// see [columnkey.Name] and [columnkey.Taken]. So `b int UNIQUE` is found under
// `b` on MySQL and MariaDB, or `b_2` where another key of the table holds `b`,
// and under `c_b_key` on PostgreSQL. A dialect whose naming is not measured
// answers nil.
func (t alterTarget) columnWithKey(name string) *schemamodel.Field {
	if t.table == nil || !columnkey.Named(t.sourcePlatform) {
		return nil
	}
	taken := columnkey.Taken(t.sourcePlatform, t.databases, *t.table)
	for _, database := range t.databases {
		for i := range database.Fields {
			field := &database.Fields[i]
			if field.StructName != t.structName || !field.Unique || strings.TrimSpace(field.UniqueExpr) != "" {
				continue
			}
			own, _ := columnkey.Name(t.sourcePlatform, t.table.Name, field.Name, taken)
			if columnkey.Same(t.sourcePlatform, own, name) {
				return field
			}
		}
	}
	return nil
}

// keyNameHeld reports whether name is a name the column's own key cannot
// take: one [columnkey.Taken] counts, or the own key of a column other than
// except.
func (t alterTarget) keyNameHeld(name string, except *schemamodel.Field) bool {
	if t.table == nil {
		return false
	}
	if columnkey.Taken(t.sourcePlatform, t.databases, *t.table)(name) {
		return true
	}
	held := t.columnWithKey(name)
	return held != nil && held != except
}

// nameColumnKey gives a column's own UNIQUE the name to, written as the
// statement wrote it. The model keeps no name on the column, so the key
// becomes a UNIQUE constraint of that name over the column, which is what the
// server holds once the key is renamed, and the column is no longer UNIQUE
// itself. A name another key holds is refused, as the servers refuse it.
func (t alterTarget) nameColumnKey(database *schemamodel.Database, field *schemamodel.Field, to, clause string) error {
	name := normalizeSQLIdentifier(t.sourcePlatform, to)
	if t.keyNameHeld(name, field) {
		return fmt.Errorf("ALTER TABLE %s %s TO %s: the table already holds a key named %s",
			t.written, clause, to, name)
	}
	t.keyAsConstraint(database, field, name)
	return nil
}

// keyAsConstraint turns the own UNIQUE of field into a UNIQUE constraint of
// the table named name, over the column. The caller has settled that the name
// is free.
func (t alterTarget) keyAsConstraint(database *schemamodel.Database, field *schemamodel.Field, name string) {
	field.Unique = false
	database.Constraints = append(database.Constraints, schemamodel.Constraint{
		StructName: t.structName,
		Name:       name,
		Type:       "UNIQUE",
		Table:      normalizeSQLTableReference("", t.qualified),
		Columns:    []string{field.Name},
	})
}

// renamesIndex reports whether the table declares an index, or a UNIQUE
// constraint, named name.
func (t alterTarget) renamesIndex(name string) bool {
	for _, database := range t.databases {
		if slices.ContainsFunc(database.Indexes, func(index schemamodel.Index) bool {
			return t.ownsIndex(index) && index.Name == name
		}) || slices.ContainsFunc(database.Constraints, func(constraint schemamodel.Constraint) bool {
			return t.ownsUniqueConstraint(constraint, name)
		}) {
			return true
		}
	}
	return false
}

// renameIndex renames the index, or the UNIQUE constraint, of the table named
// from.
// applyIndexVisibility shows or hides an index the schema declares by name.
// A UNIQUE constraint of that name is an index on the MySQL family, where the
// statement is read, and it becomes the unique index the server holds, which
// is where the model keeps visibility. A name the table holds no index under
// is refused, as the servers refuse it.
func applyIndexVisibility(target alterTarget, operation *ast.AlterIndexVisibilityOperation) error {
	name := normalizeSQLIdentifier(target.sourcePlatform, operation.IndexName)
	if target.setIndexVisibility(name, operation.Invisible) {
		return nil
	}
	return fmt.Errorf("ALTER TABLE %s ALTER INDEX %s names an index this schema does not declare by that name",
		target.written, operation.IndexName)
}

// setIndexVisibility sets the visibility of the index or UNIQUE constraint
// named name, and reports whether the table has one.
func (t alterTarget) setIndexVisibility(name string, invisible bool) bool {
	for _, database := range t.databases {
		for i := range database.Indexes {
			if t.ownsIndex(database.Indexes[i]) && database.Indexes[i].Name == name {
				database.Indexes[i].Invisible = invisible
				return true
			}
		}
		for i, constraint := range database.Constraints {
			if !t.ownsUniqueConstraint(constraint, name) {
				continue
			}
			database.Constraints = slices.Delete(database.Constraints, i, i+1)
			database.Indexes = append(database.Indexes, schemamodel.Index{
				Name:       constraint.Name,
				StructName: t.structName,
				Fields:     slices.Clone(constraint.Columns),
				Unique:     true,
				Invisible:  invisible,
				TableName:  t.qualified,
			})
			return true
		}
	}
	return false
}

func (t alterTarget) renameIndex(from, to string) {
	for _, database := range t.databases {
		for i := range database.Indexes {
			if t.ownsIndex(database.Indexes[i]) && database.Indexes[i].Name == from {
				database.Indexes[i].Name = to
				return
			}
		}
		for i := range database.Constraints {
			if t.ownsUniqueConstraint(database.Constraints[i], from) {
				database.Constraints[i].Name = to
				return
			}
		}
	}
}

// ownsUniqueConstraint reports whether constraint is a UNIQUE of the table
// named name.
func (t alterTarget) ownsUniqueConstraint(constraint schemamodel.Constraint, name string) bool {
	return constraint.StructName == t.structName && strings.EqualFold(constraint.Type, "UNIQUE") && constraint.Name == name
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
	t.table.PrimaryKeyDeferrable, t.table.PrimaryKeyInitially = false, ""
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
				field.ForeignKeyMatch, field.ForeignKeyNotEnforced = "", false
				return true
			}
			if field.Check != "" && field.CheckName == name {
				field.Check, field.CheckName, field.CheckNotEnforced = "", "", false
				return true
			}
		}
	}
	return false
}

// applyValidateConstraint validates a CHECK or foreign key the schema declares
// by name. One added NOT VALID becomes validated. One written on a column, or
// added without the clause, is validated already, and the server takes the
// statement and changes nothing. A name the table holds no CHECK or foreign
// key under is refused, as the server refuses it.
func applyValidateConstraint(target alterTarget, operation *ast.ValidateConstraintOperation) error {
	name := normalizeSQLIdentifier(target.sourcePlatform, operation.ConstraintName)
	if target.validateNamedConstraint(name) {
		return nil
	}
	return fmt.Errorf(
		"ALTER TABLE %s VALIDATE CONSTRAINT %s names no CHECK or foreign key this schema declares by that name",
		target.written, operation.ConstraintName)
}

// validateNamedConstraint marks the CHECK or foreign key named name validated,
// and reports whether the table has one.
func (t alterTarget) validateNamedConstraint(name string) bool {
	for _, database := range t.databases {
		for i := range database.Constraints {
			constraint := &database.Constraints[i]
			if constraint.StructName != t.structName || constraint.Name != name {
				continue
			}
			if strings.EqualFold(constraint.Type, "CHECK") || strings.EqualFold(constraint.Type, "FOREIGN KEY") {
				constraint.NotValid = false
				return true
			}
		}
		for _, field := range database.Fields {
			if field.StructName != t.structName {
				continue
			}
			if (field.Foreign != "" && field.ForeignKeyName == name) || (field.Check != "" && field.CheckName == name) {
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

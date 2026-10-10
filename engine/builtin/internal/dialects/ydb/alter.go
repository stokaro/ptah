package ydb

import (
	"fmt"
	"strings"

	"ptah.run/core/ast"
	"ptah.run/core/platform/capability"
	"ptah.run/core/renderer"
	"ptah.run/internal/tableref"
	"ptah.run/internal/ydbextensions"
	"ptah.run/internal/ydbtype"
)

// renderAlterTable writes one ALTER TABLE per operation.
//
// YDB takes several column changes in one ALTER, and refuses the mixes a plan
// can produce: two ADD INDEX answer `Only one index can be added by one
// operation`, and DROP INDEX beside DROP COLUMN answers `Unqualified alter
// table request` (measured on 25.1.4.7 and 26.2.1.14). One statement per
// operation is the shape every mix takes, and it is what lets the executor
// record progress after each one.
//
// The node's operations are checked before any is written, so a refusal
// leaves no prefix of the statements behind.
func (r *Renderer) renderAlterTable(node *ast.AlterTableNode) error {
	if node.Algorithm != "" || node.Lock != "" {
		return r.keyed(capability.AlterTableAlgorithmLock, "ALGORITHM and LOCK clause",
			fmt.Sprintf("ALTER TABLE %s asks for ALGORITHM=%s LOCK=%s", node.Name, node.Algorithm, node.Lock))
	}
	statements := make([]string, 0, len(node.Operations))
	for _, operation := range node.Operations {
		statement, err := r.alterStatement(node, operation)
		if err != nil {
			return err
		}
		statements = append(statements, statement...)
	}
	for _, statement := range statements {
		r.w.WriteLine(statement)
	}
	return nil
}

// alterStatement writes the statements one operation takes, which is one
// except for a column modification that changes two properties.
//
//nolint:gocyclo // one arm per operation kind; splitting the table hides which kinds it answers
func (r *Renderer) alterStatement(parent *ast.AlterTableNode, operation ast.AlterOperation) ([]string, error) {
	table := parent.Name
	prefix := "ALTER TABLE " + tablePath(table) + " "
	subject := tableref.Phrase(table)
	switch op := operation.(type) {
	case *ast.AddColumnOperation:
		clause, err := r.addColumn(table, op)
		if err != nil {
			return nil, err
		}
		statements := []string{prefix + clause + ";"}
		if op.Column.Comment != "" {
			comment, err := r.setComment(table, &ast.SetCommentOperation{Column: op.Column.Name, Comment: op.Column.Comment})
			if err != nil {
				return nil, err
			}
			statements = append(statements, comment...)
		}
		if !op.Column.Unique || r.caps.Has(capability.UniqueConstraints) {
			return statements, nil
		}
		unique, err := r.addIndexStatements(uniqueColumnIndex(table, op.Column.Name))
		if err != nil {
			return nil, err
		}
		return append(statements, unique...), nil
	case *ast.DropColumnOperation:
		return r.dropColumn(prefix, subject, op)
	case *ast.ModifyColumnOperation:
		return r.modifyColumn(prefix, table, op)
	case *ast.AlterColumnOperation:
		return r.alterColumn(prefix, table, op)
	case *ast.RenameColumnOperation:
		return nil, r.keyed(capability.RenameColumnClause, "column rename",
			fmt.Sprintf("renaming column %q of %s", op.OldName, subject))
	case *ast.AlterGeneratedColumnExpressionOperation:
		return nil, r.keyed(capability.AlterGeneratedColumnExpression, "generated column",
			fmt.Sprintf("the expression of column %q of %s", op.ColumnName, subject))
	case *ast.AddConstraintOperation:
		return r.addConstraint(table, op)
	case *ast.ValidateConstraintOperation:
		return nil, r.keyed(capability.AddConstraintNotValid, "constraint validation",
			fmt.Sprintf("validating constraint %q of %s", op.ConstraintName, subject))
	case *ast.DropConstraintOperation:
		return nil, r.keyed(capability.DropConstraintGeneric, "constraint drop",
			fmt.Sprintf("dropping constraint %q of %s", op.ConstraintName, subject))
	case *ast.RenameConstraintOperation:
		return nil, refuseFact(fmt.Sprintf("renaming constraint %q of %s", op.From, subject), "YDB has no named constraint")
	case *ast.RenameIndexOperation:
		return []string{fmt.Sprintf("%sRENAME INDEX %s TO %s;", prefix, quote(op.From), quote(op.To))}, nil
	case *ast.AlterIndexVisibilityOperation:
		return nil, r.keyed(capability.InvisibleIndexes, "invisible index",
			fmt.Sprintf("the visibility of index %q of %s", op.IndexName, subject))
	case *ast.ExtensionAlterOperation:
		registry, err := ydbextensions.Registry()
		if err != nil {
			return nil, err
		}
		return registry.Render(renderer.ExtensionContext{Target: DialectName, Capabilities: r.caps, Parent: parent}, ast.AlterExtension, op.Payload)
	case *ast.AddIndexOperation:
		return r.addIndex(table, op.Index)
	case *ast.ReplaceIndexOperation:
		return nil, refuseFact(subject, "YDB replaces an index with a DROP INDEX and an ADD INDEX, each its own statement")
	case *ast.RenameTableOperation:
		return []string{prefix + "RENAME TO " + tablePath(op.NewName) + ";"}, nil
	case *ast.SetCommentOperation:
		return r.setComment(table, op)
	case *ast.SetConstraintCommentOperation:
		// YDB names no constraint: the key has no name, and a UNIQUE
		// constraint is the unique index it renders as, whose comment is the
		// index's.
		return nil, r.keyed(capability.ConstraintComments, "constraint comment",
			fmt.Sprintf("the comment on constraint %q of %s", op.Constraint, subject))
	default:
		return nil, refuseFact(subject, fmt.Sprintf("the YDB renderer has no ALTER TABLE spelling for %T", operation))
	}
}

// addColumn writes ADD COLUMN, refusing the shapes YDB refuses on an existing
// table: a Serial (`Column addition with serial data type is unsupported`),
// NOT NULL without a default (`Cannot add not null column without default
// value`, even on an empty table), and a default on a target without
// [capability.AddColumnWithDefault] (`Adding columns with defaults is
// disabled` on 25.3 and 25.4, `Column addition with default value is not
// supported now` on 25.1).
func (r *Renderer) addColumn(table string, op *ast.AddColumnOperation) (string, error) {
	if op.Column == nil {
		return "", refuseFact(tableref.Phrase(table), "ADD COLUMN carries no column")
	}
	subject := fmt.Sprintf("adding column %q to %s", op.Column.Name, tableref.Phrase(table))
	mapping, err := ydbtype.Map(op.Column.Type, r.caps)
	hasDefault := op.Column.Default != nil && (op.Column.Default.HasLiteral() || op.Column.Default.Expression != "")
	switch {
	case op.IfNotExists:
		return "", refuseFact(subject, "YDB's ADD COLUMN has no IF NOT EXISTS guard")
	case op.Column.Primary:
		return "", r.keyed(capability.PrimaryKeyAlterable, "key change", subject+" as part of the key")
	case err == nil && mapping.Serial, op.Column.AutoInc, op.Column.IdentityGeneration != "":
		return "", refuseFact(subject, "YDB adds no Serial column to an existing table (`Column addition with serial data type is unsupported`)")
	case hasDefault && !r.caps.Has(capability.AddColumnWithDefault):
		return "", refuseKey(capability.AddColumnWithDefault, subject+" with a default")
	case !hasDefault && !op.Column.Nullable:
		return "", refuseFact(subject, "YDB adds a NOT NULL column only with a default (`Cannot add not null column without default value`)")
	}
	definition, _, err := r.columnDefinition(table, op.Column, false, "")
	if err != nil {
		return "", err
	}
	return "ADD COLUMN " + definition, nil
}

// dropColumn writes DROP COLUMN. YDB refuses to drop a key column (`Key column
// drop is not supported`), and an indexed or covered one until its index is
// gone. The renderer sees one statement and leaves both to the plan: the
// planner drops an index by its own earlier statement, and refuses a key
// change by [capability.PrimaryKeyAlterable] where the comparison reports one,
// as a PRIMARY KEY constraint change.
func (r *Renderer) dropColumn(prefix, subject string, op *ast.DropColumnOperation) ([]string, error) {
	switch {
	case op.IfExists:
		return nil, refuseFact(fmt.Sprintf("dropping column %q of %s", op.ColumnName, subject),
			"YDB's DROP COLUMN has no IF EXISTS guard")
	case op.Cascade:
		return nil, refuseFact(fmt.Sprintf("dropping column %q of %s", op.ColumnName, subject),
			"YDB's DROP COLUMN has no CASCADE; an index on the column is dropped first, by its own statement")
	}
	return []string{prefix + "DROP COLUMN " + quote(op.ColumnName) + ";"}, nil
}

// modifyColumn writes the in-place changes a column modification names. The
// column carries its full definition and Changed says which parts moved; a
// modification that says nothing about what changed is MySQL's MODIFY COLUMN,
// which YDB has no counterpart for.
func (r *Renderer) modifyColumn(prefix, table string, op *ast.ModifyColumnOperation) ([]string, error) {
	if op.Column == nil {
		return nil, refuseFact(tableref.Phrase(table), "the column modification carries no column")
	}
	subject := fmt.Sprintf("column %q of %s", op.Column.Name, tableref.Phrase(table))
	if !op.HasChanged {
		return nil, refuseFact(subject, "YDB has no MODIFY COLUMN; a column changes only its NOT NULL and its default in place")
	}
	if op.Changed.Type {
		return nil, r.keyed(capability.AlterColumnType, "column type change",
			fmt.Sprintf("changing the type of %s to %s", subject, op.Column.Type))
	}
	var statements []string
	if op.Changed.Nullability {
		change := r.dropNotNull
		if !op.Column.Nullable {
			change = r.setNotNull
		}
		statement, err := change(prefix, subject, op.Column.Name)
		if err != nil {
			return nil, err
		}
		statements = append(statements, statement)
	}
	if op.Changed.Default {
		statement, err := r.columnDefault(prefix, subject, op.Column.Name, op.Column.Type, op.Column.Default)
		if err != nil {
			return nil, err
		}
		statements = append(statements, statement)
	}
	return statements, nil
}

// alterColumn writes one ALTER COLUMN action.
func (r *Renderer) alterColumn(prefix, table string, op *ast.AlterColumnOperation) ([]string, error) {
	subject := fmt.Sprintf("column %q of %s", op.ColumnName, tableref.Phrase(table))
	var statement string
	var err error
	switch op.Action {
	case ast.AlterColumnSetNotNull:
		statement, err = r.setNotNull(prefix, subject, op.ColumnName)
	case ast.AlterColumnDropNotNull:
		statement, err = r.dropNotNull(prefix, subject, op.ColumnName)
	case ast.AlterColumnDropDefault:
		statement, err = r.columnDefault(prefix, subject, op.ColumnName, "", nil)
	case ast.AlterColumnSetDefault:
		if strings.TrimSpace(op.Type) == "" {
			return nil, refuseFact(subject, "a YDB default is typed, and SET DEFAULT here does not say the column's type")
		}
		statement, err = r.columnDefault(prefix, subject, op.ColumnName, op.Type, op.Default)
	case ast.AlterColumnSetType:
		return nil, r.keyed(capability.AlterColumnType, "column type change",
			fmt.Sprintf("changing the type of %s to %s", subject, op.Type))
	default:
		return nil, refuseFact(subject, fmt.Sprintf("ALTER COLUMN action %q has no YDB spelling", op.Action))
	}
	if err != nil {
		return nil, err
	}
	return []string{statement}, nil
}

// setNotNull writes SET NOT NULL, and refuses it on a target without
// [capability.AlterColumnSetNotNull] (`SET NOT NULL is currently not
// supported.` on 25.4 through 26.2).
func (r *Renderer) setNotNull(prefix, subject, column string) (string, error) {
	if !r.caps.Has(capability.AlterColumnSetNotNull) {
		return "", refuseKey(capability.AlterColumnSetNotNull, "making "+subject+" NOT NULL")
	}
	return prefix + "ALTER COLUMN " + quote(column) + " SET NOT NULL;", nil
}

// dropNotNull writes DROP NOT NULL where the target has
// [capability.AlterColumnDropNotNull].
func (r *Renderer) dropNotNull(prefix, subject, column string) (string, error) {
	if !r.caps.Has(capability.AlterColumnDropNotNull) {
		return "", refuseKey(capability.AlterColumnDropNotNull, "making "+subject+" nullable")
	}
	return prefix + "ALTER COLUMN " + quote(column) + " DROP NOT NULL;", nil
}

// columnDefault writes SET DEFAULT with the literal in the column's own type,
// or DROP DEFAULT where the column no longer has one. Both need
// [capability.AlterColumnDefault], which YDB has from 26.2.
func (r *Renderer) columnDefault(prefix, subject, column, declaredType string, value *ast.DefaultValue) (string, error) {
	if !r.caps.Has(capability.AlterColumnDefault) {
		return "", refuseKey(capability.AlterColumnDefault, "changing the default of "+subject)
	}
	if value == nil || (!value.HasLiteral() && strings.TrimSpace(value.Expression) == "") {
		return prefix + "ALTER COLUMN " + quote(column) + " DROP DEFAULT;", nil
	}
	mapping, err := ydbtype.Map(declaredType, r.caps)
	if err != nil {
		return "", typeRefusal(subject, err)
	}
	clause, err := r.defaultClause(subject, value, mapping.Type)
	if err != nil {
		return "", err
	}
	if clause == "" {
		return prefix + "ALTER COLUMN " + quote(column) + " DROP DEFAULT;", nil
	}
	return prefix + "ALTER COLUMN " + quote(column) + " SET " + clause + ";", nil
}

// addConstraint writes the one constraint an ALTER can add on YDB, a UNIQUE,
// as the unique index it renders as, which needs
// [capability.UniqueIndexOnExistingTable] like any unique index added to a
// table that exists. Every other constraint is refused: YDB's key never
// changes and it has no other constraint.
func (r *Renderer) addConstraint(table string, op *ast.AddConstraintOperation) ([]string, error) {
	if op.Constraint != nil && op.Constraint.Type == ast.PrimaryKeyConstraint {
		return nil, r.keyed(capability.PrimaryKeyAlterable, "key change", "adding a primary key to "+tableref.Phrase(table))
	}
	if err := r.refuseConstraint(table, op.Constraint); err != nil {
		return nil, err
	}
	index, err := uniqueConstraintIndex(table, op.Constraint)
	if err != nil {
		return nil, err
	}
	return r.addIndexStatements(index)
}

// addIndex writes an index an ALTER TABLE carries, with the table the ALTER
// names.
func (r *Renderer) addIndex(table string, index *ast.IndexNode) ([]string, error) {
	if index == nil {
		return nil, refuseFact(tableref.Phrase(table), "ADD INDEX carries no index")
	}
	withTable := *index
	withTable.Table = table
	return r.addIndexStatements(&withTable)
}

// renderDropTable writes one DROP TABLE per table. DROP TABLE takes the table's
// indexes and its Serial sequences with it (measured: the implicit sequence
// path is gone after the drop). It has no CASCADE: a view over the table is
// left behind, so a plan drops views first.
func (r *Renderer) renderDropTable(node *ast.DropTableNode) error {
	names := node.Names
	if len(names) == 0 {
		names = []string{node.Name}
	}
	guard := ""
	if node.IfExists {
		if !r.caps.Has(capability.ObjectExistenceGuards) {
			return refuseKey(capability.ObjectExistenceGuards, "DROP TABLE IF EXISTS "+strings.Join(names, ", "))
		}
		guard = " IF EXISTS"
	}
	if node.Cascade {
		return refuseFact("DROP TABLE "+strings.Join(names, ", ")+" CASCADE",
			"YDB's DROP TABLE has no CASCADE; a view over the table is left orphaned rather than dropped")
	}
	if node.Comment != "" {
		r.w.WriteLinef("-- %s", node.Comment)
	}
	for _, name := range names {
		r.w.WriteLinef("DROP TABLE%s %s;", guard, tablePath(name))
	}
	return nil
}

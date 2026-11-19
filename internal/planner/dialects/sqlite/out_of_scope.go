package sqlite

import (
	"maps"
	"slices"
	"strings"

	"ptah.run/catalog"
	"ptah.run/core/ast"
	"ptah.run/core/difftypes"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemamodel"
	"ptah.run/internal/defaultlit"
	"ptah.run/internal/indexbacking"
	"ptah.run/internal/modelast"
	"ptah.run/internal/schemaprep"
)

// outOfScope is what an exclude selector kept out of scope on one table the
// plan rebuilds, as the read reported it: [difftypes.SchemaDiff.CurrentOutOfScope]
// narrowed to the table.
//
// A part out of scope is kept as it is. The rebuild writes the new table from
// the declaration, which the selector left without these parts, so each one
// is carried into the new table from its live definition, and the plan
// refuses where a part cannot be carried as it stands. Nothing here is
// compared or changed: a column keeps its type, nullability, default and
// generation, an index and a trigger are created again from the statement
// SQLite stored for them.
type outOfScope struct {
	columns     []catalog.Column
	indexes     []catalog.Index
	constraints []catalog.Constraint
	triggers    []catalog.Trigger
}

// outOfScopeOf narrows the diff's out-of-scope parts to table.
func outOfScopeOf(diff *difftypes.SchemaDiff, table schemamodel.Table, semantics identifier.Semantics) outOfScope {
	key := semantics.QualifiedTableIdentityKey(table.QualifiedName())
	ofTable := func(schema, name string) bool {
		return semantics.QualifiedTableIdentityKey(catalog.QualifyTableName(schema, name)) == key
	}
	parts := diff.CurrentOutOfScope
	var carried outOfScope
	for _, held := range parts.Tables {
		if ofTable(held.Schema, held.Name) {
			carried.columns = append(carried.columns, held.Columns...)
		}
	}
	for _, index := range parts.Indexes {
		if ofTable(index.Schema, index.TableName) {
			carried.indexes = append(carried.indexes, index)
		}
	}
	for _, constraint := range parts.Constraints {
		if ofTable(constraint.Schema, constraint.TableName) {
			carried.constraints = append(carried.constraints, constraint)
		}
	}
	for _, trigger := range parts.Triggers {
		if ofTable(trigger.Schema, trigger.Table) {
			carried.triggers = append(carried.triggers, trigger)
		}
	}
	return carried
}

// carryColumns adds the columns out of scope to the new table and returns the
// ones the copy reads from the old table, which is every one but a generated
// column: SQLite computes that one and refuses a value for it.
//
// A column of the primary key is carried with the key, as a table constraint,
// where the declaration holds no key of its own; beside a declared key it is
// refused, since a table holds one key. A column with AUTOINCREMENT keeps the
// keyword only on its own PRIMARY KEY clause, so it is carried with the key
// inline where the declaration holds no key and the key is out of scope too;
// the returned name is that column, whose key carryConstraints then leaves
// alone. Anywhere else it is refused. A generated column whose expression
// reads a column the rebuilt table drops is refused too: SQLite would refuse
// the CREATE TABLE.
//
// A carried column keeps its place among the live columns, which observed
// lists with their positions: `SELECT *` and an INSERT without a column list
// read the columns in that order.
func carryColumns(
	node *ast.CreateTableNode,
	table schemamodel.Table,
	carried outOfScope,
	observed []catalog.Column,
	dropped map[string]bool,
	semantics identifier.Semantics,
) ([]copiedColumn, string, error) {
	positions := make(map[string]int, len(observed)+len(carried.columns))
	for _, live := range slices.Concat(observed, carried.columns) {
		positions[semantics.ColumnIdentityKey(live.Name)] = live.OrdinalPosition
	}
	var (
		copied    []copiedColumn
		inlineKey string
	)
	for _, column := range carried.columns {
		inline := column.IsAutoIncrement && !declaresPrimaryKey(node) && carriesKeyOf(carried.constraints, column.Name, semantics)
		switch {
		case column.IsPrimaryKey && declaresPrimaryKey(node):
			return nil, "", unsupportedFeaturef(
				"rebuilding table %s cannot keep column %s as it is: an exclude selector keeps it out of scope, "+
					"and it is part of the primary key, which the rebuilt table takes from the declaration. "+
					"Exclude the whole table, or declare the column and stop excluding it",
				table.QualifiedName(), column.Name)
		case column.IsAutoIncrement && !inline:
			return nil, "", unsupportedFeaturef(
				"rebuilding table %s cannot keep column %s as it is: an exclude selector keeps it out of scope, "+
					"and Ptah cannot carry its AUTOINCREMENT, which SQLite takes only on the column's own PRIMARY KEY. "+
					"Exclude the whole table, or declare the column and stop excluding it",
				table.QualifiedName(), column.Name)
		}
		if column.GeneratedExpression != nil {
			for _, gone := range slices.Sorted(maps.Keys(dropped)) {
				if expressionReads(*column.GeneratedExpression, gone, semantics) {
					return nil, "", unsupportedFeaturef(
						"rebuilding table %s cannot keep column %s as it is: an exclude selector keeps it out of scope, "+
							"and its generated expression reads column %s, which the rebuilt table drops. Keep the column, "+
							"or exclude the whole table",
						table.QualifiedName(), column.Name, gone)
				}
			}
		}
		carriedNode := carriedColumnNode(column)
		if inline {
			carriedNode.Primary, inlineKey = true, column.Name
			carriedNode.SetAutoIncrement()
		}
		insertAtLivePosition(node, carriedNode, column.OrdinalPosition, positions, semantics)
		if column.GeneratedKind == "" {
			copied = append(copied, copiedColumn{name: column.Name, selectExpr: quoteIdentifier(column.Name)})
		}
	}
	return copied, inlineKey, nil
}

// carriesKeyOf reports whether constraints, the constraints out of scope,
// hold a primary key over column alone.
func carriesKeyOf(constraints []catalog.Constraint, column string, semantics identifier.Semantics) bool {
	return slices.ContainsFunc(constraints, func(constraint catalog.Constraint) bool {
		return keyOver(constraint, column, semantics)
	})
}

// keyOver reports whether constraint is a primary key over column alone.
func keyOver(constraint catalog.Constraint, column string, semantics identifier.Semantics) bool {
	columns := constraintColumns(constraint)
	return indexbacking.KindOf(constraint.Type) == indexbacking.PrimaryKey && len(columns) == 1 &&
		semantics.ColumnIdentityKey(columns[0]) == semantics.ColumnIdentityKey(column)
}

// insertAtLivePosition writes column, which the live table holds at ordinal,
// into the new table after the nearest column the live table holds before it,
// or first where none comes before it. positions holds each live column's
// place, keyed by identity; a declared column the live table lacks has none
// and is passed over.
func insertAtLivePosition(node *ast.CreateTableNode, column *ast.ColumnNode, ordinal int, positions map[string]int, semantics identifier.Semantics) {
	at, nearest := 0, 0
	for i, held := range node.Columns {
		position := positions[semantics.ColumnIdentityKey(held.Name)]
		if position > nearest && position < ordinal {
			at, nearest = i+1, position
		}
	}
	node.Columns = slices.Insert(node.Columns, at, column)
}

// carriedColumnNode is the column the read reported, for the renderer to write
// as it stands: the declared type verbatim, its collation, its nullability,
// its default and its generation.
func carriedColumnNode(column catalog.Column) *ast.ColumnNode {
	declared := column.ColumnType
	if strings.TrimSpace(declared) == "" {
		declared = column.DataType
	}
	node := &ast.ColumnNode{
		Name:               column.Name,
		Type:               declared,
		TypeIsDeclaredText: true,
		Collate:            column.Collate,
		Nullable:           !strings.EqualFold(column.IsNullable, "NO"),
	}
	if column.ColumnDefault != nil {
		node.Default = &ast.DefaultValue{Expression: carriedDefault(*column.ColumnDefault)}
	}
	if column.GeneratedExpression != nil {
		node.GeneratedExpression = *column.GeneratedExpression
		node.GeneratedKind = column.GeneratedKind
	}
	return node
}

// carriedDefault writes a default SQLite reported for a column back in a form
// SQLite reads as the same default.
//
// SQLite reports a literal as it was written and an expression without the
// parentheses its grammar requires around one: `DEFAULT (1+2)` reads back as
// `1+2`, which is not a default clause. A literal is written bare, so the
// stored text stays what it was, and anything else goes back inside
// parentheses.
//
// A default written as one identifier, bare or quoted, is the string it
// spells: `DEFAULT abc`, `DEFAULT "dq"` and `DEFAULT [br]` store the text abc,
// dq and br. It goes back as that string literal, since in parentheses SQLite
// reads it as a column and refuses the default as not constant.
func carriedDefault(value string) string {
	trimmed := strings.TrimSpace(value)
	switch strings.ToUpper(trimmed) {
	case "NULL", "TRUE", "FALSE", "CURRENT_TIME", "CURRENT_DATE", "CURRENT_TIMESTAMP":
		return trimmed
	}
	if defaultlit.IsPlainNumber(strings.TrimPrefix(trimmed, "+")) || singleQuoted(trimmed) || blobLiteral(trimmed) {
		return trimmed
	}
	if spelled, ok := identifierDefault(trimmed); ok {
		return "'" + strings.ReplaceAll(spelled, "'", "''") + "'"
	}
	return "(" + trimmed + ")"
}

// identifierDefault returns the text a default of one identifier spells, bare
// or quoted, and whether value is one.
func identifierDefault(value string) (string, bool) {
	tokens := tokenizeSQL(value)
	if len(tokens) != 1 || !tokens[0].word {
		return "", false
	}
	return tokens[0].text, true
}

// singleQuoted reports whether value is one string literal, with every quote
// inside it doubled. `'a' || 'b'` starts and ends with a quote and is not one.
func singleQuoted(value string) bool {
	if len(value) < 2 || value[0] != '\'' || value[len(value)-1] != '\'' {
		return false
	}
	inner := value[1 : len(value)-1]
	return !strings.Contains(strings.ReplaceAll(inner, "''", ""), "'")
}

// blobLiteral reports whether value is one blob literal such as X'00ff'.
func blobLiteral(value string) bool {
	if len(value) < 3 || (value[0] != 'x' && value[0] != 'X') {
		return false
	}
	hex := value[1:]
	if !singleQuoted(hex) {
		return false
	}
	return strings.Trim(hex[1:len(hex)-1], "0123456789abcdefABCDEF") == ""
}

// declaresPrimaryKey reports whether the table the declaration writes holds a
// primary key, on a column or as a table constraint.
func declaresPrimaryKey(node *ast.CreateTableNode) bool {
	for _, column := range node.Columns {
		if column.Primary {
			return true
		}
	}
	for _, constraint := range node.Constraints {
		if constraint.Type == ast.PrimaryKeyConstraint {
			return true
		}
	}
	return false
}

// carryConstraints adds the constraints out of scope to the new table.
//
// A primary key is carried where the declaration holds none, which is the case
// when the selector that kept it out of scope named the declared key too; a
// table holds one key, so beside a declared key it is refused. The key over
// inlineKey, the AUTOINCREMENT column carryColumns carried with its key, is
// already on that column. A constraint
// over a column the new table does not hold is refused by name, and so is a
// CHECK whose expression names one: SQLite would refuse the CREATE TABLE, or
// the rule would go with the column.
func carryConstraints(
	node *ast.CreateTableNode,
	table schemamodel.Table,
	constraints []catalog.Constraint,
	inlineKey string,
	columns, dropped map[string]bool,
	semantics identifier.Semantics,
) error {
	for _, constraint := range constraints {
		if inlineKey != "" && keyOver(constraint, inlineKey, semantics) {
			continue
		}
		if indexbacking.KindOf(constraint.Type) == indexbacking.PrimaryKey && declaresPrimaryKey(node) {
			return unsupportedFeaturef(
				"rebuilding table %s cannot keep primary key %s as it is: an exclude selector keeps it out of scope, "+
					"and the rebuilt table takes its key from the declaration. Exclude the whole table, or declare the key "+
					"and stop excluding it",
				table.QualifiedName(), constraint.Name)
		}
		for _, column := range constraintColumns(constraint) {
			if !columns[semantics.ColumnIdentityKey(column)] {
				return unsupportedFeaturef(
					"rebuilding table %s cannot keep constraint %s as it is: an exclude selector keeps it out of scope, "+
						"and the rebuilt table has no column %s. Keep the column, or exclude the whole table",
					table.QualifiedName(), constraint.Name, column)
			}
		}
		if constraint.CheckClause != nil {
			for _, column := range slices.Sorted(maps.Keys(dropped)) {
				if expressionReads(*constraint.CheckClause, column, semantics) {
					return unsupportedFeaturef(
						"rebuilding table %s cannot keep constraint %s as it is: an exclude selector keeps it out of scope, "+
							"and its expression reads column %s, which the rebuilt table drops. Keep the column, or exclude "+
							"the whole table",
						table.QualifiedName(), constraint.Name, column)
				}
			}
		}
		if carried := modelast.FromConstraint(carriedConstraint(constraint, table.Name)); carried != nil {
			node.AddConstraint(carried)
		}
	}
	return nil
}

// constraintColumns lists the columns a constraint holds.
func constraintColumns(constraint catalog.Constraint) []string {
	if len(constraint.ColumnNames) > 0 {
		return constraint.ColumnNames
	}
	if constraint.ColumnName != "" && constraint.CheckClause == nil {
		return []string{constraint.ColumnName}
	}
	return nil
}

// carriedConstraint is the constraint the read reported, in the form the
// lowering takes.
//
// The reader names a constraint the table's statement left unnamed: a UNIQUE
// after the index SQLite built for it, `sqlite_autoindex_<table>_<n>`, a
// primary key `<table>_pkey`, and a foreign key `fk_<table>_<column>`. None of
// these names is in the statement, and SQLite refuses an object name that
// starts with `sqlite_`, so the carried constraint goes back unnamed under
// each.
func carriedConstraint(constraint catalog.Constraint, table string) schemamodel.Constraint {
	name := constraint.Name
	switch indexbacking.KindOf(constraint.Type) {
	case indexbacking.PrimaryKey:
		if name == table+"_pkey" {
			name = ""
		}
	case indexbacking.ForeignKey:
		if name == schemaprep.GenerateForeignKeyName(table, constraint.ColumnName) {
			name = ""
		}
	}
	if strings.HasPrefix(strings.ToLower(name), "sqlite_") {
		name = ""
	}
	carried := schemamodel.Constraint{
		Name:        name,
		Type:        constraint.Type,
		Columns:     constraintColumns(constraint),
		Deferrable:  constraint.Deferrable,
		Initially:   constraint.Initially,
		Match:       constraint.Match,
		NotEnforced: constraint.NotEnforced,
	}
	if constraint.CheckClause != nil {
		carried.CheckExpression = *constraint.CheckClause
	}
	if constraint.ForeignTable != nil {
		carried.ForeignTable = catalog.QualifyTableName(constraint.ForeignSchema, *constraint.ForeignTable)
		carried.ForeignColumns = constraint.ForeignColumns
		if len(carried.ForeignColumns) == 0 && constraint.ForeignColumn != nil {
			carried.ForeignColumns = []string{*constraint.ForeignColumn}
		}
	}
	if constraint.DeleteRule != nil {
		carried.OnDelete = *constraint.DeleteRule
	}
	if constraint.UpdateRule != nil {
		carried.OnUpdate = *constraint.UpdateRule
	}
	return carried
}

// carryTriggers creates the triggers out of scope again from the statement
// SQLite stored for each. DROP TABLE takes a table's triggers with it.
//
// SQLite resolves the columns a trigger's body names when the trigger fires,
// not when it is created, so a trigger that reads a column the rebuilt table
// drops would be created and then fail every statement that fires it. Such a
// trigger is refused by name, as a CHECK that reads a dropped column is. A
// name the trigger writes into another table's column list, or qualifies with
// another table, belongs to that table and is left alone (see references.go).
func carryTriggers(table schemamodel.Table, triggers []catalog.Trigger, dropped map[string]bool, semantics identifier.Semantics) ([]ast.Node, error) {
	nodes := make([]ast.Node, 0, len(triggers))
	for _, trigger := range triggers {
		definition := strings.TrimRight(strings.TrimSpace(trigger.Definition), ";")
		if definition == "" {
			return nil, unsupportedFeaturef(
				"rebuilding table %s cannot keep trigger %s as it is: an exclude selector keeps it out of scope, "+
					"and the read kept no statement to create it again from. Exclude the whole table, or declare the trigger",
				table.QualifiedName(), trigger.Name)
		}
		for _, column := range slices.Sorted(maps.Keys(dropped)) {
			reference := relationReference{table: table.Name, column: column, onTable: true, semantics: semantics}
			if reference.triggerReads(definition, "") {
				return nil, unsupportedFeaturef(
					"rebuilding table %s cannot keep trigger %s as it is: an exclude selector keeps it out of scope, "+
						"and its statement reads column %s, which the rebuilt table drops. Keep the column, or exclude "+
						"the whole table",
					table.QualifiedName(), trigger.Name, column)
			}
		}
		nodes = append(nodes, ast.NewRawSQL(definition+";"))
	}
	return nodes, nil
}

package parser

import "ptah.run/core/ast"

// ColumnTypeChange is the target of a parsed ALTER TABLE type conversion.
// Table and Column keep their source quoting. Type is the complete type
// spelling, including precision, scale and modifiers.
type ColumnTypeChange struct {
	Table  string
	Column string
	Type   string
}

// ReadColumnTypeChanges reads explicit column type conversions from SQL text, in source order.
// Other operations contribute no entry. Invalid or unsupported syntax returns
// an error and no changes, and empty text returns an empty slice.
//
// This read returns types alone, never a schema or executable AST. It can read
// the explicit type in MODIFY on a dialect whose MODIFY keeps omitted column
// properties, such as ClickHouse. Normal schema parsing refuses that form
// because treating it as a whole replacement would clear those properties.
func ReadColumnTypeChanges(text string, options ...Option) ([]ColumnTypeChange, error) {
	p := NewParser(text, options...)
	p.columnTypesOnly = true
	parsed, err := p.Parse()
	if err != nil {
		return nil, err
	}
	var changes []ColumnTypeChange
	for _, statement := range parsed.Statements {
		table, ok := statement.(*ast.AlterTableNode)
		if !ok {
			continue
		}
		for _, operation := range table.Operations {
			column, typ := operationType(operation)
			if typ != "" {
				changes = append(changes, ColumnTypeChange{Table: table.Name, Column: column, Type: typ})
			}
		}
	}
	return changes, nil
}

func operationType(operation ast.AlterOperation) (columnName, typ string) {
	switch operation := operation.(type) {
	case *ast.ModifyColumnOperation:
		if operation.Column != nil {
			return operation.Column.Name, operation.Column.Type
		}
	case *ast.AlterColumnOperation:
		if operation.Action == ast.AlterColumnSetType {
			return operation.ColumnName, operation.Type
		}
	}
	return "", ""
}

package mysql

import (
	"context"
	"fmt"
	"slices"

	"ptah.run/catalog"
	"ptah.run/internal/readscope"
	"ptah.run/internal/revisiontable"
)

// foreignKeyConstraintType is TABLE_CONSTRAINTS.CONSTRAINT_TYPE for a foreign
// key.
const foreignKeyConstraintType = "FOREIGN KEY"

// withHiddenForeignKeys adds to constraints the foreign keys of dbName that
// KEY_COLUMN_USAGE lists and TABLE_CONSTRAINTS hid, for a MariaDB read whose run
// leaves constraints out.
//
// MariaDB hides TABLE_CONSTRAINTS and REFERENTIAL_CONSTRAINTS from an account
// holding only SELECT, and still lists each foreign key's columns and referenced
// table in KEY_COLUMN_USAGE (measured on 11.8 and 12.3). A run that leaves
// constraints out does not refuse that account, and its comparison still needs
// to know which foreign key stands on which index: the server refuses to drop
// the last index a foreign key can use, so a plan that drops an index under an
// unseen key fails at apply. A key read this way has no referential actions,
// which a run that leaves constraints out never compares.
//
// A read whose run needs constraints refuses that account instead, and an
// account that sees TABLE_CONSTRAINTS gets nothing added here.
func (r *Reader) withHiddenForeignKeys(
	ctx context.Context,
	account sessionAccount,
	dbName string,
	foreignSchema foreignSchemaRule,
	constraints []catalog.Constraint,
) ([]catalog.Constraint, error) {
	if !account.mariaDB || readscope.Needs(ctx, readscope.Constraints) {
		return constraints, nil
	}
	query := `
		SELECT
			CONSTRAINT_NAME,
			TABLE_NAME,
			COLUMN_NAME,
			COALESCE(REFERENCED_TABLE_SCHEMA, ''),
			REFERENCED_TABLE_NAME,
			COALESCE(REFERENCED_COLUMN_NAME, '')
		FROM information_schema.KEY_COLUMN_USAGE
		WHERE TABLE_SCHEMA = ?
		AND REFERENCED_TABLE_NAME IS NOT NULL
		AND TABLE_NAME NOT IN (` + revisiontable.NativeSQLNames + `)
		ORDER BY TABLE_NAME, CONSTRAINT_NAME, ORDINAL_POSITION`
	rows, err := r.db.QueryContext(ctx, query, dbName)
	if err != nil {
		return nil, fmt.Errorf("read foreign keys from KEY_COLUMN_USAGE: %w", err)
	}
	defer rows.Close()

	seen := func(table, name string) bool {
		return slices.ContainsFunc(constraints, func(constraint catalog.Constraint) bool {
			return constraint.Type == foreignKeyConstraintType && constraint.TableName == table && constraint.Name == name
		})
	}
	hidden := make(map[constraintKey]*catalog.Constraint)
	var order []constraintKey
	for rows.Next() {
		var name, table, column, referencedSchema, referencedTable, referencedColumn string
		if err := rows.Scan(&name, &table, &column, &referencedSchema, &referencedTable, &referencedColumn); err != nil {
			return nil, fmt.Errorf("read foreign keys from KEY_COLUMN_USAGE: %w", err)
		}
		if seen(table, name) {
			continue
		}
		key := constraintKey{table: table, name: name, constraintType: foreignKeyConstraintType}
		constraint, exists := hidden[key]
		if !exists {
			constraint = newConstraint(name, table, foreignKeyConstraintType,
				constraintRefs{referencedTable: referencedTable, referencedColumn: referencedColumn}, checkConstraintClauses{})
			constraint.ForeignSchema = foreignSchema(referencedSchema)
			constraint.ColumnName = column
			hidden[key] = constraint
			order = append(order, key)
		}
		constraint.ColumnNames = append(constraint.ColumnNames, column)
		if referencedColumn != "" {
			constraint.ForeignColumns = append(constraint.ForeignColumns, referencedColumn)
		}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read foreign keys from KEY_COLUMN_USAGE: %w", err)
	}
	for _, key := range order {
		constraints = append(constraints, *hidden[key])
	}
	return constraints, nil
}

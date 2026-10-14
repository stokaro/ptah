package migratorext

import (
	"context"
	"fmt"
)

// InformationSchemaPresence returns the statement that counts the base tables
// named table in schema through information_schema, and what it binds.
func InformationSchemaPresence(schema, table string) (string, []any, error) {
	return `SELECT COUNT(*)
FROM information_schema.tables
WHERE table_schema = ? AND table_name = ? AND table_type = 'BASE TABLE'`, []any{schema, table}, nil
}

// InformationSchemaColumnExists asks information_schema whether table has a
// column named column in schema. query binds the schema, the table and the
// column, in that order; rebind turns its ? placeholders into the engine's.
func InformationSchemaColumnExists(
	ctx context.Context,
	q Queryer,
	query string,
	schema string,
	table Table,
	column string,
) (bool, error) {
	var count int
	if err := q.QueryRowContext(ctx, query, schema, table.Name, column).Scan(&count); err != nil {
		return false, fmt.Errorf("failed to inspect migrations metadata column %s: %w", column, err)
	}
	return count > 0, nil
}

// InformationSchemaColumnsQuery counts one column of one table through
// information_schema, binding the schema, the table and the column.
const InformationSchemaColumnsQuery = `
SELECT COUNT(*)
FROM information_schema.columns
WHERE table_schema = ? AND table_name = ? AND column_name = ?`

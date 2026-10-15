// Package clickhouse gives migration/migrator what ClickHouse does
// differently from what its descriptor states: the catalog queries about the
// migrator's own tables, which ClickHouse answers from its system tables.
package clickhouse

import (
	"context"
	"fmt"

	"ptah.run/core/platform"
	"ptah.run/internal/migratorext"
)

// Services returns ClickHouse's services.
func Services() migratorext.Services {
	return migratorext.Services{
		Dialects: []string{platform.ClickHouse},
		Catalog:  catalog{},
	}
}

type catalog struct{}

// PresenceQuery asks system.tables in the configured schema or the
// connection's database, leaving temporary tables out.
func (catalog) PresenceQuery(table migratorext.Table) (string, []any, error) {
	return `SELECT count()
FROM system.tables
WHERE database = ? AND name = ? AND is_temporary = 0`, []any{table.ConfiguredOrConnectionSchema(), table.Name}, nil
}

// ColumnExists asks system.columns in the current database.
func (catalog) ColumnExists(ctx context.Context, q migratorext.Queryer, table migratorext.Table, column string) (bool, error) {
	var count int
	if err := q.QueryRowContext(
		ctx,
		`SELECT count() FROM system.columns WHERE database = currentDatabase() AND table = ? AND name = ?`,
		table.Name,
		column,
	).Scan(&count); err != nil {
		return false, fmt.Errorf("failed to inspect migrations metadata column %s: %w", column, err)
	}
	return count > 0, nil
}

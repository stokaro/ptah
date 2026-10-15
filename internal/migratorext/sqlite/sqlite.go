// Package sqlite gives migration/migrator what SQLite does differently from
// what its descriptor states: the catalog queries about the migrator's own
// tables, which SQLite answers from sqlite_schema and PRAGMA table_info.
package sqlite

import (
	"context"
	"database/sql"
	"fmt"

	"ptah.run/core/platform"
	"ptah.run/internal/migratorext"
)

// Services returns SQLite's services.
func Services() migratorext.Services {
	return migratorext.Services{
		Dialects: []string{platform.SQLite},
		Catalog:  catalog{},
		// In-memory SQLite has one physical connection, so using the pool for
		// checkpoints while that connection is pinned would deadlock.
		PinnedMetadata: true,
	}
}

type catalog struct{}

// PresenceQuery asks the sqlite_schema of the configured or connected
// database, main when there is neither, comparing the name as SQLite does.
func (catalog) PresenceQuery(table migratorext.Table) (string, []any, error) {
	schema := table.ConfiguredOrConnectionSchema()
	if schema == "" {
		schema = "main"
	}
	return fmt.Sprintf(
		"SELECT COUNT(*) FROM %s.sqlite_schema WHERE type = 'table' AND name = ? COLLATE NOCASE",
		table.Quote(schema),
	), []any{table.Name}, nil
}

// ColumnExists reads PRAGMA table_info on a connection of its own.
func (catalog) ColumnExists(ctx context.Context, q migratorext.Queryer, table migratorext.Table, column string) (bool, error) {
	conn, err := q.Conn(ctx)
	if err != nil {
		return false, fmt.Errorf("failed to inspect migrations metadata column %s: %w", column, err)
	}
	defer conn.Close()

	rows, err := conn.QueryContext(ctx, "PRAGMA table_info("+table.Quote(table.Name)+")")
	if err != nil {
		return false, fmt.Errorf("failed to inspect migrations metadata column %s: %w", column, err)
	}
	defer rows.Close()

	for rows.Next() {
		var (
			cid          int
			columnName   string
			dataType     string
			notNull      int
			defaultValue sql.NullString
			primaryKey   int
		)
		if err := rows.Scan(&cid, &columnName, &dataType, &notNull, &defaultValue, &primaryKey); err != nil {
			return false, fmt.Errorf("failed to scan migrations metadata column %s: %w", column, err)
		}
		if columnName == column {
			return true, nil
		}
	}
	if err := rows.Err(); err != nil {
		return false, fmt.Errorf("failed to inspect migrations metadata column %s: %w", column, err)
	}
	return false, nil
}

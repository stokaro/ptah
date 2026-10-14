package migrator

import (
	"context"
	"fmt"
	"slices"
	"strings"

	"ptah.run/core/sqlutil"
)

// ydbNativeRevisionColumns are the columns of the native revision table, in
// the order the YDB descriptor's revision table declares them.
var ydbNativeRevisionColumns = []string{
	"version", "description", "applied_at", "state", "applied", "total",
	"error", "error_stmt", "execution_time_ms", "checksum",
}

// metadataTableDescriber is what the migrator asks a YDB schema reader about
// one of its own tables. YDB has no information_schema and no SQL that lists
// tables or columns, so the question goes to the scheme and table services,
// which only the reader reaches.
type metadataTableDescriber interface {
	TableColumns(ctx context.Context, schema, name string) (columns []string, exists bool, err error)
}

// usesDescribedMetadata reports whether the migrator learns about its tables
// by describing them rather than by querying a catalog: whether the
// connection's schema reader can describe one of them, which the reader of an
// engine with no catalog SQL could ask does.
func (m *Migrator) usesDescribedMetadata() bool {
	if m.conn == nil {
		return false
	}
	_, ok := m.conn.Reader().(metadataTableDescriber)
	return ok
}

// describeMetadataTable returns the columns of the metadata table named table,
// in the metadata schema, and whether it exists.
func (m *Migrator) describeMetadataTable(ctx context.Context, table string) ([]string, bool, error) {
	describer, ok := m.conn.Reader().(metadataTableDescriber)
	if !ok {
		return nil, false, fmt.Errorf("the %s schema reader cannot describe the migration metadata table %s",
			m.connectionDialect(), m.qualifiedMetadataTable(table))
	}
	columns, exists, err := describer.TableColumns(ctx, m.metadataTableSchemaName(), table)
	if err != nil {
		return nil, false, fmt.Errorf("describe migration metadata table %s: %w", m.qualifiedMetadataTable(table), err)
	}
	return columns, exists, nil
}

// metadataTableExists reports whether the metadata table named table exists,
// asking the catalog through migrationTablePresenceQuery or, on YDB, by
// describing it.
func (m *Migrator) metadataTableExists(ctx context.Context, table string) (bool, error) {
	if m.usesDescribedMetadata() {
		_, exists, err := m.describeMetadataTable(ctx, table)
		return exists, err
	}
	query, args, err := migrationTablePresenceQuery(
		m.connectionDialect(),
		m.metadataTableSchemaName(),
		m.connectionSchemaName(),
		table,
		m.quoteIdentifier,
	)
	if err != nil {
		return false, err
	}
	var count int64
	if err := m.conn.QueryRowContext(ctx, sqlutil.Rebind(m.connectionDialect(), query), args...).Scan(&count); err != nil {
		return false, err
	}
	return count > 0, nil
}

// requireYDBRevisionColumns refuses a native revision table on YDB that lacks
// a column the current layout writes.
//
// The other engines bring an older table up to the current layout with ALTER
// TABLE ADD. No Ptah wrote a revision table on YDB in an older layout, so a
// table that lacks a column was made by something else, and YDB cannot add a
// NOT NULL column to a table that has rows on every line Ptah supports: before
// 26.1 it refuses ADD COLUMN ... NOT NULL even with a default. So the table is
// refused by name rather than altered.
func (m *Migrator) requireYDBRevisionColumns(ctx context.Context) error {
	columns, exists, err := m.describeMetadataTable(ctx, m.migrationsTableName())
	if err != nil {
		return err
	}
	if !exists {
		return fmt.Errorf("migration metadata table %s does not exist after it was created", m.qualifiedMigrationsTable())
	}
	var missing []string
	for _, column := range ydbNativeRevisionColumns {
		if !slices.Contains(columns, column) {
			missing = append(missing, column)
		}
	}
	if len(missing) == 0 {
		return nil
	}
	return fmt.Errorf("migration metadata table %s has no column %s, so Ptah did not create it; "+
		"drop it and let Ptah create it, or configure another migrations table",
		m.qualifiedMigrationsTable(), strings.Join(missing, ", "))
}

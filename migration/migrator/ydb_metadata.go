package migrator

import (
	"context"
	"fmt"
	"slices"
	"strings"

	"ptah.run/core/platform"
	"ptah.run/core/sqlutil"
)

// The YDB spelling of the migrator's tables.
//
// YQL has no inline PRIMARY KEY, so each key is a table-level clause. Every
// integer column is Int64, because the YDB connection widens a Go int to 64
// bits and YDB refuses an Int64 value for an Int32 column. Every text column is
// Utf8; partial_hashes too, since a Utf8 value cannot be written to a Json or
// JsonDocument column (measured on YDB 26.2.1.14: `Failed to convert 'j': Utf8
// to Optional<JsonDocument>`, and the same for String), so the column holds the
// JSON document as text, as it does on SQL Server and ClickHouse. Timestamp
// rather than Timestamp64, because every line Ptah supports has it and a
// revision is never older than 1970. No column has a default: the migrator
// writes every column of every row it inserts.

func ydbRevisionsTableDDL(qualifiedTable string) string {
	return fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
    version Int64 NOT NULL,
    description Utf8 NOT NULL,
    applied_at Timestamp NOT NULL,
    state Utf8 NOT NULL,
    applied Int64 NOT NULL,
    total Int64 NOT NULL,
    error Utf8,
    error_stmt Utf8,
    execution_time_ms Int64 NOT NULL,
    checksum Utf8 NOT NULL,
    PRIMARY KEY (version)
)`, qualifiedTable)
}

func ydbAtlasRevisionsTableDDL(qualifiedTable string) string {
	return fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
    version Utf8 NOT NULL,
    description Utf8 NOT NULL,
    type Int64 NOT NULL,
    applied Int64 NOT NULL,
    total Int64 NOT NULL,
    executed_at Timestamp NOT NULL,
    execution_time Int64 NOT NULL,
    error Utf8,
    error_stmt Utf8,
    hash Utf8 NOT NULL,
    partial_hashes Utf8,
    operator_version Utf8 NOT NULL,
    PRIMARY KEY (version)
)`, qualifiedTable)
}

func ydbMigrationLogDDL(qualifiedTable string) string {
	return fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
    run_id Utf8 NOT NULL,
    seq Int64 NOT NULL,
    operation Utf8 NOT NULL,
    version Int64 NOT NULL,
    state Utf8 NOT NULL,
    actor Utf8,
    actor_source Utf8 NOT NULL,
    checksum Utf8,
    logged_at Timestamp NOT NULL,
    error Utf8,
    PRIMARY KEY (run_id, seq)
)`, qualifiedTable)
}

func ydbMigrationTagsTableDDL(qualifiedTable string) string {
	return fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
    tag Utf8 NOT NULL,
    version Int64 NOT NULL,
    recorded_at Timestamp NOT NULL,
    PRIMARY KEY (tag)
)`, qualifiedTable)
}

// ydbNativeRevisionColumns are the columns of the native revision table, in
// the order ydbRevisionsTableDDL declares them.
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
// by describing them rather than by querying a catalog.
func (m *Migrator) usesDescribedMetadata() bool {
	return platform.NormalizeDialect(m.connectionDialect()) == platform.YDB
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

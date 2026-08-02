package migrator

import (
	"context"
	"fmt"
	"strings"

	"go.5x5.cz/ptah/core/platform"
	"go.5x5.cz/ptah/core/sqlutil"
)

func (m *Migrator) migrationsTableExists(ctx context.Context) (bool, error) {
	table := m.migrationsTableName()
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
	query = sqlutil.Rebind(m.connectionDialect(), query)
	if err := m.conn.QueryRowContext(ctx, query, args...).Scan(&count); err != nil {
		return false, err
	}
	return count > 0, nil
}

func (m *Migrator) migrationsTableUsesLegacyRevisionLayout(ctx context.Context) (bool, error) {
	rows, err := m.conn.QueryContext(
		ctx,
		fmt.Sprintf("SELECT * FROM %s WHERE 1 = 0", m.qualifiedMigrationsTable()),
	)
	if err != nil {
		return false, fmt.Errorf("failed to inspect migrations metadata columns: %w", err)
	}
	defer rows.Close()
	columns, err := rows.Columns()
	if err != nil {
		return false, fmt.Errorf("failed to read migrations metadata columns: %w", err)
	}
	present := make(map[string]struct{}, len(columns))
	for _, column := range columns {
		present[strings.ToLower(column)] = struct{}{}
	}
	baseColumns := []string{"version", "description", "applied_at"}
	if missing := missingMetadataColumns(present, baseColumns); len(missing) > 0 {
		return false, fmt.Errorf("invalid migrations metadata layout: missing base columns %s", strings.Join(missing, ", "))
	}
	currentColumns := []string{"state", "applied", "total", "error", "error_stmt", "execution_time_ms", "checksum"}
	missing := missingMetadataColumns(present, currentColumns)
	if len(missing) == len(currentColumns) {
		return true, nil
	}
	if len(missing) > 0 {
		return false, fmt.Errorf("incomplete migrations metadata layout: missing columns %s", strings.Join(missing, ", "))
	}
	return false, nil
}

func missingMetadataColumns(present map[string]struct{}, required []string) []string {
	missing := make([]string, 0, len(required))
	for _, column := range required {
		if _, ok := present[column]; !ok {
			missing = append(missing, column)
		}
	}
	return missing
}

func migrationTablePresenceQuery(
	dialect string,
	configuredSchema string,
	connectionSchema string,
	table string,
	quoteIdentifier func(string) string,
) (string, []any, error) {
	switch platform.NormalizeDialect(dialect) {
	case platform.Postgres, platform.CockroachDB, platform.YugabyteDB:
		if configuredSchema == "" {
			// Unqualified CREATE TABLE targets current_schema(), even when an older
			// table with the same name exists later in search_path. Inspect exactly
			// that schema so dry-run and real initialization resolve the same table.
			return `SELECT COUNT(*)
FROM information_schema.tables
WHERE table_schema = current_schema() AND table_name = ? AND table_type = 'BASE TABLE'`, []any{table}, nil
		}
		return informationSchemaTablePresenceQuery(configuredSchema, table)
	case platform.Spanner, platform.MySQL, platform.MariaDB:
		return informationSchemaTablePresenceQuery(
			configuredOrConnectionSchema(configuredSchema, connectionSchema),
			table,
		)
	case platform.ClickHouse:
		return `SELECT count()
FROM system.tables
WHERE database = ? AND name = ? AND is_temporary = 0`, []any{
				configuredOrConnectionSchema(configuredSchema, connectionSchema),
				table,
			}, nil
	case platform.SQLite:
		schema := configuredOrConnectionSchema(configuredSchema, connectionSchema)
		if schema == "" {
			schema = "main"
		}
		return fmt.Sprintf(
			"SELECT COUNT(*) FROM %s.sqlite_schema WHERE type = 'table' AND name = ? COLLATE NOCASE",
			quoteIdentifier(schema),
		), []any{table}, nil
	case platform.SQLServer:
		return `SELECT COUNT(*)
FROM sys.tables AS t
JOIN sys.schemas AS s ON s.schema_id = t.schema_id
WHERE s.name = ? AND t.name = ?`, []any{
				configuredOrConnectionSchema(configuredSchema, connectionSchema),
				table,
			}, nil
	default:
		return "", nil, fmt.Errorf("unsupported migration metadata dialect %q", dialect)
	}
}

func informationSchemaTablePresenceQuery(schema, table string) (string, []any, error) {
	return `SELECT COUNT(*)
FROM information_schema.tables
WHERE table_schema = ? AND table_name = ? AND table_type = 'BASE TABLE'`, []any{schema, table}, nil
}

func configuredOrConnectionSchema(configured, connection string) string {
	if configured != "" {
		return configured
	}
	return connection
}

package migrator

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"slices"
	"strings"

	"go.5x5.cz/ptah/core/platform"
	"go.5x5.cz/ptah/core/sqlutil"
	"go.5x5.cz/ptah/internal/lexer"
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

func (m *Migrator) requireTransactionalMetadataEngine(ctx context.Context) error {
	if !implicitCommitDialect(m.connectionDialect()) {
		return nil
	}
	schema := configuredOrConnectionSchema(m.metadataTableSchemaName(), m.connectionSchemaName())
	var engine string
	err := m.conn.QueryRowContext(ctx, `SELECT engine
FROM information_schema.tables
WHERE table_schema = ? AND table_name = ? AND table_type = 'BASE TABLE'`, schema, m.migrationsTableName()).Scan(&engine)
	if err != nil {
		return fmt.Errorf("failed to inspect migrations metadata storage engine: %w", err)
	}
	if !strings.EqualFold(engine, "InnoDB") {
		return fmt.Errorf(
			"migrations metadata table %s must use InnoDB to track MySQL-family implicit commits; found %s",
			m.qualifiedMigrationsTable(),
			engine,
		)
	}
	return nil
}

func (m *Migrator) requireTransactionalTargetEngines(ctx context.Context) error {
	if !implicitCommitDialect(m.connectionDialect()) {
		return nil
	}
	var defaultEngine string
	if err := m.conn.QueryRowContext(ctx, "SELECT @@SESSION.default_storage_engine").Scan(&defaultEngine); err != nil {
		return fmt.Errorf("failed to inspect MySQL-family default storage engine: %w", err)
	}
	if !strings.EqualFold(defaultEngine, "InnoDB") {
		return fmt.Errorf(
			"tx-mode file requires InnoDB on MySQL-family databases; default storage engine is %s",
			defaultEngine,
		)
	}
	schema := m.connectionSchemaName()
	var table, engine string
	err := m.conn.QueryRowContext(ctx, `SELECT table_name, engine
FROM information_schema.tables
WHERE table_schema = ? AND table_type = 'BASE TABLE'
  AND UPPER(COALESCE(engine, '')) <> 'INNODB'
ORDER BY table_name
LIMIT 1`, schema).Scan(&table, &engine)
	if errors.Is(err, sql.ErrNoRows) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("failed to inspect MySQL-family target table storage engines: %w", err)
	}
	return fmt.Errorf(
		"tx-mode file requires InnoDB target tables on MySQL-family databases; table %s.%s uses %s",
		schema,
		table,
		engine,
	)
}

type mysqlTransactionalCatalog struct {
	otherSchemas    map[string]struct{}
	unsafeRelations map[string]struct{}
	routines        map[string]struct{}
}

func (m *Migrator) requireTransactionalTargetIsolation(
	ctx context.Context,
	migration *Migration,
	direction MigrationDirection,
) error {
	if !implicitCommitDialect(m.connectionDialect()) {
		return nil
	}
	catalog, err := m.loadMySQLTransactionalCatalog(ctx)
	if err != nil {
		return err
	}
	statements := splitSQLStatementsForConnection(m.conn, migrationSQLForDirection(migration, direction))
	for i, statement := range statements {
		tokens := significantSQLTokens(statement, m.connectionDialect())
		if schema, referenced := mysqlReferencedSchema(tokens, catalog.otherSchemas); referenced {
			return fmt.Errorf(
				"migration %d cannot run tx-mode file statement %d because it references database %s outside "+
					"the selected database; use a connection scoped to that database or tx-mode none",
				migration.Version,
				i+1,
				schema,
			)
		}
		if relation, referenced := mysqlReferencedCatalogName(tokens, catalog.unsafeRelations); referenced {
			return fmt.Errorf(
				"migration %d cannot run tx-mode file statement %d because relation %s has indirect behavior "+
					"that Ptah cannot tie to the transaction witness; use direct InnoDB tables or tx-mode none",
				migration.Version,
				i+1,
				relation,
			)
		}
		if routine, invoked := mysqlInvokedRoutine(tokens, catalog.routines); invoked {
			return fmt.Errorf(
				"migration %d cannot run tx-mode file statement %d because routine %s can execute SQL outside "+
					"Ptah's transaction witness; use direct SQL or tx-mode none",
				migration.Version,
				i+1,
				routine,
			)
		}
	}
	return nil
}

func (m *Migrator) loadMySQLTransactionalCatalog(ctx context.Context) (mysqlTransactionalCatalog, error) {
	schema := m.connectionSchemaName()
	catalog := mysqlTransactionalCatalog{
		otherSchemas:    make(map[string]struct{}),
		unsafeRelations: make(map[string]struct{}),
		routines:        make(map[string]struct{}),
	}
	if err := m.collectMySQLCatalogNames(
		ctx,
		"SELECT schema_name FROM information_schema.schemata WHERE LOWER(schema_name) <> LOWER(?)",
		[]any{schema},
		func(name string) { catalog.otherSchemas[strings.ToLower(name)] = struct{}{} },
	); err != nil {
		return mysqlTransactionalCatalog{}, fmt.Errorf("failed to inspect MySQL-family database boundaries: %w", err)
	}
	if err := m.collectMySQLCatalogNames(
		ctx,
		"SELECT table_name FROM information_schema.views WHERE table_schema = ?",
		[]any{schema},
		func(name string) { catalog.unsafeRelations[strings.ToLower(name)] = struct{}{} },
	); err != nil {
		return mysqlTransactionalCatalog{}, fmt.Errorf("failed to inspect MySQL-family views: %w", err)
	}
	if err := m.collectMySQLCatalogNames(
		ctx,
		"SELECT event_object_table FROM information_schema.triggers WHERE trigger_schema = ?",
		[]any{schema},
		func(name string) { catalog.unsafeRelations[strings.ToLower(name)] = struct{}{} },
	); err != nil {
		return mysqlTransactionalCatalog{}, fmt.Errorf("failed to inspect MySQL-family triggers: %w", err)
	}
	if err := m.collectMySQLCatalogNames(
		ctx,
		"SELECT routine_name FROM information_schema.routines WHERE routine_schema = ?",
		[]any{schema},
		func(name string) { catalog.routines[strings.ToLower(name)] = struct{}{} },
	); err != nil {
		return mysqlTransactionalCatalog{}, fmt.Errorf("failed to inspect MySQL-family routines: %w", err)
	}
	return catalog, nil
}

func (m *Migrator) collectMySQLCatalogNames(
	ctx context.Context,
	query string,
	args []any,
	collect func(string),
) error {
	rows, err := m.conn.QueryContext(ctx, query, args...)
	if err != nil {
		return err
	}
	defer func() { _ = rows.Close() }()
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return err
		}
		collect(name)
	}
	return rows.Err()
}

func mysqlReferencedSchema(tokens []lexer.Token, names map[string]struct{}) (string, bool) {
	for i, token := range tokens {
		name, identifier := mysqlIdentifierValue(token)
		if !identifier || i+2 >= len(tokens) || tokens[i+1].Value != "." {
			continue
		}
		if _, referenced := mysqlIdentifierValue(tokens[i+2]); !referenced {
			continue
		}
		relationReference := mysqlDirectRelationReference(tokens, i)
		routineReference := i+3 < len(tokens) && tokens[i+3].Value == "("
		if !relationReference && !routineReference {
			continue
		}
		if _, known := names[strings.ToLower(name)]; known {
			return name, true
		}
	}
	return "", false
}

func mysqlReferencedCatalogName(tokens []lexer.Token, names map[string]struct{}) (string, bool) {
	for i, token := range tokens {
		name, identifier := mysqlIdentifierValue(token)
		if !identifier {
			continue
		}
		_, known := names[strings.ToLower(name)]
		if !known || !mysqlRelationReference(tokens, i) {
			continue
		}
		return name, true
	}
	return "", false
}

func mysqlRelationReference(tokens []lexer.Token, index int) bool {
	if index >= 2 && tokens[index-1].Value == "." {
		return mysqlDirectRelationReference(tokens, index-2)
	}
	return mysqlDirectRelationReference(tokens, index)
}

func mysqlDirectRelationReference(tokens []lexer.Token, index int) bool {
	if index > 0 && matchesAnyKeyword(tokens[index-1], "FROM", "JOIN", "INTO", "UPDATE", "TABLE", "USING") {
		return true
	}
	if index > 0 && tokens[index-1].Value == "," && mysqlCommaContinuesRelationList(tokens[:index-1]) {
		return true
	}
	return mysqlFollowsDMLTargetPrefix(tokens[:index])
}

func mysqlCommaContinuesRelationList(tokens []lexer.Token) bool {
	depth := 0
	for _, v := range slices.Backward(tokens) {
		switch v.Value {
		case ")":
			depth++
		case "(":
			if depth == 0 {
				return false
			}
			depth--
		}
		if depth != 0 || v.Type != lexer.TokenIdentifier {
			continue
		}
		if matchesAnyKeyword(v, "FROM", "UPDATE", "USING") {
			return true
		}
		if matchesAnyKeyword(v, "SELECT", "SET", "VALUES", "WHERE", "GROUP", "HAVING", "ORDER", "LIMIT") {
			return false
		}
	}
	return false
}

func mysqlFollowsDMLTargetPrefix(tokens []lexer.Token) bool {
	for _, v := range slices.Backward(tokens) {
		token := v
		if matchesAnyKeyword(token, "LOW_PRIORITY", "DELAYED", "HIGH_PRIORITY", "IGNORE", "INTO") {
			continue
		}
		return matchesAnyKeyword(token, "INSERT", "REPLACE", "UPDATE")
	}
	return false
}

func mysqlInvokedRoutine(tokens []lexer.Token, routines map[string]struct{}) (string, bool) {
	for i, token := range tokens {
		name, identifier := mysqlIdentifierValue(token)
		if !identifier || i+1 >= len(tokens) || tokens[i+1].Value != "(" {
			continue
		}
		if _, known := routines[strings.ToLower(name)]; known {
			return name, true
		}
	}
	return "", false
}

func mysqlIdentifierValue(token lexer.Token) (string, bool) {
	if token.Type != lexer.TokenIdentifier {
		return "", false
	}
	value := token.Value
	if len(value) >= 2 && value[0] == '`' && value[len(value)-1] == '`' {
		return strings.ReplaceAll(value[1:len(value)-1], "``", "`"), true
	}
	return value, true
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

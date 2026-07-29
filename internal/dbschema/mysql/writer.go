package mysql

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/stokaro/ptah/core/platform"
	"github.com/stokaro/ptah/dbschema/types"
	"github.com/stokaro/ptah/internal/sqlident"
)

const sessionRestoreTimeout = 5 * time.Second

func quoteIdent(name string) string {
	return sqlident.Quote(platform.MySQL, name)
}

func quoteQualifiedIdent(schema, name string) string {
	return sqlident.Qualified(platform.MySQL, schema, name)
}

// Writer writes schemas to MySQL/MariaDB databases
type Writer struct {
	db      *sql.DB
	schema  string
	dialect string
	dryRun  bool
}

type transactionWriter struct {
	mu     sync.Mutex
	tx     *sql.Tx
	schema string
	dryRun bool
}

// NewMySQLWriter creates a new MySQL-family schema writer.
func NewMySQLWriter(db *sql.DB, schema, dialect string) *Writer {
	return &Writer{
		db:      db,
		schema:  schema,
		dialect: platform.NormalizeDialect(dialect),
	}
}

// ExecuteSQL executes a standalone SQL statement. Values
// must be passed via args and referenced through `?` placeholders; the SQL
// string itself should never be assembled with fmt.Sprintf for value
// interpolation. Identifiers (table/column names) cannot be parameterized
// and must be escaped via quoteIdent before being substituted in.
func (w *Writer) ExecuteSQL(ctx context.Context, sqlExpr string, args ...any) error {
	if w.dryRun {
		slog.Info("[DRY RUN] Would execute SQL", "sql", sqlExpr, "args", args)
		return nil
	}
	if w.db == nil {
		return fmt.Errorf("no database connection")
	}

	_, err := w.db.ExecContext(ctx, sqlExpr, args...)
	if err != nil {
		return fmt.Errorf("SQL execution failed: %w\nSQL: %s", err, sqlExpr)
	}
	return nil
}

// BeginTransaction starts a transaction and returns a transaction-scoped
// writer. The parent writer keeps no active transaction state.
func (w *Writer) BeginTransaction(ctx context.Context) (types.SchemaTransaction, error) {
	if w.dryRun {
		slog.Info("[DRY RUN] Would begin transaction")
		return &transactionWriter{schema: w.schema, dryRun: true}, nil
	}
	if w.db == nil {
		return nil, fmt.Errorf("no database connection")
	}

	tx, err := w.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	return &transactionWriter{tx: tx, schema: w.schema}, nil
}

// ExecuteSQL executes SQL against the transaction.
func (w *transactionWriter) ExecuteSQL(ctx context.Context, sqlExpr string, args ...any) error {
	if w.dryRun {
		slog.Info("[DRY RUN] Would execute SQL", "sql", sqlExpr, "args", args)
		return nil
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.tx == nil {
		return fmt.Errorf("transaction is closed")
	}
	_, err := w.tx.ExecContext(ctx, sqlExpr, args...)
	if err != nil {
		return fmt.Errorf("SQL execution failed: %w\nSQL: %s", err, sqlExpr)
	}
	return nil
}

// Commit commits the transaction.
func (w *transactionWriter) Commit() error {
	if w.dryRun {
		slog.Info("[DRY RUN] Would commit transaction")
		return nil
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.tx == nil {
		return fmt.Errorf("transaction is closed")
	}
	err := w.tx.Commit()
	w.tx = nil
	return err
}

// Rollback rolls back the transaction.
func (w *transactionWriter) Rollback() error {
	if w.dryRun {
		slog.Info("[DRY RUN] Would rollback transaction")
		return nil
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.tx == nil {
		return nil
	}
	err := w.tx.Rollback()
	w.tx = nil
	return err
}

// IsDryRun returns whether dry-run mode is enabled.
func (w *transactionWriter) IsDryRun() bool { return w.dryRun }

type cleanupObject struct {
	Name string
	Kind string
}

type cleanupForeignKey struct {
	Table string
	Name  string
}

// DropAllTables drops all user schema objects in the configured database.
func (w *Writer) DropAllTables(ctx context.Context) (resultErr error) {
	if w.dryRun {
		return nil
	}
	if w.db == nil {
		return fmt.Errorf("no database connection")
	}

	conn, err := w.db.Conn(ctx)
	if err != nil {
		return fmt.Errorf("mysql: acquire cleanup connection: %w", err)
	}
	defer func() {
		closeErr := conn.Close()
		if closeErr != nil && !errors.Is(closeErr, sql.ErrConnDone) {
			resultErr = errors.Join(resultErr, fmt.Errorf("mysql: close cleanup connection: %w", closeErr))
		}
	}()

	schema, err := w.cleanupSchema(ctx, conn)
	if err != nil {
		return err
	}
	foreignKeyChecks, err := readForeignKeyChecks(ctx, conn)
	if err != nil {
		return err
	}
	if !foreignKeyChecks {
		defer func() {
			cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), sessionRestoreTimeout)
			defer cancel()
			if _, restoreErr := conn.ExecContext(cleanupCtx, "SET FOREIGN_KEY_CHECKS = 0"); restoreErr != nil {
				discardConn(conn)
				resultErr = errors.Join(resultErr, fmt.Errorf("mysql: restore foreign key checks: %w", restoreErr))
			}
		}()
		if _, err := conn.ExecContext(ctx, "SET FOREIGN_KEY_CHECKS = 1"); err != nil {
			return fmt.Errorf("mysql: enable foreign key checks for cleanup: %w", err)
		}
	}

	foreignKeys, err := listInternalForeignKeys(ctx, conn, schema)
	if err != nil {
		return err
	}
	objects, err := listCleanupObjects(ctx, conn, schema)
	if err != nil {
		return err
	}
	// Run dependency checks immediately before the first destructive statement.
	// FOREIGN_KEY_CHECKS remains enabled, so a cross-database foreign key
	// created after this preflight still makes DROP TABLE fail.
	if err := rejectExternalForeignKeys(ctx, conn, schema); err != nil {
		return err
	}
	if err := rejectExternalViews(ctx, conn, schema, w.dialect); err != nil {
		return err
	}

	for _, foreignKey := range foreignKeys {
		//nolint:gosec // G202: schema and catalog identifiers are emitted only through identifier quoting.
		dropSQL := "ALTER TABLE " + quoteQualifiedIdent(schema, foreignKey.Table) +
			" DROP FOREIGN KEY " + quoteIdent(foreignKey.Name)
		if _, err := conn.ExecContext(ctx, dropSQL); err != nil {
			return fmt.Errorf(
				"failed to drop foreign key %s on table %s: SQL execution failed: %w\nSQL: %s",
				foreignKey.Name,
				foreignKey.Table,
				err,
				dropSQL,
			)
		}
	}

	// MySQL DDL implicitly commits, so cleanup deliberately avoids a transaction.
	for _, object := range objects {
		// Identifiers cannot be bound as parameters; quoteIdent escapes every
		// backtick in the schema and name returned by information_schema.
		//nolint:gosec // G202: schema and object.Name are emitted only through identifier quoting.
		dropSQL := "DROP " + object.Kind + " IF EXISTS " + quoteQualifiedIdent(schema, object.Name)
		if _, err := conn.ExecContext(ctx, dropSQL); err != nil {
			return fmt.Errorf("failed to drop %s %s: SQL execution failed: %w\nSQL: %s",
				strings.ToLower(object.Kind), object.Name, err, dropSQL)
		}
	}

	return nil
}

func (w *Writer) cleanupSchema(ctx context.Context, conn *sql.Conn) (string, error) {
	if strings.TrimSpace(w.schema) != "" {
		return w.schema, nil
	}
	var schema sql.NullString
	if err := conn.QueryRowContext(ctx, "SELECT DATABASE()").Scan(&schema); err != nil {
		return "", fmt.Errorf("mysql: read current database: %w", err)
	}
	if !schema.Valid || strings.TrimSpace(schema.String) == "" {
		return "", fmt.Errorf("mysql: cleanup requires a selected database")
	}
	return schema.String, nil
}

func readForeignKeyChecks(ctx context.Context, conn *sql.Conn) (bool, error) {
	var value bool
	if err := conn.QueryRowContext(ctx, "SELECT @@SESSION.FOREIGN_KEY_CHECKS").Scan(&value); err != nil {
		return false, fmt.Errorf("mysql: read foreign key checks: %w", err)
	}
	return value, nil
}

func rejectExternalForeignKeys(ctx context.Context, conn *sql.Conn, schema string) error {
	var count int
	if err := conn.QueryRowContext(ctx, `
		SELECT COUNT(*)
		FROM (
			SELECT DISTINCT constraint_schema, constraint_name
			FROM information_schema.key_column_usage
			WHERE referenced_table_schema = ?
			  AND constraint_schema <> ?
		) AS external_foreign_keys
	`, schema, schema).Scan(&count); err != nil {
		return fmt.Errorf("mysql: inspect cross-database foreign keys: %w", err)
	}
	if count > 0 {
		return fmt.Errorf("mysql: refusing to clean database %q: %d foreign key constraints from other databases reference it", schema, count)
	}
	return nil
}

func listInternalForeignKeys(
	ctx context.Context,
	conn *sql.Conn,
	schema string,
) ([]cleanupForeignKey, error) {
	rows, err := conn.QueryContext(ctx, `
		SELECT table_name, constraint_name
		FROM (
			SELECT DISTINCT table_name, constraint_name
			FROM information_schema.key_column_usage
			WHERE constraint_schema = ?
			  AND referenced_table_name IS NOT NULL
		) AS internal_foreign_keys
		ORDER BY table_name, constraint_name
	`, schema)
	if err != nil {
		return nil, fmt.Errorf("mysql: query internal foreign keys: %w", err)
	}
	defer rows.Close()

	var foreignKeys []cleanupForeignKey
	for rows.Next() {
		var foreignKey cleanupForeignKey
		if err := rows.Scan(&foreignKey.Table, &foreignKey.Name); err != nil {
			return nil, fmt.Errorf("mysql: scan internal foreign key: %w", err)
		}
		foreignKeys = append(foreignKeys, foreignKey)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("mysql: iterate internal foreign keys: %w", err)
	}

	return foreignKeys, nil
}

func rejectExternalViews(ctx context.Context, conn *sql.Conn, schema, dialect string) error {
	count, err := externalViewCount(ctx, conn, schema, dialect)
	if err != nil {
		return err
	}
	if count > 0 {
		return fmt.Errorf(
			"mysql: refusing to clean database %q: %d views from other databases reference it",
			schema,
			count,
		)
	}
	return nil
}

func externalViewCount(ctx context.Context, conn *sql.Conn, schema, dialect string) (int, error) {
	if platform.NormalizeDialect(dialect) == platform.MariaDB {
		return mariaDBExternalViewCount(ctx, conn, schema)
	}
	return mySQLExternalViewCount(ctx, conn, schema)
}

func mySQLExternalViewCount(ctx context.Context, conn *sql.Conn, schema string) (int, error) {
	var count int
	if err := conn.QueryRowContext(ctx, `
		SELECT COUNT(*)
		FROM (
			SELECT DISTINCT view_schema, view_name
			FROM information_schema.view_table_usage
			WHERE table_schema = ?
			  AND view_schema <> ?
		) AS external_views
	`, schema, schema).Scan(&count); err != nil {
		return 0, fmt.Errorf("mysql: inspect cross-database views: %w", err)
	}
	return count, nil
}

func mariaDBExternalViewCount(ctx context.Context, conn *sql.Conn, schema string) (int, error) {
	// MariaDB has no information_schema.view_table_usage relation. It
	// normalizes table references in VIEW_DEFINITION to fully qualified,
	// backtick-quoted names, so the exact database qualifier is a conservative
	// dependency signal. Keeping the qualifier as a bound value avoids mixing
	// metadata values into the query text.
	var count int
	if err := conn.QueryRowContext(ctx, `
		SELECT COUNT(*)
		FROM information_schema.views
		WHERE table_schema <> ?
		  AND INSTR(view_definition, ?) > 0
	`, schema, quoteIdent(schema)+".").Scan(&count); err != nil {
		return 0, fmt.Errorf("mariadb: inspect cross-database views: %w", err)
	}
	return count, nil
}

func listCleanupObjects(ctx context.Context, conn *sql.Conn, schema string) ([]cleanupObject, error) {
	rows, err := conn.QueryContext(ctx, `
		SELECT object_name, object_kind
		FROM (
			SELECT
				table_name AS object_name,
				CASE table_type
					WHEN 'VIEW' THEN 'VIEW'
					WHEN 'SEQUENCE' THEN 'SEQUENCE'
					ELSE 'TABLE'
				END AS object_kind,
				CASE table_type
					WHEN 'VIEW' THEN 10
					WHEN 'SEQUENCE' THEN 40
					ELSE 30
				END AS priority
			FROM information_schema.tables
			WHERE table_schema = ?
			  AND table_type IN ('BASE TABLE', 'VIEW', 'SEQUENCE', 'SYSTEM VERSIONED')

			UNION ALL

			SELECT routine_name, routine_type, 20
			FROM information_schema.routines
			WHERE routine_schema = ?

			UNION ALL

			SELECT event_name, 'EVENT', 0
			FROM information_schema.events
			WHERE event_schema = ?
		) AS cleanup_objects
		ORDER BY priority, object_kind, object_name
	`, schema, schema, schema)
	if err != nil {
		return nil, fmt.Errorf("mysql: query schema objects: %w", err)
	}
	defer rows.Close()

	var objects []cleanupObject
	for rows.Next() {
		var object cleanupObject
		if err := rows.Scan(&object.Name, &object.Kind); err != nil {
			return nil, fmt.Errorf("mysql: scan schema object: %w", err)
		}
		objects = append(objects, object)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("mysql: iterate schema objects: %w", err)
	}

	return objects, nil
}

func discardConn(conn *sql.Conn) {
	// Never return a connection with unknown session state to the pool.
	_ = conn.Raw(func(any) error {
		return driver.ErrBadConn
	})
}

// isCreateTableStatement checks if a SQL statement is a CREATE TABLE statement
func (w *Writer) isCreateTableStatement(sqlExpr string) bool {
	return strings.HasPrefix(strings.ToUpper(strings.TrimSpace(sqlExpr)), "CREATE TABLE")
}

// isCreateIndexStatement checks if a SQL statement is a CREATE INDEX statement
func (w *Writer) isCreateIndexStatement(sqlExpr string) bool {
	return strings.Contains(strings.ToUpper(strings.TrimSpace(sqlExpr)), "CREATE") &&
		strings.Contains(strings.ToUpper(strings.TrimSpace(sqlExpr)), "INDEX")
}

// extractTableNameFromCreateTable extracts table name from CREATE TABLE statement
func (w *Writer) extractTableNameFromCreateTable(sqlExpr string) string {
	// Simple regex to extract table name from "CREATE TABLE tablename ("
	parts := strings.Fields(strings.TrimSpace(sqlExpr))
	if len(parts) >= 3 && strings.ToUpper(parts[0]) == "CREATE" && strings.ToUpper(parts[1]) == "TABLE" {
		return strings.TrimSuffix(parts[2], "(")
	}
	return ""
}

// extractTableNameFromCreateIndex extracts table name from CREATE INDEX statement
func (w *Writer) extractTableNameFromCreateIndex(sqlExpr string) string {
	// Look for "ON tablename" pattern
	parts := strings.Fields(strings.TrimSpace(sqlExpr))
	for i, part := range parts {
		if strings.ToUpper(part) == "ON" && i+1 < len(parts) {
			return strings.TrimSuffix(parts[i+1], "(")
		}
	}
	return ""
}

// tableExists checks if a table exists in the database
func (w *Writer) tableExists(tableName string) bool { //nolint:unused // TODO: verify why this is not used
	if w.dryRun {
		// In dry run mode, assume table doesn't exist to show all operations
		return false
	}

	var exists bool
	checkSQL := `
		SELECT EXISTS (
			SELECT 1 FROM information_schema.tables
			WHERE table_schema = DATABASE() AND table_name = ?
		)`

	err := w.db.QueryRow(checkSQL, tableName).Scan(&exists)
	return err == nil && exists
}

// SetDryRun enables or disables dry run mode
func (w *Writer) SetDryRun(dryRun bool) {
	w.dryRun = dryRun
}

// IsDryRun returns whether dry run mode is enabled
func (w *Writer) IsDryRun() bool {
	return w.dryRun
}

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

	"github.com/stokaro/ptah/dbschema/types"
)

const sessionRestoreTimeout = 5 * time.Second

// quoteIdent returns a safely-backtick-quoted MySQL/MariaDB identifier.
// Embedded backticks are doubled so that values coming from
// information_schema (or any other untrusted-shaped string) cannot terminate
// the quoted identifier and inject DDL.
func quoteIdent(name string) string {
	return "`" + strings.ReplaceAll(name, "`", "``") + "`"
}

// Writer writes schemas to MySQL/MariaDB databases
type Writer struct {
	db     *sql.DB
	schema string
	dryRun bool
}

type transactionWriter struct {
	mu     sync.Mutex
	tx     *sql.Tx
	schema string
	dryRun bool
}

// NewMySQLWriter creates a new MySQL schema writer
func NewMySQLWriter(db *sql.DB, schema string) *Writer {
	return &Writer{
		db:     db,
		schema: schema,
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

// DropAllTables drops ALL tables in the database (COMPLETE CLEANUP!)
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

	if _, err := conn.ExecContext(ctx, "SET FOREIGN_KEY_CHECKS = 0"); err != nil {
		return fmt.Errorf("failed to disable foreign key checks: %w", err)
	}
	defer func() {
		cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), sessionRestoreTimeout)
		defer cancel()
		if _, restoreErr := conn.ExecContext(cleanupCtx, "SET FOREIGN_KEY_CHECKS = 1"); restoreErr != nil {
			discardConn(conn)
			resultErr = errors.Join(resultErr, fmt.Errorf("mysql: restore foreign key checks: %w", restoreErr))
		}
	}()

	tables, err := listTables(ctx, conn)
	if err != nil {
		return err
	}

	// MySQL DDL implicitly commits, so cleanup deliberately avoids a transaction.
	for _, tableName := range tables {
		// Identifiers cannot be bound as parameters; quoteIdent escapes every
		// backtick in the name returned by information_schema.
		//nolint:gosec // G202: tableName is emitted only through identifier quoting.
		dropSQL := "DROP TABLE IF EXISTS " + quoteIdent(tableName)
		if _, err := conn.ExecContext(ctx, dropSQL); err != nil {
			return fmt.Errorf("failed to drop table %s: SQL execution failed: %w\nSQL: %s", tableName, err, dropSQL)
		}
	}

	return nil
}

func listTables(ctx context.Context, conn *sql.Conn) ([]string, error) {
	rows, err := conn.QueryContext(ctx, `
		SELECT table_name
		FROM information_schema.tables
		WHERE table_schema = DATABASE() AND table_type = 'BASE TABLE'
		ORDER BY table_name
	`)
	if err != nil {
		return nil, fmt.Errorf("mysql: query tables: %w", err)
	}
	defer rows.Close()

	var tables []string
	for rows.Next() {
		var tableName string
		if err := rows.Scan(&tableName); err != nil {
			return nil, fmt.Errorf("mysql: scan table name: %w", err)
		}
		tables = append(tables, tableName)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("mysql: iterate tables: %w", err)
	}

	return tables, nil
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

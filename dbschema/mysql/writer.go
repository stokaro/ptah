package mysql

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"strings"
)

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

// ExecuteSQL executes a SQL statement against the active transaction. Values
// must be passed via args and referenced through `?` placeholders; the SQL
// string itself should never be assembled with fmt.Sprintf for value
// interpolation. Identifiers (table/column names) cannot be parameterized
// and must be escaped via quoteIdent before being substituted in.
func (w *Writer) ExecuteSQL(ctx context.Context, sqlExpr string, args ...any) error {
	if w.dryRun {
		slog.Info("[DRY RUN] Would execute SQL", "sql", sqlExpr, "args", args)
		return nil
	}

	if w.tx == nil {
		return fmt.Errorf("no active transaction")
	}

	_, err := w.tx.ExecContext(ctx, sqlExpr, args...)
	if err != nil {
		return fmt.Errorf("SQL execution failed: %w\nSQL: %s", err, sqlExpr)
	}
	return nil
}

// BeginTransaction starts a new transaction
func (w *Writer) BeginTransaction() error {
	if w.dryRun {
		slog.Info("[DRY RUN] Would begin transaction")
		return nil
	}

	if w.tx != nil {
		return fmt.Errorf("transaction already active")
	}

	tx, err := w.db.Begin()
	if err != nil {
		return err
	}
	w.tx = tx
	return nil
}

// CommitTransaction commits the current transaction
func (w *Writer) CommitTransaction() error {
	if w.dryRun {
		slog.Info("[DRY RUN] Would commit transaction")
		return nil
	}

	if w.tx == nil {
		return fmt.Errorf("no active transaction")
	}

	err := w.tx.Commit()
	w.tx = nil
	return err
}

// RollbackTransaction rolls back the current transaction
func (w *Writer) RollbackTransaction() error {
	if w.dryRun {
		slog.Info("[DRY RUN] Would rollback transaction")
		return nil
	}

	if w.tx == nil {
		return nil // No transaction to rollback
	}

	err := w.tx.Rollback()
	w.tx = nil
	return err
}

// DropAllTables drops ALL tables in the database (COMPLETE CLEANUP!)
func (w *Writer) DropAllTables() error {
	slog.Info("WARNING: This will drop ALL tables in the database!")

	// Start transaction
	if err := w.BeginTransaction(); err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}

	// Rollback on error, commit on success
	defer func() {
		if w.tx != nil {
			w.RollbackTransaction() // TODO: weird - it always rolls back
		}
	}()

	ctx := context.Background()

	// Disable foreign key checks to avoid dependency issues
	if err := w.ExecuteSQL(ctx, "SET FOREIGN_KEY_CHECKS = 0"); err != nil {
		return fmt.Errorf("failed to disable foreign key checks: %w", err)
	}

	var tables []string

	if w.dryRun {
		// In dry run mode, simulate some tables for demonstration
		tables = []string{"example_table1", "example_table2", "example_table3"}
	} else {
		// Get all tables in the current database
		tablesQuery := `
			SELECT table_name
			FROM information_schema.tables
			WHERE table_schema = DATABASE() AND table_type = 'BASE TABLE'
			ORDER BY table_name`

		rows, err := w.db.Query(tablesQuery)
		if err != nil {
			return fmt.Errorf("failed to query tables: %w", err)
		}
		defer rows.Close()

		for rows.Next() {
			var tableName string
			if err := rows.Scan(&tableName); err != nil {
				return fmt.Errorf("failed to scan table name: %w", err)
			}
			tables = append(tables, tableName)
		}
	}

	// Drop all tables. Identifiers cannot be bound as parameters; quoteIdent
	// doubles any embedded backtick so that a name harvested from
	// information_schema cannot break out of the quoted identifier.
	for _, tableName := range tables {
		dropSQL := "DROP TABLE IF EXISTS " + quoteIdent(tableName)
		slog.Info("Dropping table", "tableName", tableName)
		if err := w.ExecuteSQL(ctx, dropSQL); err != nil {
			return fmt.Errorf("failed to drop table %s: %w", tableName, err)
		}
	}

	// Re-enable foreign key checks
	if err := w.ExecuteSQL(ctx, "SET FOREIGN_KEY_CHECKS = 1"); err != nil {
		return fmt.Errorf("failed to re-enable foreign key checks: %w", err)
	}

	// Commit transaction
	if err := w.CommitTransaction(); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	slog.Info("Successfully dropped tables", "count", len(tables))
	return nil
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

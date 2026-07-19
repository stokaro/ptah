package sqlite

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"strings"
)

func quoteIdent(name string) string {
	return `"` + strings.ReplaceAll(name, `"`, `""`) + `"`
}

// Writer applies schema changes to a SQLite database.
type Writer struct {
	db     *sql.DB
	tx     *sql.Tx
	schema string
	dryRun bool
}

// NewSQLiteWriter creates a SQLite schema writer.
func NewSQLiteWriter(db *sql.DB, schema string) *Writer {
	if schema == "" {
		schema = "main"
	}
	return &Writer{db: db, schema: schema}
}

// ExecuteSQL executes a SQL statement against the active transaction.
func (w *Writer) ExecuteSQL(ctx context.Context, sqlExpr string, args ...any) error {
	if w.dryRun {
		slog.Info("[DRY RUN] Would execute SQL", "sql", sqlExpr, "args", args)
		return nil
	}
	if w.tx == nil {
		return fmt.Errorf("no active transaction")
	}
	if _, err := w.tx.ExecContext(ctx, sqlExpr, args...); err != nil {
		return fmt.Errorf("sqlite: SQL execution failed: %w\nSQL: %s", err, sqlExpr)
	}
	return nil
}

// BeginTransaction starts a transaction.
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

// CommitTransaction commits the active transaction.
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

// RollbackTransaction rolls back the active transaction.
func (w *Writer) RollbackTransaction() error {
	if w.dryRun {
		slog.Info("[DRY RUN] Would rollback transaction")
		return nil
	}
	if w.tx == nil {
		return nil
	}
	err := w.tx.Rollback()
	w.tx = nil
	return err
}

// DropAllTables drops all user tables from the configured SQLite schema.
func (w *Writer) DropAllTables() error {
	slog.Info("WARNING: This will drop ALL tables in the SQLite database")

	if w.dryRun {
		for _, table := range []string{"<dry-run stub>"} {
			slog.Info("[DRY RUN] Would drop table", "tableName", table)
		}
		return nil
	}

	ctx := context.Background()
	tables, err := w.listTables(ctx)
	if err != nil {
		return err
	}

	if _, err := w.db.ExecContext(ctx, "PRAGMA foreign_keys = OFF"); err != nil {
		return fmt.Errorf("sqlite: disable foreign keys: %w", err)
	}
	defer func() {
		if _, err := w.db.ExecContext(ctx, "PRAGMA foreign_keys = ON"); err != nil {
			slog.Warn("failed to restore SQLite foreign_keys pragma", "error", err)
		}
	}()

	if err := w.BeginTransaction(); err != nil {
		return fmt.Errorf("sqlite: begin drop transaction: %w", err)
	}
	defer func() {
		if w.tx != nil {
			_ = w.RollbackTransaction()
		}
	}()

	for _, table := range tables {
		slog.Info("Dropping table", "tableName", table)
		if err := w.ExecuteSQL(ctx, "DROP TABLE IF EXISTS "+quoteIdent(table)); err != nil {
			return fmt.Errorf("sqlite: drop table %s: %w", table, err)
		}
	}
	if err := w.CommitTransaction(); err != nil {
		return fmt.Errorf("sqlite: commit drop transaction: %w", err)
	}

	slog.Info("Successfully dropped tables", "count", len(tables))
	return nil
}

func (w *Writer) listTables(ctx context.Context) ([]string, error) {
	rows, err := w.db.QueryContext(ctx, `
		SELECT name
		FROM sqlite_schema
		WHERE type = 'table'
		  AND name NOT LIKE 'sqlite_%'
		  AND name <> 'schema_migrations'
		ORDER BY name
	`)
	if err != nil {
		return nil, fmt.Errorf("sqlite: list tables: %w", err)
	}
	defer rows.Close()

	var tables []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, fmt.Errorf("sqlite: scan table name: %w", err)
		}
		tables = append(tables, name)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("sqlite: iterate tables: %w", err)
	}
	return tables, nil
}

// SetDryRun toggles dry-run mode.
func (w *Writer) SetDryRun(dryRun bool) { w.dryRun = dryRun }

// IsDryRun reports whether dry-run mode is active.
func (w *Writer) IsDryRun() bool { return w.dryRun }

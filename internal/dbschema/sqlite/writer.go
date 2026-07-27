package sqlite

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

func quoteIdent(name string) string {
	return `"` + strings.ReplaceAll(name, `"`, `""`) + `"`
}

// Writer applies schema changes to a SQLite database.
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

// NewSQLiteWriter creates a SQLite schema writer.
func NewSQLiteWriter(db *sql.DB, schema string) *Writer {
	if schema == "" {
		schema = "main"
	}
	return &Writer{db: db, schema: schema}
}

// ExecuteSQL executes a standalone SQL statement.
func (w *Writer) ExecuteSQL(ctx context.Context, sqlExpr string, args ...any) error {
	if w.dryRun {
		slog.Info("[DRY RUN] Would execute SQL", "sql", sqlExpr, "args", args)
		return nil
	}
	if w.db == nil {
		return fmt.Errorf("no database connection")
	}
	if _, err := w.db.ExecContext(ctx, sqlExpr, args...); err != nil {
		return fmt.Errorf("sqlite: SQL execution failed: %w\nSQL: %s", err, sqlExpr)
	}
	return nil
}

// BeginTransaction starts a transaction and returns a transaction-scoped writer.
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
	if _, err := w.tx.ExecContext(ctx, sqlExpr, args...); err != nil {
		return fmt.Errorf("sqlite: SQL execution failed: %w\nSQL: %s", err, sqlExpr)
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

// IsDryRun reports whether dry-run mode is active.
func (w *transactionWriter) IsDryRun() bool { return w.dryRun }

// DropAllTables drops all user tables from the configured SQLite schema.
func (w *Writer) DropAllTables(ctx context.Context) (resultErr error) {
	if w.dryRun {
		return nil
	}
	if w.db == nil {
		return fmt.Errorf("no database connection")
	}

	conn, err := w.db.Conn(ctx)
	if err != nil {
		return fmt.Errorf("sqlite: acquire cleanup connection: %w", err)
	}
	defer func() {
		closeErr := conn.Close()
		if closeErr != nil && !errors.Is(closeErr, sql.ErrConnDone) {
			resultErr = errors.Join(resultErr, fmt.Errorf("sqlite: close cleanup connection: %w", closeErr))
		}
	}()

	var tx *sql.Tx
	committed := false

	if _, err := conn.ExecContext(ctx, "PRAGMA foreign_keys = OFF"); err != nil {
		return fmt.Errorf("sqlite: disable foreign keys: %w", err)
	}
	defer func() {
		if tx != nil && !committed {
			rollbackErr := tx.Rollback()
			if rollbackErr != nil && !errors.Is(rollbackErr, sql.ErrTxDone) {
				resultErr = errors.Join(resultErr, fmt.Errorf("sqlite: roll back drop transaction: %w", rollbackErr))
			}
		}

		cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), sessionRestoreTimeout)
		defer cancel()
		if _, restoreErr := conn.ExecContext(cleanupCtx, "PRAGMA foreign_keys = ON"); restoreErr != nil {
			discardConn(conn)
			resultErr = errors.Join(resultErr, fmt.Errorf("sqlite: restore foreign keys: %w", restoreErr))
		}
	}()

	tables, err := w.listTables(ctx, conn)
	if err != nil {
		return err
	}

	tx, err = conn.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("sqlite: begin drop transaction: %w", err)
	}
	txWriter := &transactionWriter{tx: tx, schema: w.schema}

	for _, table := range tables {
		if err := txWriter.ExecuteSQL(ctx, "DROP TABLE IF EXISTS "+quoteIdent(table)); err != nil {
			return fmt.Errorf("sqlite: drop table %s: %w", table, err)
		}
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("sqlite: commit drop transaction: %w", err)
	}
	committed = true

	return nil
}

func (w *Writer) listTables(ctx context.Context, conn *sql.Conn) ([]string, error) {
	rows, err := conn.QueryContext(ctx, `
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

func discardConn(conn *sql.Conn) {
	// Never return a connection with unknown session state to the pool.
	_ = conn.Raw(func(any) error {
		return driver.ErrBadConn
	})
}

// SetDryRun toggles dry-run mode.
func (w *Writer) SetDryRun(dryRun bool) { w.dryRun = dryRun }

// IsDryRun reports whether dry-run mode is active.
func (w *Writer) IsDryRun() bool { return w.dryRun }

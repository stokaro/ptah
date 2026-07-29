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

	"github.com/stokaro/ptah/core/platform"
	"github.com/stokaro/ptah/dbschema/types"
	"github.com/stokaro/ptah/internal/sqlident"
)

const sessionRestoreTimeout = 5 * time.Second

func quoteQualifiedIdent(schema, name string) string {
	return sqlident.Qualified(platform.SQLite, schema, name)
}

// Writer applies schema changes to a SQLite database.
type Writer struct {
	db     *sql.DB
	conn   *sql.Conn
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
	return &Writer{db: db, schema: normalizeSchema(schema)}
}

// NewSQLiteWriterForConnection creates a writer pinned to an existing SQLite
// connection. Use it for connection-local state such as attached databases.
// The caller retains ownership of conn.
func NewSQLiteWriterForConnection(conn *sql.Conn, schema string) *Writer {
	return &Writer{conn: conn, schema: normalizeSchema(schema)}
}

// ExecuteSQL executes a standalone SQL statement.
func (w *Writer) ExecuteSQL(ctx context.Context, sqlExpr string, args ...any) error {
	if w.dryRun {
		slog.Info("[DRY RUN] Would execute SQL", "sql", sqlExpr, "args", args)
		return nil
	}
	if w.db == nil && w.conn == nil {
		return fmt.Errorf("no database connection")
	}
	if _, err := w.execContext(ctx, sqlExpr, args...); err != nil {
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
	if w.db == nil && w.conn == nil {
		return nil, fmt.Errorf("no database connection")
	}
	tx, err := w.beginTx(ctx)
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

type cleanupObject struct {
	Name string
	Type string
}

// DropAllTables drops all user tables and views from the configured SQLite schema.
func (w *Writer) DropAllTables(ctx context.Context) (resultErr error) {
	if w.dryRun {
		return nil
	}
	conn, release, err := w.acquireCleanupConnection(ctx)
	if err != nil {
		return fmt.Errorf("sqlite: acquire cleanup connection: %w", err)
	}
	defer func() {
		closeErr := release()
		if closeErr != nil && !errors.Is(closeErr, sql.ErrConnDone) {
			resultErr = errors.Join(resultErr, fmt.Errorf("sqlite: close cleanup connection: %w", closeErr))
		}
	}()

	var tx *sql.Tx
	committed := false

	foreignKeys, err := readForeignKeys(ctx, conn)
	if err != nil {
		return err
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
		restoreSQL := "PRAGMA foreign_keys = 0"
		if foreignKeys {
			restoreSQL = "PRAGMA foreign_keys = 1"
		}
		if _, restoreErr := conn.ExecContext(cleanupCtx, restoreSQL); restoreErr != nil {
			discardConn(conn)
			resultErr = errors.Join(resultErr, fmt.Errorf("sqlite: restore foreign keys: %w", restoreErr))
		}
	}()
	if _, err := conn.ExecContext(ctx, "PRAGMA foreign_keys = OFF"); err != nil {
		return fmt.Errorf("sqlite: disable foreign keys: %w", err)
	}

	tx, err = conn.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("sqlite: begin drop transaction: %w", err)
	}
	objects, err := w.listCleanupObjects(ctx, tx)
	if err != nil {
		return err
	}
	txWriter := &transactionWriter{tx: tx, schema: w.schema}

	schema := w.cleanupSchema()
	for _, object := range objects {
		statement := "DROP " + strings.ToUpper(object.Type) + " IF EXISTS " +
			quoteQualifiedIdent(schema, object.Name)
		if err := txWriter.ExecuteSQL(ctx, statement); err != nil {
			return fmt.Errorf("sqlite: drop %s %s: %w", object.Type, object.Name, err)
		}
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("sqlite: commit drop transaction: %w", err)
	}
	committed = true

	return nil
}

func readForeignKeys(ctx context.Context, conn *sql.Conn) (bool, error) {
	var value bool
	if err := conn.QueryRowContext(ctx, "PRAGMA foreign_keys").Scan(&value); err != nil {
		return false, fmt.Errorf("sqlite: read foreign keys setting: %w", err)
	}
	return value, nil
}

func normalizeSchema(schema string) string {
	if schema == "" {
		return "main"
	}
	return schema
}

func (w *Writer) execContext(ctx context.Context, sqlExpr string, args ...any) (sql.Result, error) {
	if w.conn != nil {
		return w.conn.ExecContext(ctx, sqlExpr, args...)
	}
	return w.db.ExecContext(ctx, sqlExpr, args...)
}

func (w *Writer) beginTx(ctx context.Context) (*sql.Tx, error) {
	if w.conn != nil {
		return w.conn.BeginTx(ctx, nil)
	}
	return w.db.BeginTx(ctx, nil)
}

func (w *Writer) acquireCleanupConnection(ctx context.Context) (*sql.Conn, func() error, error) {
	if w.conn != nil {
		return w.conn, func() error { return nil }, nil
	}
	if w.db == nil {
		return nil, nil, fmt.Errorf("no database connection")
	}
	conn, err := w.db.Conn(ctx)
	if err != nil {
		return nil, nil, err
	}
	return conn, conn.Close, nil
}

func (w *Writer) cleanupSchema() string {
	if strings.TrimSpace(w.schema) == "" {
		return "main"
	}
	return w.schema
}

func (w *Writer) listCleanupObjects(ctx context.Context, tx *sql.Tx) ([]cleanupObject, error) {
	const query = `
		SELECT name, CASE type WHEN 'view' THEN 'view' ELSE 'table' END
		FROM pragma_table_list
		WHERE schema = ?
		  AND type IN ('table', 'view', 'virtual')
		  AND name NOT LIKE 'sqlite_%'
		  AND name <> 'schema_migrations'
		ORDER BY CASE type WHEN 'view' THEN 0 ELSE 1 END, name
	`
	rows, err := tx.QueryContext(ctx, query, w.cleanupSchema())
	if err != nil {
		return nil, fmt.Errorf("sqlite: list schema objects: %w", err)
	}
	defer rows.Close()

	var objects []cleanupObject
	for rows.Next() {
		var object cleanupObject
		if err := rows.Scan(&object.Name, &object.Type); err != nil {
			return nil, fmt.Errorf("sqlite: scan schema object: %w", err)
		}
		objects = append(objects, object)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("sqlite: iterate schema objects: %w", err)
	}
	return objects, nil
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

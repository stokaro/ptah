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

	"go.5x5.cz/ptah/core/platform"
	"go.5x5.cz/ptah/dbschema/types"
	"go.5x5.cz/ptah/internal/sqlident"
	"go.5x5.cz/ptah/internal/sqlrunner"
)

const sessionRestoreTimeout = 5 * time.Second

func quoteQualifiedIdent(schema, name string) string {
	return sqlident.Qualified(platform.SQLite, schema, name)
}

// Writer applies schema changes to a SQLite database.
type Writer struct {
	db     sqlrunner.Runner
	conn   *sql.Conn
	schema string
	dryRun bool
}

type transactionWriter struct {
	mu     sync.Mutex
	tx     *sql.Tx
	schema string
	dryRun bool
	// session is the connection-level state this transaction borrowed, or nil
	// when it borrowed none. It is verified before the commit and given back
	// after the transaction ends, whichever way it ends.
	session *foreignKeySession
}

// NewSQLiteWriter creates a SQLite schema writer.
func NewSQLiteWriter(db *sql.DB, schema string) *Writer {
	if db == nil {
		return NewSQLiteWriterForRunner(nil, schema)
	}
	return NewSQLiteWriterForRunner(db, schema)
}

// NewSQLiteWriterForRunner creates a writer bound to a pool-backed runner.
func NewSQLiteWriterForRunner(runner sqlrunner.Runner, schema string) *Writer {
	return &Writer{db: runner, schema: normalizeSchema(schema)}
}

// NewSQLiteWriterForConnection creates a writer pinned to an existing SQLite
// connection. Use it for connection-local state such as attached databases.
// The caller retains ownership of conn.
func NewSQLiteWriterForConnection(conn *sql.Conn, schema string) *Writer {
	return NewSQLiteWriterForPinnedRunner(
		sqlrunner.NewConn(context.Background(), conn),
		conn,
		schema,
	)
}

// NewSQLiteWriterForPinnedRunner creates a writer bound to runner while
// retaining the underlying session for SQLite connection-local cleanup.
func NewSQLiteWriterForPinnedRunner(
	runner sqlrunner.Runner,
	conn *sql.Conn,
	schema string,
) *Writer {
	return &Writer{
		db:     runner,
		conn:   conn,
		schema: normalizeSchema(schema),
	}
}

// ExecuteSQL executes a standalone SQL statement.
func (w *Writer) ExecuteSQL(ctx context.Context, sqlExpr string, args ...any) error {
	_, err := w.ExecContext(ctx, sqlExpr, args...)
	if err != nil {
		return fmt.Errorf("sqlite: SQL execution failed: %w\nSQL: %s", err, sqlExpr)
	}
	return nil
}

// ExecContext executes a standalone SQL statement and returns its driver
// result. DatabaseConnection uses this method when a connection-bound writer
// is installed as its active executor.
func (w *Writer) ExecContext(ctx context.Context, sqlExpr string, args ...any) (sql.Result, error) {
	if w.dryRun {
		slog.Info("[DRY RUN] Would execute SQL", "sql", sqlExpr, "args", args)
		return nil, nil
	}
	if w.db == nil && w.conn == nil {
		return nil, fmt.Errorf("no database connection")
	}
	return w.execContext(ctx, sqlExpr, args...)
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
	if err := w.session.verify(w.tx); err != nil {
		// The rebuild orphaned a row. Rolling back here rather than returning
		// the error alone is what keeps the refusal meaningful: a caller that
		// only logs a failed commit would otherwise leave the transaction open.
		rollbackErr := w.tx.Rollback()
		w.tx = nil
		w.session.restore()
		if rollbackErr != nil && !errors.Is(rollbackErr, sql.ErrTxDone) {
			return errors.Join(err, fmt.Errorf("sqlite: roll back rebuild transaction: %w", rollbackErr))
		}
		return err
	}
	err := w.tx.Commit()
	w.tx = nil
	w.session.restore()
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
	w.session.restore()
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
	return w.dropAllTables(ctx, false)
}

// DropDatabaseRealm removes every user object from a pinned SQLite main
// database. Replay callers use this stronger contract so revision state cannot
// leak between runs.
func (w *Writer) DropDatabaseRealm(ctx context.Context) error {
	if w.conn == nil {
		return fmt.Errorf("sqlite: database-realm cleanup requires a pinned connection")
	}
	if !strings.EqualFold(w.cleanupSchema(), "main") {
		return fmt.Errorf(
			"sqlite: database-realm cleanup requires schema %q, got %q",
			"main",
			w.cleanupSchema(),
		)
	}
	if err := inspectDatabaseRealm(ctx, w.conn); err != nil {
		return err
	}
	if err := w.dropAllTables(ctx, true); err != nil {
		return err
	}
	return verifyDatabaseRealmEmpty(ctx, w.conn)
}

func (w *Writer) dropAllTables(ctx context.Context, includeRevisionTable bool) (resultErr error) {
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

	if err := refuseCleanupWithTempViews(ctx, conn, w.cleanupSchema()); err != nil {
		return err
	}

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
	objects, err := w.listCleanupObjects(ctx, tx, includeRevisionTable)
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

func inspectDatabaseRealm(ctx context.Context, conn *sql.Conn) error {
	attachments, err := listAuxiliaryDatabases(ctx, conn)
	if err != nil {
		return err
	}
	if len(attachments) > 0 {
		return fmt.Errorf(
			"sqlite: refusing database-realm cleanup with attached databases: %s",
			strings.Join(attachments, ", "),
		)
	}
	tempObjects, err := listSchemaObjects(ctx, conn, "temp")
	if err != nil {
		return err
	}
	if len(tempObjects) > 0 {
		return fmt.Errorf(
			"sqlite: refusing database-realm cleanup with TEMP objects: %s",
			strings.Join(tempObjects, ", "),
		)
	}
	return nil
}

func verifyDatabaseRealmEmpty(ctx context.Context, conn *sql.Conn) error {
	if err := inspectDatabaseRealm(ctx, conn); err != nil {
		return err
	}
	objects, err := listSchemaObjects(ctx, conn, "main")
	if err != nil {
		return err
	}
	if len(objects) > 0 {
		return fmt.Errorf(
			"sqlite: database-realm cleanup left user objects: %s",
			strings.Join(objects, ", "),
		)
	}
	return nil
}

func listAuxiliaryDatabases(ctx context.Context, conn *sql.Conn) ([]string, error) {
	rows, err := conn.QueryContext(ctx, "PRAGMA database_list")
	if err != nil {
		return nil, fmt.Errorf("sqlite: inspect attached databases: %w", err)
	}
	defer rows.Close()

	var names []string
	for rows.Next() {
		var sequence int
		var name, filename string
		if err := rows.Scan(&sequence, &name, &filename); err != nil {
			return nil, fmt.Errorf("sqlite: scan attached database: %w", err)
		}
		if !strings.EqualFold(name, "main") && !strings.EqualFold(name, "temp") {
			names = append(names, name)
		}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("sqlite: iterate attached databases: %w", err)
	}
	return names, nil
}

func listSchemaObjects(ctx context.Context, conn *sql.Conn, schema string) ([]string, error) {
	// #nosec G202 -- schema is an identifier quoted by sqlident, not an SQL value.
	query := "SELECT type || ':' || name FROM " + quoteQualifiedIdent(schema, "sqlite_schema") + `
		-- Escaped: LIKE reads a bare _ as a single-character wildcard, so
		-- 'sqlite_%' also matches user objects such as sqlitedata. SQLite
		-- reserves the prefix WITH the underscore (stokaro/ptah#1291).
		WHERE name NOT LIKE 'sqlite\_%' ESCAPE '\'
		ORDER BY type, name
	`
	rows, err := conn.QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("sqlite: inspect %s schema objects: %w", schema, err)
	}
	defer rows.Close()

	var objects []string
	for rows.Next() {
		var object string
		if err := rows.Scan(&object); err != nil {
			return nil, fmt.Errorf("sqlite: scan %s schema object: %w", schema, err)
		}
		objects = append(objects, object)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("sqlite: iterate %s schema objects: %w", schema, err)
	}
	return objects, nil
}

func refuseCleanupWithTempViews(ctx context.Context, conn *sql.Conn, schema string) error {
	var exists bool
	const query = `SELECT EXISTS (SELECT 1 FROM temp.sqlite_schema WHERE type = 'view')`
	if err := conn.QueryRowContext(ctx, query).Scan(&exists); err != nil {
		return fmt.Errorf("sqlite: inspect TEMP views: %w", err)
	}
	if exists {
		return fmt.Errorf(
			"sqlite: refusing to clean schema %q: TEMP views exist on the cleanup connection and may depend on it",
			schema,
		)
	}
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
	connector, ok := w.db.(sqlrunner.Connector)
	if !ok {
		return nil, nil, fmt.Errorf("database runner cannot acquire a cleanup connection")
	}
	conn, err := connector.Conn(ctx)
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

func (w *Writer) listCleanupObjects(
	ctx context.Context,
	tx *sql.Tx,
	includeRevisionTable bool,
) ([]cleanupObject, error) {
	const query = `
		SELECT name, CASE type WHEN 'view' THEN 'view' ELSE 'table' END
		FROM pragma_table_list
		WHERE schema = ?
		  AND type IN ('table', 'view', 'virtual')
		  AND name NOT LIKE 'sqlite\_%' ESCAPE '\'
		  AND (? OR name <> 'schema_migrations')
		ORDER BY CASE type WHEN 'view' THEN 0 ELSE 1 END, name
	`
	rows, err := tx.QueryContext(ctx, query, w.cleanupSchema(), includeRevisionTable)
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

package postgres

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"

	"github.com/stokaro/ptah/core/goschema"
	"github.com/stokaro/ptah/core/platform"
	"github.com/stokaro/ptah/dbschema/types"
	"github.com/stokaro/ptah/internal/sqlident"
)

func quoteIdent(name string) string {
	return sqlident.Quote(platform.Postgres, name)
}

// PostgreSQLWriter writes schemas to PostgreSQL databases
type PostgreSQLWriter struct {
	db     *sql.DB
	schema string
	dryRun bool
}

type postgresTransactionWriter struct {
	mu     sync.Mutex
	tx     *sql.Tx
	schema string
	dryRun bool
}

// NewPostgreSQLWriter creates a new PostgreSQL schema writer
func NewPostgreSQLWriter(db *sql.DB, schema string) *PostgreSQLWriter {
	if schema == "" {
		schema = "public"
	}
	return &PostgreSQLWriter{
		db:     db,
		schema: schema,
	}
}

// writeEnums creates all enum types
func (w *PostgreSQLWriter) writeEnums(enums []goschema.Enum) error { //nolint:unused // TODO: verify why this is not used
	for _, enum := range enums {
		// Check if enum already exists (skip in dry run mode)
		var exists bool
		if !w.dryRun {
			checkSQL := `
				SELECT EXISTS (
					SELECT 1 FROM pg_type t
					JOIN pg_namespace n ON n.oid = t.typnamespace
					WHERE t.typname = $1 AND n.nspname = $2
				)`

			err := w.db.QueryRow(checkSQL, enum.Name, w.schema).Scan(&exists)
			if err != nil {
				return fmt.Errorf("failed to check if enum %s exists: %w", enum.Name, err)
			}

			if exists {
				slog.Info("Enum already exists, skipping...", "enumName", enum.Name)
				continue
			}
		}

		// CREATE TYPE cannot use bind parameters: identifiers and enum-value
		// literals must be substituted into the SQL text directly. Route the
		// enum name through quoteIdent and escape the literal values by
		// doubling any embedded single quote, per the SQL standard.
		values := make([]string, len(enum.Values))
		for i, v := range enum.Values {
			values[i] = "'" + strings.ReplaceAll(v, "'", "''") + "'"
		}

		createEnumSQL := "CREATE TYPE " + quoteIdent(enum.Name) +
			" AS ENUM (" + strings.Join(values, ", ") + ")"

		slog.Info("Creating enum...", "enumName", enum.Name)
		if err := w.ExecuteSQL(context.Background(), createEnumSQL); err != nil {
			return fmt.Errorf("failed to create enum %s: %w", enum.Name, err)
		}
	}
	return nil
}

// ExecuteSQL executes a standalone SQL statement. Values
// must be passed via args and referenced through `$N` placeholders; the SQL
// string itself should never be assembled with fmt.Sprintf for value
// interpolation. Identifiers (table/column names) cannot be parameterized
// and must be escaped via quoteIdent before being substituted in.
func (w *PostgreSQLWriter) ExecuteSQL(ctx context.Context, sqlExpr string, args ...any) error {
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
func (w *PostgreSQLWriter) BeginTransaction(ctx context.Context) (types.SchemaTransaction, error) {
	if w.dryRun {
		slog.Info("[DRY RUN] Would begin transaction")
		return &postgresTransactionWriter{schema: w.schema, dryRun: true}, nil
	}
	if w.db == nil {
		return nil, fmt.Errorf("no database connection")
	}

	tx, err := w.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	return &postgresTransactionWriter{tx: tx, schema: w.schema}, nil
}

// ExecuteSQL executes SQL against the transaction.
func (w *postgresTransactionWriter) ExecuteSQL(ctx context.Context, sqlExpr string, args ...any) error {
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
func (w *postgresTransactionWriter) Commit() error {
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
func (w *postgresTransactionWriter) Rollback() error {
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
func (w *postgresTransactionWriter) IsDryRun() bool { return w.dryRun }

type postgresCleanupObject struct {
	Kind      string
	Name      string
	Statement string
}

func (w *PostgreSQLWriter) rejectSchemaScopedExtensions(ctx context.Context, tx *sql.Tx) error {
	// DROP EXTENSION removes every member regardless of the member's schema.
	// Refuse it here because a schema-scoped cleanup cannot safely own that
	// database-wide operation, even when DROP EXTENSION uses RESTRICT.
	var count int
	var first string
	err := tx.QueryRowContext(ctx, `
		SELECT COUNT(*), COALESCE(MIN(e.extname), '')
		FROM pg_extension e
		JOIN pg_namespace n ON n.oid = e.extnamespace
		WHERE n.nspname = $1
	`, w.schema).Scan(&count, &first)
	if err != nil {
		return fmt.Errorf("failed to inspect schema-owned extensions: %w", err)
	}
	if count > 0 {
		return fmt.Errorf(
			"refusing to clean schema %q: extension %q is owned by it; "+
				"schema-scoped cleanup cannot prove that every extension member is confined to the schema",
			w.schema,
			first,
		)
	}
	return nil
}

func (w *PostgreSQLWriter) collectAllObjects(
	ctx context.Context,
	tx *sql.Tx,
) ([]postgresCleanupObject, error) {
	rows, err := tx.QueryContext(ctx, `
		WITH cleanup_objects AS (
			SELECT
				0 AS priority,
				'constraint'::text AS object_kind,
				con.conname AS object_name,
				format(
					'ALTER TABLE %I.%I DROP CONSTRAINT IF EXISTS %I RESTRICT',
					n.nspname,
					c.relname,
					con.conname
				) AS drop_statement
			FROM pg_constraint con
			JOIN pg_class c ON c.oid = con.conrelid
			JOIN pg_namespace n ON n.oid = c.relnamespace
			WHERE n.nspname = $1
			  AND con.contype = 'f'

			UNION ALL

			SELECT
				CASE c.relkind
					WHEN 'v' THEN 10
					WHEN 'm' THEN 10
					WHEN 'S' THEN 30
					ELSE 20
				END,
				CASE c.relkind
					WHEN 'v' THEN 'view'
					WHEN 'm' THEN 'materialized view'
					WHEN 'f' THEN 'foreign table'
					WHEN 'S' THEN 'sequence'
					ELSE 'table'
				END,
				c.relname,
				format(
					CASE c.relkind
						WHEN 'v' THEN 'DROP VIEW IF EXISTS %I.%I RESTRICT'
						WHEN 'm' THEN 'DROP MATERIALIZED VIEW IF EXISTS %I.%I RESTRICT'
						WHEN 'f' THEN 'DROP FOREIGN TABLE IF EXISTS %I.%I RESTRICT'
						WHEN 'S' THEN 'DROP SEQUENCE IF EXISTS %I.%I RESTRICT'
						ELSE 'DROP TABLE IF EXISTS %I.%I RESTRICT'
					END,
					n.nspname,
					c.relname
				)
			FROM pg_class c
			JOIN pg_namespace n ON n.oid = c.relnamespace
			WHERE n.nspname = $1
			  AND c.relkind IN ('r', 'p', 'v', 'm', 'f', 'S')

			UNION ALL

			SELECT
				40,
				CASE p.prokind
					WHEN 'p' THEN 'procedure'
					WHEN 'a' THEN 'aggregate'
					ELSE 'function'
				END,
				p.proname,
				format(
					'DROP %s IF EXISTS %I.%I(%s) RESTRICT',
					CASE p.prokind
						WHEN 'p' THEN 'PROCEDURE'
						WHEN 'a' THEN 'AGGREGATE'
						ELSE 'FUNCTION'
					END,
					n.nspname,
					p.proname,
					pg_get_function_identity_arguments(p.oid)
				)
			FROM pg_proc p
			JOIN pg_namespace n ON n.oid = p.pronamespace
			WHERE n.nspname = $1
			  AND p.prokind IN ('f', 'p', 'a', 'w')
			  AND NOT EXISTS (
				SELECT 1
				FROM pg_depend d
				WHERE d.classid = 'pg_proc'::regclass
				  AND d.objid = p.oid
				  AND d.deptype = 'i'
			  )

			UNION ALL

			SELECT
				50,
				'type',
				t.typname,
				format('DROP TYPE IF EXISTS %I.%I RESTRICT', n.nspname, t.typname)
			FROM pg_type t
			JOIN pg_namespace n ON n.oid = t.typnamespace
			LEFT JOIN pg_class c ON c.oid = t.typrelid
			WHERE n.nspname = $1
				  AND (
				t.typtype IN ('e', 'd', 'r')
				OR (t.typtype = 'c' AND c.relkind = 'c')
			  )
		)
		SELECT object_kind, object_name, drop_statement
		FROM cleanup_objects
		ORDER BY priority, object_kind, object_name
	`, w.schema)
	if err != nil {
		return nil, fmt.Errorf("failed to query schema objects: %w", err)
	}
	defer rows.Close()

	var objects []postgresCleanupObject
	for rows.Next() {
		var object postgresCleanupObject
		if err := rows.Scan(&object.Kind, &object.Name, &object.Statement); err != nil {
			return nil, fmt.Errorf("failed to scan schema object: %w", err)
		}
		objects = append(objects, object)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("failed to iterate schema objects: %w", err)
	}
	return objects, nil
}

func tryDropCleanupObject(
	ctx context.Context,
	tx *sql.Tx,
	object postgresCleanupObject,
) (dropErr error, controlErr error) {
	const savepoint = "ptah_cleanup_object"

	if _, err := tx.ExecContext(ctx, "SAVEPOINT "+savepoint); err != nil {
		return nil, fmt.Errorf("failed to create cleanup savepoint: %w", err)
	}

	if _, err := tx.ExecContext(ctx, object.Statement); err != nil {
		dropErr = fmt.Errorf("SQL execution failed: %w\nSQL: %s", err, object.Statement)
		if _, rollbackErr := tx.ExecContext(ctx, "ROLLBACK TO SAVEPOINT "+savepoint); rollbackErr != nil {
			controlErr = fmt.Errorf("failed to roll back cleanup savepoint: %w", rollbackErr)
		}
		if _, releaseErr := tx.ExecContext(ctx, "RELEASE SAVEPOINT "+savepoint); releaseErr != nil {
			controlErr = errors.Join(controlErr, fmt.Errorf("failed to release cleanup savepoint: %w", releaseErr))
		}
		return dropErr, controlErr
	}

	if _, err := tx.ExecContext(ctx, "RELEASE SAVEPOINT "+savepoint); err != nil {
		return nil, fmt.Errorf("failed to release cleanup savepoint: %w", err)
	}
	return nil, nil
}

func dropCleanupObjects(
	ctx context.Context,
	tx *sql.Tx,
	objects []postgresCleanupObject,
) error {
	// RESTRICT remains the final authority for every drop. Savepoints let a
	// later internal dependent disappear before its dependency is retried,
	// while an external or unknown dependency eventually stops all progress
	// and causes the outer transaction to roll back.
	pending := objects
	for len(pending) > 0 {
		remaining := make([]postgresCleanupObject, 0, len(pending))
		var firstDropErr error

		for _, object := range pending {
			dropErr, controlErr := tryDropCleanupObject(ctx, tx, object)
			if controlErr != nil {
				return fmt.Errorf(
					"failed to drop %s %s: %w",
					object.Kind,
					object.Name,
					errors.Join(dropErr, controlErr),
				)
			}
			if dropErr != nil {
				if firstDropErr == nil {
					firstDropErr = fmt.Errorf("failed to drop %s %s: %w", object.Kind, object.Name, dropErr)
				}
				remaining = append(remaining, object)
			}
		}

		if len(remaining) == 0 {
			return nil
		}
		if len(remaining) == len(pending) {
			return firstDropErr
		}
		pending = remaining
	}
	return nil
}

// DropAllTables drops all user objects in the configured database schema.
func (w *PostgreSQLWriter) DropAllTables(ctx context.Context) (resultErr error) {
	if w.dryRun {
		return nil
	}
	if w.db == nil {
		return fmt.Errorf("no database connection")
	}

	sqlTx, err := w.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	committed := false
	defer func() {
		if !committed {
			rollbackErr := sqlTx.Rollback()
			if rollbackErr != nil && !errors.Is(rollbackErr, sql.ErrTxDone) {
				resultErr = errors.Join(resultErr, fmt.Errorf("failed to roll back transaction: %w", rollbackErr))
			}
		}
	}()

	if err := w.rejectSchemaScopedExtensions(ctx, sqlTx); err != nil {
		return err
	}
	objects, err := w.collectAllObjects(ctx, sqlTx)
	if err != nil {
		return err
	}

	if err := dropCleanupObjects(ctx, sqlTx, objects); err != nil {
		return fmt.Errorf("refusing to clean schema %q: %w", w.schema, err)
	}

	if err := sqlTx.Commit(); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}
	committed = true

	return nil
}

// SetDryRun enables or disables dry run mode
func (w *PostgreSQLWriter) SetDryRun(dryRun bool) {
	w.dryRun = dryRun
}

// IsDryRun returns whether dry run mode is enabled
func (w *PostgreSQLWriter) IsDryRun() bool {
	return w.dryRun
}

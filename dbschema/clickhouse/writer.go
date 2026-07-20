package clickhouse

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"strings"

	"github.com/stokaro/ptah/dbschema/types"
)

// quoteIdent returns a safely-backtick-quoted ClickHouse identifier.
// Embedded backticks are doubled so that values coming from system.tables
// (or any other untrusted-shaped string) cannot terminate the quoted
// identifier and inject DDL.
func quoteIdent(name string) string {
	return "`" + strings.ReplaceAll(name, "`", "``") + "`"
}

// Writer applies schema changes to a ClickHouse server.
//
// ClickHouse does not provide cross-statement transactions in the way the
// other dialects Ptah supports do (experimental transactions exist only
// against MergeTree-family tables and require explicit opt-in), so the
// Begin/Commit/Rollback methods are deliberate no-ops. ExecuteSQL still
// records dry-run output unchanged, so callers using DRY_RUN see the same
// shape of trace they get from the other dialects.
type Writer struct {
	db     *sql.DB
	schema string
	dryRun bool
}

type transactionWriter struct {
	writer *Writer
}

// NewClickHouseWriter constructs a Writer.
func NewClickHouseWriter(db *sql.DB, schema string) *Writer {
	return &Writer{db: db, schema: schema}
}

// ExecuteSQL executes a SQL statement against the ClickHouse server. Values
// must be passed via args and referenced through `?` placeholders;
// clickhouse-go/v2 binds them as native driver parameters. The SQL string
// itself should never be assembled with fmt.Sprintf for value interpolation.
// Identifiers (table/column names) cannot be parameterized and must be
// escaped via quoteIdent before being substituted in.
//
// Unlike the PostgreSQL / MySQL writers, ClickHouse runs each statement
// standalone (see the package doc for why transactions are no-ops here),
// so ExecuteSQL goes straight to db.ExecContext rather than to a held
// *sql.Tx.
func (w *Writer) ExecuteSQL(ctx context.Context, sqlExpr string, args ...any) error {
	if w.dryRun {
		slog.Info("[DRY RUN] Would execute SQL", "sql", sqlExpr, "args", args)
		return nil
	}
	if w.db == nil {
		return fmt.Errorf("no database connection")
	}
	if _, err := w.db.ExecContext(ctx, sqlExpr, args...); err != nil {
		return fmt.Errorf("clickhouse: SQL execution failed: %w\nSQL: %s", err, sqlExpr)
	}
	return nil
}

// BeginTransaction is a no-op for ClickHouse. Multi-statement transactions
// are experimental and require explicit opt-in per session; the migration
// engine has no protection model that depends on them, so this is left
// as a no-op rather than silently enabling experimental flags.
func (w *Writer) BeginTransaction(_ context.Context) (types.SchemaTransaction, error) {
	if w.dryRun {
		slog.Info("[DRY RUN] Would begin transaction (no-op on ClickHouse)")
	}
	return &transactionWriter{writer: w}, nil
}

// ExecuteSQL executes SQL through the underlying ClickHouse writer.
func (w *transactionWriter) ExecuteSQL(ctx context.Context, sqlExpr string, args ...any) error {
	return w.writer.ExecuteSQL(ctx, sqlExpr, args...)
}

// Commit is a no-op for ClickHouse — see BeginTransaction.
func (w *transactionWriter) Commit() error {
	if w.writer.dryRun {
		slog.Info("[DRY RUN] Would commit transaction (no-op on ClickHouse)")
	}
	return nil
}

// Rollback is a no-op for ClickHouse — see BeginTransaction.
func (w *transactionWriter) Rollback() error {
	if w.writer.dryRun {
		slog.Info("[DRY RUN] Would rollback transaction (no-op on ClickHouse)")
	}
	return nil
}

// IsDryRun reports whether dry-run mode is active.
func (w *transactionWriter) IsDryRun() bool { return w.writer.IsDryRun() }

// DropAllTables drops every base table in the configured database.
// Uses DROP TABLE … SYNC so subsequent CREATE TABLE statements don't race
// against the async drop.
//
// Identifiers cannot be bound as parameters; quoteIdent doubles any
// embedded backtick so a name harvested from system.tables cannot break
// out of the quoted identifier. The explicit "contains backtick" rejection
// below is defence-in-depth — in a real ClickHouse deployment system.tables
// will not contain such names, but rejecting them outright keeps parity
// with the postgres/mysql writers and makes the safety property obvious.
func (w *Writer) DropAllTables() error {
	slog.Info("WARNING: This will drop ALL tables in the ClickHouse database")

	// DropAllTables matches the dialect-agnostic SchemaWriter signature
	// (func() error), so we use a background context for the listing query
	// and per-table drops — same pattern mysql uses internally.
	ctx := context.Background()

	var tables []string
	if w.dryRun {
		tables = []string{"<dry-run stub>"}
		slog.Info("[DRY RUN] DropAllTables using stub table list", "tables", tables)
	} else {
		rows, err := w.db.QueryContext(ctx, `
			SELECT name FROM system.tables
			WHERE database = currentDatabase()
			  AND is_temporary = 0
			  AND (
			    engine LIKE '%MergeTree'
			    OR engine = 'Memory'
			    OR engine = 'Log'
			    OR engine = 'TinyLog'
			    OR engine = 'StripeLog'
			  )
			  AND engine NOT LIKE '%View'
			ORDER BY name
		`)
		if err != nil {
			return fmt.Errorf("clickhouse: list tables: %w", err)
		}
		defer rows.Close()
		for rows.Next() {
			var name string
			if err := rows.Scan(&name); err != nil {
				return fmt.Errorf("clickhouse: scan table name: %w", err)
			}
			tables = append(tables, name)
		}
		if err := rows.Err(); err != nil {
			return fmt.Errorf("clickhouse: iterate tables: %w", err)
		}
	}

	for _, name := range tables {
		if w.dryRun {
			slog.Info("[DRY RUN] Would drop table", "tableName", name)
			continue
		}
		if strings.Contains(name, "`") {
			return fmt.Errorf("clickhouse: refusing to drop table %q: name contains a backtick", name)
		}
		slog.Info("Dropping table", "tableName", name)
		if err := w.ExecuteSQL(ctx, "DROP TABLE IF EXISTS "+quoteIdent(name)+" SYNC"); err != nil {
			return err
		}
	}

	slog.Info("Successfully dropped tables", "count", len(tables))
	return nil
}

// SetDryRun toggles dry-run mode.
func (w *Writer) SetDryRun(dryRun bool) { w.dryRun = dryRun }

// IsDryRun reports whether dry-run mode is active.
func (w *Writer) IsDryRun() bool { return w.dryRun }

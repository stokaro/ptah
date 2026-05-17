package clickhouse

import (
	"database/sql"
	"fmt"
	"log/slog"
)

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

// NewClickHouseWriter constructs a Writer.
func NewClickHouseWriter(db *sql.DB, schema string) *Writer {
	return &Writer{db: db, schema: schema}
}

// ExecuteSQL runs a single SQL statement, or logs it under dry-run.
func (w *Writer) ExecuteSQL(stmt string) error {
	if w.dryRun {
		slog.Info("[DRY RUN] Would execute SQL", "sql", stmt)
		return nil
	}
	if _, err := w.db.Exec(stmt); err != nil {
		return fmt.Errorf("clickhouse: SQL execution failed: %w\nSQL: %s", err, stmt)
	}
	return nil
}

// BeginTransaction is a no-op for ClickHouse. Multi-statement transactions
// are experimental and require explicit opt-in per session; the migration
// engine has no protection model that depends on them, so this is left
// as a no-op rather than silently enabling experimental flags.
func (w *Writer) BeginTransaction() error {
	if w.dryRun {
		slog.Info("[DRY RUN] Would begin transaction (no-op on ClickHouse)")
	}
	return nil
}

// CommitTransaction is a no-op for ClickHouse — see BeginTransaction.
func (w *Writer) CommitTransaction() error {
	if w.dryRun {
		slog.Info("[DRY RUN] Would commit transaction (no-op on ClickHouse)")
	}
	return nil
}

// RollbackTransaction is a no-op for ClickHouse — see BeginTransaction.
func (w *Writer) RollbackTransaction() error {
	if w.dryRun {
		slog.Info("[DRY RUN] Would rollback transaction (no-op on ClickHouse)")
	}
	return nil
}

// DropAllTables drops every base table in the configured database.
// Uses DROP TABLE … SYNC so subsequent CREATE TABLE statements don't race
// against the async drop.
func (w *Writer) DropAllTables() error {
	slog.Info("WARNING: This will drop ALL tables in the ClickHouse database")

	var tables []string
	if w.dryRun {
		tables = []string{"example_table_a", "example_table_b"}
	} else {
		rows, err := w.db.Query(`
			SELECT name FROM system.tables
			WHERE database = currentDatabase() AND engine NOT LIKE '%View'
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
		slog.Info("Dropping table", "tableName", name)
		if err := w.ExecuteSQL(fmt.Sprintf("DROP TABLE IF EXISTS %s SYNC", name)); err != nil {
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

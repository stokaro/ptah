package oracle

import (
	"context"
	"fmt"
	"log/slog"
	"strings"

	"go.5x5.cz/ptah/catalog"
	"go.5x5.cz/ptah/core/platform"
	"go.5x5.cz/ptah/internal/sqlident"
	"go.5x5.cz/ptah/internal/sqlrunner"
)

// Writer applies schema changes to an Oracle server.
//
// The transaction methods are deliberate no-ops, and unlike ClickHouse's the
// reason is not that transactions are missing: Oracle has them, and they do not
// cover DDL. Measured on 23.26.2.0.0 and 21.3.0.0.0 alike, a CREATE TABLE
// issued inside an explicit transaction is still in user_tables after the
// ROLLBACK, because Oracle commits the transaction in progress before every
// schema statement. Returning a transaction whose Commit and Rollback do
// nothing states that plainly; opening a real one would promise an atomicity
// the engine does not provide, which is what capability.TransactionalDDL reads
// false for and what internal/ddltx classifies as ImplicitCommit.
type Writer struct {
	db     sqlrunner.Runner
	schema string
	dryRun bool
}

type transactionWriter struct {
	writer *Writer
}

// NewOracleWriterForRunner constructs a writer scoped to one schema.
func NewOracleWriterForRunner(db sqlrunner.Runner, schema string) *Writer {
	return &Writer{db: db, schema: strings.ToUpper(strings.TrimSpace(schema))}
}

func (w *Writer) SetDryRun(dryRun bool) { w.dryRun = dryRun }

func (w *Writer) IsDryRun() bool { return w.dryRun }

func (w *Writer) ExecuteSQL(ctx context.Context, sqlExpr string, args ...any) error {
	if w.dryRun {
		slog.Info("[DRY RUN] Would execute SQL", "sql", sqlExpr, "args", args)
		return nil
	}
	if w.db == nil {
		return fmt.Errorf("no database connection")
	}
	if _, err := w.db.ExecContext(ctx, sqlExpr, args...); err != nil {
		return fmt.Errorf("oracle: SQL execution failed: %w\nSQL: %s", err, sqlExpr)
	}
	return nil
}

// BeginTransaction returns a transaction whose Commit and Rollback are no-ops.
// See the Writer doc for the measurement behind that.
func (w *Writer) BeginTransaction(_ context.Context) (catalog.SchemaTransaction, error) {
	if w.dryRun {
		slog.Info("[DRY RUN] Would begin transaction (a no-op for Oracle DDL)")
	}
	return &transactionWriter{writer: w}, nil
}

func (w *transactionWriter) ExecuteSQL(ctx context.Context, sqlExpr string, args ...any) error {
	return w.writer.ExecuteSQL(ctx, sqlExpr, args...)
}

func (w *transactionWriter) IsDryRun() bool { return w.writer.IsDryRun() }

// Commit is a no-op: every schema statement already committed itself.
func (w *transactionWriter) Commit() error {
	if w.writer.dryRun {
		slog.Info("[DRY RUN] Would commit transaction (a no-op for Oracle DDL)")
	}
	return nil
}

// Rollback is a no-op for the same reason. It does not report an error,
// because there is nothing wrong with the request -- the engine simply has
// nothing left to undo.
func (w *transactionWriter) Rollback() error {
	if w.writer.dryRun {
		slog.Info("[DRY RUN] Would roll back transaction (a no-op for Oracle DDL)")
	}
	return nil
}

// cleanupObjectQuery lists the objects DropAllTables removes, newest first
// within each kind.
//
// It reads USER_OBJECTS rather than ALL_OBJECTS, and that is the confinement:
// the view shows only what the connected account owns, so a cleanup cannot
// reach another schema even if the account has been granted access to one.
//
// Two exclusions are load-bearing. An index or a trigger Oracle created for a
// constraint disappears with its table, and naming it separately would answer
// ORA-02429 or drop something that is already gone. A table in the recycle bin
// -- name starting BIN$ -- is already dropped; it is listed by USER_OBJECTS
// until the bin is purged, and asking to drop it again is an error about a
// table nobody has.
const cleanupObjectQuery = `
SELECT o.object_type, o.object_name
FROM user_objects o
WHERE o.object_type IN ('TABLE', 'VIEW', 'MATERIALIZED VIEW', 'SEQUENCE', 'SYNONYM', 'TRIGGER', 'TYPE')
  AND o.object_name NOT LIKE 'BIN$%'
  AND o.generated = 'N'
ORDER BY CASE o.object_type
           WHEN 'TRIGGER' THEN 1
           WHEN 'MATERIALIZED VIEW' THEN 2
           WHEN 'VIEW' THEN 3
           WHEN 'TABLE' THEN 4
           WHEN 'SEQUENCE' THEN 5
           WHEN 'SYNONYM' THEN 6
           ELSE 7
         END,
         o.object_name`

// DropAllTables removes every object the connected account owns.
//
// Tables are dropped with CASCADE CONSTRAINTS so a cycle of foreign keys does
// not decide the order, and with PURGE so the objects do not land in the
// recycle bin -- where they would keep their storage and be listed by
// USER_OBJECTS on the next run.
func (w *Writer) DropAllTables(ctx context.Context) error {
	if w.db == nil {
		return fmt.Errorf("no database connection")
	}
	rows, err := w.db.QueryContext(ctx, cleanupObjectQuery)
	if err != nil {
		return fmt.Errorf("oracle: list objects to drop: %w", err)
	}
	type object struct{ kind, name string }
	var objects []object
	for rows.Next() {
		var found object
		if err := rows.Scan(&found.kind, &found.name); err != nil {
			rows.Close()
			return fmt.Errorf("oracle: list objects to drop: %w", err)
		}
		objects = append(objects, found)
	}
	closeErr := rows.Close()
	if err := rows.Err(); err != nil {
		return fmt.Errorf("oracle: list objects to drop: %w", err)
	}
	if closeErr != nil {
		return fmt.Errorf("oracle: list objects to drop: %w", closeErr)
	}

	for _, found := range objects {
		if err := w.ExecuteSQL(ctx, dropStatement(found.kind, found.name)); err != nil {
			return err
		}
	}
	return nil
}

func dropStatement(kind, name string) string {
	quoted := sqlident.Quote(platform.Oracle, name)
	switch kind {
	case "TABLE":
		return "DROP TABLE " + quoted + " CASCADE CONSTRAINTS PURGE"
	case "TYPE":
		return "DROP TYPE " + quoted + " FORCE"
	default:
		return "DROP " + kind + " " + quoted
	}
}

package mssql

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"strings"
	"sync"

	"github.com/stokaro/ptah/dbschema/types"
)

func quoteIdent(name string) string {
	return "[" + strings.ReplaceAll(name, "]", "]]") + "]"
}

func quoteQualified(schema, name string) string {
	if schema == "" {
		return quoteIdent(name)
	}
	return quoteIdent(schema) + "." + quoteIdent(name)
}

// Writer applies schema changes to SQL Server.
type Writer struct {
	db     *sql.DB
	schema string
	dryRun bool
}

type foreignKey struct {
	Schema           string
	Table            string
	Name             string
	ReferencedSchema string
	ReferencedTable  string
}

type transactionWriter struct {
	mu     sync.Mutex
	tx     *sql.Tx
	dryRun bool
}

func NewSQLServerWriter(db *sql.DB, schema string) *Writer {
	if schema == "" {
		schema = "dbo"
	}
	return &Writer{db: db, schema: schema}
}

func (w *Writer) ExecuteSQL(ctx context.Context, sqlExpr string, args ...any) error {
	if w.dryRun {
		slog.Info("[DRY RUN] Would execute SQL", "sql", sqlExpr, "args", args)
		return nil
	}
	if w.db == nil {
		return fmt.Errorf("no database connection")
	}
	if _, err := w.db.ExecContext(ctx, sqlExpr, args...); err != nil {
		return fmt.Errorf("sqlserver: SQL execution failed: %w\nSQL: %s", err, sqlExpr)
	}
	return nil
}

func (w *Writer) BeginTransaction(ctx context.Context) (types.SchemaTransaction, error) {
	if w.dryRun {
		slog.Info("[DRY RUN] Would begin transaction")
		return &transactionWriter{dryRun: true}, nil
	}
	if w.db == nil {
		return nil, fmt.Errorf("no database connection")
	}
	tx, err := w.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	return &transactionWriter{tx: tx}, nil
}

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
		return fmt.Errorf("sqlserver: SQL execution failed: %w\nSQL: %s", err, sqlExpr)
	}
	return nil
}

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

func (w *transactionWriter) IsDryRun() bool { return w.dryRun }

func (w *Writer) DropAllTables() error {
	slog.Info("WARNING: This will drop ALL tables in the SQL Server database")

	if w.dryRun {
		slog.Info("[DRY RUN] Would drop SQL Server user tables", "schema", w.schema)
		return nil
	}

	ctx := context.Background()
	tables, err := w.listTables(ctx)
	if err != nil {
		return err
	}
	foreignKeys, err := w.listForeignKeys(ctx)
	if err != nil {
		return err
	}
	if err := w.rejectExternalForeignKeys(foreignKeys); err != nil {
		return err
	}

	tx, err := w.BeginTransaction(ctx)
	if err != nil {
		return fmt.Errorf("sqlserver: begin drop transaction: %w", err)
	}
	committed := false
	defer func() {
		if !committed {
			_ = tx.Rollback()
		}
	}()

	for _, fk := range foreignKeys {
		qualified := quoteQualified(fk.Schema, fk.Table)
		constraint := quoteIdent(fk.Name)
		if err := tx.ExecuteSQL(ctx, "ALTER TABLE "+qualified+" DROP CONSTRAINT "+constraint); err != nil {
			return fmt.Errorf("sqlserver: drop foreign key %s on %s: %w", constraint, qualified, err)
		}
	}
	for _, table := range tables {
		qualified := quoteQualified(table.Schema, table.Name)
		slog.Info("Dropping table", "tableName", qualified)
		if err := tx.ExecuteSQL(ctx, "DROP TABLE IF EXISTS "+qualified); err != nil {
			return fmt.Errorf("sqlserver: drop table %s: %w", qualified, err)
		}
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("sqlserver: commit drop transaction: %w", err)
	}
	committed = true
	return nil
}

func (w *Writer) rejectExternalForeignKeys(foreignKeys []foreignKey) error {
	var blockers []string
	for _, fk := range foreignKeys {
		if strings.EqualFold(fk.Schema, w.schema) {
			continue
		}
		blockers = append(blockers, fmt.Sprintf(
			"%s.%s.%s references %s.%s",
			fk.Schema,
			fk.Table,
			fk.Name,
			fk.ReferencedSchema,
			fk.ReferencedTable,
		))
	}
	if len(blockers) == 0 {
		return nil
	}
	return fmt.Errorf(
		"sqlserver: cannot drop schema %s tables because external foreign keys reference them: %s",
		quoteIdent(w.schema),
		strings.Join(blockers, "; "),
	)
}

func (w *Writer) listTables(ctx context.Context) ([]types.DBTable, error) {
	rows, err := w.db.QueryContext(ctx, `
		SELECT s.name, t.name
		FROM sys.tables AS t
		JOIN sys.schemas AS s ON s.schema_id = t.schema_id
		WHERE t.is_ms_shipped = 0
		  AND s.name = @p1
		ORDER BY s.name, t.name
	`, w.schema)
	if err != nil {
		return nil, fmt.Errorf("sqlserver: list tables: %w", err)
	}
	defer rows.Close()

	var tables []types.DBTable
	for rows.Next() {
		var table types.DBTable
		if err := rows.Scan(&table.Schema, &table.Name); err != nil {
			return nil, fmt.Errorf("sqlserver: scan table name: %w", err)
		}
		tables = append(tables, table)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("sqlserver: iterate tables: %w", err)
	}
	return tables, nil
}

func (w *Writer) listForeignKeys(ctx context.Context) ([]foreignKey, error) {
	rows, err := w.db.QueryContext(ctx, `
		SELECT DISTINCT ps.name, pt.name, fk.name, rs.name, rt.name
		FROM sys.foreign_keys AS fk
		JOIN sys.tables AS pt ON pt.object_id = fk.parent_object_id
		JOIN sys.schemas AS ps ON ps.schema_id = pt.schema_id
		JOIN sys.tables AS rt ON rt.object_id = fk.referenced_object_id
		JOIN sys.schemas AS rs ON rs.schema_id = rt.schema_id
		WHERE pt.is_ms_shipped = 0
		  AND rt.is_ms_shipped = 0
		  AND (ps.name = @p1 OR rs.name = @p1)
		ORDER BY ps.name, pt.name, fk.name
	`, w.schema)
	if err != nil {
		return nil, fmt.Errorf("sqlserver: list foreign keys: %w", err)
	}
	defer rows.Close()

	var foreignKeys []foreignKey
	for rows.Next() {
		var fk foreignKey
		if err := rows.Scan(&fk.Schema, &fk.Table, &fk.Name, &fk.ReferencedSchema, &fk.ReferencedTable); err != nil {
			return nil, fmt.Errorf("sqlserver: scan foreign key: %w", err)
		}
		foreignKeys = append(foreignKeys, fk)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("sqlserver: iterate foreign keys: %w", err)
	}
	return foreignKeys, nil
}

func (w *Writer) SetDryRun(dryRun bool) { w.dryRun = dryRun }

func (w *Writer) IsDryRun() bool { return w.dryRun }

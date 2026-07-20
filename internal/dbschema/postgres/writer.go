package postgres

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"strings"
	"sync"

	"github.com/stokaro/ptah/core/goschema"
	"github.com/stokaro/ptah/dbschema/types"
)

// quoteIdent returns a safely-quoted PostgreSQL identifier. Embedded double
// quotes are doubled per the SQL standard so that values coming from
// information_schema (or any other untrusted-shaped string) cannot terminate
// the quoted identifier and inject DDL.
func quoteIdent(name string) string {
	return `"` + strings.ReplaceAll(name, `"`, `""`) + `"`
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

func (w *PostgreSQLWriter) collectAllObjects() (tables []string, enums []string, sequences []string, err error) { //revive:disable-line:function-result-limit // It's acceptable here
	if w.dryRun {
		// In dry run mode, simulate some tables/enums/sequences for demonstration
		tables = []string{"example_table1", "example_table2"}
		enums = []string{"example_enum1", "example_enum2"}
		sequences = []string{"example_table1_id_seq", "example_table2_id_seq"}
		return tables, enums, sequences, nil
	}

	// Get all tables in the schema
	tablesQuery := `
			SELECT table_name
			FROM information_schema.tables
			WHERE table_schema = $1 AND table_type = 'BASE TABLE'
			ORDER BY table_name`

	rows, err := w.db.Query(tablesQuery, w.schema)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("failed to query tables: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var tableName string
		if err := rows.Scan(&tableName); err != nil {
			return nil, nil, nil, fmt.Errorf("failed to scan table name: %w", err)
		}
		tables = append(tables, tableName)
	}

	// Get all custom types (enums) in the schema
	enumsQuery := `
			SELECT typname
			FROM pg_type t
			JOIN pg_namespace n ON t.typnamespace = n.oid
			WHERE n.nspname = $1 AND t.typtype = 'e'
			ORDER BY typname`

	enumRows, err := w.db.Query(enumsQuery, w.schema)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("failed to query enums: %w", err)
	}
	defer enumRows.Close()

	for enumRows.Next() {
		var enumName string
		if err := enumRows.Scan(&enumName); err != nil {
			return nil, nil, nil, fmt.Errorf("failed to scan enum name: %w", err)
		}
		enums = append(enums, enumName)
	}

	// Get all sequences in the schema
	sequencesQuery := `
			SELECT sequence_name
			FROM information_schema.sequences
			WHERE sequence_schema = $1
			ORDER BY sequence_name`

	seqRows, err := w.db.Query(sequencesQuery, w.schema)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("failed to query sequences: %w", err)
	}
	defer seqRows.Close()

	for seqRows.Next() {
		var sequenceName string
		if err := seqRows.Scan(&sequenceName); err != nil {
			return nil, nil, nil, fmt.Errorf("failed to scan sequence name: %w", err)
		}
		sequences = append(sequences, sequenceName)
	}

	return tables, enums, sequences, nil
}

// DropAllTables drops ALL tables and enums in the database schema (COMPLETE CLEANUP!)
func (w *PostgreSQLWriter) DropAllTables() error {
	slog.Warn("WARNING: This will drop ALL tables and enums in the database!")

	tx, err := w.BeginTransaction(context.Background())
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}

	committed := false
	defer func() {
		if !committed {
			_ = tx.Rollback()
		}
	}()

	tables, enums, sequences, err := w.collectAllObjects()
	if err != nil {
		return err
	}

	ctx := context.Background()

	// Drop all tables with CASCADE to handle dependencies. Identifiers cannot
	// be bound as parameters; quoteIdent doubles any embedded `"` so a hostile
	// name coming back from information_schema cannot break out of the quoted
	// identifier.
	for _, tableName := range tables {
		dropSQL := "DROP TABLE IF EXISTS " + quoteIdent(tableName) + " CASCADE"
		slog.Info("Dropping table...", "tableName", tableName)
		if err := tx.ExecuteSQL(ctx, dropSQL); err != nil {
			return fmt.Errorf("failed to drop table %s: %w", tableName, err)
		}
	}

	// Drop all enums
	for _, enumName := range enums {
		dropSQL := "DROP TYPE IF EXISTS " + quoteIdent(enumName) + " CASCADE"
		slog.Info("Dropping enum...", "enumName", enumName)
		if err := tx.ExecuteSQL(ctx, dropSQL); err != nil {
			return fmt.Errorf("failed to drop enum %s: %w", enumName, err)
		}
	}

	// Drop all sequences
	for _, sequenceName := range sequences {
		dropSQL := "DROP SEQUENCE IF EXISTS " + quoteIdent(sequenceName) + " CASCADE"
		slog.Info("Dropping sequence...", "sequenceName", sequenceName)
		if err := tx.ExecuteSQL(ctx, dropSQL); err != nil {
			return fmt.Errorf("failed to drop sequence %s: %w", sequenceName, err)
		}
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}
	committed = true

	slog.Info("All tables and enums dropped successfully!", "tables", len(tables), "enums", len(enums), "sequences", len(sequences))
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

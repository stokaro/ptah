package dbschema

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"ptah.run/internal/sqlident"
)

// ReadTableRows reads the current rows of table, projected onto the requested
// column set, from the database behind conn.
//
// The SELECT is built with dialect-aware, safely-escaped identifiers keyed off
// conn.Info().Dialect, so callers never hand-quote table or column names. Names
// are spelled the way the schema renderers spell them: quoted on every dialect
// but Oracle, and bare on Oracle wherever Oracle accepts a bare name, so a
// table Ptah created as ora_flags is read as ORA_FLAGS rather than as a quoted
// "ora_flags" that does not exist. When schema is non-empty the table is
// schema-qualified. columns is required and drives both the projection and the
// keys of the returned maps: each returned row is a map[string]any keyed by the
// requested column names, in the exact spelling passed in.
//
// A value from a binary column (bytea, binary, varbinary, blob, image or raw) is
// returned as []byte holding exactly the bytes the database stores. Several
// drivers also scan character, numeric, date and JSON columns as []byte; those
// values are returned as string, so text compares stably regardless of driver.
// All other values are returned as scanned.
//
// No ORDER BY is applied, so the returned row order is whatever the database
// yields and must not be relied upon. This suits set-oriented callers such as
// migration/datadiff.Compute, which key rows by their declared key columns and
// are order-independent by construction.
//
// The context governs query execution and row iteration; canceling it aborts
// the read with the context error wrapped in the returned error.
func ReadTableRows(ctx context.Context, conn *DatabaseConnection, schema, table string, columns []string) ([]map[string]any, error) {
	if conn == nil {
		return nil, errors.New("dbschema: ReadTableRows requires a non-nil connection")
	}
	if strings.TrimSpace(table) == "" {
		return nil, errors.New("dbschema: ReadTableRows requires a table name")
	}
	if len(columns) == 0 {
		return nil, errors.New("dbschema: ReadTableRows requires at least one column")
	}
	// Duplicate column names would collapse into a single map key while the
	// column-count guard below still passed, silently dropping a column, so
	// reject them up front.
	seen := make(map[string]struct{}, len(columns))
	for _, col := range columns {
		if _, dup := seen[col]; dup {
			return nil, fmt.Errorf("dbschema: ReadTableRows got duplicate column %q", col)
		}
		seen[col] = struct{}{}
	}

	dialect := conn.Info().Dialect

	quoted := make([]string, len(columns))
	for i, col := range columns {
		quoted[i] = sqlident.Ident(dialect, col)
	}
	query := "SELECT " + strings.Join(quoted, ", ") + " FROM " + sqlident.QualifiedIdent(dialect, schema, table)

	rows, err := conn.QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("dbschema: read rows of table %q: %w", table, err)
	}
	defer rows.Close()

	columnTypes, err := rows.ColumnTypes()
	if err != nil {
		return nil, fmt.Errorf("dbschema: read columns of table %q: %w", table, err)
	}
	if len(columnTypes) != len(columns) {
		return nil, fmt.Errorf("dbschema: table %q returned %d columns, want %d", table, len(columnTypes), len(columns))
	}
	binary := make([]bool, len(columnTypes))
	for i, columnType := range columnTypes {
		binary[i] = holdsBinary(columnType.DatabaseTypeName())
	}

	holders := make([]any, len(columns))
	scanTargets := make([]any, len(columns))
	for i := range holders {
		scanTargets[i] = &holders[i]
	}

	var result []map[string]any
	for rows.Next() {
		if err := rows.Scan(scanTargets...); err != nil {
			return nil, fmt.Errorf("dbschema: scan row of table %q: %w", table, err)
		}
		row := make(map[string]any, len(columns))
		for i, col := range columns {
			value := holders[i]
			// A binary value keeps its bytes. Converted to string it reaches
			// the renderer as text, and a text literal is what a server
			// refuses for a byte that is not valid UTF-8, stores as a
			// different value when a backslash reads as a bytea escape, or
			// refuses outright on SQL Server (stokaro/ptah#3297).
			if raw, isBytes := value.([]byte); isBytes && !binary[i] {
				value = string(raw)
			}
			row[col] = value
		}
		result = append(result, row)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("dbschema: iterate rows of table %q: %w", table, err)
	}

	return result, nil
}

// holdsBinary reports whether a column the driver describes as databaseType
// holds binary values.
//
// The name is what decides, because the Go type cannot. go-sql-driver/mysql
// scans character, numeric, date and JSON columns as []byte, go-mssqldb does the
// same for DECIMAL and MONEY, and pgx for JSON, JSONB and XML; each of them names
// the column type. go-ora and modernc SQLite return []byte only for a binary
// value, and go-ora names no column type at all, as SQLite does not for a column
// declared without one, so a column with no name keeps its bytes. A length
// suffix such as VARBINARY(16) is ignored.
func holdsBinary(databaseType string) bool {
	name, _, _ := strings.Cut(strings.ToUpper(strings.TrimSpace(databaseType)), "(")
	switch strings.TrimSpace(name) {
	case "", "BYTEA", "BINARY", "VARBINARY", "BLOB", "TINYBLOB", "MEDIUMBLOB", "LONGBLOB", "IMAGE":
		return true
	default:
		return false
	}
}

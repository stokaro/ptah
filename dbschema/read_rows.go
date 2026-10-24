package dbschema

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"slices"
	"strings"

	"ptah.run/internal/connector"
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
// A value from a binary column (bytea, binary, varbinary, blob, image or raw,
// and YDB's String and Yson) is returned as []byte holding exactly the bytes
// the database stores. Several drivers also scan character, numeric, date and
// JSON columns as []byte; those values are returned as string, so text
// compares stably regardless of driver. On YDB a Decimal is returned as its
// digits in a string and a moment as a time.Time in UTC, where the driver hands
// over a type of its own and the local zone. All other values are returned as
// scanned.
//
// No ORDER BY is applied, so the returned row order is whatever the database
// yields and must not be relied upon. This suits set-oriented callers such as
// migration/datadiff.Compute, which key rows by their declared key columns and
// are order-independent by construction.
//
// The context governs query execution and row iteration; canceling it aborts
// the read with the context error wrapped in the returned error.
//
// A connection whose owner reads rows itself, an executable provider's
// session, needs the columns that identify a row: [ReadKeyedTableRows] reads
// it, and ReadTableRows refuses it.
func ReadTableRows(ctx context.Context, conn *DatabaseConnection, schema, table string, columns []string) ([]map[string]any, error) {
	if err := checkRowRead(conn, table, columns); err != nil {
		return nil, err
	}
	if _, owned := conn.native.(nativeRowReader); owned {
		return nil, errors.New("dbschema: the owner of this connection reads rows in key order; read them with ReadKeyedTableRows")
	}
	return readTableRows(ctx, conn, schema, table, columns)
}

// nativeRowReader is a native session whose owner reads table rows itself,
// in the order of the key columns, which are among columns. A table the
// database does not hold is an error that names it.
type nativeRowReader interface {
	ReadTableRows(ctx context.Context, schema, table string, columns, keys []string) ([]map[string]any, error)
}

// ReadKeyedTableRows reads the rows of table as [ReadTableRows] does, where
// keys are the columns that identify a row; each of them must be among
// columns, in the same spelling.
//
// On a connection whose owner reads rows itself, an executable provider's
// session, the owner reads them, in key order and page by page, each value in
// the form the owner returns it. On any other connection it is ReadTableRows,
// and the keys are only checked.
func ReadKeyedTableRows(ctx context.Context, conn *DatabaseConnection, schema, table string, columns, keys []string) ([]map[string]any, error) {
	if err := checkRowRead(conn, table, columns); err != nil {
		return nil, err
	}
	if len(keys) == 0 {
		return nil, errors.New("dbschema: ReadKeyedTableRows requires at least one key column")
	}
	for _, key := range keys {
		if !slices.Contains(columns, key) {
			return nil, fmt.Errorf("dbschema: key column %q is not among the columns read", key)
		}
	}
	if reader, owned := conn.native.(nativeRowReader); owned {
		rows, err := reader.ReadTableRows(ctx, schema, table, slices.Clone(columns), slices.Clone(keys))
		if err != nil {
			return nil, fmt.Errorf("dbschema: read rows of table %q: %w", table, err)
		}
		return rows, nil
	}
	return readTableRows(ctx, conn, schema, table, columns)
}

func checkRowRead(conn *DatabaseConnection, table string, columns []string) error {
	if conn == nil {
		return errors.New("dbschema: ReadTableRows requires a non-nil connection")
	}
	if strings.TrimSpace(table) == "" {
		return errors.New("dbschema: ReadTableRows requires a table name")
	}
	if len(columns) == 0 {
		return errors.New("dbschema: ReadTableRows requires at least one column")
	}
	// Duplicate column names would collapse into a single map key while the
	// column-count guard below still passed, silently dropping a column, so
	// reject them up front.
	seen := make(map[string]struct{}, len(columns))
	for _, col := range columns {
		if _, dup := seen[col]; dup {
			return fmt.Errorf("dbschema: ReadTableRows got duplicate column %q", col)
		}
		seen[col] = struct{}{}
	}
	return nil
}

func readTableRows(ctx context.Context, conn *DatabaseConnection, schema, table string, columns []string) ([]map[string]any, error) {
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
	values := newColumnValues(conn.engine(), columnTypes)

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
			value, err := values.value(i, holders[i])
			if err != nil {
				return nil, fmt.Errorf("dbschema: column %q of table %q: %w", col, table, err)
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

// columnValues turns what the driver scanned for each column into the value
// ReadTableRows returns.
type columnValues struct {
	binary  []bool
	service connector.Service
}

// newColumnValues asks the dialect's connector which columns hold binary
// values: the driver names the column type, and only the engine knows which
// of its names are binary.
func newColumnValues(service connector.Service, columnTypes []*sql.ColumnType) columnValues {
	values := columnValues{
		binary:  make([]bool, len(columnTypes)),
		service: service,
	}
	for i, columnType := range columnTypes {
		values.binary[i] = service.HoldsBytes(columnType.DatabaseTypeName())
	}
	return values
}

// value is the value column i returns for what the driver scanned.
//
// A binary value keeps its bytes. Converted to string it reaches the renderer
// as text, and a text literal is what a server refuses for a byte that is not
// valid UTF-8, stores as a different value when a backslash reads as a bytea
// escape, or refuses outright on SQL Server (stokaro/ptah#3297). On YDB the
// driver's own Decimal and its local moments become plain values.
func (v columnValues) value(i int, scanned any) (any, error) {
	if raw, isBytes := scanned.([]byte); isBytes && !v.binary[i] {
		scanned = string(raw)
	}
	return v.service.RowValue(scanned)
}

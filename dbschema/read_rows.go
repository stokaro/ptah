package dbschema

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/stokaro/ptah/internal/sqlident"
)

// ReadTableRows reads the current rows of table, projected onto the requested
// column set, from the database behind conn.
//
// The SELECT is built with dialect-aware, safely-quoted identifiers (via
// internal/sqlident) keyed off conn.Info().Dialect, so callers never hand-quote
// table or column names. When schema is non-empty the table is
// schema-qualified. columns is required and drives both the projection and the
// keys of the returned maps: each returned row is a map[string]any keyed by the
// requested column names, in the exact spelling passed in. []byte scan results
// (several drivers return []byte for text and numeric columns) are converted to
// string so that values compare stably regardless of driver; all other values
// are returned as scanned.
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

	dialect := conn.Info().Dialect

	quoted := make([]string, len(columns))
	for i, col := range columns {
		quoted[i] = sqlident.Quote(dialect, col)
	}
	query := "SELECT " + strings.Join(quoted, ", ") + " FROM " + sqlident.Qualified(dialect, schema, table)

	rows, err := conn.QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("dbschema: read rows of table %q: %w", table, err)
	}
	defer rows.Close()

	names, err := rows.Columns()
	if err != nil {
		return nil, fmt.Errorf("dbschema: read columns of table %q: %w", table, err)
	}
	if len(names) != len(columns) {
		return nil, fmt.Errorf("dbschema: table %q returned %d columns, want %d", table, len(names), len(columns))
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
			row[col] = normalizeScannedValue(holders[i])
		}
		result = append(result, row)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("dbschema: iterate rows of table %q: %w", table, err)
	}

	return result, nil
}

// normalizeScannedValue converts driver []byte results (several drivers return
// []byte for text and numeric columns) to string, leaving all other values
// untouched, so scanned values compare stably regardless of driver.
func normalizeScannedValue(v any) any {
	if b, ok := v.([]byte); ok {
		return string(b)
	}
	return v
}

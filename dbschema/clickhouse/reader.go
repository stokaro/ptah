package clickhouse

import (
	"database/sql"
	"fmt"
	"strings"

	"github.com/stokaro/ptah/dbschema/types"
)

// Reader reads schema information from a ClickHouse server.
//
// It only queries system tables; it never holds an explicit transaction
// (ClickHouse's read consistency is per-query against the storage engine,
// and the system tables are MergeTree-backed views).
type Reader struct {
	db     *sql.DB
	schema string
}

// NewClickHouseReader creates a reader for the given database/schema.
// `schema` corresponds to the ClickHouse database name; if empty it
// defaults to `currentDatabase()` resolved on each query.
func NewClickHouseReader(db *sql.DB, schema string) *Reader {
	return &Reader{db: db, schema: schema}
}

// ReadSchema returns the database, columns and data-skipping indices for
// the configured database. Constraints, RLS, functions, etc. are reported
// as empty slices — those concepts have no direct ClickHouse equivalent
// in the shape Ptah's diff layer understands today.
func (r *Reader) ReadSchema() (*types.DBSchema, error) {
	dbName, err := r.resolveDatabaseName()
	if err != nil {
		return nil, err
	}

	tables, err := r.readTables(dbName)
	if err != nil {
		return nil, fmt.Errorf("clickhouse: read tables: %w", err)
	}
	indexes, err := r.readSkippingIndexes(dbName)
	if err != nil {
		return nil, fmt.Errorf("clickhouse: read indexes: %w", err)
	}
	return &types.DBSchema{
		Tables:  tables,
		Indexes: indexes,
	}, nil
}

func (r *Reader) resolveDatabaseName() (string, error) {
	if r.schema != "" {
		return r.schema, nil
	}
	var name string
	if err := r.db.QueryRow("SELECT currentDatabase()").Scan(&name); err != nil {
		return "", fmt.Errorf("clickhouse: resolve current database: %w", err)
	}
	return name, nil
}

func (r *Reader) readTables(dbName string) ([]types.DBTable, error) {
	rows, err := r.db.Query(`
		SELECT name, comment
		FROM system.tables
		WHERE database = ? AND engine NOT LIKE '%View'
		ORDER BY name
	`, dbName)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var tables []types.DBTable
	for rows.Next() {
		var name, comment string
		if err := rows.Scan(&name, &comment); err != nil {
			return nil, err
		}
		t := types.DBTable{Name: name, Type: "TABLE", Comment: comment}
		columns, err := r.readColumns(dbName, name)
		if err != nil {
			return nil, fmt.Errorf("clickhouse: read columns for %s: %w", name, err)
		}
		t.Columns = columns
		tables = append(tables, t)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return tables, nil
}

func (r *Reader) readColumns(dbName, table string) ([]types.DBColumn, error) {
	rows, err := r.db.Query(`
		SELECT name, type, default_kind, default_expression, position, comment
		FROM system.columns
		WHERE database = ? AND table = ?
		ORDER BY position
	`, dbName, table)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var cols []types.DBColumn
	for rows.Next() {
		var (
			name, dataType, defaultKind, defaultExpr, comment string
			position                                          int
		)
		if err := rows.Scan(&name, &dataType, &defaultKind, &defaultExpr, &position, &comment); err != nil {
			return nil, err
		}
		nullable := "NO"
		if strings.HasPrefix(dataType, "Nullable(") {
			nullable = "YES"
		}
		col := types.DBColumn{
			Name:            name,
			DataType:        dataType,
			ColumnType:      dataType,
			IsNullable:      nullable,
			OrdinalPosition: position,
		}
		if defaultKind != "" && defaultExpr != "" {
			expr := defaultExpr
			col.ColumnDefault = &expr
		}
		cols = append(cols, col)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return cols, nil
}

func (r *Reader) readSkippingIndexes(dbName string) ([]types.DBIndex, error) {
	rows, err := r.db.Query(`
		SELECT table, name, expr, type
		FROM system.data_skipping_indices
		WHERE database = ?
		ORDER BY table, name
	`, dbName)
	if err != nil {
		// system.data_skipping_indices was added in 21.x; if a very old
		// server doesn't expose it, return no indexes rather than failing
		// the entire schema read.
		if strings.Contains(err.Error(), "data_skipping_indices") {
			return nil, nil
		}
		return nil, err
	}
	defer rows.Close()

	var indexes []types.DBIndex
	for rows.Next() {
		var table, name, expr, idxType string
		if err := rows.Scan(&table, &name, &expr, &idxType); err != nil {
			return nil, err
		}
		indexes = append(indexes, types.DBIndex{
			Name:       name,
			TableName:  table,
			Columns:    []string{expr},
			Definition: fmt.Sprintf("INDEX %s %s TYPE %s", name, expr, idxType),
		})
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return indexes, nil
}

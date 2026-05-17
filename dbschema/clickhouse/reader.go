package clickhouse

import (
	"database/sql"
	"fmt"
	"strings"

	"github.com/stokaro/ptah/dbschema/types"
)

// Engines we consider "real data tables" for schema introspection. The
// MergeTree family covers production workloads; Memory/Log/TinyLog/StripeLog
// cover the common non-replicated developer/test workloads. Materialised
// views and Distributed engines are intentionally excluded because they are
// not part of the schema-as-data-shape Ptah's diff layer reasons about.
//
// We use a positive allowlist and ALSO keep `NOT LIKE '%View'` as a guard so
// that any unanticipated view-style engine still gets filtered out even if it
// somehow matches a MergeTree pattern.

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
		WHERE database = ?
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
		// ClickHouse columns can have several flavours of default
		// (DEFAULT, MATERIALIZED, ALIAS, EPHEMERAL). Only the plain DEFAULT
		// flavour is a schema-level default value comparable to the other
		// dialects' notion of ColumnDefault. The MATERIALIZED / ALIAS /
		// EPHEMERAL kinds round-trip through GeneratedKind +
		// GeneratedExpression so the schema read is lossless; the planner
		// currently ignores those columns until the annotation-side surface
		// for declaring them is wired through goschema.
		switch defaultKind {
		case "DEFAULT":
			if defaultExpr != "" {
				expr := defaultExpr
				col.ColumnDefault = &expr
			}
		case "MATERIALIZED", "ALIAS", "EPHEMERAL":
			if defaultExpr != "" {
				expr := defaultExpr
				col.GeneratedExpression = &expr
			}
			col.GeneratedKind = defaultKind
		}
		cols = append(cols, col)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return cols, nil
}

// skippingIndexTablePresent reports whether system.data_skipping_indices is
// available on the connected server.
func (r *Reader) skippingIndexTablePresent() (bool, error) {
	var n uint64
	err := r.db.QueryRow(`
		SELECT count()
		FROM system.tables
		WHERE database = 'system' AND name = 'data_skipping_indices'
	`).Scan(&n)
	if err != nil {
		return false, err
	}
	return n > 0, nil
}

func (r *Reader) readSkippingIndexes(dbName string) ([]types.DBIndex, error) {
	// system.data_skipping_indices was added in 21.x; on very old servers it
	// is absent. Feature-detect by probing system.tables before querying so
	// real failures aren't swallowed by an error-substring sniff.
	present, err := r.skippingIndexTablePresent()
	if err != nil {
		return nil, fmt.Errorf("clickhouse: detect system.data_skipping_indices: %w", err)
	}
	if !present {
		return nil, nil
	}

	// system.data_skipping_indices exposes `granularity` as UInt64. The
	// driver decodes that into uint64 by default, so scan into that type
	// explicitly and cast on the way out.
	rows, err := r.db.Query(`
		SELECT table, name, expr, type, granularity
		FROM system.data_skipping_indices
		WHERE database = ?
		ORDER BY table, name
	`, dbName)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var indexes []types.DBIndex
	for rows.Next() {
		var (
			table, name, expr, idxType string
			granularity                uint64
		)
		if err := rows.Scan(&table, &name, &expr, &idxType, &granularity); err != nil {
			return nil, err
		}
		// Populate Columns[0] = expression for back-compat with the
		// existing diff layer (which compares Columns), AND set Expression
		// for richer downstream diffing once that's wired up. The duality
		// is intentional and documented on types.DBIndex.
		indexes = append(indexes, types.DBIndex{
			Name:        name,
			TableName:   table,
			Columns:     []string{expr},
			Definition:  fmt.Sprintf("INDEX %s %s TYPE %s GRANULARITY %d", name, expr, idxType, granularity),
			Type:        idxType,
			Expression:  expr,
			Granularity: int(granularity),
		})
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return indexes, nil
}

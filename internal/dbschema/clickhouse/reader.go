package clickhouse

import (
	"fmt"
	"strings"

	"go.5x5.cz/ptah/dbschema/types"
	"go.5x5.cz/ptah/internal/sqlrunner"
)

// Engines we consider "real data tables" for table introspection. The
// MergeTree family covers production workloads; Memory/Log/TinyLog/StripeLog
// cover the common non-replicated developer/test workloads. View-style and
// Distributed engines are excluded from the table read: views and materialized
// views are read as their own object kinds below, and Distributed is not part
// of the schema-as-data-shape Ptah's diff layer reasons about.
//
// We use a positive allowlist and ALSO keep `NOT LIKE '%View'` as a guard so
// that any unanticipated view-style engine still gets filtered out even if it
// somehow matches a MergeTree pattern. The allowlist alone is not enough for
// materialized views, whose own storage table is a real MergeTree table; see
// materializedViewInnerTablesSubquery.

// Reader reads schema information from a ClickHouse server.
//
// It only queries system tables; it never holds an explicit transaction
// (ClickHouse's read consistency is per-query against the storage engine,
// and the system tables are MergeTree-backed views).
type Reader struct {
	db     sqlrunner.Runner
	schema string
}

// NewClickHouseReader creates a reader for the given database/schema.
// `schema` corresponds to the ClickHouse database name; if empty it
// defaults to `currentDatabase()` resolved on each query.
func NewClickHouseReader(db sqlrunner.Runner, schema string) *Reader {
	return &Reader{db: db, schema: schema}
}

// ReadSchema returns tables, columns, data-skipping indexes, plain views and
// materialized views for the configured database. Constraints, RLS, functions,
// and other shapes with no direct equivalent in Ptah's ClickHouse model remain
// empty.
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
	views, err := r.readViews(dbName)
	if err != nil {
		return nil, fmt.Errorf("clickhouse: read views: %w", err)
	}
	matViews, err := r.readMaterializedViews(dbName)
	if err != nil {
		return nil, fmt.Errorf("clickhouse: read materialized views: %w", err)
	}
	return &types.DBSchema{
		Tables:   tables,
		Indexes:  indexes,
		Views:    views,
		MatViews: matViews,
	}, nil
}

// materializedViewInnerTablesSubquery names the storage tables ClickHouse
// creates for the materialized views in one database.
//
// A materialized view declared with a storage clause rather than a TO target
// owns a real MergeTree table, and that table is in the same database with the
// same engine as a table the user declared. Measured on server 26.7.3.19,
// creating two materialized views left system.tables reporting
// ".inner_id.<uuid>" MergeTree rows next to "users", so the table read has to
// subtract them by name or every materialized view arrives as a table nobody
// declared -- and, once planning followed, as a DROP TABLE for the storage of a
// view the desired schema still asks for.
//
// Both spellings are subtracted: ".inner_id.<uuid>" is what an Atomic database
// names the storage, and ".inner.<view name>" is the older Ordinary spelling.
// The set is derived from the materialized views themselves rather than from a
// leading-dot name pattern, so a declared table is never dropped from the read
// merely for how it is spelled.
const materializedViewInnerTablesSubquery = `
		SELECT concat('.inner_id.', toString(uuid))
		FROM system.tables
		WHERE database = ? AND engine = 'MaterializedView'
		UNION ALL
		SELECT concat('.inner.', name)
		FROM system.tables
		WHERE database = ? AND engine = 'MaterializedView'
`

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
	columnsByTable, err := r.readColumnsByTable(dbName)
	if err != nil {
		return nil, fmt.Errorf("clickhouse: read columns: %w", err)
	}

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
		  AND name NOT IN (`+materializedViewInnerTablesSubquery+`)
		ORDER BY name
	`, dbName, dbName, dbName)
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
		t.Columns = columnsByTable[name]
		tables = append(tables, t)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return tables, nil
}

func (r *Reader) readViews(dbName string) ([]types.DBView, error) {
	rows, err := r.db.Query(`
		SELECT name, as_select, comment
		FROM system.tables
		WHERE database = ?
		  AND is_temporary = 0
		  AND engine = 'View'
		ORDER BY name
	`, dbName)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var views []types.DBView
	for rows.Next() {
		view := types.DBView{Schema: dbName, CheckOption: "NONE"}
		if err := rows.Scan(&view.Name, &view.Body, &view.Comment); err != nil {
			return nil, err
		}
		views = append(views, view)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return views, nil
}

// readMaterializedViews reads the materialized views of one database.
//
// The definition comes from system.tables.as_select, the same column the plain
// view read uses. Measured on server 26.7.3.19, as_select carries the SELECT
// alone -- no storage clause and no POPULATE -- and a body written with
// qualified names comes back byte for byte, so the desired body and the read
// body compare directly.
//
// RefreshStrategy is reported as "manual" to match the PostgreSQL reader's
// default. ClickHouse has no refresh statement at all, so no read could report
// anything narrower, and the comparator does not diff the field.
func (r *Reader) readMaterializedViews(dbName string) ([]types.DBMatView, error) {
	rows, err := r.db.Query(`
		SELECT name, as_select, comment
		FROM system.tables
		WHERE database = ?
		  AND is_temporary = 0
		  AND engine = 'MaterializedView'
		ORDER BY name
	`, dbName)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var views []types.DBMatView
	for rows.Next() {
		view := types.DBMatView{Schema: dbName, RefreshStrategy: "manual"}
		if err := rows.Scan(&view.Name, &view.Body, &view.Comment); err != nil {
			return nil, err
		}
		views = append(views, view)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return views, nil
}

func (r *Reader) readColumnsByTable(dbName string) (map[string][]types.DBColumn, error) {
	rows, err := r.db.Query(`
		SELECT table, name, type, default_kind, default_expression, position, comment
		FROM system.columns
		WHERE database = ?
		ORDER BY table, position
	`, dbName)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	columnsByTable := make(map[string][]types.DBColumn)
	for rows.Next() {
		var (
			tableName                                         string
			name, dataType, defaultKind, defaultExpr, comment string
			position                                          int
		)
		if err := rows.Scan(&tableName, &name, &dataType, &defaultKind, &defaultExpr, &position, &comment); err != nil {
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
		columnsByTable[tableName] = append(columnsByTable[tableName], col)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return columnsByTable, nil
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
	// The same inner-table subtraction the table read applies: an index on a
	// materialized view's storage belongs to a table this reader does not
	// report, and an index whose TableName names nothing in the schema is a
	// change the comparator cannot resolve.
	rows, err := r.db.Query(`
		SELECT table, name, expr, type, granularity
		FROM system.data_skipping_indices
		WHERE database = ?
		  AND table NOT IN (`+materializedViewInnerTablesSubquery+`)
		ORDER BY table, name
	`, dbName, dbName, dbName)
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

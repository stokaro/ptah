package clickhouse

import (
	"fmt"
	"strings"

	"go.5x5.cz/ptah/core/coverage"
	"go.5x5.cz/ptah/core/platform/capability"
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
	caps   capability.Capabilities

	// version is what `SELECT version()` answered, e.g. "26.7.3.19", or empty
	// for a reader built without one. Nothing branches on it: the RBAC reads
	// name only catalog columns the oldest declared line already has, which is
	// the simpler answer and the one that cannot drift. It is carried so a
	// failing catalog read can say which server refused it -- see
	// [Reader.onServer].
	version string
}

// NewClickHouseReader creates a reader for the given database/schema.
// `schema` corresponds to the ClickHouse database name; if empty it
// defaults to `currentDatabase()` resolved on each query.
func NewClickHouseReader(db sqlrunner.Runner, schema string) *Reader {
	return NewClickHouseReaderWithCapabilities(db, schema, "", capability.ClickHouse24())
}

// NewClickHouseReaderWithCapabilities creates a ClickHouse reader whose role and
// grant reads are gated by the target's capabilities and whose diagnostics can
// name the server version.
//
// Roles and grants are read only under [capability.RoleManagement]. A caller
// whose preset does not carry it reads exactly what this reader read before RBAC
// existed: no system.roles query, no system.grants query, and empty Roles,
// RolesOutOfScope and Grants. That is the gate stokaro/ptah#1025 asks for, and
// it is a capability rather than a version check because whether Ptah manages
// ClickHouse RBAC on a given target is a decision about the target, not about
// which columns its catalog happens to have.
func NewClickHouseReaderWithCapabilities(
	db sqlrunner.Runner,
	schema string,
	version string,
	caps capability.Capabilities,
) *Reader {
	return &Reader{db: db, schema: schema, caps: caps, version: version}
}

// ReadSchema returns tables, columns, data-skipping indexes, plain views and
// materialized views for the configured database, plus roles and grants when
// the target carries [capability.RoleManagement]. Constraints, RLS, functions,
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
	schema := &types.DBSchema{
		Tables:   tables,
		Indexes:  indexes,
		Views:    views,
		MatViews: matViews,
	}
	if r.caps.Has(capability.RoleManagement) {
		if err := r.readRBACInto(dbName, schema); err != nil {
			// An account that may not read the access catalog must not lose the
			// whole description over it. Reading system.roles and system.grants
			// needs a privilege reading a table does not, and the capability
			// preset is a statement about the SERVER -- it cannot know what the
			// connected account was granted. Measured on 26.7.3.19: an account
			// holding SELECT, SHOW TABLES and SHOW COLUMNS on one database gets
			// code 497 from both catalog queries, so without this branch every
			// ClickHouse read by such an account failed at exit 2 -- including
			// the read of a schema that declares no role at all, which has
			// always worked and has nothing to do with RBAC.
			//
			// Recording Role as not described is what makes the degradation
			// safe rather than silent. The comparator refuses to conclude "this
			// role is missing" from a read that admits it did not look, so a
			// declared role is reported as an undecided addition instead of
			// planned as a CREATE ROLE nothing verified. Nothing destructive
			// can follow: both role and grant removal are decided from live
			// rows, and there are none.
			if !isAccessDenied(err) {
				return nil, err
			}
			schema.NotDescribed = schema.NotDescribed.WithKind(coverage.Role)
		}
	}
	return schema, nil
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
// Which spelling the storage has is decided per view, by the view's own row,
// not by emitting both. A database engine of Atomic gives every table a UUID and
// names the storage ".inner_id.<the view's uuid>"; an Ordinary database leaves
// the UUID at all zeros and names it ".inner.<view name>". Measured on both ends
// of the range this preset covers, 26.7.3.19 and 24.10.4.191:
//
//	database engine   system.tables rows for a view named "mv"
//	Atomic            mv (uuid a5bb…)  .inner_id.a5bb…  (uuid of its own)
//	Ordinary          mv (uuid 0000…)  .inner.mv        (uuid 0000…)
//
// Subtracting both spellings unconditionally deletes a real table from the read.
// A leading dot is legal in a quoted ClickHouse name, and on the same servers
//
//	CREATE TABLE `.inner.mv` (x UInt64) ENGINE = MergeTree ORDER BY x
//
// succeeds in an Atomic database alongside the view "mv" whose storage is
// ".inner_id.<uuid>". The union arm invented ".inner.mv" and filtered that
// user table out of both the table read and the index read.
//
// The set stays derived from the materialized views themselves rather than from
// a leading-dot name pattern, so a declared table is never dropped from the read
// merely for how it is spelled.
//
// One case this still cannot separate, and it is a derivation rather than an
// answer: in an ORDINARY database a materialized view "mv" created with
// TO <target> owns no storage, and ".inner.mv" is nevertheless exactly what a
// storage-owning view of that name would be called. Measured on 26.7.3.19, both
// statements are accepted and the target may even be that table:
//
//	CREATE TABLE wf9d_ord2.`.inner.mv` (c UInt64) ENGINE = MergeTree ORDER BY tuple()
//	CREATE MATERIALIZED VIEW wf9d_ord2.mv TO wf9d_ord2.`.inner.mv` AS SELECT …
//
// so a real table spelled that way is subtracted from the table and index reads.
// system.tables on 26.7.3.19 answers this exactly, with target_database and
// target_table, and the read cannot use them: 24.10.4.191's system.tables has 34
// columns and neither of those two, and naming a column a supported server does
// not have turns a working read into an error. An Atomic database is unaffected
// -- the derived name carries the view's own UUID.
//
// DropAllTables no longer derives anything: it takes its table inventory after
// the views are dropped, so the guess above cannot make the reset leave a table
// behind.
const materializedViewInnerTablesSubquery = `
		SELECT ` + innerTableNameExpression + `
		FROM system.tables
		WHERE database = ? AND engine = 'MaterializedView'
`

// innerTableNameExpression names the storage table of the materialized-view row
// it is evaluated over, choosing the spelling the row's own database engine
// uses.
const innerTableNameExpression = `if(
		         uuid = toUUID('00000000-0000-0000-0000-000000000000'),
		         concat('.inner.', name),
		         concat('.inner_id.', toString(uuid))
		       )`

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
	`, dbName, dbName)
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
//
// What this read cannot tell apart, stated rather than hidden: a view created
// with `TO <target table>` routes its rows into a table the user owns and has no
// inner storage of its own, and system.tables reports it with the same engine
// and the same as_select as a view that does own its storage. Measured on
// 26.7.3.19:
//
//	name    engine            as_select
//	mv_to   MaterializedView  SELECT count() AS c FROM wf9d_alias.users
//	create_table_query: CREATE MATERIALIZED VIEW wf9d_alias.mv_to
//	                    TO wf9d_alias.mv_target (`c` UInt64) AS SELECT ...
//
// The target survives only in create_table_query, and types.DBMatView has
// nowhere to put it, so such a view is reported as an ordinary one: it compares
// as synchronized against a declaration of the same query, and a body change
// would be planned as a drop and a create that recreates it without the target.
// Representing the shape would be a model change (a target on the shared node,
// the renderer emitting `TO`, the comparator diffing it); refusing the read
// outright would take away a read that works today. Neither is decided here --
// the support matrix says plainly not to manage a `TO` view with Ptah.
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
	`, dbName, dbName)
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

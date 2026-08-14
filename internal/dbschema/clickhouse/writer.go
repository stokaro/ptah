package clickhouse

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"strings"

	"go.5x5.cz/ptah/core/platform"
	"go.5x5.cz/ptah/dbschema/types"
	"go.5x5.cz/ptah/internal/sqlident"
	"go.5x5.cz/ptah/internal/sqlrunner"
)

func quoteIdent(name string) string {
	return sqlident.Quote(platform.ClickHouse, name)
}

func quoteQualifiedIdent(database, name string) string {
	return sqlident.Qualified(platform.ClickHouse, database, name)
}

// Writer applies schema changes to a ClickHouse server.
//
// ClickHouse does not provide cross-statement transactions in the way the
// other dialects Ptah supports do (experimental transactions exist only
// against MergeTree-family tables and require explicit opt-in), so the
// Begin/Commit/Rollback methods are deliberate no-ops. ExecuteSQL still
// records dry-run output unchanged, so callers using DRY_RUN see the same
// shape of trace they get from the other dialects.
type Writer struct {
	db     sqlrunner.Runner
	schema string
	dryRun bool
}

type transactionWriter struct {
	writer *Writer
}

type databaseRealmObjectKind uint8

const (
	databaseRealmObjectUnknown databaseRealmObjectKind = iota
	databaseRealmObjectView
	databaseRealmObjectMaterializedView
	databaseRealmObjectLiveView
	databaseRealmObjectWindowView
	databaseRealmObjectDictionary
	databaseRealmObjectTable
)

type databaseRealmObject struct {
	name      string
	engine    string
	createSQL string
	kind      databaseRealmObjectKind
}

const databaseRealmObjectsQuery = `
	SELECT name, engine, create_table_query
	FROM system.tables
	WHERE database = ?
	  AND is_temporary = 0
	ORDER BY name
`

const databaseRealmEngineQuery = `
	SELECT engine
	FROM system.databases
	WHERE name = ?
`

const databaseRealmVersionQuery = `SELECT version()`

const databaseRealmTemporaryObjectsQuery = `
	SELECT name
	FROM system.tables
	WHERE is_temporary = 1
	ORDER BY name
`

const databaseRealmGlobalPrivilegesQuery = `
	CHECK GRANT SHOW DATABASES, SHOW TABLES ON *.*
`

// ClickHouse does not expose dependency metadata for ordinary views. Fail
// closed when another user database contains an object capable of referencing
// the cleanup realm, because proving that object independent would require
// parsing its engine or SELECT sub-language.
const databaseRealmExternalDependenciesQuery = `
	SELECT database, name, engine
	FROM system.tables
	WHERE database != ?
	  AND database NOT IN (
	    'INFORMATION_SCHEMA',
	    '_temporary_and_external_tables',
	    'information_schema',
	    'system'
	  )
	  AND engine IN (
	    'Buffer',
	    'Dictionary',
	    'Distributed',
	    'LiveView',
	    'MaterializedView',
	    'Merge',
	    'View',
	    'WindowView'
	  )
	ORDER BY database, name
	LIMIT 1
`

var databaseRealmCleanupEngines = []string{
	"Atomic",
	"Lazy",
	"Memory",
	"Ordinary",
}

var protectedClickHouseDatabases = []string{
	"INFORMATION_SCHEMA",
	"_temporary_and_external_tables",
	"information_schema",
	"system",
}

// NewClickHouseWriter constructs a Writer.
func NewClickHouseWriter(db *sql.DB, schema string) *Writer {
	return NewClickHouseWriterForRunner(db, schema)
}

// NewClickHouseWriterForRunner constructs a writer bound to a pool or pinned
// database session.
func NewClickHouseWriterForRunner(runner sqlrunner.Runner, schema string) *Writer {
	return &Writer{db: runner, schema: schema}
}

// ExecuteSQL executes a SQL statement against the ClickHouse server. Values
// must be passed via args and referenced through `?` placeholders;
// clickhouse-go/v2 binds them as native driver parameters. The SQL string
// itself should never be assembled with fmt.Sprintf for value interpolation.
// Identifiers (table/column names) cannot be parameterized and must be
// escaped via quoteIdent before being substituted in.
//
// Unlike the PostgreSQL / MySQL writers, ClickHouse runs each statement
// standalone (see the package doc for why transactions are no-ops here),
// so ExecuteSQL goes straight to db.ExecContext rather than to a held
// *sql.Tx.
func (w *Writer) ExecuteSQL(ctx context.Context, sqlExpr string, args ...any) error {
	if w.dryRun {
		slog.Info("[DRY RUN] Would execute SQL", "sql", sqlExpr, "args", args)
		return nil
	}
	if w.db == nil {
		return fmt.Errorf("no database connection")
	}
	if _, err := w.db.ExecContext(ctx, sqlExpr, args...); err != nil {
		return fmt.Errorf("clickhouse: SQL execution failed: %w\nSQL: %s", err, sqlExpr)
	}
	return nil
}

// BeginTransaction is a no-op for ClickHouse. Multi-statement transactions
// are experimental and require explicit opt-in per session; the migration
// engine has no protection model that depends on them, so this is left
// as a no-op rather than silently enabling experimental flags.
func (w *Writer) BeginTransaction(_ context.Context) (types.SchemaTransaction, error) {
	if w.dryRun {
		slog.Info("[DRY RUN] Would begin transaction (no-op on ClickHouse)")
	}
	return &transactionWriter{writer: w}, nil
}

// ExecuteSQL executes SQL through the underlying ClickHouse writer.
func (w *transactionWriter) ExecuteSQL(ctx context.Context, sqlExpr string, args ...any) error {
	return w.writer.ExecuteSQL(ctx, sqlExpr, args...)
}

// Commit is a no-op for ClickHouse — see BeginTransaction.
func (w *transactionWriter) Commit() error {
	if w.writer.dryRun {
		slog.Info("[DRY RUN] Would commit transaction (no-op on ClickHouse)")
	}
	return nil
}

// Rollback is a no-op for ClickHouse — see BeginTransaction.
func (w *transactionWriter) Rollback() error {
	if w.writer.dryRun {
		slog.Info("[DRY RUN] Would rollback transaction (no-op on ClickHouse)")
	}
	return nil
}

// IsDryRun reports whether dry-run mode is active.
func (w *transactionWriter) IsDryRun() bool { return w.writer.IsDryRun() }

// currentDatabaseCleanupViewsQuery lists the view-like objects DropAllTables
// destroys: the two engines the ClickHouse reader reports and the renderer
// emits. LiveView and WindowView are deliberately absent — the reader never
// reports them, so destroying them would put the writer ahead of the cleanup
// plan internal/schemaclean builds from that reader.
const currentDatabaseCleanupViewsQuery = `
	SELECT name FROM system.tables
	WHERE database = currentDatabase()
	  AND is_temporary = 0
	  AND engine IN ('View', 'MaterializedView')
	ORDER BY name
`

// currentDatabaseCleanupTablesQuery lists the base tables DropAllTables
// destroys.
//
// It subtracts nothing, and the ordering in DropAllTables is why: the views are
// already gone when this runs, so a materialized view's storage table went with
// its owner and whatever is still standing is a table in its own right. A
// subtraction here could only guess at which storage belongs to which view, and
// a guess is what left a real table behind -- see DropAllTables.
const currentDatabaseCleanupTablesQuery = `
	SELECT name FROM system.tables
	WHERE database = currentDatabase()
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
`

// DropAllTables drops every view, materialized view and base table in the
// configured database. Uses DROP … SYNC so subsequent CREATE statements don't
// race against the async drop.
//
// Views go first, and they go as whole objects. A materialized view owns a
// storage table that the base-table engine allowlist matches on its own, and
// measured on server 26.7.3.19 a DROP TABLE on that ".inner_id.<uuid>"
// succeeds at exit 0 while leaving the view itself in system.tables, so a
// SELECT from the view then fails with
// "Table ... does not exist. (UNKNOWN_TABLE)". DROP VIEW removes the view and
// its storage together.
//
// The table inventory is therefore taken AFTER the views are dropped, not
// before. Deriving the storage names first and subtracting them is a guess, and
// the guess is wrong in a case ClickHouse allows: in an Ordinary database a
// materialized view "mv" created with TO owns no storage at all, while
// ".inner.mv" is what a storage-owning view of that name WOULD be called, so a
// real table spelled that way -- including the view's own TO target -- was
// subtracted from the reset and left standing. Measured on 26.7.3.19:
//
//	CREATE TABLE wf9d_ord2.`.inner.mv` (c UInt64) ENGINE = MergeTree ORDER BY tuple()
//	CREATE MATERIALIZED VIEW wf9d_ord2.mv TO wf9d_ord2.`.inner.mv` AS SELECT …
//	-> both accepted; system.tables reports ".inner.mv" MergeTree and "mv" MaterializedView
//
// Asking after the drops needs no guess: a name still present is a table.
// system.tables gained target_database/target_table, which names the storage
// exactly, but the read cannot use it -- 24.10.4.191's system.tables has 34
// columns and neither of those two, and naming a column a supported server does
// not have would turn a working read into an error.
//
// Leaving view-like objects standing is not a smaller destructive scope, it is
// a reset that does not reset. Every caller of this method — the shadow replay
// in migration/generator, the dev-database cleanup in internal/atlasschema, the
// integration harness — replays DDL into the database afterwards, and measured
// on 26.7.3.19 both halves of that replay fail:
//
//	CREATE VIEW <db>.plain_v              -> code: 57, Table <db>.plain_v already exists
//	CREATE MATERIALIZED VIEW <db>.stored_v -> code: 57, Table <db>.stored_v already exists
//
// DropDatabaseRealm remains the stronger contract: it also removes
// dictionaries, live views and window views, and it refuses rather than
// proceeding when catalog visibility cannot be proven.
//
// Identifiers cannot be bound as parameters; quoteIdent doubles any
// embedded backtick so a name harvested from system.tables cannot break
// out of the quoted identifier. The explicit "contains backtick" rejection
// below is defense-in-depth — in a real ClickHouse deployment system.tables
// will not contain such names, but rejecting them outright keeps parity
// with the postgres/mysql writers and makes the safety property obvious.
func (w *Writer) DropAllTables(ctx context.Context) error {
	if w.dryRun {
		return nil
	}
	if w.db == nil {
		return fmt.Errorf("no database connection")
	}

	views, err := w.listCleanupNames(ctx, currentDatabaseCleanupViewsQuery, "views")
	if err != nil {
		return err
	}
	if err := w.dropCleanupObjects(ctx, "VIEW", views); err != nil {
		return err
	}

	// After the view drops, not before: see the ordering note above.
	tables, err := w.listCleanupNames(ctx, currentDatabaseCleanupTablesQuery, "tables")
	if err != nil {
		return err
	}
	return w.dropCleanupObjects(ctx, "TABLE", tables)
}

func (w *Writer) listCleanupNames(ctx context.Context, query, kind string) ([]string, error) {
	rows, err := w.db.QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("clickhouse: list %s: %w", kind, err)
	}
	defer rows.Close()

	var names []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, fmt.Errorf("clickhouse: scan %s name: %w", kind, err)
		}
		names = append(names, name)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("clickhouse: iterate %s: %w", kind, err)
	}
	return names, nil
}

func (w *Writer) dropCleanupObjects(ctx context.Context, dropKind string, names []string) error {
	for _, name := range names {
		if strings.Contains(name, "`") {
			return fmt.Errorf(
				"clickhouse: refusing to drop %s %q: name contains a backtick",
				strings.ToLower(dropKind),
				name,
			)
		}
		if err := w.ExecuteSQL(
			ctx,
			"DROP "+dropKind+" IF EXISTS "+quoteIdent(name)+" SYNC",
		); err != nil {
			return err
		}
	}
	return nil
}

// DropDatabaseRealm removes every persistent object from the explicitly
// configured ClickHouse database. Unlike DropAllTables, this stronger cleanup
// contract does not depend on the connection's current database.
func (w *Writer) DropDatabaseRealm(ctx context.Context) error {
	if w.dryRun {
		return nil
	}
	if w.db == nil {
		return fmt.Errorf("no database connection")
	}
	if w.schema == "" {
		return fmt.Errorf("clickhouse: database-realm cleanup requires a configured database")
	}
	if slices.Contains(protectedClickHouseDatabases, w.schema) {
		return fmt.Errorf("clickhouse: refusing database-realm cleanup of protected database %q", w.schema)
	}
	if err := w.validateDatabaseRealmTarget(ctx); err != nil {
		return err
	}

	objects, err := w.listDatabaseRealmObjects(ctx)
	if err != nil {
		return err
	}
	for _, object := range orderDatabaseRealmObjects(objects) {
		if err := w.ExecuteSQL(ctx, object.dropSQL(w.schema)); err != nil {
			return err
		}
	}

	if err := w.checkDatabaseRealmPrivileges(ctx); err != nil {
		return fmt.Errorf("clickhouse: verify database %q remains fully visible: %w", w.schema, err)
	}
	remaining, err := w.listDatabaseRealmObjects(ctx)
	if err != nil {
		return fmt.Errorf("clickhouse: verify database %q is empty: %w", w.schema, err)
	}
	if len(remaining) != 0 {
		return fmt.Errorf(
			"clickhouse: database-realm cleanup left persistent objects in %q: %s",
			w.schema,
			describeDatabaseRealmObjects(remaining),
		)
	}
	return nil
}

func (w *Writer) validateDatabaseRealmTarget(ctx context.Context) error {
	var version string
	if err := w.db.QueryRowContext(ctx, databaseRealmVersionQuery).Scan(&version); err != nil {
		return fmt.Errorf("clickhouse: inspect server version: %w", err)
	}
	checkGrants, err := supportsCheckGrant(version)
	if err != nil {
		return err
	}
	if !checkGrants {
		return fmt.Errorf(
			"clickhouse: refusing database-realm cleanup on server version %q: "+
				"ClickHouse 24.11 or newer is required to prove complete catalog visibility with CHECK GRANT",
			version,
		)
	}
	if err := w.checkDatabaseRealmPrivileges(ctx); err != nil {
		return err
	}

	var engine string
	if err := w.db.QueryRowContext(ctx, databaseRealmEngineQuery, w.schema).Scan(&engine); err != nil {
		return fmt.Errorf("clickhouse: inspect database %q: %w", w.schema, err)
	}
	if !slices.Contains(databaseRealmCleanupEngines, engine) {
		return fmt.Errorf(
			"clickhouse: refusing to clean database %q with unsupported engine %q",
			w.schema,
			engine,
		)
	}

	temporaryObjects, err := w.listTemporaryObjects(ctx)
	if err != nil {
		return err
	}
	if len(temporaryObjects) != 0 {
		return fmt.Errorf(
			"clickhouse: refusing database-realm cleanup while session-temporary objects exist: %q",
			temporaryObjects,
		)
	}
	return nil
}

func supportsCheckGrant(version string) (bool, error) {
	var major int
	var minor int
	count, err := fmt.Sscanf(version, "%d.%d", &major, &minor)
	if err != nil || count != 2 {
		return false, fmt.Errorf("clickhouse: cannot parse server version %q", version)
	}
	return major > 24 || major == 24 && minor >= 11, nil
}

func (w *Writer) checkDatabaseRealmPrivileges(ctx context.Context) error {
	query := "CHECK GRANT SHOW DATABASES, SHOW TABLES, DROP TABLE, DROP VIEW, " +
		"DROP DICTIONARY ON " + quoteIdent(w.schema) + ".*"
	var granted uint8
	if err := w.db.QueryRowContext(ctx, query).Scan(&granted); err != nil {
		return fmt.Errorf("clickhouse: check database-realm cleanup privileges for %q: %w", w.schema, err)
	}
	if granted != 1 {
		return fmt.Errorf(
			"clickhouse: database-realm cleanup requires SHOW DATABASES, SHOW TABLES, "+
				"DROP TABLE, DROP VIEW, and DROP DICTIONARY on %s.*",
			quoteIdent(w.schema),
		)
	}
	if err := w.db.QueryRowContext(ctx, databaseRealmGlobalPrivilegesQuery).Scan(&granted); err != nil {
		return fmt.Errorf("clickhouse: check global catalog visibility: %w", err)
	}
	if granted != 1 {
		return fmt.Errorf(
			"clickhouse: database-realm cleanup requires SHOW DATABASES and SHOW TABLES on *.* " +
				"to prove that external dependencies are absent",
		)
	}
	return nil
}

func (w *Writer) listTemporaryObjects(ctx context.Context) ([]string, error) {
	rows, err := w.db.QueryContext(ctx, databaseRealmTemporaryObjectsQuery)
	if err != nil {
		return nil, fmt.Errorf("clickhouse: list session-temporary objects: %w", err)
	}
	defer rows.Close()

	var objects []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, fmt.Errorf("clickhouse: scan session-temporary object: %w", err)
		}
		objects = append(objects, name)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("clickhouse: iterate session-temporary objects: %w", err)
	}
	return objects, nil
}

func (w *Writer) listDatabaseRealmObjects(ctx context.Context) ([]databaseRealmObject, error) {
	if err := w.rejectExternalDatabaseRealmDependencies(ctx); err != nil {
		return nil, err
	}
	rows, err := w.db.QueryContext(ctx, databaseRealmObjectsQuery, w.schema)
	if err != nil {
		return nil, fmt.Errorf("clickhouse: list persistent objects in database %q: %w", w.schema, err)
	}
	defer rows.Close()

	var objects []databaseRealmObject
	for rows.Next() {
		var object databaseRealmObject
		if err := rows.Scan(&object.name, &object.engine, &object.createSQL); err != nil {
			return nil, fmt.Errorf(
				"clickhouse: scan persistent object in database %q: %w",
				w.schema,
				err,
			)
		}
		object.kind = classifyDatabaseRealmObject(object.engine, object.createSQL)
		if object.kind == databaseRealmObjectUnknown {
			return nil, fmt.Errorf(
				"clickhouse: refusing to clean database %q: cannot safely classify "+
					"persistent object %q with engine %q and creation statement %q",
				w.schema,
				object.name,
				object.engine,
				object.createSQL,
			)
		}
		objects = append(objects, object)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf(
			"clickhouse: iterate persistent objects in database %q: %w",
			w.schema,
			err,
		)
	}
	return objects, nil
}

func (w *Writer) rejectExternalDatabaseRealmDependencies(ctx context.Context) error {
	var database string
	var name string
	var engine string
	err := w.db.QueryRowContext(
		ctx,
		databaseRealmExternalDependenciesQuery,
		w.schema,
	).Scan(&database, &name, &engine)
	if errors.Is(err, sql.ErrNoRows) {
		return nil
	}
	if err != nil {
		return fmt.Errorf(
			"clickhouse: inspect external dependencies on database %q: %w",
			w.schema,
			err,
		)
	}
	return fmt.Errorf(
		"clickhouse: refusing to clean database %q: external object %s with engine %q "+
			"may depend on the cleanup realm",
		w.schema,
		quoteQualifiedIdent(database, name),
		engine,
	)
}

func classifyDatabaseRealmObject(engine, createSQL string) databaseRealmObjectKind {
	createKind := classifyDatabaseRealmCreateStatement(createSQL)
	switch createKind {
	case databaseRealmObjectView:
		if engine == "View" {
			return createKind
		}
	case databaseRealmObjectMaterializedView:
		if engine == "MaterializedView" {
			return createKind
		}
	case databaseRealmObjectLiveView:
		if engine == "LiveView" {
			return createKind
		}
	case databaseRealmObjectWindowView:
		if engine == "WindowView" {
			return createKind
		}
	case databaseRealmObjectDictionary:
		if engine == "Dictionary" {
			return createKind
		}
	case databaseRealmObjectTable:
		if !isDatabaseRealmViewEngine(engine) {
			return createKind
		}
	}
	return databaseRealmObjectUnknown
}

func classifyDatabaseRealmCreateStatement(createSQL string) databaseRealmObjectKind {
	fields := strings.Fields(strings.ToUpper(createSQL))
	if len(fields) < 2 || fields[0] != "CREATE" {
		return databaseRealmObjectUnknown
	}

	objectIndex := 1
	if len(fields) >= 4 && fields[1] == "OR" && fields[2] == "REPLACE" {
		objectIndex = 3
	}
	if len(fields) <= objectIndex {
		return databaseRealmObjectUnknown
	}

	switch fields[objectIndex] {
	case "LIVE":
		if len(fields) > objectIndex+1 && fields[objectIndex+1] == "VIEW" {
			return databaseRealmObjectLiveView
		}
	case "MATERIALIZED":
		if len(fields) > objectIndex+1 && fields[objectIndex+1] == "VIEW" {
			return databaseRealmObjectMaterializedView
		}
	case "WINDOW":
		if len(fields) > objectIndex+1 && fields[objectIndex+1] == "VIEW" {
			return databaseRealmObjectWindowView
		}
	case "VIEW":
		return databaseRealmObjectView
	case "DICTIONARY":
		return databaseRealmObjectDictionary
	case "TABLE":
		return databaseRealmObjectTable
	}
	return databaseRealmObjectUnknown
}

func isDatabaseRealmViewEngine(engine string) bool {
	return slices.Contains([]string{"LiveView", "MaterializedView", "View", "WindowView"}, engine)
}

func isDatabaseRealmViewKind(kind databaseRealmObjectKind) bool {
	switch kind {
	case databaseRealmObjectView,
		databaseRealmObjectMaterializedView,
		databaseRealmObjectLiveView,
		databaseRealmObjectWindowView:
		return true
	case databaseRealmObjectUnknown,
		databaseRealmObjectDictionary,
		databaseRealmObjectTable:
		return false
	}
	return false
}

func orderDatabaseRealmObjects(objects []databaseRealmObject) []databaseRealmObject {
	ordered := make([]databaseRealmObject, 0, len(objects))
	for _, object := range objects {
		if isDatabaseRealmViewKind(object.kind) {
			ordered = append(ordered, object)
		}
	}
	for _, object := range objects {
		if object.kind == databaseRealmObjectTable && object.engine == "Dictionary" {
			ordered = append(ordered, object)
		}
	}
	for _, object := range objects {
		if object.kind == databaseRealmObjectDictionary {
			ordered = append(ordered, object)
		}
	}
	for _, object := range objects {
		if object.kind == databaseRealmObjectTable && object.engine != "Dictionary" {
			ordered = append(ordered, object)
		}
	}
	return ordered
}

func (o databaseRealmObject) dropSQL(database string) string {
	dropKind := "TABLE"
	switch o.kind {
	case databaseRealmObjectView,
		databaseRealmObjectMaterializedView,
		databaseRealmObjectLiveView,
		databaseRealmObjectWindowView:
		dropKind = "VIEW"
	case databaseRealmObjectDictionary:
		dropKind = "DICTIONARY"
	}
	return "DROP " + dropKind + " IF EXISTS " +
		quoteQualifiedIdent(database, o.name) + " SYNC"
}

func describeDatabaseRealmObjects(objects []databaseRealmObject) string {
	descriptions := make([]string, 0, len(objects))
	for _, object := range objects {
		descriptions = append(descriptions, fmt.Sprintf("%s (%s)", object.name, object.engine))
	}
	return strings.Join(descriptions, ", ")
}

// SetDryRun toggles dry-run mode.
func (w *Writer) SetDryRun(dryRun bool) { w.dryRun = dryRun }

// IsDryRun reports whether dry-run mode is active.
func (w *Writer) IsDryRun() bool { return w.dryRun }

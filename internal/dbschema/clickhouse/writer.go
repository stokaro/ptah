package clickhouse

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"slices"
	"strings"

	"github.com/stokaro/ptah/core/platform"
	"github.com/stokaro/ptah/dbschema/types"
	"github.com/stokaro/ptah/internal/sqlident"
	"github.com/stokaro/ptah/internal/sqlrunner"
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

var databaseRealmCleanupEngines = []string{
	"Atomic",
	"Lazy",
	"Memory",
	"Ordinary",
	"Replicated",
	"Shared",
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

// DropAllTables drops every base table in the configured database.
// Uses DROP TABLE … SYNC so subsequent CREATE TABLE statements don't race
// against the async drop.
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

	rows, err := w.db.QueryContext(ctx, `
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
	`)
	if err != nil {
		return fmt.Errorf("clickhouse: list tables: %w", err)
	}
	defer rows.Close()

	var tables []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return fmt.Errorf("clickhouse: scan table name: %w", err)
		}
		tables = append(tables, name)
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("clickhouse: iterate tables: %w", err)
	}

	for _, name := range tables {
		if strings.Contains(name, "`") {
			return fmt.Errorf("clickhouse: refusing to drop table %q: name contains a backtick", name)
		}
		if err := w.ExecuteSQL(ctx, "DROP TABLE IF EXISTS "+quoteIdent(name)+" SYNC"); err != nil {
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
	checkGrants, err := w.validateDatabaseRealmTarget(ctx)
	if err != nil {
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

	if checkGrants {
		if err := w.checkDatabaseRealmPrivileges(ctx); err != nil {
			return fmt.Errorf("clickhouse: verify database %q remains fully visible: %w", w.schema, err)
		}
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

func (w *Writer) validateDatabaseRealmTarget(ctx context.Context) (bool, error) {
	var version string
	if err := w.db.QueryRowContext(ctx, databaseRealmVersionQuery).Scan(&version); err != nil {
		return false, fmt.Errorf("clickhouse: inspect server version: %w", err)
	}
	checkGrants, err := supportsCheckGrant(version)
	if err != nil {
		return false, err
	}
	// CHECK GRANT was added in ClickHouse 24.11. Older supported servers
	// still get preclassification and residual checks, but cannot prove
	// effective grants without parsing role-aware SHOW GRANTS output.
	if checkGrants {
		if err := w.checkDatabaseRealmPrivileges(ctx); err != nil {
			return false, err
		}
	}

	var engine string
	if err := w.db.QueryRowContext(ctx, databaseRealmEngineQuery, w.schema).Scan(&engine); err != nil {
		return false, fmt.Errorf("clickhouse: inspect database %q: %w", w.schema, err)
	}
	if !slices.Contains(databaseRealmCleanupEngines, engine) {
		return false, fmt.Errorf(
			"clickhouse: refusing to clean database %q with unsupported engine %q",
			w.schema,
			engine,
		)
	}

	temporaryObjects, err := w.listTemporaryObjects(ctx)
	if err != nil {
		return false, err
	}
	if len(temporaryObjects) != 0 {
		return false, fmt.Errorf(
			"clickhouse: refusing database-realm cleanup while session-temporary objects exist: %q",
			temporaryObjects,
		)
	}
	return checkGrants, nil
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

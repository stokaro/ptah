package postgres

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"strings"
	"sync"

	"go.5x5.cz/ptah/core/goschema"
	"go.5x5.cz/ptah/core/platform"
	"go.5x5.cz/ptah/core/platform/capability"
	"go.5x5.cz/ptah/dbschema/types"
	"go.5x5.cz/ptah/internal/sqlident"
	"go.5x5.cz/ptah/internal/sqlrunner"
)

var protectedPostgresDatabases = []string{
	"postgres",
	"template0",
	"template1",
}

var protectedCockroachDatabases = []string{
	"defaultdb",
	"postgres",
	"system",
}

var protectedYugabyteDatabases = []string{
	"postgres",
	"system_platform",
	"template0",
	"template1",
	"yugabyte",
}

func quoteIdent(name string) string {
	return sqlident.Quote(platform.Postgres, name)
}

// PostgreSQLWriter writes schemas to PostgreSQL databases
type PostgreSQLWriter struct {
	db     sqlrunner.Runner
	schema string
	dryRun bool
	// caps decides whether cleanup may take a transaction. The writer cannot
	// ask the server: the answer has to be known BEFORE the transaction is
	// opened, and on a server that refuses DDL there the probe would be the
	// failure it is trying to avoid. The connection layer already resolves the
	// set, and the reader beside this writer already takes it.
	caps capability.Capabilities
}

type postgresTransactionWriter struct {
	mu     sync.Mutex
	tx     *sql.Tx
	schema string
	dryRun bool
}

// NewPostgreSQLWriter creates a new PostgreSQL schema writer
func NewPostgreSQLWriter(db *sql.DB, schema string) *PostgreSQLWriter {
	if db == nil {
		return NewPostgreSQLWriterForRunner(nil, schema)
	}
	return NewPostgreSQLWriterForRunner(db, schema)
}

// NewPostgreSQLWriterForRunner creates a writer bound to a pool or pinned
// database session.
//
// It assumes the server accepts DDL inside a transaction, which every
// PostgreSQL-family engine except Spanner does. Callers that know the dialect
// should use [NewPostgreSQLWriterForRunnerWithCapabilities].
func NewPostgreSQLWriterForRunner(
	runner sqlrunner.Runner,
	schema string,
) *PostgreSQLWriter {
	return NewPostgreSQLWriterForRunnerWithCapabilities(
		runner, schema, capability.Capabilities{
			capability.DDLInsideTransaction: true,
			capability.CatalogRecursiveCTE:  true,
		})
}

// NewPostgreSQLWriterForRunnerWithCapabilities creates a writer that knows what
// the server accepts.
func NewPostgreSQLWriterForRunnerWithCapabilities(
	runner sqlrunner.Runner,
	schema string,
	caps capability.Capabilities,
) *PostgreSQLWriter {
	if schema == "" {
		schema = "public"
	}
	return &PostgreSQLWriter{
		db:     runner,
		schema: schema,
		caps:   caps,
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

// SchemaQueryRunner exposes the live transaction to readers and session-state
// statements that must observe the same locked catalog snapshot. Dry-run and
// closed transactions deliberately expose no runner.
func (w *postgresTransactionWriter) SchemaQueryRunner() sqlrunner.Runner {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.dryRun || w.tx == nil {
		return nil
	}
	return postgresTransactionRunner{tx: w.tx}
}

type postgresTransactionRunner struct {
	tx *sql.Tx
}

func (postgresTransactionRunner) BeginTx(context.Context, *sql.TxOptions) (*sql.Tx, error) {
	return nil, errors.New("nested transaction is unavailable for a transaction-scoped schema reader")
}

func (r postgresTransactionRunner) Exec(query string, args ...any) (sql.Result, error) {
	return r.tx.Exec(query, args...)
}

func (r postgresTransactionRunner) ExecContext(
	ctx context.Context,
	query string,
	args ...any,
) (sql.Result, error) {
	return r.tx.ExecContext(ctx, query, args...)
}

func (r postgresTransactionRunner) QueryContext(
	ctx context.Context,
	query string,
	args ...any,
) (*sql.Rows, error) {
	return r.tx.QueryContext(ctx, query, args...)
}

func (r postgresTransactionRunner) Query(query string, args ...any) (*sql.Rows, error) {
	return r.tx.Query(query, args...)
}

func (r postgresTransactionRunner) QueryRow(query string, args ...any) *sql.Row {
	return r.tx.QueryRow(query, args...)
}

func (r postgresTransactionRunner) QueryRowContext(
	ctx context.Context,
	query string,
	args ...any,
) *sql.Row {
	return r.tx.QueryRowContext(ctx, query, args...)
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

type postgresCleanupObject struct {
	Kind      string
	Schema    string
	Name      string
	Statement string
}

type postgresCleanupCapabilities struct {
	retryFailedDDL           bool
	lockManagedRelations     bool
	inspectPartitionEdges    bool
	inspectDatabaseArtifacts bool
	cleanupLargeObjects      bool
	preservePublicSchema     bool
	protectedDatabases       []string
	systemExtensions         []string
}

// cleanupConn is the part of a database handle the schema cleanup uses.
//
// Both *sql.Tx and *sql.DB satisfy it, which is the whole point: the cleanup
// runs inside a transaction where the server accepts DDL there, and directly on
// the pool where it does not. Spanner refuses DDL in an explicit transaction --
// `DDL statements are only allowed outside explicit transactions`, SQLSTATE
// 25000 -- so a cleanup that always took one could never finish (#1811).
type cleanupConn interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
}

type postgresCleanupScope struct {
	schemas                []string
	extensionNamespaceJoin string
	systemExtensions       []string
}

func inspectCleanupCapabilities(
	ctx context.Context,
	tx cleanupConn,
) (postgresCleanupCapabilities, error) {
	var version string
	if err := tx.QueryRowContext(ctx, "SELECT version()").Scan(&version); err != nil {
		return postgresCleanupCapabilities{}, fmt.Errorf(
			"failed to inspect PostgreSQL-family cleanup capabilities: %w",
			err,
		)
	}

	version = strings.ToLower(version)
	switch {
	case strings.Contains(version, "cockroachdb"):
		return postgresCleanupCapabilities{
			preservePublicSchema: true,
			protectedDatabases:   protectedCockroachDatabases,
		}, nil
	case strings.Contains(version, "yugabytedb"),
		strings.Contains(version, "yugabyte"),
		strings.Contains(version, "-yb-"):
		return postgresCleanupCapabilities{
			retryFailedDDL:           true,
			inspectPartitionEdges:    true,
			inspectDatabaseArtifacts: true,
			protectedDatabases:       protectedYugabyteDatabases,
			systemExtensions:         []string{"pg_stat_statements", "plpgsql"},
		}, nil
	default:
		isPostgreSQL := strings.Contains(version, "postgresql")
		return postgresCleanupCapabilities{
			retryFailedDDL:           true,
			lockManagedRelations:     isPostgreSQL,
			inspectPartitionEdges:    true,
			inspectDatabaseArtifacts: isPostgreSQL,
			cleanupLargeObjects:      isPostgreSQL,
			protectedDatabases:       protectedPostgresDatabases,
			systemExtensions:         []string{"plpgsql"},
		}, nil
	}
}

func (c postgresCleanupCapabilities) dropObjects(
	ctx context.Context,
	tx cleanupConn,
	objects []postgresCleanupObject,
) error {
	if c.retryFailedDDL {
		// The retry loop drops each object inside a SAVEPOINT, so it needs a
		// real transaction. The capability is only ever set on the path that
		// opened one; the check is here so a future caller that forgets gets a
		// diagnostic rather than a panic.
		sqlTx, ok := tx.(*sql.Tx)
		if !ok {
			return fmt.Errorf("cleanup retry needs a transaction, which this server does not accept DDL in")
		}
		return dropCleanupObjects(ctx, sqlTx, objects)
	}
	return dropCleanupObjectsOnce(ctx, tx, objects)
}

func postgresSchemaCleanupScope(schemas []string) postgresCleanupScope {
	return postgresCleanupScope{
		schemas:                schemas,
		extensionNamespaceJoin: "JOIN managed_namespaces n ON n.oid = e.extnamespace",
	}
}

func postgresDatabaseCleanupScope(
	schemas,
	systemExtensions []string,
) postgresCleanupScope {
	return postgresCleanupScope{
		schemas:                schemas,
		extensionNamespaceJoin: "JOIN pg_namespace n ON n.oid = e.extnamespace",
		systemExtensions:       systemExtensions,
	}
}

type postgresSchemaPrivilege struct {
	grantee     string
	privilege   string
	grantOption bool
}

type postgresSchemaMetadata struct {
	exists           bool
	owner            string
	aclIsDefault     bool
	commentStatement string
	privileges       []postgresSchemaPrivilege
}

type postgresDatabaseCleanupPlan struct {
	capabilities postgresCleanupCapabilities
	rootMetadata postgresSchemaMetadata
	schemas      []string
	objects      []postgresCleanupObject
}

func (w *PostgreSQLWriter) rejectSchemaScopedExtensions(ctx context.Context, tx cleanupConn) error {
	// DROP EXTENSION removes every member regardless of the member's schema.
	// Refuse it here because a schema-scoped cleanup cannot safely own that
	// database-wide operation, even when DROP EXTENSION uses RESTRICT.
	var count int
	var first string
	err := tx.QueryRowContext(ctx, `
		SELECT COUNT(*), COALESCE(MIN(e.extname), '')
		FROM pg_extension e
		JOIN pg_namespace n ON n.oid = e.extnamespace
		WHERE n.nspname = $1
	`, w.schema).Scan(&count, &first)
	if err != nil {
		return fmt.Errorf("failed to inspect schema-owned extensions: %w", err)
	}
	if count > 0 {
		return fmt.Errorf(
			"refusing to clean schema %q: extension %q is owned by it; "+
				"schema-scoped cleanup cannot prove that every extension member is confined to the schema",
			w.schema,
			first,
		)
	}
	return nil
}

//nolint:funlen // The dependency-ordered catalog query is kept in one auditable unit.
func (w *PostgreSQLWriter) collectAllObjects(
	ctx context.Context,
	tx cleanupConn,
	scope postgresCleanupScope,
) ([]postgresCleanupObject, error) {
	if len(scope.schemas) == 0 {
		return nil, nil
	}

	extensionFilter := ""
	args := stringsToAny(scope.schemas)
	if len(scope.systemExtensions) > 0 {
		extensionFilter = "WHERE e.extname NOT IN (" +
			postgresPlaceholdersFrom(len(args)+1, len(scope.systemExtensions)) + ")"
		args = append(args, stringsToAny(scope.systemExtensions)...)
	}
	query := strings.ReplaceAll(`
		WITH {{RECURSIVE}}
		managed_namespaces AS (
			SELECT n.oid, n.nspname
			FROM pg_namespace n
			WHERE n.nspname IN ({{SCHEMA_PLACEHOLDERS}})
		),
		managed_views AS (
			SELECT c.oid
			FROM pg_class c
			JOIN managed_namespaces n ON n.oid = c.relnamespace
			AND c.relkind IN ('v', 'm')
		),
		view_dependencies AS (
			{{VIEW_DEPENDENCIES}}
		),
		view_depths AS (
			SELECT view.oid, 0 AS depth
			FROM managed_views view
			WHERE NOT EXISTS (
				SELECT 1
				FROM view_dependencies dependency
				WHERE dependency.dependent_oid = view.oid
			)

			{{VIEW_DEPTH_RECURSION}}
		),
		view_order AS (
			SELECT oid, MAX(depth) AS depth
			FROM view_depths
			GROUP BY oid
		),
		cleanup_objects AS (
			SELECT
				-10 AS priority,
				0 AS dependency_depth,
				'extension'::text AS object_kind,
				n.nspname AS object_schema,
				e.extname AS object_name,
				format('DROP EXTENSION IF EXISTS %I RESTRICT', e.extname) AS drop_statement
			FROM pg_extension e
			{{EXTENSION_NAMESPACE_JOIN}}
			{{SYSTEM_EXTENSION_FILTER}}

			UNION ALL

			SELECT
				0 AS priority,
				0 AS dependency_depth,
				'constraint'::text AS object_kind,
				n.nspname AS object_schema,
				con.conname AS object_name,
				format(
					'ALTER TABLE %I.%I DROP CONSTRAINT IF EXISTS %I RESTRICT',
					n.nspname,
					c.relname,
					con.conname
				) AS drop_statement
			FROM pg_constraint con
			JOIN pg_class c ON c.oid = con.conrelid
			JOIN managed_namespaces n ON n.oid = c.relnamespace
			WHERE con.contype = 'f'

			UNION ALL

			SELECT
				CASE c.relkind
					WHEN 'v' THEN 10
					WHEN 'm' THEN 10
					WHEN 'S' THEN 30
					ELSE 20
				END,
				COALESCE(view_order.depth, 0),
				CASE c.relkind
					WHEN 'v' THEN 'view'
					WHEN 'm' THEN 'materialized view'
					WHEN 'f' THEN 'foreign table'
					WHEN 'S' THEN 'sequence'
					ELSE 'table'
				END,
				n.nspname,
				c.relname,
				format(
					CASE c.relkind
						WHEN 'v' THEN 'DROP VIEW IF EXISTS %I.%I RESTRICT'
						WHEN 'm' THEN 'DROP MATERIALIZED VIEW IF EXISTS %I.%I RESTRICT'
						WHEN 'f' THEN 'DROP FOREIGN TABLE IF EXISTS %I.%I RESTRICT'
						WHEN 'S' THEN 'DROP SEQUENCE IF EXISTS %I.%I RESTRICT'
						ELSE 'DROP TABLE IF EXISTS %I.%I RESTRICT'
					END,
					n.nspname,
					c.relname
				)
			FROM pg_class c
			JOIN managed_namespaces n ON n.oid = c.relnamespace
			LEFT JOIN view_order ON view_order.oid = c.oid
			WHERE c.relkind IN ('r', 'p', 'v', 'm', 'f', 'S')

			UNION ALL

			SELECT
				40,
				0,
				CASE p.prokind
					WHEN 'p' THEN 'procedure'
					WHEN 'a' THEN 'aggregate'
					ELSE 'function'
				END,
				n.nspname,
				p.proname,
				format(
					'DROP %s IF EXISTS %I.%I(%s) RESTRICT',
					CASE p.prokind
						WHEN 'p' THEN 'PROCEDURE'
						WHEN 'a' THEN 'AGGREGATE'
						ELSE 'FUNCTION'
					END,
					n.nspname,
					p.proname,
					pg_get_function_identity_arguments(p.oid)
				)
			FROM pg_proc p
			JOIN managed_namespaces n ON n.oid = p.pronamespace
			WHERE p.prokind IN ('f', 'p', 'a', 'w')
			  AND NOT EXISTS (
				SELECT 1
				FROM pg_depend d
				WHERE d.classid = 'pg_proc'::regclass
				  AND d.objid = p.oid
				  AND d.deptype = 'i'
			  )

			UNION ALL

			SELECT
				50,
				0,
				'type',
				n.nspname,
				t.typname,
				format('DROP TYPE IF EXISTS %I.%I RESTRICT', n.nspname, t.typname)
			FROM pg_type t
			JOIN managed_namespaces n ON n.oid = t.typnamespace
			LEFT JOIN pg_class c ON c.oid = t.typrelid
			WHERE (
				t.typtype IN ('e', 'd', 'r')
				OR (t.typtype = 'c' AND c.relkind = 'c')
			  )

			UNION ALL

			SELECT
				60,
				0,
				'collation',
				n.nspname,
				c.collname,
				format(
					'DROP COLLATION IF EXISTS %I.%I RESTRICT',
					n.nspname,
					c.collname
				)
			FROM pg_collation c
			JOIN managed_namespaces n ON n.oid = c.collnamespace

			UNION ALL

			SELECT DISTINCT
				70,
				0,
				'default privilege',
				n.nspname,
				format(
					'%s/%s/%s',
					pg_get_userbyid(d.defaclrole),
					d.defaclobjtype,
					CASE acl.grantee
						WHEN 0 THEN 'PUBLIC'
						ELSE pg_get_userbyid(acl.grantee)
					END
				),
				format(
					'ALTER DEFAULT PRIVILEGES FOR ROLE %I IN SCHEMA %I ' ||
					'REVOKE ALL PRIVILEGES ON %s FROM %s',
					pg_get_userbyid(d.defaclrole),
					n.nspname,
					CASE d.defaclobjtype
						WHEN 'r' THEN 'TABLES'
						WHEN 'S' THEN 'SEQUENCES'
						WHEN 'f' THEN 'FUNCTIONS'
						WHEN 'T' THEN 'TYPES'
					END,
					CASE acl.grantee
						WHEN 0 THEN 'PUBLIC'
						ELSE format('%I', pg_get_userbyid(acl.grantee))
					END
				)
			FROM pg_default_acl d
			JOIN managed_namespaces n ON n.oid = d.defaclnamespace
			CROSS JOIN LATERAL aclexplode(d.defaclacl) acl
			WHERE d.defaclobjtype IN ('r', 'S', 'f', 'T')
		)
		SELECT object_kind, object_schema, object_name, drop_statement
		FROM cleanup_objects
		ORDER BY priority, dependency_depth DESC, object_schema, object_kind, object_name
	`, "{{SCHEMA_PLACEHOLDERS}}", postgresPlaceholders(len(scope.schemas)))
	query = strings.ReplaceAll(query, "{{EXTENSION_NAMESPACE_JOIN}}", scope.extensionNamespaceJoin)
	query = strings.ReplaceAll(query, "{{SYSTEM_EXTENSION_FILTER}}", extensionFilter)
	query = applyViewOrderingShape(query, viewOrderingShapeFor(w.caps))
	rows, err := tx.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("failed to query schema objects: %w", err)
	}
	defer rows.Close()

	var objects []postgresCleanupObject
	for rows.Next() {
		var object postgresCleanupObject
		if err := rows.Scan(&object.Kind, &object.Schema, &object.Name, &object.Statement); err != nil {
			return nil, fmt.Errorf("failed to scan schema object: %w", err)
		}
		objects = append(objects, object)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("failed to iterate schema objects: %w", err)
	}
	return objects, nil
}

func postgresPlaceholders(count int) string {
	return postgresPlaceholdersFrom(1, count)
}

func postgresPlaceholdersFrom(start, count int) string {
	placeholders := make([]string, count)
	for i := range count {
		placeholders[i] = fmt.Sprintf("$%d", start+i)
	}
	return strings.Join(placeholders, ", ")
}

func stringsToAny(values []string) []any {
	args := make([]any, len(values))
	for i := range values {
		args[i] = values[i]
	}
	return args
}

// viewOrderingShape names the two forms the cleanup query takes.
type viewOrderingShape int

const (
	// recursiveViewOrdering walks pg_rewrite so a view is dropped after the
	// views it selects from.
	recursiveViewOrdering viewOrderingShape = iota
	// flatViewOrdering asks neither question, for a server that cannot answer
	// them.
	flatViewOrdering
)

// viewOrderingShapeFor picks the shape a server can answer.
func viewOrderingShapeFor(caps capability.Capabilities) viewOrderingShape {
	if caps.Has(capability.CatalogRecursiveCTE) {
		return recursiveViewOrdering
	}
	return flatViewOrdering
}

// flatViewDependencies stands in for the walk, keeping the CTE present and
// type-correct so everything downstream joins against it unchanged while
// pg_rewrite stays out of the query entirely.
const flatViewDependencies = `SELECT c.oid AS dependent_oid, c.oid AS referenced_oid
			FROM pg_class c
			WHERE false`

// realViewDependencies is the pg_rewrite walk that pairs a view with the views
// it selects from. It is only substituted where the server has views at all.
const realViewDependencies = `SELECT
				dependent.oid AS dependent_oid,
				referenced.oid AS referenced_oid
			FROM pg_rewrite rewrite
			JOIN pg_class dependent ON dependent.oid = rewrite.ev_class
			JOIN managed_namespaces dependent_namespace
				ON dependent_namespace.oid = dependent.relnamespace
			JOIN pg_depend dependency
				ON dependency.classid = 'pg_rewrite'::regclass
			   AND dependency.objid = rewrite.oid
			   AND dependency.refclassid = 'pg_class'::regclass
			JOIN pg_class referenced ON referenced.oid = dependency.refobjid
			JOIN managed_namespaces referenced_namespace
				ON referenced_namespace.oid = referenced.relnamespace
			WHERE dependent.relkind IN ('v', 'm')
			  AND referenced.relkind IN ('v', 'm')
			  AND dependent.oid <> referenced.oid`

// applyViewOrderingShape fills the three placeholders that decide whether the
// cleanup query orders nested views recursively.
//
// The recursion exists only to drop a view after the views it selects from. A
// server with no views has nothing to order, and asking for the recursion there
// is not free: Cloud Spanner's PostgreSQL interface rewrites a catalog
// reference by prepending its own `WITH pg_class AS (...)`, which produces two
// WITH clauses and a syntax error when the query already opened with `WITH
// RECURSIVE`. Measured on the pinned emulator, `WITH RECURSIVE m AS (SELECT
// relname FROM pg_class) ...` fails while the same query without RECURSIVE
// succeeds (#1811).
//
// The stubs keep the CTEs present and type-correct so everything downstream
// joins against them unchanged, and keep pg_rewrite and pg_depend out of the
// query entirely on a server that has no views to describe.
func applyViewOrderingShape(query string, shape viewOrderingShape) string {
	if shape == flatViewOrdering {
		query = strings.ReplaceAll(query, "{{RECURSIVE}}", "")
		query = strings.ReplaceAll(query, "{{VIEW_DEPENDENCIES}}", flatViewDependencies)
		return strings.ReplaceAll(query, "{{VIEW_DEPTH_RECURSION}}", "")
	}
	recursive, dependencies, depthRecursion := "RECURSIVE", realViewDependencies, `
			UNION ALL

			SELECT dependency.dependent_oid, depth.depth + 1
			FROM view_depths depth
			JOIN view_dependencies dependency
				ON dependency.referenced_oid = depth.oid`
	query = strings.ReplaceAll(query, "{{RECURSIVE}}", recursive)
	query = strings.ReplaceAll(query, "{{VIEW_DEPENDENCIES}}", dependencies)
	return strings.ReplaceAll(query, "{{VIEW_DEPTH_RECURSION}}", depthRecursion)
}

func (w *PostgreSQLWriter) lockManagedRelations(
	ctx context.Context,
	tx cleanupConn,
	objects []postgresCleanupObject,
) error {
	relations := make([]string, 0, len(objects))
	for _, object := range objects {
		switch object.Kind {
		case "table", "foreign table":
			relations = append(relations, quoteIdent(object.Schema)+"."+quoteIdent(object.Name))
		}
	}
	if len(relations) == 0 {
		return nil
	}

	// Identifiers come from the catalog snapshot and are quoted individually;
	// LOCK TABLE does not support identifier bind parameters.
	// #nosec G202 -- LOCK TABLE cannot bind the individually quoted catalog identifiers.
	statement := "LOCK TABLE " + strings.Join(relations, ", ") + " IN ACCESS EXCLUSIVE MODE"
	if _, err := tx.ExecContext(ctx, statement); err != nil {
		return fmt.Errorf("failed to lock managed relations before cleanup: %w", err)
	}
	return nil
}

func (w *PostgreSQLWriter) rejectCrossSchemaPartitionEdges(ctx context.Context, tx cleanupConn) error {
	var parentSchema string
	var parentName string
	var childSchema string
	var childName string
	err := tx.QueryRowContext(ctx, `
		SELECT
			parent_namespace.nspname,
			parent.relname,
			child_namespace.nspname,
			child.relname
		FROM pg_inherits inheritance
		JOIN pg_class parent ON parent.oid = inheritance.inhparent
		JOIN pg_namespace parent_namespace ON parent_namespace.oid = parent.relnamespace
		JOIN pg_class child ON child.oid = inheritance.inhrelid
		JOIN pg_namespace child_namespace ON child_namespace.oid = child.relnamespace
		WHERE child.relispartition
		  AND (
			(parent_namespace.nspname = $1 AND child_namespace.nspname <> $1)
			OR (child_namespace.nspname = $1 AND parent_namespace.nspname <> $1)
		  )
		ORDER BY parent_namespace.nspname, parent.relname, child_namespace.nspname, child.relname
		LIMIT 1
	`, w.schema).Scan(&parentSchema, &parentName, &childSchema, &childName)
	if errors.Is(err, sql.ErrNoRows) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("failed to inspect partition relationships: %w", err)
	}

	return fmt.Errorf(
		"refusing to clean schema %q: partition %q.%q is attached to "+
			"partitioned table %q.%q across the schema boundary",
		w.schema,
		childSchema,
		childName,
		parentSchema,
		parentName,
	)
}

func tryDropCleanupObject(
	ctx context.Context,
	tx *sql.Tx,
	object postgresCleanupObject,
) (dropErr, controlErr error) {
	const savepoint = "ptah_cleanup_object"

	if _, err := tx.ExecContext(ctx, "SAVEPOINT "+savepoint); err != nil {
		return nil, fmt.Errorf("failed to create cleanup savepoint: %w", err)
	}

	if _, err := tx.ExecContext(ctx, object.Statement); err != nil {
		dropErr = fmt.Errorf("SQL execution failed: %w\nSQL: %s", err, object.Statement)
		if _, rollbackErr := tx.ExecContext(ctx, "ROLLBACK TO SAVEPOINT "+savepoint); rollbackErr != nil {
			controlErr = fmt.Errorf("failed to roll back cleanup savepoint: %w", rollbackErr)
		}
		if _, releaseErr := tx.ExecContext(ctx, "RELEASE SAVEPOINT "+savepoint); releaseErr != nil {
			controlErr = errors.Join(controlErr, fmt.Errorf("failed to release cleanup savepoint: %w", releaseErr))
		}
		return dropErr, controlErr
	}

	if _, err := tx.ExecContext(ctx, "RELEASE SAVEPOINT "+savepoint); err != nil {
		return nil, fmt.Errorf("failed to release cleanup savepoint: %w", err)
	}
	return nil, nil
}

func dropCleanupObjectsOnce(
	ctx context.Context,
	tx cleanupConn,
	objects []postgresCleanupObject,
) error {
	for _, object := range objects {
		if _, err := tx.ExecContext(ctx, object.Statement); err != nil {
			return fmt.Errorf(
				"failed to drop %s %s: SQL execution failed: %w\nSQL: %s",
				object.Kind,
				object.Name,
				err,
				object.Statement,
			)
		}
	}
	return nil
}

func dropCleanupObjects(
	ctx context.Context,
	tx *sql.Tx,
	objects []postgresCleanupObject,
) error {
	// RESTRICT remains the final authority for every drop. Savepoints let a
	// later internal dependent disappear before its dependency is retried,
	// while an external or unknown dependency eventually stops all progress
	// and causes the outer transaction to roll back.
	pending := objects
	for len(pending) > 0 {
		remaining := make([]postgresCleanupObject, 0, len(pending))
		var firstDropErr error

		for _, object := range pending {
			dropErr, controlErr := tryDropCleanupObject(ctx, tx, object)
			if controlErr != nil {
				return fmt.Errorf(
					"failed to drop %s %s: %w",
					object.Kind,
					object.Name,
					errors.Join(dropErr, controlErr),
				)
			}
			if dropErr != nil {
				if firstDropErr == nil {
					firstDropErr = fmt.Errorf("failed to drop %s %s: %w", object.Kind, object.Name, dropErr)
				}
				remaining = append(remaining, object)
			}
		}

		if len(remaining) == 0 {
			return nil
		}
		if len(remaining) == len(pending) {
			return firstDropErr
		}
		pending = remaining
	}
	return nil
}

// DropAllTables drops all user objects in the configured database schema.
func (w *PostgreSQLWriter) DropAllTables(ctx context.Context) error {
	return w.dropSchemaObjects(ctx)
}

// DropDatabaseRealm removes every user schema and recreates the configured
// root schema. CockroachDB's immutable public schema is preserved in place,
// but its user objects are removed and verified like every other realm object.
func (w *PostgreSQLWriter) DropDatabaseRealm(ctx context.Context) error {
	if w.dryRun {
		return nil
	}
	if w.db == nil {
		return fmt.Errorf("no database connection")
	}
	if isPostgresSystemSchema(w.schema) {
		return fmt.Errorf(
			"refusing to clean PostgreSQL database realm with system root schema %q",
			w.schema,
		)
	}
	return w.dropDatabaseRealm(ctx)
}

func rejectProtectedPostgresDatabase(
	ctx context.Context,
	tx *sql.Tx,
	protectedDatabases []string,
) error {
	var database string
	if err := tx.QueryRowContext(ctx, "SELECT current_database()").Scan(&database); err != nil {
		return fmt.Errorf("failed to inspect current PostgreSQL database for cleanup: %w", err)
	}
	if slices.Contains(protectedDatabases, strings.ToLower(database)) {
		return fmt.Errorf(
			"refusing to clean protected PostgreSQL-family database %q",
			database,
		)
	}
	return nil
}

func rejectPostgresDatabaseScopedArtifacts(ctx context.Context, tx *sql.Tx) error {
	var kind string
	var name string
	err := tx.QueryRowContext(ctx, `
		WITH database_scoped_artifacts AS (
			SELECT
				'publication'::text AS object_kind,
				publication.pubname AS object_name
			FROM pg_publication publication

			UNION ALL
			SELECT
				'subscription',
				subscription.subname
			FROM pg_subscription subscription
			WHERE subscription.subdbid = (
				SELECT database.oid
				FROM pg_database database
				WHERE database.datname = current_database()
			)

			UNION ALL
			SELECT
				'logical replication slot',
				slot.slot_name
			FROM pg_replication_slots slot
			WHERE slot.slot_type = 'logical'
			  AND slot.datoid = (
				SELECT database.oid
				FROM pg_database database
				WHERE database.datname = current_database()
			  )

			UNION ALL
			SELECT
				'event trigger',
				event_trigger.evtname
			FROM pg_event_trigger event_trigger
			WHERE NOT EXISTS (
				SELECT 1
				FROM pg_depend dependency
				WHERE dependency.classid = 'pg_event_trigger'::regclass
				  AND dependency.objid = event_trigger.oid
				  AND dependency.deptype = 'e'
			)

			UNION ALL
			SELECT
				'foreign-data wrapper',
				wrapper.fdwname
			FROM pg_foreign_data_wrapper wrapper
			WHERE NOT EXISTS (
				SELECT 1
				FROM pg_depend dependency
				WHERE dependency.classid = 'pg_foreign_data_wrapper'::regclass
				  AND dependency.objid = wrapper.oid
				  AND dependency.deptype = 'e'
			)

			UNION ALL
			SELECT
				'foreign server',
				server.srvname
			FROM pg_foreign_server server
			WHERE NOT EXISTS (
				SELECT 1
				FROM pg_depend dependency
				WHERE dependency.classid = 'pg_foreign_server'::regclass
				  AND dependency.objid = server.oid
				  AND dependency.deptype = 'e'
			)

			UNION ALL
			SELECT
				'user mapping',
				format(
					'%s@%s',
					COALESCE(role.rolname, 'PUBLIC'),
					server.srvname
				)
			FROM pg_user_mapping mapping
			LEFT JOIN pg_roles role ON role.oid = mapping.umuser
			JOIN pg_foreign_server server ON server.oid = mapping.umserver
			WHERE NOT EXISTS (
				SELECT 1
				FROM pg_depend dependency
				WHERE dependency.classid = 'pg_user_mapping'::regclass
				  AND dependency.objid = mapping.oid
				  AND dependency.deptype = 'e'
			)
		)
		SELECT object_kind, object_name
		FROM database_scoped_artifacts
		ORDER BY object_kind, object_name
		LIMIT 1
	`).Scan(&kind, &name)
	if errors.Is(err, sql.ErrNoRows) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("failed to inspect PostgreSQL database-scoped artifacts: %w", err)
	}
	return fmt.Errorf(
		"refusing to clean PostgreSQL database realm with unsupported database-scoped %s %q",
		kind,
		name,
	)
}

func cleanupPostgresLargeObjects(ctx context.Context, tx *sql.Tx) error {
	if _, err := tx.ExecContext(ctx, `
		SELECT lo_unlink(oid)
		FROM pg_largeobject_metadata
		ORDER BY oid
	`); err != nil {
		return fmt.Errorf("failed to remove PostgreSQL large objects: %w", err)
	}
	return nil
}

func verifyPostgresLargeObjects(ctx context.Context, tx *sql.Tx) error {
	var oid uint32
	err := tx.QueryRowContext(ctx, `
		SELECT oid
		FROM pg_largeobject_metadata
		ORDER BY oid
		LIMIT 1
	`).Scan(&oid)
	if errors.Is(err, sql.ErrNoRows) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("failed to verify PostgreSQL large object cleanup: %w", err)
	}
	return fmt.Errorf("PostgreSQL database realm cleanup left residual large object %d", oid)
}

func (w *PostgreSQLWriter) dropSchemaObjects(ctx context.Context) (resultErr error) {
	if w.dryRun {
		return nil
	}
	if w.db == nil {
		return fmt.Errorf("no database connection")
	}

	// A server that refuses DDL inside an explicit transaction gets the cleanup
	// run directly on the pool. That trades atomicity away -- a cleanup that
	// fails halfway leaves objects behind -- which is acceptable for the
	// scratch database this drops and is the only way the run can finish at all
	// (#1811). Every other PostgreSQL-family engine keeps the transaction.
	if !w.caps.Has(capability.DDLInsideTransaction) {
		return w.dropSchemaObjectsWithoutTransaction(ctx)
	}

	sqlTx, err := w.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	committed := false
	defer func() {
		if !committed {
			rollbackErr := sqlTx.Rollback()
			if rollbackErr != nil && !errors.Is(rollbackErr, sql.ErrTxDone) {
				resultErr = errors.Join(resultErr, fmt.Errorf("failed to roll back transaction: %w", rollbackErr))
			}
		}
	}()

	capabilities, err := inspectCleanupCapabilities(ctx, sqlTx)
	if err != nil {
		return err
	}
	if err := w.rejectSchemaScopedExtensions(ctx, sqlTx); err != nil {
		return err
	}
	objects, err := w.collectAllObjects(
		ctx,
		sqlTx,
		postgresSchemaCleanupScope([]string{w.schema}),
	)
	if err != nil {
		return err
	}
	if capabilities.lockManagedRelations {
		if err := w.lockManagedRelations(ctx, sqlTx, objects); err != nil {
			return err
		}
	}
	if capabilities.inspectPartitionEdges {
		if err := w.rejectCrossSchemaPartitionEdges(ctx, sqlTx); err != nil {
			return err
		}
	}

	dropErr := capabilities.dropObjects(ctx, sqlTx, objects)
	if dropErr != nil {
		return fmt.Errorf("refusing to clean schema %q: %w", w.schema, dropErr)
	}

	if err := sqlTx.Commit(); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}
	committed = true

	return nil
}

// dropSchemaObjectsWithoutTransaction is dropSchemaObjects for a server that
// refuses DDL inside one.
//
// Two capabilities are deliberately dropped with the transaction rather than
// carried into it. The retry loop drops each object inside a SAVEPOINT, and the
// relation lock is released at commit; both are transaction constructs, and
// pretending otherwise would either error or silently do nothing. What survives
// is RESTRICT, which is the authority on dependency order either way, so an
// object that cannot be dropped still fails loudly instead of being skipped.
func (w *PostgreSQLWriter) dropSchemaObjectsWithoutTransaction(ctx context.Context) error {
	capabilities, err := inspectCleanupCapabilities(ctx, w.db)
	if err != nil {
		return err
	}
	capabilities.retryFailedDDL = false
	if err := w.rejectSchemaScopedExtensions(ctx, w.db); err != nil {
		return err
	}
	objects, err := w.collectAllObjects(ctx, w.db, postgresSchemaCleanupScope([]string{w.schema}))
	if err != nil {
		return err
	}
	if capabilities.inspectPartitionEdges {
		if err := w.rejectCrossSchemaPartitionEdges(ctx, w.db); err != nil {
			return err
		}
	}
	if err := capabilities.dropObjects(ctx, w.db, objects); err != nil {
		return fmt.Errorf("refusing to clean schema %q: %w", w.schema, err)
	}
	return nil
}

func (w *PostgreSQLWriter) dropDatabaseRealm(ctx context.Context) (resultErr error) {
	sqlTx, err := w.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer func() {
		finishPostgresCleanupTransaction(sqlTx, &resultErr)
	}()

	plan, err := w.planDatabaseRealmCleanup(ctx, sqlTx)
	if err != nil {
		return err
	}
	preservedSchemas, err := w.executeDatabaseRealmCleanup(ctx, sqlTx, plan)
	if err != nil {
		return err
	}
	if err := verifyCompletedPostgresDatabaseCleanup(ctx, sqlTx, preservedSchemas, plan.capabilities); err != nil {
		return err
	}
	if err := sqlTx.Commit(); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}
	return nil
}

func finishPostgresCleanupTransaction(tx *sql.Tx, resultErr *error) {
	rollbackErr := tx.Rollback()
	if rollbackErr != nil && !errors.Is(rollbackErr, sql.ErrTxDone) {
		*resultErr = errors.Join(*resultErr, fmt.Errorf("failed to roll back transaction: %w", rollbackErr))
	}
}

func (w *PostgreSQLWriter) planDatabaseRealmCleanup(
	ctx context.Context,
	tx *sql.Tx,
) (postgresDatabaseCleanupPlan, error) {
	capabilities, err := inspectCleanupCapabilities(ctx, tx)
	if err != nil {
		return postgresDatabaseCleanupPlan{}, err
	}
	if err := rejectProtectedPostgresDatabase(ctx, tx, capabilities.protectedDatabases); err != nil {
		return postgresDatabaseCleanupPlan{}, err
	}
	if capabilities.inspectDatabaseArtifacts {
		if err := rejectPostgresDatabaseScopedArtifacts(ctx, tx); err != nil {
			return postgresDatabaseCleanupPlan{}, err
		}
	}
	rootMetadata, err := capturePostgresSchemaMetadata(ctx, tx, w.schema)
	if err != nil {
		return postgresDatabaseCleanupPlan{}, err
	}
	schemas, err := collectUserSchemas(ctx, tx)
	if err != nil {
		return postgresDatabaseCleanupPlan{}, err
	}
	objects, err := w.collectAllObjects(
		ctx,
		tx,
		postgresDatabaseCleanupScope(schemas, capabilities.systemExtensions),
	)
	if err != nil {
		return postgresDatabaseCleanupPlan{}, err
	}
	if capabilities.lockManagedRelations {
		if err := w.lockManagedRelations(ctx, tx, objects); err != nil {
			return postgresDatabaseCleanupPlan{}, err
		}
	}
	return postgresDatabaseCleanupPlan{
		capabilities: capabilities,
		rootMetadata: rootMetadata,
		schemas:      schemas,
		objects:      objects,
	}, nil
}

func (w *PostgreSQLWriter) executeDatabaseRealmCleanup(
	ctx context.Context,
	tx *sql.Tx,
	plan postgresDatabaseCleanupPlan,
) ([]string, error) {
	dropErr := plan.capabilities.dropObjects(ctx, tx, plan.objects)
	if dropErr != nil {
		return nil, fmt.Errorf("refusing to clean PostgreSQL database realm: %w", dropErr)
	}
	if plan.capabilities.cleanupLargeObjects {
		if err := cleanupPostgresLargeObjects(ctx, tx); err != nil {
			return nil, err
		}
	}

	preservedSchemas := []string{w.schema}
	droppableSchemas := plan.schemas
	if plan.capabilities.preservePublicSchema {
		preservedSchemas = appendUniqueString(preservedSchemas, "public")
		droppableSchemas = excludeString(droppableSchemas, "public")
	}
	if err := dropPostgresUserSchemas(ctx, tx, droppableSchemas); err != nil {
		return nil, err
	}
	if !plan.capabilities.preservePublicSchema || w.schema != "public" {
		if err := restorePostgresRootSchema(ctx, tx, w.schema, plan.rootMetadata); err != nil {
			return nil, err
		}
	}
	// "public" comes back even when it is not the root schema.
	//
	// It used to be the root on every run, because the connection reported
	// "public" unconditionally, so dropping it and restoring it above was one
	// step. Now that the root follows the dev URL's search_path, selecting any
	// other schema left "public" dropped and never restored -- and a migration
	// that writes `public.users` then failed with `schema "public" does not
	// exist`, which is the same shape of damage this whole change set is fixing,
	// just moved to the other schema.
	//
	// Emptying it is the point of a realm cleanup; removing it is not. Every
	// PostgreSQL database is created with it, and DDL that names no schema
	// resolves there for any caller who did not select another one.
	if postgresCleanupDroppedPublicSchema(w.schema, plan) {
		if err := restorePostgresPublicSchema(ctx, tx); err != nil {
			return nil, err
		}
		preservedSchemas = appendUniqueString(preservedSchemas, "public")
	}
	return preservedSchemas, nil
}

func verifyCompletedPostgresDatabaseCleanup(
	ctx context.Context,
	tx *sql.Tx,
	preservedSchemas []string,
	capabilities postgresCleanupCapabilities,
) error {
	if err := verifyPostgresDatabaseRealm(
		ctx,
		tx,
		preservedSchemas,
		capabilities.systemExtensions,
	); err != nil {
		return err
	}
	if capabilities.cleanupLargeObjects {
		if err := verifyPostgresLargeObjects(ctx, tx); err != nil {
			return err
		}
	}
	return nil
}

func collectUserSchemas(ctx context.Context, tx *sql.Tx) ([]string, error) {
	rows, err := tx.QueryContext(ctx, `
		SELECT n.nspname
		FROM pg_namespace n
		WHERE n.nspname <> 'information_schema'
		  AND n.nspname <> 'crdb_internal'
		  AND n.nspname NOT LIKE 'pg\_%' ESCAPE '\'
		ORDER BY n.nspname
	`)
	if err != nil {
		return nil, fmt.Errorf("failed to query PostgreSQL user schemas: %w", err)
	}
	defer rows.Close()

	var schemas []string
	for rows.Next() {
		var schema string
		if err := rows.Scan(&schema); err != nil {
			return nil, fmt.Errorf("failed to scan PostgreSQL user schema: %w", err)
		}
		schemas = append(schemas, schema)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("failed to iterate PostgreSQL user schemas: %w", err)
	}
	return schemas, nil
}

func capturePostgresSchemaMetadata(
	ctx context.Context,
	tx *sql.Tx,
	schema string,
) (postgresSchemaMetadata, error) {
	var metadata postgresSchemaMetadata
	err := tx.QueryRowContext(ctx, `
		SELECT
			pg_get_userbyid(n.nspowner),
			n.nspacl IS NULL,
			format(
				'COMMENT ON SCHEMA %I IS %L',
				n.nspname,
				obj_description(n.oid, 'pg_namespace')
			)
		FROM pg_namespace n
		WHERE n.nspname = $1
	`, schema).Scan(
		&metadata.owner,
		&metadata.aclIsDefault,
		&metadata.commentStatement,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return postgresSchemaMetadata{}, nil
	}
	if err != nil {
		return postgresSchemaMetadata{}, fmt.Errorf(
			"failed to capture root schema %q metadata: %w",
			schema,
			err,
		)
	}
	metadata.exists = true
	if metadata.aclIsDefault {
		return metadata, nil
	}

	rows, err := tx.QueryContext(ctx, `
		SELECT
			CASE acl.grantee
				WHEN 0 THEN 'PUBLIC'
				ELSE pg_get_userbyid(acl.grantee)
			END,
			acl.privilege_type,
			acl.is_grantable
		FROM pg_namespace n
		CROSS JOIN LATERAL aclexplode(n.nspacl) acl
		WHERE n.nspname = $1
		ORDER BY 1, 2, 3
	`, schema)
	if err != nil {
		return postgresSchemaMetadata{}, fmt.Errorf(
			"failed to capture root schema %q privileges: %w",
			schema,
			err,
		)
	}
	defer rows.Close()

	for rows.Next() {
		var privilege postgresSchemaPrivilege
		if err := rows.Scan(&privilege.grantee, &privilege.privilege, &privilege.grantOption); err != nil {
			return postgresSchemaMetadata{}, fmt.Errorf(
				"failed to scan root schema %q privilege: %w",
				schema,
				err,
			)
		}
		metadata.privileges = append(metadata.privileges, privilege)
	}
	if err := rows.Err(); err != nil {
		return postgresSchemaMetadata{}, fmt.Errorf(
			"failed to iterate root schema %q privileges: %w",
			schema,
			err,
		)
	}
	if err := validatePostgresSchemaPrivileges(metadata.privileges); err != nil {
		return postgresSchemaMetadata{}, err
	}
	return metadata, nil
}

func dropPostgresUserSchemas(ctx context.Context, tx *sql.Tx, schemas []string) error {
	for _, schema := range schemas {
		// Schema names come from pg_namespace and are quoted as identifiers;
		// PostgreSQL-family DDL does not support identifier bind parameters.
		// #nosec G202 -- The catalog identifier is quoted through sqlident.
		statement := "DROP SCHEMA IF EXISTS " + quoteIdent(schema) + " RESTRICT"
		if _, err := tx.ExecContext(ctx, statement); err != nil {
			return fmt.Errorf(
				"failed to drop user schema %q from PostgreSQL database realm: %w",
				schema,
				err,
			)
		}
	}
	return nil
}

func restorePostgresRootSchema(
	ctx context.Context,
	tx *sql.Tx,
	schema string,
	metadata postgresSchemaMetadata,
) error {
	// #nosec G202 -- Schema and owner identifiers are quoted through sqlident.
	statement := "CREATE SCHEMA " + quoteIdent(schema)
	if metadata.exists {
		statement += " AUTHORIZATION " + quoteIdent(metadata.owner)
	}
	if _, err := tx.ExecContext(ctx, statement); err != nil {
		return fmt.Errorf("failed to recreate root schema %q: %w", schema, err)
	}
	if !metadata.exists {
		return nil
	}
	if !metadata.aclIsDefault {
		if err := restorePostgresSchemaPrivileges(ctx, tx, schema, metadata); err != nil {
			return err
		}
	}
	if _, err := tx.ExecContext(ctx, metadata.commentStatement); err != nil {
		return fmt.Errorf("failed to restore root schema %q comment: %w", schema, err)
	}
	return nil
}

func restorePostgresSchemaPrivileges(
	ctx context.Context,
	tx *sql.Tx,
	schema string,
	metadata postgresSchemaMetadata,
) error {
	grantees := []string{"PUBLIC", metadata.owner}
	for _, privilege := range metadata.privileges {
		grantees = appendUniqueString(grantees, privilege.grantee)
	}
	for _, grantee := range grantees {
		// #nosec G202 -- Schema and role identifiers are quoted through sqlident.
		statement := "REVOKE ALL PRIVILEGES ON SCHEMA " + quoteIdent(schema) +
			" FROM " + quotePostgresRole(grantee)
		if _, err := tx.ExecContext(ctx, statement); err != nil {
			return fmt.Errorf(
				"failed to reset root schema %q privileges for %q: %w",
				schema,
				grantee,
				err,
			)
		}
	}
	for _, privilege := range metadata.privileges {
		// #nosec G202 -- Privilege is allow-listed; identifiers are quoted through sqlident.
		statement := "GRANT " + privilege.privilege + " ON SCHEMA " +
			quoteIdent(schema) + " TO " + quotePostgresRole(privilege.grantee)
		if privilege.grantOption {
			statement += " WITH GRANT OPTION"
		}
		if _, err := tx.ExecContext(ctx, statement); err != nil {
			return fmt.Errorf(
				"failed to restore root schema %q privilege for %q: %w",
				schema,
				privilege.grantee,
				err,
			)
		}
	}
	return nil
}

func validatePostgresSchemaPrivileges(privileges []postgresSchemaPrivilege) error {
	for _, privilege := range privileges {
		if privilege.privilege != "CREATE" && privilege.privilege != "USAGE" {
			return fmt.Errorf(
				"refusing to restore unsupported PostgreSQL schema privilege %q",
				privilege.privilege,
			)
		}
	}
	return nil
}

func verifyPostgresDatabaseRealm(
	ctx context.Context,
	tx *sql.Tx,
	preservedSchemas,
	systemExtensions []string,
) error {
	schemas, err := collectUserSchemas(ctx, tx)
	if err != nil {
		return err
	}
	expected := make(map[string]struct{}, len(preservedSchemas))
	for _, schema := range preservedSchemas {
		expected[schema] = struct{}{}
	}
	for _, schema := range schemas {
		if _, ok := expected[schema]; !ok {
			return fmt.Errorf(
				"PostgreSQL database realm cleanup left residual user schema %q",
				schema,
			)
		}
		delete(expected, schema)
	}
	for schema := range expected {
		return fmt.Errorf(
			"PostgreSQL database realm cleanup did not recreate preserved schema %q",
			schema,
		)
	}
	if err := verifyPostgresUserExtensions(ctx, tx, systemExtensions); err != nil {
		return err
	}

	query := strings.ReplaceAll(`
		WITH managed_namespaces AS (
			SELECT n.oid, n.nspname
			FROM pg_namespace n
			WHERE n.nspname IN ({{SCHEMA_PLACEHOLDERS}})
		),
		residual_objects AS (
			SELECT 'relation'::text AS object_kind, n.nspname, c.relname
			FROM pg_class c
			JOIN managed_namespaces n ON n.oid = c.relnamespace

			UNION ALL
			SELECT 'routine', n.nspname, p.proname
			FROM pg_proc p
			JOIN managed_namespaces n ON n.oid = p.pronamespace

			UNION ALL
			SELECT 'type', n.nspname, t.typname
			FROM pg_type t
			JOIN managed_namespaces n ON n.oid = t.typnamespace

			UNION ALL
			SELECT 'collation', n.nspname, c.collname
			FROM pg_collation c
			JOIN managed_namespaces n ON n.oid = c.collnamespace

			UNION ALL
			SELECT 'conversion', n.nspname, c.conname
			FROM pg_conversion c
			JOIN managed_namespaces n ON n.oid = c.connamespace

			UNION ALL
			SELECT 'operator', n.nspname, o.oprname
			FROM pg_operator o
			JOIN managed_namespaces n ON n.oid = o.oprnamespace

			UNION ALL
			SELECT 'operator class', n.nspname, o.opcname
			FROM pg_opclass o
			JOIN managed_namespaces n ON n.oid = o.opcnamespace

			UNION ALL
			SELECT 'operator family', n.nspname, o.opfname
			FROM pg_opfamily o
			JOIN managed_namespaces n ON n.oid = o.opfnamespace

			UNION ALL
			SELECT 'text search configuration', n.nspname, c.cfgname
			FROM pg_ts_config c
			JOIN managed_namespaces n ON n.oid = c.cfgnamespace

			UNION ALL
			SELECT 'text search dictionary', n.nspname, d.dictname
			FROM pg_ts_dict d
			JOIN managed_namespaces n ON n.oid = d.dictnamespace

			UNION ALL
			SELECT 'text search parser', n.nspname, p.prsname
			FROM pg_ts_parser p
			JOIN managed_namespaces n ON n.oid = p.prsnamespace

			UNION ALL
			SELECT 'text search template', n.nspname, t.tmplname
			FROM pg_ts_template t
			JOIN managed_namespaces n ON n.oid = t.tmplnamespace

			UNION ALL
			SELECT 'extension', n.nspname, e.extname
			FROM pg_extension e
			JOIN managed_namespaces n ON n.oid = e.extnamespace

			UNION ALL
			SELECT 'default privilege', n.nspname, d.oid::text
			FROM pg_default_acl d
			JOIN managed_namespaces n ON n.oid = d.defaclnamespace
		)
		SELECT object_kind, nspname, relname
		FROM residual_objects
		ORDER BY object_kind, nspname, relname
		LIMIT 1
	`, "{{SCHEMA_PLACEHOLDERS}}", postgresPlaceholders(len(preservedSchemas)))

	var kind string
	var schema string
	var name string
	err = tx.QueryRowContext(ctx, query, stringsToAny(preservedSchemas)...).Scan(
		&kind,
		&schema,
		&name,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("failed to verify PostgreSQL database realm cleanup: %w", err)
	}
	return fmt.Errorf(
		"PostgreSQL database realm cleanup left residual %s %q.%q",
		kind,
		schema,
		name,
	)
}

func verifyPostgresUserExtensions(
	ctx context.Context,
	tx *sql.Tx,
	systemExtensions []string,
) error {
	query := "SELECT e.extname FROM pg_extension e"
	var args []any
	if len(systemExtensions) > 0 {
		query += " WHERE e.extname NOT IN (" + postgresPlaceholders(len(systemExtensions)) + ")"
		args = stringsToAny(systemExtensions)
	}
	query += " ORDER BY e.extname LIMIT 1"

	var extension string
	err := tx.QueryRowContext(ctx, query, args...).Scan(&extension)
	if errors.Is(err, sql.ErrNoRows) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("failed to verify PostgreSQL user extension cleanup: %w", err)
	}
	return fmt.Errorf(
		"PostgreSQL database realm cleanup left residual user extension %q",
		extension,
	)
}

func isPostgresSystemSchema(schema string) bool {
	return schema == "information_schema" ||
		schema == "crdb_internal" ||
		strings.HasPrefix(schema, "pg_")
}

func quotePostgresRole(role string) string {
	if role == "PUBLIC" {
		return role
	}
	return quoteIdent(role)
}

func appendUniqueString(values []string, value string) []string {
	if slices.Contains(values, value) {
		return values
	}
	return append(values, value)
}

func excludeString(values []string, excluded string) []string {
	result := make([]string, 0, len(values))
	for _, value := range values {
		if value != excluded {
			result = append(result, value)
		}
	}
	return result
}

// SetDryRun enables or disables dry run mode
func (w *PostgreSQLWriter) SetDryRun(dryRun bool) {
	w.dryRun = dryRun
}

// IsDryRun returns whether dry run mode is enabled
func (w *PostgreSQLWriter) IsDryRun() bool {
	return w.dryRun
}

// postgresCleanupDroppedPublicSchema reports whether the realm cleanup dropped
// "public" without restoring it, which is true only when it existed, is not the
// root schema, and the server does not preserve it in place.
//
// The three exclusions are each a case that is already handled: a database with
// no "public" schema has nothing to put back, and recreating one would hand the
// caller a schema they never had; "public" as the root is restored by
// restorePostgresRootSchema with its recorded owner and grants; and a server
// that preserves it never dropped it.
func postgresCleanupDroppedPublicSchema(rootSchema string, plan postgresDatabaseCleanupPlan) bool {
	if rootSchema == "public" || plan.capabilities.preservePublicSchema {
		return false
	}
	return slices.Contains(plan.schemas, "public")
}

// restorePostgresPublicSchema recreates "public" after a realm cleanup dropped
// it.
//
// The recreation is deliberately plain. The root schema is restored with its
// recorded owner and grants because the caller keeps working inside it; a
// non-root "public" is being restored only so that DDL naming it resolves, and
// inventing privileges for it would be asserting something the cleanup never
// measured.
func restorePostgresPublicSchema(ctx context.Context, tx *sql.Tx) error {
	if _, err := tx.ExecContext(ctx, `CREATE SCHEMA "public"`); err != nil {
		return fmt.Errorf("failed to recreate the public schema after realm cleanup: %w", err)
	}
	return nil
}

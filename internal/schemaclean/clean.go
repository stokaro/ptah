// Package schemaclean plans and executes destructive schema cleanup.
package schemaclean

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"slices"
	"strings"

	"go.5x5.cz/ptah/dbschema"
	dbschematypes "go.5x5.cz/ptah/dbschema/types"
	"go.5x5.cz/ptah/internal/revisiontable"
	"go.5x5.cz/ptah/internal/sqlident"
)

// Object kinds a cleanup plan can name. Every constant here must correspond to
// an object kind that at least one dialect's SchemaWriter.DropAllTables really
// destroys — see coverageFor for the per-dialect mapping and for the writer
// files that define each dialect's coverage.
const (
	ObjectTypeAggregate        = "aggregate"
	ObjectTypeCollation        = "collation"
	ObjectTypeComposite        = "composite"
	ObjectTypeDefaultPrivilege = "default_privilege"
	ObjectTypeDomain           = "domain"
	ObjectTypeEnum             = "enum"
	ObjectTypeEvent            = "event"
	ObjectTypeForeignKey       = "foreign_key"
	ObjectTypeForeignTable     = "foreign_table"
	ObjectTypeFunction         = "function"
	ObjectTypeMaterializedView = "materialized_view"
	ObjectTypeProcedure        = "procedure"
	ObjectTypeRange            = "range"
	ObjectTypeSequence         = "sequence"
	ObjectTypeTable            = "table"
	ObjectTypeView             = "view"
)

type Options struct {
	DryRun bool
}

// InspectOptions configures policy checks over the same catalog snapshot that
// cleanup planning consumes.
type InspectOptions struct {
	// ValidateSchema runs after catalog inspection and before the plan is built.
	// Nil accepts every schema. A validator can refuse dependent objects that
	// disappear with a listed parent but are intentionally absent from Plan.
	ValidateSchema func(*dbschematypes.DBSchema) error
}

// Plan is the report an operator reviews before approving a destructive
// cleanup. Its contract is: it names every object for which
// SchemaWriter.DropAllTables issues its own drop statement on this dialect.
//
// It deliberately does not name dependent objects that vanish as collateral of
// a listed drop and for which the writer issues no statement of its own —
// indexes, triggers, non-foreign-key constraints, RLS policies, comments, and
// (on PostgreSQL) the constructor functions a range type owns. Foreign keys are
// listed on the dialects whose writer drops them explicitly before the tables.
// Enumerating the full transitive closure of a DROP is not tractable; "what the
// writer names" is.
type Plan struct {
	Objects []Object
	Changes []Change

	// executionChanges is kept separate from Changes because Changes is a
	// stable, alphabetical report while a scoped cleanup must drop known
	// dependents before the objects they depend on.
	executionChanges []Change
	executionDepths  map[executionObjectIdentity]int
}

type Object struct {
	Type         string
	Schema       string
	Table        string
	Name         string
	SelectorName string
	Parameters   string
	Implicit     bool
	Command      string
}

// SnapshotWithinWriterScope returns the reader snapshot restricted where the
// cleanup writer itself is restricted. PostgreSQL extensions are database-wide
// reader inventory, while schema cleanup can only reason about extensions
// owned by its configured schema; global extensions must not veto an otherwise
// schema-local clean.
func SnapshotWithinWriterScope(
	schema *dbschematypes.DBSchema,
	dialect string,
	writerSchema string,
) *dbschematypes.DBSchema {
	if schema == nil || !isPostgresFamily(dialect) {
		return schema
	}
	writerSchema = strings.TrimSpace(writerSchema)
	if writerSchema == "" {
		writerSchema = "public"
	}
	owned := *schema
	owned.Extensions = slices.DeleteFunc(
		slices.Clone(schema.Extensions),
		func(extension dbschematypes.DBExtension) bool {
			return strings.TrimSpace(extension.Schema) != writerSchema
		},
	)
	return &owned
}

// Change is one line of the cleanup report: an object that Apply destroys, plus
// a rendered DROP statement describing that destruction.
//
// An unscoped Apply delegates to SchemaWriter.DropAllTables, which rebuilds its
// own statements from the live catalog. ApplyPlan executes Cmd for a scoped
// cleanup, but in a separate dependency-safe order rather than this report's
// alphabetical order. PostgreSQL-family commands use RESTRICT: the scoped
// executor removes selected known dependents first, and the server refuses a
// parent whose remaining dependency was outside the selected set.
type Change struct {
	Type       string
	Schema     string
	Table      string
	Name       string
	Parameters string
	Cmd        string
}

// dialectCoverage records which object kinds a dialect's
// dbschema/types.SchemaWriter.DropAllTables implementation actually destroys.
// It is the one place this package decides what a cleanup plan is allowed to
// claim, so plan coverage and execution coverage cannot drift kind by kind.
//
// Tables are not a field: every writer drops tables, so every dialect's plan
// lists them unconditionally.
//
// Each field is populated in coverageFor, which names the writer file and the
// function inside it that decides that dialect's coverage. Widening a writer
// REQUIRES widening the matching entry here; the live tests in
// clean_live_test.go turn a missed widening into a failure by comparing the
// plan against the objects that really disappeared.
type dialectCoverage struct {
	// Reader-sourced kinds, taken from the dbschema reader's schema snapshot.
	foreignKeys       bool
	views             bool
	materializedViews bool
	enums             bool
	domains           bool
	composites        bool
	ranges            bool
	functions         bool

	// Runtime-sourced kinds: the reader's snapshot does not carry them, so
	// Inspect queries the live catalog for them. See inspectRuntimeObjects.
	postgresRuntimeObjects bool
	mysqlStoredObjects     bool

	// revisionTables records that this dialect's writer destroys Ptah's own
	// migration bookkeeping tables while its reader hides them by name, so
	// Inspect has to find them in the live catalog. See inspectRevisionTables.
	revisionTables bool
}

func Inspect(conn *dbschema.DatabaseConnection) (Plan, error) {
	return InspectWithOptions(conn, InspectOptions{})
}

// InspectWithOptions returns a cleanup plan after applying caller-selected
// validation to the reader snapshot. The returned plan also includes the
// writer-only live-catalog objects that the reader cannot represent.
func InspectWithOptions(conn *dbschema.DatabaseConnection, opts InspectOptions) (Plan, error) {
	schema, err := conn.Reader().ReadSchema()
	if err != nil {
		return Plan{}, fmt.Errorf("inspect schema before cleanup: %w", err)
	}
	if opts.ValidateSchema != nil {
		if err := opts.ValidateSchema(schema); err != nil {
			return Plan{}, err
		}
	}
	dialect := conn.Info().Dialect
	objects := cleanupObjects(schema, dialect)
	runtimeObjects, err := inspectRuntimeObjects(conn)
	if err != nil {
		return Plan{}, err
	}
	objects = append(objects, runtimeObjects...)
	executionDepths, err := inspectPostgresViewDependencyDepths(conn, objects)
	if err != nil {
		return Plan{}, err
	}
	revisionObjects, err := inspectRevisionTables(conn)
	if err != nil {
		return Plan{}, err
	}
	objects = append(objects, unlistedObjects(objects, revisionObjects)...)
	return planFromObjects(objects, dialect, executionDepths), nil
}

func Execute(ctx context.Context, conn *dbschema.DatabaseConnection, opts Options) (Plan, error) {
	plan, err := Inspect(conn)
	if err != nil {
		return Plan{}, err
	}
	if opts.DryRun {
		return plan, nil
	}
	if err := Apply(ctx, conn); err != nil {
		return Plan{}, err
	}
	return plan, nil
}

func Apply(ctx context.Context, conn *dbschema.DatabaseConnection) error {
	conn.SchemaWriter().SetDryRun(false)
	if err := conn.SchemaWriter().DropAllTables(ctx); err != nil {
		return fmt.Errorf("drop schema objects: %w", err)
	}
	return nil
}

// ApplyPlan executes exactly the changes plan carries, in the plan's
// dependency-safe execution order.
//
// It is the execution half of a narrowed plan. [Apply] hands the whole database
// to the writer's DropAllTables, which is correct only when the plan describes
// the whole database; a caller that removed objects from the plan must not then
// reach for a routine that ignores the plan, or the flag that narrowed it would
// print one thing and destroy another.
func ApplyPlan(ctx context.Context, conn *dbschema.DatabaseConnection, plan Plan) error {
	conn.SchemaWriter().SetDryRun(false)
	changes := executableChanges(plan)
	if len(changes) == 0 {
		return nil
	}
	if isPostgresFamily(conn.Info().Dialect) {
		return applyPostgresFamilyPlan(ctx, conn, changes)
	}
	return applyPlanChanges(ctx, conn.Writer(), changes)
}

func executableChanges(plan Plan) []Change {
	if plan.executionChanges != nil {
		return plan.executionChanges
	}
	// A literal Plan remains executable for internal callers, but plans built
	// by PlanFromObjects always carry the dependency-safe order.
	return plan.Changes
}

func applyPostgresFamilyPlan(
	ctx context.Context,
	conn *dbschema.DatabaseConnection,
	changes []Change,
) error {
	tx, err := conn.SchemaWriter().BeginTransaction(ctx)
	if err != nil {
		return fmt.Errorf("begin scoped schema cleanup: %w", err)
	}
	if err := applyPostgresFamilyPlanChanges(ctx, tx, changes); err != nil {
		return errors.Join(err, cleanupRollbackError(tx.Rollback()))
	}
	if err := tx.Commit(); err != nil {
		return errors.Join(
			fmt.Errorf("commit scoped schema cleanup: %w", err),
			cleanupRollbackError(tx.Rollback()),
		)
	}
	return nil
}

func applyPostgresFamilyPlanChanges(
	ctx context.Context,
	tx dbschematypes.SchemaTransaction,
	changes []Change,
) error {
	// RESTRICT is the safety boundary. Savepoints let a selected dependent
	// disappear before retrying its selected dependency, while an external or
	// unknown dependency eventually stops progress and rolls back the outer
	// transaction. This mirrors PostgreSQLWriter's unscoped cleanup algorithm
	// without widening the selected object set.
	pending := slices.Clone(changes)
	for len(pending) > 0 {
		remaining := make([]Change, 0, len(pending))
		var firstDropErr error

		for _, change := range pending {
			if strings.TrimSpace(change.Cmd) == "" {
				continue
			}
			dropErr, controlErr := tryApplyPostgresFamilyPlanChange(ctx, tx, change)
			if controlErr != nil {
				return errors.Join(dropErr, controlErr)
			}
			if dropErr != nil {
				if firstDropErr == nil {
					firstDropErr = dropErr
				}
				remaining = append(remaining, change)
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

func tryApplyPostgresFamilyPlanChange(
	ctx context.Context,
	tx dbschematypes.SchemaTransaction,
	change Change,
) (dropErr error, controlErr error) {
	const savepoint = "ptah_scoped_cleanup_object"

	if err := tx.ExecuteSQL(ctx, "SAVEPOINT "+savepoint); err != nil {
		return nil, fmt.Errorf("create scoped cleanup savepoint: %w", err)
	}
	if err := tx.ExecuteSQL(ctx, change.Cmd); err != nil {
		dropErr = fmt.Errorf("drop schema object %s %q: %w", change.Type, change.Name, err)
		if rollbackErr := tx.ExecuteSQL(ctx, "ROLLBACK TO SAVEPOINT "+savepoint); rollbackErr != nil {
			controlErr = fmt.Errorf("roll back scoped cleanup savepoint: %w", rollbackErr)
		}
		if releaseErr := tx.ExecuteSQL(ctx, "RELEASE SAVEPOINT "+savepoint); releaseErr != nil {
			controlErr = errors.Join(
				controlErr,
				fmt.Errorf("release scoped cleanup savepoint: %w", releaseErr),
			)
		}
		return dropErr, controlErr
	}
	if err := tx.ExecuteSQL(ctx, "RELEASE SAVEPOINT "+savepoint); err != nil {
		return nil, fmt.Errorf("release scoped cleanup savepoint: %w", err)
	}
	return nil, nil
}

func applyPlanChanges(
	ctx context.Context,
	executor dbschematypes.SchemaExecutor,
	changes []Change,
) error {
	for _, change := range changes {
		if strings.TrimSpace(change.Cmd) == "" {
			continue
		}
		if err := executor.ExecuteSQL(ctx, change.Cmd); err != nil {
			return fmt.Errorf("drop schema object %s %q: %w", change.Type, change.Name, err)
		}
	}
	return nil
}

func cleanupRollbackError(err error) error {
	if err == nil || errors.Is(err, sql.ErrTxDone) {
		return nil
	}
	return fmt.Errorf("roll back scoped schema cleanup: %w", err)
}

func PlanFromSchema(schema *dbschematypes.DBSchema, dialect string) Plan {
	if schema == nil {
		return Plan{}
	}
	return PlanFromObjects(cleanupObjects(schema, dialect), dialect)
}

func PlanFromObjects(objects []Object, dialect string) Plan {
	return planFromObjects(objects, dialect, nil)
}

// WithObjects returns a plan narrowed to objects while retaining live catalog
// execution metadata from the receiver. Compatibility adapters use it after
// selectors remove objects from an inspected plan.
func (p Plan) WithObjects(objects []Object, dialect string) Plan {
	return planFromObjects(objects, dialect, p.executionDepths)
}

func planFromObjects(
	objects []Object,
	dialect string,
	executionDepths map[executionObjectIdentity]int,
) Plan {
	executionObjects := slices.Clone(objects)
	sortExecutionObjects(executionObjects, dialect, executionDepths)
	executionChanges := changesFromObjects(executionObjects, dialect)

	objects = slices.Clone(objects)
	sortObjects(objects)
	return Plan{
		Objects:          objects,
		Changes:          changesFromObjects(objects, dialect),
		executionChanges: executionChanges,
		executionDepths:  executionDepths,
	}
}

func changesFromObjects(objects []Object, dialect string) []Change {
	changes := make([]Change, 0, len(objects))
	for _, object := range objects {
		changes = append(changes, Change{
			Type:       object.Type,
			Schema:     object.Schema,
			Table:      object.Table,
			Name:       object.Name,
			Parameters: object.Parameters,
			Cmd:        dropCommand(dialect, object),
		})
	}
	return changes
}

// coverageFor maps a dialect onto the object kinds its writer destroys.
//
// PostgreSQL family — internal/dbschema/postgres/writer.go, DropAllTables ->
// dropSchemaObjects -> collectAllObjects. The catalog query emits foreign-key
// constraints, views, materialized views, foreign tables, sequences, tables,
// functions/procedures/aggregates, types (enum, domain, range, composite),
// collations, and default privileges.
//
// MySQL and MariaDB — internal/dbschema/mysql/writer.go, DropAllTables ->
// dropAllTablesOnConnection -> dropDatabaseObjects, which drops the foreign
// keys from listInternalForeignKeys and then the objects from
// listCleanupObjects: events, views, routines (FUNCTION and PROCEDURE), tables,
// and MariaDB sequences.
//
// SQLite — internal/dbschema/sqlite/writer.go, DropAllTables -> dropAllTables
// -> listCleanupObjects, which reads pragma_table_list and emits only tables
// (including virtual tables) and views.
//
// SQL Server — internal/dbschema/mssql/writer.go, DropAllTables drops the
// foreign keys from listForeignKeys and the tables from listTables, and nothing
// else. Its reader does populate Views, but the writer never drops them, so
// views must stay out of the plan.
//
// ClickHouse — internal/dbschema/clickhouse/writer.go, DropAllTables selects
// from system.tables filtered to persistent table engines with
// `engine NOT LIKE '%View'`, so it drops base tables only.
//
// The revisionTables field below is not about kinds but about names: the
// PostgreSQL, MySQL and SQL Server readers exclude Ptah's own revision tables
// from every snapshot by name, while those dialects' writers drop them as
// ordinary tables. Measured for issue #1111 by censusing each catalog before
// and after a real cleanup:
//
//	PostgreSQL 18   reader hides schema_migrations         planned 2, destroyed 3
//	MariaDB 11      reader hides schema_migrations         planned 3, destroyed 4
//	SQL Server 2022 reader hides both revision tables      planned 1, destroyed 3
//	ClickHouse 24   reader hides nothing                   planned 3, destroyed 3
//	SQLite          reader hides schema_migrations and the
//	                writer keeps it (dropAllTables passes
//	                includeRevisionTable=false)            planned 3, destroyed 3
//
// So ClickHouse and SQLite must stay false: ClickHouse already reports both
// tables through the reader, and SQLite would be reporting a table that
// survives the cleanup.
func coverageFor(dialect string) dialectCoverage {
	normalized := normalizeDialect(dialect)
	switch normalized {
	case "postgres", "postgresql", "cockroachdb", "yugabytedb":
		return dialectCoverage{
			foreignKeys:       true,
			views:             true,
			materializedViews: true,
			enums:             true,
			domains:           true,
			composites:        true,
			ranges:            true,
			functions:         true,
			// The writer-only live-catalog inventory stays restricted to
			// PostgreSQL proper because that is the only PostgreSQL-family engine
			// this package has been measured against. CockroachDB and YugabyteDB
			// plans remain conservative until their cleanup inventories are
			// measured independently.
			postgresRuntimeObjects: normalized == "postgres" || normalized == "postgresql",
			revisionTables:         true,
		}
	case "mysql", "mariadb":
		return dialectCoverage{
			foreignKeys:        true,
			views:              true,
			mysqlStoredObjects: true,
			revisionTables:     true,
		}
	case "sqlite", "sqlite3":
		return dialectCoverage{views: true}
	case "sqlserver", "mssql":
		return dialectCoverage{foreignKeys: true, revisionTables: true}
	default:
		// ClickHouse, and any dialect this package has not measured: report
		// tables only, which every writer drops.
		//
		// Spanner lands here even though dbschema hands it the PostgreSQL
		// writer, because Spanner's PostgreSQL interface does not accept the
		// CASCADE and DROP TYPE forms the PostgreSQL rows above render, and no
		// Spanner instance was available to measure the real coverage against.
		// Its plan therefore still understates cleanup.
		return dialectCoverage{}
	}
}

func normalizeDialect(dialect string) string {
	return strings.ToLower(strings.TrimSpace(dialect))
}

// cleanupObjects turns the reader's schema snapshot into plan rows. Tables are
// unconditional; every other kind is gated on this dialect's writer coverage,
// paired with its collector below so a coverage field can never be added
// without a collector or the other way round.
func cleanupObjects(schema *dbschematypes.DBSchema, dialect string) []Object {
	if schema == nil {
		return nil
	}
	coverage := coverageFor(dialect)
	objects := tableObjects(schema)
	for _, kind := range []struct {
		covered bool
		collect func(*dbschematypes.DBSchema) []Object
	}{
		{coverage.foreignKeys, foreignKeyObjects},
		{coverage.views, viewObjects},
		{coverage.materializedViews, materializedViewObjects},
		{coverage.enums, enumObjects},
		{coverage.domains, domainObjects},
		{coverage.composites, compositeObjects},
		{coverage.ranges, rangeObjects},
	} {
		if !kind.covered {
			continue
		}
		objects = append(objects, kind.collect(schema)...)
	}
	if coverage.functions {
		objects = append(objects, functionObjects(schema, dialect)...)
	}
	return objects
}

func tableObjects(schema *dbschematypes.DBSchema) []Object {
	objects := make([]Object, 0, len(schema.Tables))
	for _, table := range schema.Tables {
		// Readers that surface views through DBSchema.Tables as well as
		// DBSchema.Views tag them here, and viewObjects reports them once.
		if !isCleanupTableType(table.Type) {
			continue
		}
		objects = append(objects, Object{
			Type:   ObjectTypeTable,
			Schema: table.Schema,
			Name:   table.Name,
		})
	}
	return objects
}

func foreignKeyObjects(schema *dbschematypes.DBSchema) []Object {
	objects := make([]Object, 0, len(schema.Constraints))
	for _, constraint := range schema.Constraints {
		if !isForeignKeyConstraint(constraint) {
			continue
		}
		objects = append(objects, Object{
			Type:   ObjectTypeForeignKey,
			Schema: constraint.Schema,
			Table:  constraint.TableName,
			Name:   constraint.Name,
		})
	}
	return objects
}

func viewObjects(schema *dbschematypes.DBSchema) []Object {
	objects := make([]Object, 0, len(schema.Views))
	for _, view := range schema.Views {
		objects = append(objects, Object{
			Type:   ObjectTypeView,
			Schema: view.Schema,
			Name:   view.Name,
		})
	}
	return objects
}

func materializedViewObjects(schema *dbschematypes.DBSchema) []Object {
	objects := make([]Object, 0, len(schema.MatViews))
	for _, matView := range schema.MatViews {
		objects = append(objects, Object{
			Type:   ObjectTypeMaterializedView,
			Schema: matView.Schema,
			Name:   matView.Name,
		})
	}
	return objects
}

// enumObjects reports PostgreSQL enum types. DBEnum carries no schema: the
// reader reads enums per schema but does not record which one, so enum rows
// stay unqualified.
func enumObjects(schema *dbschematypes.DBSchema) []Object {
	objects := make([]Object, 0, len(schema.Enums))
	for _, enum := range schema.Enums {
		objects = append(objects, Object{
			Type: ObjectTypeEnum,
			Name: enum.Name,
		})
	}
	return objects
}

func domainObjects(schema *dbschematypes.DBSchema) []Object {
	objects := make([]Object, 0, len(schema.Domains))
	for _, domain := range schema.Domains {
		objects = append(objects, Object{
			Type:   ObjectTypeDomain,
			Schema: domain.Schema,
			Name:   domain.Name,
		})
	}
	return objects
}

func compositeObjects(schema *dbschematypes.DBSchema) []Object {
	objects := make([]Object, 0, len(schema.Composites))
	for _, composite := range schema.Composites {
		objects = append(objects, Object{
			Type:   ObjectTypeComposite,
			Schema: composite.Schema,
			Name:   composite.Name,
		})
	}
	return objects
}

func rangeObjects(schema *dbschematypes.DBSchema) []Object {
	objects := make([]Object, 0, len(schema.Ranges))
	for _, rangeType := range schema.Ranges {
		objects = append(objects, Object{
			Type:   ObjectTypeRange,
			Schema: rangeType.Schema,
			Name:   rangeType.Name,
		})
	}
	return objects
}

// functionObjects reports PostgreSQL functions. Parameters remains the full
// declaration form that inspect and report consumers expect, while the
// pre-rendered command uses PostgreSQL's distinct overload identity. Older or
// synthetic snapshots without identity metadata retain the former Parameters
// fallback instead of losing their executable cleanup capability.
func functionObjects(schema *dbschematypes.DBSchema, dialect string) []Object {
	objects := make([]Object, 0, len(schema.Functions))
	for _, function := range schema.Functions {
		identityArguments := function.Parameters
		if function.IdentityArguments != nil {
			identityArguments = *function.IdentityArguments
		}
		name := sqlident.Qualified(dialect, function.Schema, function.Name)
		objects = append(objects, Object{
			Type:       ObjectTypeFunction,
			Schema:     function.Schema,
			Name:       function.Name,
			Parameters: function.Parameters,
			Command:    dropRoutineCommand(dialect, "FUNCTION", name, identityArguments),
		})
	}
	return objects
}

// inspectRuntimeObjects enumerates the object kinds a dialect's writer destroys
// that the dbschema reader's snapshot does not carry.
func inspectRuntimeObjects(conn *dbschema.DatabaseConnection) ([]Object, error) {
	return InspectRuntimeObjects(conn, nil)
}

// InspectRuntimeObjects returns catalog objects that the ordinary schema
// reader cannot model but which remain relevant to live-schema policy. When
// schemas is non-empty, only those schema scopes are inventoried; nil uses the
// connection's cleanup scope. The query is read-only and does not build or
// execute a cleanup plan.
func InspectRuntimeObjects(conn *dbschema.DatabaseConnection, schemas []string) ([]Object, error) {
	coverage := coverageFor(conn.Info().Dialect)
	if !coverage.postgresRuntimeObjects && !coverage.mysqlStoredObjects {
		return nil, nil
	}
	objects := []Object{}
	for _, schema := range runtimeObjectSchemas(conn, schemas) {
		var scoped []Object
		var err error
		if coverage.postgresRuntimeObjects {
			scoped, err = inspectPostgresRuntimeObjects(conn, schema)
		} else {
			scoped, err = inspectMySQLStoredObjects(conn, schema)
		}
		if err != nil {
			return nil, err
		}
		objects = append(objects, scoped...)
	}
	return objects, nil
}

func runtimeObjectSchemas(conn *dbschema.DatabaseConnection, schemas []string) []string {
	if len(schemas) == 0 {
		return []string{strings.TrimSpace(conn.Info().Schema)}
	}
	result := make([]string, 0, len(schemas))
	for _, schema := range schemas {
		schema = strings.TrimSpace(schema)
		if schema == "" || slices.Contains(result, schema) {
			continue
		}
		result = append(result, schema)
	}
	if len(result) == 0 {
		return []string{strings.TrimSpace(conn.Info().Schema)}
	}
	return result
}

func inspectPostgresRuntimeObjects(conn *dbschema.DatabaseConnection, schema string) ([]Object, error) {
	schema = strings.TrimSpace(schema)
	if schema == "" {
		schema = "public"
	}
	rows, err := conn.Query(`
		WITH sequence_objects AS (
			SELECT
				c.oid,
				n.nspname,
				c.relname,
				owner_tbl.relname AS owner_table,
				CASE
					WHEN dep.refobjid IS NULL THEN false
					WHEN dep.deptype = 'i' THEN true
					WHEN owner_col_type.typtype = 'd' THEN false
					ELSE EXISTS (
						SELECT 1
						FROM pg_attrdef ad
						JOIN pg_depend dd ON dd.classid = 'pg_attrdef'::regclass
							AND dd.objid = ad.oid
							AND dd.refclassid = 'pg_class'::regclass
							AND dd.refobjid = c.oid
							AND dd.deptype = 'n'
						WHERE ad.adrelid = dep.refobjid AND ad.adnum = dep.refobjsubid
					)
				END AS is_implicit
			FROM pg_class c
			JOIN pg_namespace n ON n.oid = c.relnamespace
			LEFT JOIN pg_depend dep ON dep.objid = c.oid
				AND dep.classid = 'pg_class'::regclass
				AND dep.refclassid = 'pg_class'::regclass
				AND dep.deptype IN ('a', 'i')
				AND dep.refobjsubid > 0
			LEFT JOIN pg_class owner_tbl ON owner_tbl.oid = dep.refobjid
			LEFT JOIN pg_attribute owner_col ON owner_col.attrelid = dep.refobjid
				AND owner_col.attnum = dep.refobjsubid
			LEFT JOIN pg_type owner_col_type ON owner_col_type.oid = owner_col.atttypid
			WHERE n.nspname = $1 AND c.relkind = 'S'
		),
		runtime_objects AS (
			SELECT
				'sequence'::text AS object_kind,
				nspname AS object_schema,
				relname AS object_name,
				relname AS selector_name,
				is_implicit,
				COALESCE(owner_table, '') AS owner_table,
				format('DROP SEQUENCE IF EXISTS %I.%I RESTRICT', nspname, relname) AS drop_statement
			FROM sequence_objects

			UNION ALL

			SELECT
				'foreign_table',
				n.nspname,
				c.relname,
				c.relname,
				false,
				'',
				format('DROP FOREIGN TABLE IF EXISTS %I.%I RESTRICT', n.nspname, c.relname)
			FROM pg_class c
			JOIN pg_namespace n ON n.oid = c.relnamespace
			WHERE n.nspname = $1 AND c.relkind = 'f'

			UNION ALL

			SELECT
				CASE p.prokind
					WHEN 'p' THEN 'procedure'
					WHEN 'a' THEN 'aggregate'
					ELSE 'function'
				END,
				n.nspname,
				format('%s(%s)', p.proname, pg_get_function_identity_arguments(p.oid)),
				p.proname,
				false,
				'',
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
			JOIN pg_namespace n ON n.oid = p.pronamespace
			WHERE n.nspname = $1
			  AND p.prokind IN ('p', 'a', 'w')
			  AND NOT EXISTS (
				SELECT 1
				FROM pg_depend d
				WHERE d.classid = 'pg_proc'::regclass
				  AND d.objid = p.oid
				  AND d.deptype = 'i'
			  )

			UNION ALL

			SELECT
				'collation',
				n.nspname,
				c.collname,
				c.collname,
				false,
				'',
				format('DROP COLLATION IF EXISTS %I.%I RESTRICT', n.nspname, c.collname)
			FROM pg_collation c
			JOIN pg_namespace n ON n.oid = c.collnamespace
			WHERE n.nspname = $1

			UNION ALL

			SELECT DISTINCT
				'default_privilege',
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
					'%s:%s:%s',
					pg_get_userbyid(d.defaclrole),
					d.defaclobjtype,
					CASE acl.grantee
						WHEN 0 THEN 'PUBLIC'
						ELSE pg_get_userbyid(acl.grantee)
					END
				),
				false,
				'',
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
			JOIN pg_namespace n ON n.oid = d.defaclnamespace
			CROSS JOIN LATERAL aclexplode(d.defaclacl) acl
			WHERE n.nspname = $1
			  AND d.defaclobjtype IN ('r', 'S', 'f', 'T')
		)
		SELECT object_kind, object_schema, object_name, selector_name, is_implicit, owner_table, drop_statement
		FROM runtime_objects
		ORDER BY object_kind, object_name`, schema)
	if err != nil {
		return nil, fmt.Errorf("inspect PostgreSQL runtime objects: %w", err)
	}
	defer rows.Close()

	objects := []Object{}
	for rows.Next() {
		var kind string
		var schema string
		var name string
		var selectorName string
		var implicit bool
		var table sql.NullString
		var command string
		if err := rows.Scan(&kind, &schema, &name, &selectorName, &implicit, &table, &command); err != nil {
			return nil, fmt.Errorf("scan PostgreSQL runtime object: %w", err)
		}
		objectType, ok := postgresRuntimeObjectType(kind)
		if !ok {
			return nil, fmt.Errorf("inspect PostgreSQL runtime objects: unsupported object kind %q", kind)
		}
		objects = append(objects, Object{
			Type:         objectType,
			Schema:       schema,
			Table:        table.String,
			Name:         name,
			SelectorName: selectorName,
			Implicit:     implicit,
			Command:      command,
		})
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate PostgreSQL runtime objects: %w", err)
	}
	return objects, nil
}

func inspectPostgresViewDependencyDepths(
	conn *dbschema.DatabaseConnection,
	objects []Object,
) (map[executionObjectIdentity]int, error) {
	if !isPostgres(conn.Info().Dialect) {
		return nil, nil
	}
	schema := strings.TrimSpace(conn.Info().Schema)
	if schema == "" {
		schema = "public"
	}
	rows, err := conn.Query(`
		WITH RECURSIVE
		managed_views AS (
			SELECT c.oid, c.relnamespace, c.relname, c.relkind
			FROM pg_class c
			JOIN pg_namespace n ON n.oid = c.relnamespace
			WHERE n.nspname = $1 AND c.relkind IN ('v', 'm')
		),
		view_dependencies AS (
			SELECT
				dependent.oid AS dependent_oid,
				referenced.oid AS referenced_oid
			FROM pg_rewrite rewrite
			JOIN managed_views dependent ON dependent.oid = rewrite.ev_class
			JOIN pg_depend dependency
				ON dependency.classid = 'pg_rewrite'::regclass
			   AND dependency.objid = rewrite.oid
			   AND dependency.refclassid = 'pg_class'::regclass
			JOIN managed_views referenced ON referenced.oid = dependency.refobjid
			WHERE dependent.oid <> referenced.oid
		),
		view_depths AS (
			SELECT view.oid, 0 AS depth
			FROM managed_views view
			WHERE NOT EXISTS (
				SELECT 1
				FROM view_dependencies dependency
				WHERE dependency.dependent_oid = view.oid
			)

			UNION ALL

			SELECT dependency.dependent_oid, depth.depth + 1
			FROM view_depths depth
			JOIN view_dependencies dependency
				ON dependency.referenced_oid = depth.oid
		)
		SELECT
			view.relname,
			view.relkind,
			COALESCE(MAX(depth.depth), 0)
		FROM managed_views view
		LEFT JOIN view_depths depth ON depth.oid = view.oid
		GROUP BY view.oid, view.relname, view.relkind
		ORDER BY view.relname
	`, schema)
	if err != nil {
		return nil, fmt.Errorf("inspect PostgreSQL view dependencies before cleanup: %w", err)
	}
	defer rows.Close()

	depths := make(map[postgresViewIdentity]int)
	for rows.Next() {
		var name string
		var relkind string
		var depth int
		if err := rows.Scan(&name, &relkind, &depth); err != nil {
			return nil, fmt.Errorf("scan PostgreSQL view dependency: %w", err)
		}
		objectType := ObjectTypeView
		if relkind == "m" {
			objectType = ObjectTypeMaterializedView
		}
		depths[postgresViewIdentity{objectType: objectType, schema: schema, name: name}] = depth
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate PostgreSQL view dependencies: %w", err)
	}
	executionDepths := make(map[executionObjectIdentity]int)
	for i := range objects {
		objectSchema := strings.TrimSpace(objects[i].Schema)
		if objectSchema == "" {
			objectSchema = schema
		}
		depth := depths[postgresViewIdentity{
			objectType: objects[i].Type,
			schema:     objectSchema,
			name:       objects[i].Name,
		}]
		if depth > 0 {
			executionDepths[objectExecutionIdentity(objects[i])] = depth
		}
	}
	return executionDepths, nil
}

type postgresViewIdentity struct {
	objectType string
	schema     string
	name       string
}

type executionObjectIdentity struct {
	objectType string
	schema     string
	table      string
	name       string
	parameters string
}

func objectExecutionIdentity(object Object) executionObjectIdentity {
	return executionObjectIdentity{
		objectType: object.Type,
		schema:     object.Schema,
		table:      object.Table,
		name:       object.Name,
		parameters: object.Parameters,
	}
}

// inspectMySQLStoredObjects reports the stored programs and MariaDB sequences
// that the MySQL writer's listCleanupObjects returns but the MySQL reader never
// records: information_schema.routines, information_schema.events, and the
// SEQUENCE rows of information_schema.tables.
//
// The scope mirrors mysql.Writer.cleanupSchema: the configured schema when the
// connection pins one, otherwise the session's current database.
func inspectMySQLStoredObjects(conn *dbschema.DatabaseConnection, schema string) ([]Object, error) {
	schema = strings.TrimSpace(schema)
	rows, err := conn.Query(`
		SELECT object_name, object_kind
		FROM (
			SELECT routine_name AS object_name, routine_type AS object_kind
			FROM information_schema.routines
			WHERE routine_schema = COALESCE(NULLIF(?, ''), DATABASE())

			UNION ALL

			SELECT event_name, 'EVENT'
			FROM information_schema.events
			WHERE event_schema = COALESCE(NULLIF(?, ''), DATABASE())

			UNION ALL

			SELECT table_name, 'SEQUENCE'
			FROM information_schema.tables
			WHERE table_schema = COALESCE(NULLIF(?, ''), DATABASE())
			  AND table_type = 'SEQUENCE'
		) AS cleanup_objects
		ORDER BY object_kind, object_name`,
		schema, schema, schema,
	)
	if err != nil {
		return nil, fmt.Errorf("inspect stored objects: %w", err)
	}
	defer rows.Close()

	objects := []Object{}
	for rows.Next() {
		var name string
		var kind string
		if err := rows.Scan(&name, &kind); err != nil {
			return nil, fmt.Errorf("scan stored object: %w", err)
		}
		objectType, ok := mysqlStoredObjectType(kind)
		if !ok {
			return nil, fmt.Errorf("inspect stored objects: unsupported object kind %q", kind)
		}
		objects = append(objects, Object{
			Type: objectType,
			Name: name,
		})
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate stored objects: %w", err)
	}
	return objects, nil
}

// inspectRevisionTables reports the migration bookkeeping tables that this
// dialect's writer destroys but its reader hides.
//
// The dbschema readers exclude Ptah's revision tables from every schema
// snapshot by name. That is right for schema comparison — a diff must not pit
// Ptah's own bookkeeping against a user's model — and wrong for the single
// consumer that is enumerating what it is about to destroy rather than
// comparing. Widening the plan's object kinds cannot reach these tables,
// because they are filtered before kinds are considered.
//
// The names come from internal/revisiontable, which is also where
// migration/migrator derives its own defaults, so the two cannot drift. A
// second literal here would report nothing on any setup whose revision table is
// not the one the literal was written against — the same silent under-report
// this function exists to fix.
func inspectRevisionTables(conn *dbschema.DatabaseConnection) ([]Object, error) {
	dialect := conn.Info().Dialect
	if !coverageFor(dialect).revisionTables {
		return nil, nil
	}
	names := revisiontable.DefaultNames()
	query, args := revisionTableProbe(dialect, strings.TrimSpace(conn.Info().Schema), names)
	if query == "" {
		// A dialect marked as covered above but given no probe below. Reporting
		// nothing restores the old under-report rather than running some other
		// dialect's catalog query against it.
		return nil, fmt.Errorf("inspect cleanup revision tables: no catalog probe for dialect %q", dialect)
	}
	rows, err := conn.Query(query, args...)
	if err != nil {
		return nil, fmt.Errorf("inspect cleanup revision tables: %w", err)
	}
	defer rows.Close()

	objects := []Object{}
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, fmt.Errorf("scan cleanup revision table: %w", err)
		}
		// Schema is deliberately left empty, matching what every measured
		// reader records for a table inside the connection's own schema. The
		// rendered command then reads like the plan's other table rows, and the
		// duplicate check below compares like against like.
		objects = append(objects, Object{Type: ObjectTypeTable, Name: name})
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate cleanup revision tables: %w", err)
	}
	return objects, nil
}

// revisionTableProbe builds the catalog query that finds which of the
// migrator's bookkeeping tables exist inside the cleanup scope.
//
// The scope mirrors each writer's own, because the plan may only name what the
// writer really destroys: the connection's schema for PostgreSQL and SQL Server
// — defaulting to the same "public" and "dbo" those writers default to — and
// the session database for MySQL. A revision table sitting in some other schema
// is out of the writer's reach, so it must stay out of the plan.
//
// An unrecognized dialect yields an empty query rather than falling through to
// somebody else's catalog: marking a dialect covered without teaching this
// function how to read its catalog is a mistake that should surface, not one
// that should send pg_class to a database that has none.
func revisionTableProbe(dialect, schema string, names []string) (string, []any) {
	switch {
	case isSQLServer(dialect):
		if schema == "" {
			schema = "dbo"
		}
		args := append([]any{schema}, namesAsArgs(names)...)
		return `
			SELECT t.name
			FROM sys.tables AS t
			JOIN sys.schemas AS s ON s.schema_id = t.schema_id
			WHERE t.is_ms_shipped = 0
			  AND s.name = @p1
			  AND t.name IN (` + revisionTablePlaceholders(dialect, 2, len(names)) + `)
			ORDER BY t.name`, args
	case isMySQLFamily(dialect):
		// COALESCE mirrors mysql.Writer.cleanupSchema: the configured schema
		// when the connection pins one, otherwise the session's database.
		args := append([]any{schema}, namesAsArgs(names)...)
		return `
			SELECT table_name
			FROM information_schema.tables
			WHERE table_schema = COALESCE(NULLIF(?, ''), DATABASE())
			  AND table_type = 'BASE TABLE'
			  AND table_name IN (` + revisionTablePlaceholders(dialect, 2, len(names)) + `)
			ORDER BY table_name`, args
	case isPostgresFamily(dialect):
		if schema == "" {
			schema = "public"
		}
		args := append([]any{schema}, namesAsArgs(names)...)
		// relkind 'r' and 'p' are ordinary and partitioned tables. Anything the
		// writer would drop under a different statement is excluded, so a
		// same-named view or sequence cannot enter the plan as a table.
		return `
			SELECT c.relname
			FROM pg_class c
			JOIN pg_namespace n ON n.oid = c.relnamespace
			WHERE n.nspname = $1
			  AND c.relkind IN ('r', 'p')
			  AND c.relname IN (` + revisionTablePlaceholders(dialect, 2, len(names)) + `)
			ORDER BY c.relname`, args
	default:
		return "", nil
	}
}

func namesAsArgs(names []string) []any {
	args := make([]any, len(names))
	for i, name := range names {
		args[i] = name
	}
	return args
}

func revisionTablePlaceholders(dialect string, start, count int) string {
	parts := make([]string, count)
	for i := range parts {
		switch {
		case isSQLServer(dialect):
			parts[i] = fmt.Sprintf("@p%d", start+i)
		case isPostgresFamily(dialect):
			parts[i] = fmt.Sprintf("$%d", start+i)
		default:
			parts[i] = "?"
		}
	}
	return strings.Join(parts, ", ")
}

// unlistedObjects returns the entries of candidates that listed does not
// already name, comparing on kind and name only.
//
// Ignoring the schema is what makes the revision-table probe safe to run on a
// dialect whose reader already surfaces one of the two names — PostgreSQL
// reports atlas_schema_revisions but hides schema_migrations — without the plan
// listing that table twice. Dropping the schema from the comparison can only
// suppress an addition, never invent one, so the worst case is the pre-existing
// behavior rather than a new line on a destructive plan.
func unlistedObjects(listed, candidates []Object) []Object {
	type key struct{ objectType, name string }
	seen := make(map[key]struct{}, len(listed))
	for _, object := range listed {
		seen[key{object.Type, object.Name}] = struct{}{}
	}
	missing := make([]Object, 0, len(candidates))
	for _, candidate := range candidates {
		identity := key{candidate.Type, candidate.Name}
		if _, ok := seen[identity]; ok {
			continue
		}
		seen[identity] = struct{}{}
		missing = append(missing, candidate)
	}
	return missing
}

func mysqlStoredObjectType(kind string) (string, bool) {
	switch strings.ToUpper(strings.TrimSpace(kind)) {
	case "EVENT":
		return ObjectTypeEvent, true
	case "FUNCTION":
		return ObjectTypeFunction, true
	case "PROCEDURE":
		return ObjectTypeProcedure, true
	case "SEQUENCE":
		return ObjectTypeSequence, true
	default:
		return "", false
	}
}

func postgresRuntimeObjectType(kind string) (string, bool) {
	switch strings.ToLower(strings.TrimSpace(kind)) {
	case "aggregate":
		return ObjectTypeAggregate, true
	case "collation":
		return ObjectTypeCollation, true
	case "default_privilege":
		return ObjectTypeDefaultPrivilege, true
	case "foreign_table":
		return ObjectTypeForeignTable, true
	case "function":
		return ObjectTypeFunction, true
	case "procedure":
		return ObjectTypeProcedure, true
	case "sequence":
		return ObjectTypeSequence, true
	default:
		return "", false
	}
}

// sortObjects orders the report by (type, schema, table, name, parameters):
// object kind first, alphabetically by the ObjectType* string, then by location
// within the kind. That is what orders objects of different kinds relative to
// each other — nothing else does, and in particular nothing about dependency
// direction does.
//
// This is a REPORT ORDER, NOT AN EXECUTION ORDER. Reading it top to bottom does
// not describe the sequence cleanup performs, and replaying the rendered Cmd
// strings in this order would fail: "table" sorts before "view", so a view's
// backing table is listed first even though cleanup drops views first. An
// unscoped Apply delegates to SchemaWriter.DropAllTables. A scoped ApplyPlan
// uses sortExecutionObjects below; it preserves the writer's known cross-kind
// dependencies and PostgreSQL's live view dependency depth without changing
// this stable report.
//
// Alphabetical-by-kind is chosen because it is dialect independent, stable
// across releases, and diffs cleanly when a schema gains or loses one object.
// Adding a kind slots it in alphabetically without reordering the kinds already
// present relative to one another.
func sortObjects(objects []Object) {
	slices.SortFunc(objects, func(a, b Object) int {
		if cmp := strings.Compare(a.Type, b.Type); cmp != 0 {
			return cmp
		}
		if cmp := strings.Compare(a.Schema, b.Schema); cmp != 0 {
			return cmp
		}
		if cmp := strings.Compare(a.Table, b.Table); cmp != 0 {
			return cmp
		}
		if cmp := strings.Compare(a.Name, b.Name); cmp != 0 {
			return cmp
		}
		return strings.Compare(a.Parameters, b.Parameters)
	})
}

// sortExecutionObjects orders known dependents before the objects they can
// depend on. The writer's catalog query remains authoritative for an unscoped
// cleanup; this order is for ApplyPlan, whose selected object set cannot be
// handed to DropAllTables without widening the destructive scope.
//
// PostgreSQL is the discriminating case: views depend on other views and
// tables, tables own implicit SERIAL and identity sequences, and routines can
// depend on types. Foreign keys are removed before every relation. Live
// PostgreSQL view depth orders same-kind dependents before their dependencies.
// MySQL and MariaDB use the same broad relation order but destroy stored
// programs after tables, matching their cleanup writer.
func sortExecutionObjects(
	objects []Object,
	dialect string,
	executionDepths map[executionObjectIdentity]int,
) {
	slices.SortFunc(objects, func(a, b Object) int {
		if cmp := executionPriority(dialect, a.Type) - executionPriority(dialect, b.Type); cmp != 0 {
			return cmp
		}
		if cmp := executionDepths[objectExecutionIdentity(b)] -
			executionDepths[objectExecutionIdentity(a)]; cmp != 0 {
			return cmp
		}
		if cmp := strings.Compare(a.Schema, b.Schema); cmp != 0 {
			return cmp
		}
		if cmp := strings.Compare(a.Table, b.Table); cmp != 0 {
			return cmp
		}
		if cmp := strings.Compare(a.Name, b.Name); cmp != 0 {
			return cmp
		}
		return strings.Compare(a.Parameters, b.Parameters)
	})
}

func executionPriority(dialect, objectType string) int {
	if isMySQLFamily(dialect) {
		switch objectType {
		case ObjectTypeView, ObjectTypeMaterializedView:
			return 0
		case ObjectTypeForeignKey:
			return 10
		case ObjectTypeTable, ObjectTypeForeignTable:
			return 20
		case ObjectTypeEvent, ObjectTypeFunction, ObjectTypeProcedure, ObjectTypeAggregate:
			return 30
		case ObjectTypeSequence:
			return 40
		default:
			return 100
		}
	}

	switch objectType {
	case ObjectTypeForeignKey:
		return 0
	case ObjectTypeView, ObjectTypeMaterializedView:
		return 10
	case ObjectTypeTable, ObjectTypeForeignTable:
		return 20
	case ObjectTypeSequence:
		return 30
	case ObjectTypeEvent, ObjectTypeFunction, ObjectTypeProcedure, ObjectTypeAggregate:
		return 40
	case ObjectTypeComposite, ObjectTypeDomain, ObjectTypeEnum, ObjectTypeRange:
		return 50
	case ObjectTypeCollation:
		return 60
	case ObjectTypeDefaultPrivilege:
		return 70
	default:
		return 100
	}
}

func dropCommand(dialect string, object Object) string {
	if strings.TrimSpace(object.Command) != "" {
		return object.Command
	}
	name := sqlident.Qualified(dialect, object.Schema, object.Name)
	switch object.Type {
	case ObjectTypeComposite, ObjectTypeEnum, ObjectTypeRange:
		// Composite, enum, and range types are all pg_type rows, and
		// PostgreSQL drops all three with DROP TYPE.
		return "DROP TYPE IF EXISTS " + name + " RESTRICT"
	case ObjectTypeDomain:
		return "DROP DOMAIN IF EXISTS " + name + " RESTRICT"
	case ObjectTypeEvent:
		return "DROP EVENT IF EXISTS " + name
	case ObjectTypeForeignKey:
		return dropForeignKeyCommand(dialect, object)
	case ObjectTypeFunction:
		return dropRoutineCommand(dialect, "FUNCTION", name, object.Parameters)
	case ObjectTypeMaterializedView:
		return "DROP MATERIALIZED VIEW IF EXISTS " + name + " RESTRICT"
	case ObjectTypeProcedure:
		return dropRoutineCommand(dialect, "PROCEDURE", name, object.Parameters)
	case ObjectTypeSequence:
		return dropSequenceCommand(dialect, name)
	case ObjectTypeTable:
		return dropTableCommand(dialect, name)
	case ObjectTypeView:
		return dropViewCommand(dialect, name)
	default:
		return ""
	}
}

func dropForeignKeyCommand(dialect string, object Object) string {
	table := sqlident.Qualified(dialect, object.Schema, object.Table)
	constraint := sqlident.Quote(dialect, object.Name)
	if isMySQLFamily(dialect) {
		return "ALTER TABLE " + table + " DROP FOREIGN KEY " + constraint
	}
	return "ALTER TABLE " + table + " DROP CONSTRAINT " + constraint
}

func dropRoutineCommand(dialect, keyword, name, parameters string) string {
	if isPostgresFamily(dialect) {
		return "DROP " + keyword + " IF EXISTS " + name + "(" + parameters + ") RESTRICT"
	}
	return "DROP " + keyword + " IF EXISTS " + name
}

func dropSequenceCommand(dialect, name string) string {
	if isPostgresFamily(dialect) {
		return "DROP SEQUENCE IF EXISTS " + name + " RESTRICT"
	}
	return "DROP SEQUENCE IF EXISTS " + name
}

func dropViewCommand(dialect, name string) string {
	if isPostgresFamily(dialect) {
		return "DROP VIEW IF EXISTS " + name + " RESTRICT"
	}
	return "DROP VIEW IF EXISTS " + name
}

func dropTableCommand(dialect, name string) string {
	if isPostgresFamily(dialect) {
		return "DROP TABLE IF EXISTS " + name + " RESTRICT"
	}
	if normalizeDialect(dialect) == "clickhouse" {
		return "DROP TABLE IF EXISTS " + name + " SYNC"
	}
	return "DROP TABLE IF EXISTS " + name
}

func isCleanupTableType(tableType string) bool {
	switch strings.ToUpper(strings.TrimSpace(tableType)) {
	case "", "TABLE", "BASE TABLE":
		return true
	default:
		return false
	}
}

func isForeignKeyConstraint(constraint dbschematypes.DBConstraint) bool {
	return strings.EqualFold(strings.TrimSpace(constraint.Type), "FOREIGN KEY")
}

func isPostgresFamily(dialect string) bool {
	switch normalizeDialect(dialect) {
	case "postgres", "postgresql", "cockroachdb", "yugabytedb":
		return true
	default:
		return false
	}
}

func isPostgres(dialect string) bool {
	switch normalizeDialect(dialect) {
	case "postgres", "postgresql":
		return true
	default:
		return false
	}
}

func isMySQLFamily(dialect string) bool {
	switch normalizeDialect(dialect) {
	case "mysql", "mariadb":
		return true
	default:
		return false
	}
}

func isSQLServer(dialect string) bool {
	switch normalizeDialect(dialect) {
	case "sqlserver", "mssql":
		return true
	default:
		return false
	}
}

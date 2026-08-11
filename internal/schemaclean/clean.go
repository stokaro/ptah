// Package schemaclean plans and executes destructive schema cleanup.
package schemaclean

import (
	"context"
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
	ObjectTypeComposite        = "composite"
	ObjectTypeDomain           = "domain"
	ObjectTypeEnum             = "enum"
	ObjectTypeEvent            = "event"
	ObjectTypeForeignKey       = "foreign_key"
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
}

type Object struct {
	Type   string
	Schema string
	Table  string
	Name   string
}

// Change is one line of the cleanup report: an object that Apply destroys, plus
// a rendered DROP statement describing that destruction.
//
// Cmd is documentation, not the statement Apply runs. Apply delegates to
// SchemaWriter.DropAllTables, which builds its own statements from a live
// catalog query; those differ in detail (the PostgreSQL writer, for instance,
// emits RESTRICT where this report renders CASCADE, and drops overloaded
// functions by full signature where this report renders the bare name). Cmd
// exists so an operator can read what a plan line means before approving a
// destructive command.
type Change struct {
	Type   string
	Schema string
	Table  string
	Name   string
	Cmd    string
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
	postgresSequences  bool
	mysqlStoredObjects bool

	// revisionTables records that this dialect's writer destroys Ptah's own
	// migration bookkeeping tables while its reader hides them by name, so
	// Inspect has to find them in the live catalog. See inspectRevisionTables.
	revisionTables bool
}

func Inspect(conn *dbschema.DatabaseConnection) (Plan, error) {
	return InspectWithOptions(conn, InspectOptions{})
}

// InspectWithOptions returns a cleanup plan after applying caller-selected
// validation to the exact catalog snapshot used to build it.
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
	revisionObjects, err := inspectRevisionTables(conn)
	if err != nil {
		return Plan{}, err
	}
	objects = append(objects, unlistedObjects(objects, revisionObjects)...)
	return PlanFromObjects(objects, dialect), nil
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

// ApplyPlan executes exactly the changes plan carries, in plan order.
//
// It is the execution half of a narrowed plan. [Apply] hands the whole database
// to the writer's DropAllTables, which is correct only when the plan describes
// the whole database; a caller that removed objects from the plan must not then
// reach for a routine that ignores the plan, or the flag that narrowed it would
// print one thing and destroy another.
func ApplyPlan(ctx context.Context, conn *dbschema.DatabaseConnection, plan Plan) error {
	conn.SchemaWriter().SetDryRun(false)
	executor := conn.Writer()
	for _, change := range plan.Changes {
		if strings.TrimSpace(change.Cmd) == "" {
			continue
		}
		if err := executor.ExecuteSQL(ctx, change.Cmd); err != nil {
			return fmt.Errorf("drop schema object %s %q: %w", change.Type, change.Name, err)
		}
	}
	return nil
}

func PlanFromSchema(schema *dbschematypes.DBSchema, dialect string) Plan {
	if schema == nil {
		return Plan{}
	}
	return PlanFromObjects(cleanupObjects(schema, dialect), dialect)
}

func PlanFromObjects(objects []Object, dialect string) Plan {
	objects = append([]Object(nil), objects...)
	sortObjects(objects)
	changes := make([]Change, 0, len(objects))
	for _, object := range objects {
		changes = append(changes, Change{
			Type:   object.Type,
			Schema: object.Schema,
			Table:  object.Table,
			Name:   object.Name,
			Cmd:    dropCommand(dialect, object),
		})
	}
	return Plan{
		Objects: objects,
		Changes: changes,
	}
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
			// The standalone-sequence probe stays restricted to PostgreSQL
			// proper because that is the only PostgreSQL-family engine this
			// package has been measured against, and widening it is not this
			// change's subject. The CockroachDB and YugabyteDB writers do drop
			// sequences, so their plans still understate cleanup by that kind.
			postgresSequences: normalized == "postgres" || normalized == "postgresql",
			revisionTables:    true,
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
		{coverage.functions, functionObjects},
	} {
		if !kind.covered {
			continue
		}
		objects = append(objects, kind.collect(schema)...)
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

// functionObjects reports PostgreSQL functions. DBFunction carries neither a
// schema nor a signature, so overloads appear as repeated rows sharing one
// name. Each row is still one function the writer destroys.
func functionObjects(schema *dbschematypes.DBSchema) []Object {
	objects := make([]Object, 0, len(schema.Functions))
	for _, function := range schema.Functions {
		objects = append(objects, Object{
			Type: ObjectTypeFunction,
			Name: function.Name,
		})
	}
	return objects
}

// inspectRuntimeObjects enumerates the object kinds a dialect's writer destroys
// that the dbschema reader's snapshot does not carry.
func inspectRuntimeObjects(conn *dbschema.DatabaseConnection) ([]Object, error) {
	coverage := coverageFor(conn.Info().Dialect)
	switch {
	case coverage.postgresSequences:
		return inspectPostgresSequences(conn)
	case coverage.mysqlStoredObjects:
		return inspectMySQLStoredObjects(conn)
	default:
		return nil, nil
	}
}

func inspectPostgresSequences(conn *dbschema.DatabaseConnection) ([]Object, error) {
	schema := strings.TrimSpace(conn.Info().Schema)
	if schema == "" {
		schema = "public"
	}
	rows, err := conn.Query(`
		SELECT sequence_name
		FROM information_schema.sequences
		WHERE sequence_schema = $1
		ORDER BY sequence_name`, schema)
	if err != nil {
		return nil, fmt.Errorf("inspect cleanup sequences: %w", err)
	}
	defer rows.Close()

	objects := []Object{}
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, fmt.Errorf("scan cleanup sequence: %w", err)
		}
		objects = append(objects, Object{
			Type:   ObjectTypeSequence,
			Schema: schema,
			Name:   name,
		})
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate cleanup sequences: %w", err)
	}
	return objects, nil
}

// inspectMySQLStoredObjects reports the stored programs and MariaDB sequences
// that the MySQL writer's listCleanupObjects returns but the MySQL reader never
// records: information_schema.routines, information_schema.events, and the
// SEQUENCE rows of information_schema.tables.
//
// The scope mirrors mysql.Writer.cleanupSchema: the configured schema when the
// connection pins one, otherwise the session's current database.
func inspectMySQLStoredObjects(conn *dbschema.DatabaseConnection) ([]Object, error) {
	schema := strings.TrimSpace(conn.Info().Schema)
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
		return nil, fmt.Errorf("inspect cleanup stored objects: %w", err)
	}
	defer rows.Close()

	objects := []Object{}
	for rows.Next() {
		var name string
		var kind string
		if err := rows.Scan(&name, &kind); err != nil {
			return nil, fmt.Errorf("scan cleanup stored object: %w", err)
		}
		objectType, ok := mysqlStoredObjectType(kind)
		if !ok {
			return nil, fmt.Errorf("inspect cleanup stored objects: unsupported object kind %q", kind)
		}
		objects = append(objects, Object{
			Type: objectType,
			Name: name,
		})
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate cleanup stored objects: %w", err)
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

// sortObjects orders the report by (type, schema, table, name): object kind
// first, alphabetically by the ObjectType* string, then by location within the
// kind. That is what orders objects of different kinds relative to each other —
// nothing else does, and in particular nothing about dependency direction does.
//
// This is a REPORT ORDER, NOT AN EXECUTION ORDER. Reading it top to bottom does
// not describe the sequence Apply performs, and replaying the rendered Cmd
// strings in this order would fail: "table" sorts before "view", so a view's
// backing table is listed first even though the writer drops views first. Apply
// delegates to SchemaWriter.DropAllTables, whose execution order comes from the
// writer's own dependency-aware catalog query — for PostgreSQL, `ORDER BY
// priority, dependency_depth DESC, ...` in internal/dbschema/postgres/writer.go.
// This package cannot reproduce that order because the reader's snapshot
// carries no dependency depth.
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
		return strings.Compare(a.Name, b.Name)
	})
}

func dropCommand(dialect string, object Object) string {
	name := sqlident.Qualified(dialect, object.Schema, object.Name)
	switch object.Type {
	case ObjectTypeComposite, ObjectTypeEnum, ObjectTypeRange:
		// Composite, enum, and range types are all pg_type rows, and
		// PostgreSQL drops all three with DROP TYPE.
		return "DROP TYPE IF EXISTS " + name + " CASCADE"
	case ObjectTypeDomain:
		return "DROP DOMAIN IF EXISTS " + name + " CASCADE"
	case ObjectTypeEvent:
		return "DROP EVENT IF EXISTS " + name
	case ObjectTypeForeignKey:
		return dropForeignKeyCommand(dialect, object)
	case ObjectTypeFunction:
		return dropRoutineCommand(dialect, "FUNCTION", name)
	case ObjectTypeMaterializedView:
		return "DROP MATERIALIZED VIEW IF EXISTS " + name + " CASCADE"
	case ObjectTypeProcedure:
		return dropRoutineCommand(dialect, "PROCEDURE", name)
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

func dropRoutineCommand(dialect, keyword, name string) string {
	if isPostgresFamily(dialect) {
		return "DROP " + keyword + " IF EXISTS " + name + " CASCADE"
	}
	return "DROP " + keyword + " IF EXISTS " + name
}

func dropSequenceCommand(dialect, name string) string {
	if isPostgresFamily(dialect) {
		return "DROP SEQUENCE IF EXISTS " + name + " CASCADE"
	}
	return "DROP SEQUENCE IF EXISTS " + name
}

func dropViewCommand(dialect, name string) string {
	if isPostgresFamily(dialect) {
		return "DROP VIEW IF EXISTS " + name + " CASCADE"
	}
	return "DROP VIEW IF EXISTS " + name
}

func dropTableCommand(dialect, name string) string {
	if isPostgresFamily(dialect) {
		return "DROP TABLE IF EXISTS " + name + " CASCADE"
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

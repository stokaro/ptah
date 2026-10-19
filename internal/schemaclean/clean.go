// Package schemaclean plans and executes destructive schema cleanup.
package schemaclean

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"slices"
	"strings"

	"ptah.run/catalog"
	"ptah.run/core/objectidentity"
	"ptah.run/core/platform"
	"ptah.run/core/platform/descriptor"
	"ptah.run/core/schemaext"
	"ptah.run/dbschema"
	"ptah.run/feature/pgextension"
	"ptah.run/internal/dbreset"
	"ptah.run/internal/revisiontable"
	"ptah.run/internal/sqlident"
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
	ObjectTypeSynonym          = "synonym"
	ObjectTypeTable            = "table"
	ObjectTypeTrigger          = "trigger"
	ObjectTypeType             = "type"
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
	ValidateSchema func(*catalog.Database) error
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
	schema *catalog.Database,
	dialect,
	writerSchema string,
) *catalog.Database {
	if schema == nil || !cleanupOf(dialect).SchemaScoped {
		return schema
	}
	writerSchema = strings.TrimSpace(writerSchema)
	if writerSchema == "" {
		writerSchema = defaultSchemaOf(dialect)
	}
	owned := *schema
	// The extension owner's observations are kept when they are installed in
	// the writer's schema. One that is not an observation names no schema to
	// keep it by and stays, so the snapshot's validation still sees it.
	owned.FeatureObjects = schema.FeatureObjects.Select(func(ref objectidentity.ID) bool {
		if ref.Kind != objectidentity.Kind(pgextension.ExtensionKind) {
			return true
		}
		object, _, err := schema.FeatureObjects.Get(ref)
		installed, observed := object.Value.(*pgextension.ObservedExtension)
		return err != nil || !observed || strings.TrimSpace(installed.Schema) == writerSchema
	})
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
// catalog.SchemaWriter.DropAllTables implementation actually destroys.
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

	// revisionTables records that this dialect's writer destroys Ptah's own
	// migration bookkeeping tables while its reader hides them by name, so
	// Inspect has to find them in the live catalog. See inspectRevisionTables.
	revisionTables bool

	// writerListing records that this dialect's writer lists what it drops,
	// each object with its drop statement, in the order it drops them, so the
	// plan is that list rather than a reading of the schema snapshot. See
	// inspectWriterObjects.
	writerListing bool
}

func Inspect(ctx context.Context, conn *dbschema.DatabaseConnection) (Plan, error) {
	return InspectWithOptions(ctx, conn, InspectOptions{})
}

// InspectWithOptions returns a cleanup plan after applying caller-selected
// validation to the reader snapshot. The returned plan also includes the
// writer-only live-catalog objects that the reader cannot represent.
func InspectWithOptions(ctx context.Context, conn *dbschema.DatabaseConnection, opts InspectOptions) (Plan, error) {
	schema, err := conn.Reader().ReadSchemaContext(ctx)
	if err != nil {
		return Plan{}, fmt.Errorf("inspect schema before cleanup: %w", err)
	}
	if opts.ValidateSchema != nil {
		if err := opts.ValidateSchema(schema); err != nil {
			return Plan{}, err
		}
	}
	dialect := conn.Info().Dialect
	if coverageFor(dialect).writerListing {
		objects, depths, err := inspectWriterObjects(ctx, conn)
		if err != nil {
			return Plan{}, err
		}
		return planFromObjects(objects, dialect, depths), nil
	}
	if IsServer(conn) {
		objects, err := inspectServer(ctx, conn)
		if err != nil {
			return Plan{}, err
		}
		return planFromObjects(objects, dialect, nil), nil
	}
	objects := cleanupObjects(schema, dialect)
	runtimeObjects, err := inspectRuntimeObjects(ctx, conn, nil)
	if err != nil {
		return Plan{}, err
	}
	objects = append(objects, runtimeObjects...)
	executionDepths, err := inspectViewDepths(ctx, conn, objects)
	if err != nil {
		return Plan{}, err
	}
	revisionObjects, err := inspectRevisionTables(ctx, conn)
	if err != nil {
		return Plan{}, err
	}
	if cleanupOf(dialect).RevisionTablesEverywhere {
		// The reader leaves these names out of every directory, so none is
		// listed already, and two directories may each hold one.
		objects = append(objects, revisionObjects...)
	} else {
		objects = append(objects, unlistedObjects(objects, revisionObjects)...)
	}
	return planFromObjects(objects, dialect, executionDepths), nil
}

func Execute(ctx context.Context, conn *dbschema.DatabaseConnection, opts Options) (Plan, error) {
	plan, err := Inspect(ctx, conn)
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
	// A whole server has no writer scope to hand DropAllTables: its cleanup
	// is the plan of its databases, executed as planned.
	if IsServer(conn) {
		plan, err := Inspect(ctx, conn)
		if err != nil {
			return err
		}
		return ApplyPlan(ctx, conn, plan)
	}
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
	return ApplyPlanWithOptions(ctx, conn, plan, ApplyPlanOptions{})
}

// ApplyPlanOptions configures validation at the destructive execution
// boundary. PostgreSQL validates through the transaction session after locking
// every planned relation that can own dependent objects and before executing
// any drop.
type ApplyPlanOptions struct {
	ValidateBeforeExecute func(catalog.SchemaExecutor) error
}

// ApplyPlanWithOptions executes exactly the changes plan carries and applies
// the optional execution-boundary validation before the first mutation.
func ApplyPlanWithOptions(
	ctx context.Context,
	conn *dbschema.DatabaseConnection,
	plan Plan,
	opts ApplyPlanOptions,
) error {
	changes := executableChanges(plan)
	if len(changes) == 0 {
		if opts.ValidateBeforeExecute != nil {
			return opts.ValidateBeforeExecute(nil)
		}
		return nil
	}
	conn.SchemaWriter().SetDryRun(false)
	if cleanupOf(conn.Info().Dialect).SchemaScoped {
		return applyPostgresFamilyPlan(ctx, conn, changes, opts)
	}
	if opts.ValidateBeforeExecute != nil {
		if err := opts.ValidateBeforeExecute(conn.Writer()); err != nil {
			return err
		}
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
	opts ApplyPlanOptions,
) error {
	tx, err := conn.SchemaWriter().BeginTransaction(ctx)
	if err != nil {
		return fmt.Errorf("begin scoped schema cleanup: %w", err)
	}
	if strategy := cleanupCatalogOf(conn); strategy != nil {
		if err := strategy.LockRelations(ctx, tx, plannedRelations(changes)); err != nil {
			return errors.Join(err, cleanupRollbackError(tx.Rollback()))
		}
	}
	if opts.ValidateBeforeExecute != nil {
		if err := opts.ValidateBeforeExecute(tx); err != nil {
			return errors.Join(err, cleanupRollbackError(tx.Rollback()))
		}
	}
	// After the caller's revalidation, never before it. That hook is the
	// compatibility gate -- strict mode re-reads the catalog under the relation
	// lock and refuses an object that appeared since the snapshot was confirmed
	// -- and this is a convenience on top of it. Running first made a
	// concurrently created view answer "would leave dependents behind" where
	// the surface owes "strict compatibility does not support cleaning live
	// schema view", which the integration suite caught.
	if err := refuseUnselectedDependents(ctx, conn, changes); err != nil {
		return errors.Join(err, cleanupRollbackError(tx.Rollback()))
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

// plannedRelations are the tables and foreign tables a scoped plan drops, in
// the plan's order: the relations its engine's catalog locks before the first
// drop; see [dbreset.CleanupCatalog.LockRelations].
func plannedRelations(changes []Change) []dbreset.Relation {
	var relations []dbreset.Relation
	for _, change := range changes {
		if change.Type != ObjectTypeTable && change.Type != ObjectTypeForeignTable {
			continue
		}
		relations = append(relations, dbreset.Relation{Schema: change.Schema, Name: change.Name})
	}
	return relations
}

func applyPostgresFamilyPlanChanges(
	ctx context.Context,
	tx catalog.SchemaTransaction,
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
	tx catalog.SchemaTransaction,
	change Change,
) (dropErr, controlErr error) {
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
	executor catalog.SchemaExecutor,
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

func PlanFromSchema(schema *catalog.Database, dialect string) Plan {
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
// from system.tables twice: once for `engine IN ('View', 'MaterializedView')`,
// which it drops with DROP VIEW, and once for persistent table engines with
// `engine NOT LIKE '%View'`, which it drops with DROP TABLE. So it drops
// tables, views and materialized views. LiveView and WindowView stay out of
// both the writer and this plan because the reader never reports them.
//
// Oracle — internal/dbschema/oracle/writer.go, DropAllTables ->
// cleanupObjects, which reads USER_OBJECTS for tables, views, materialized
// views, sequences, synonyms, triggers, types and domains, and orders each
// domain after the domains that reference it. ResetObjects lists the same
// objects through the same function with the statement each is dropped with,
// so the plan is that list (writerListing). The Oracle reader hides Ptah's
// revision tables, which the writer drops as ordinary tables, and the listing
// carries them too.
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
	cleanup := cleanupOf(dialect)
	return dialectCoverage{
		foreignKeys:       cleanup.ForeignKeys,
		views:             cleanup.Views,
		materializedViews: cleanup.MaterializedViews,
		enums:             cleanup.Enums,
		domains:           cleanup.Domains,
		composites:        cleanup.Composites,
		ranges:            cleanup.Ranges,
		functions:         cleanup.Functions,
		revisionTables:    cleanup.RevisionTables,
		writerListing:     cleanup.WriterListing,
	}
}

// cleanupOf returns what dialect's descriptor states about cleaning up a
// database of it, folding the spelling as platform.NormalizeDialect does, and
// the zero value, which reports tables only, for a dialect the registry does
// not know.
func cleanupOf(dialect string) descriptor.Cleanup {
	described, _ := platform.Lookup(dialect)
	return described.Cleanup
}

// defaultSchemaOf returns the schema an unqualified name resolves to on
// dialect, as its descriptor states it.
func defaultSchemaOf(dialect string) string {
	described, _ := platform.Lookup(dialect)
	return described.DefaultSchema
}

// cleanupObjects turns the reader's schema snapshot into plan rows. Tables are
// unconditional; every other kind is gated on this dialect's writer coverage,
// paired with its collector below so a coverage field can never be added
// without a collector or the other way round.
func cleanupObjects(schema *catalog.Database, dialect string) []Object {
	if schema == nil {
		return nil
	}
	coverage := coverageFor(dialect)
	objects := tableObjects(schema)
	for _, kind := range []struct {
		covered bool
		collect func(*catalog.Database) []Object
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

func tableObjects(schema *catalog.Database) []Object {
	objects := make([]Object, 0, len(schema.Tables))
	for _, table := range schema.Tables {
		// Readers that surface views through Database.Tables as well as
		// Database.Views tag them here, and viewObjects reports them once.
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

func foreignKeyObjects(schema *catalog.Database) []Object {
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

func viewObjects(schema *catalog.Database) []Object {
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

func materializedViewObjects(schema *catalog.Database) []Object {
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

// enumObjects reports PostgreSQL enum types. Enum carries no schema: the
// reader reads enums per schema but does not record which one, so enum rows
// stay unqualified.
func enumObjects(schema *catalog.Database) []Object {
	objects := make([]Object, 0, len(schema.Enums))
	for _, enum := range schema.Enums {
		objects = append(objects, Object{
			Type: ObjectTypeEnum,
			Name: enum.Name,
		})
	}
	return objects
}

func domainObjects(schema *catalog.Database) []Object {
	return ownerTypeObjects(schema, objectidentity.KindDomain, ObjectTypeDomain)
}

func compositeObjects(schema *catalog.Database) []Object {
	return ownerTypeObjects(schema, objectidentity.KindComposite, ObjectTypeComposite)
}

func rangeObjects(schema *catalog.Database) []Object {
	return ownerTypeObjects(schema, objectidentity.KindRange, ObjectTypeRange)
}

// ownerTypeObjects reports the types of the common kind an owner's
// observations define, such as a domain the PostgreSQL type owner holds, as
// the plan rows of the common type; see [schemaext.TypeDefiner].
func ownerTypeObjects(schema *catalog.Database, kind objectidentity.Kind, objectType string) []Object {
	var objects []Object
	for _, defined := range schemaext.DefinedTypes(schema.FeatureObjects) {
		if defined.ID.Kind == kind {
			objects = append(objects, Object{Type: objectType, Schema: defined.ID.Schema.Authored(), Name: defined.ID.Name.Source})
		}
	}
	return objects
}

// functionObjects reports PostgreSQL functions. Parameters remains the full
// declaration form that inspect and report consumers expect, while the
// pre-rendered command uses PostgreSQL's distinct overload identity. Older or
// synthetic snapshots without identity metadata retain the former Parameters
// fallback instead of losing their executable cleanup capability.
func functionObjects(schema *catalog.Database, dialect string) []Object {
	objects := make([]Object, 0, len(schema.Functions))
	for _, function := range schema.Functions {
		// Procedures are enumerated by inspectRuntimeObjects, which already
		// names the kind and emits DROP PROCEDURE. They only reach this
		// collection because the reader models them now (stokaro/ptah#1722);
		// letting them through here would list each one twice and give one of
		// the two the wrong verb, which the server refuses by name.
		if strings.EqualFold(strings.TrimSpace(function.Kind), "procedure") {
			continue
		}
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
// resetLister is a writer that lists what its DropAllTables drops.
type resetLister interface {
	ResetObjects(ctx context.Context, scope dbreset.Scope) ([]dbreset.Object, error)
}

// writerObjectTypes maps the kinds a writer's listing names to the plan's.
var writerObjectTypes = map[string]string{
	"table":             ObjectTypeTable,
	"view":              ObjectTypeView,
	"materialized view": ObjectTypeMaterializedView,
	"sequence":          ObjectTypeSequence,
	"synonym":           ObjectTypeSynonym,
	"trigger":           ObjectTypeTrigger,
	"type":              ObjectTypeType,
	"domain":            ObjectTypeDomain,
}

// inspectWriterObjects lists what the connection's writer drops, each object
// with the statement it drops the object with, and the depth that keeps the
// writer's own order when the plan is executed: the first object it drops has
// the greatest depth. A kind the plan has no name for, and an object the
// writer names no statement for, are refused rather than reported as
// something else.
func inspectWriterObjects(ctx context.Context, conn *dbschema.DatabaseConnection) ([]Object, map[executionObjectIdentity]int, error) {
	lister, ok := conn.SchemaWriter().(resetLister)
	if !ok {
		return nil, nil, fmt.Errorf("inspect schema before cleanup: the %s writer does not list what it drops", conn.Info().Dialect)
	}
	listed, err := lister.ResetObjects(ctx, dbreset.Scope{})
	if err != nil {
		return nil, nil, fmt.Errorf("inspect schema before cleanup: %w", err)
	}
	objects := make([]Object, 0, len(listed))
	depths := make(map[executionObjectIdentity]int, len(listed))
	for i, found := range listed {
		objectType, known := writerObjectTypes[found.Kind]
		if !known || strings.TrimSpace(found.Statement) == "" {
			return nil, nil, fmt.Errorf("inspect schema before cleanup: the %s writer drops %s %q, which the plan cannot name",
				conn.Info().Dialect, found.Kind, found.Name)
		}
		object := Object{Type: objectType, Schema: found.Schema, Name: found.Name, Command: found.Statement}
		objects = append(objects, object)
		depths[objectExecutionIdentity(object)] = len(listed) - i
	}
	return objects, depths, nil
}

// catalogWriter is a writer that offers what its engine's catalog answers a
// cleanup plan where the schema reader cannot; see [dbreset.CleanupCatalog].
// A writer that is not one, or offers none, leaves the plan to the snapshot.
type catalogWriter interface {
	CleanupCatalog() dbreset.CleanupCatalog
}

// cleanupCatalogOf returns the catalog strategy of conn's writer, or nil
// where the writer offers none.
func cleanupCatalogOf(conn *dbschema.DatabaseConnection) dbreset.CleanupCatalog {
	return writerCatalog(conn.SchemaWriter())
}

// writerCatalog returns the catalog strategy writer offers, or nil where it
// is not a [catalogWriter] or offers none.
func writerCatalog(writer catalog.SchemaWriter) dbreset.CleanupCatalog {
	offering, ok := writer.(catalogWriter)
	if !ok {
		return nil
	}
	return offering.CleanupCatalog()
}

// InspectRuntimeObjects returns catalog objects that the ordinary schema
// reader cannot model but which remain relevant to live-schema policy: the
// ones the connection's writer lists from its catalog
// ([dbreset.CleanupCatalog.Objects]). When schemas is non-empty, only those
// schema scopes are inventoried; nil uses the connection's cleanup scope. The
// query is read-only and does not build or execute a cleanup plan.
//
// It takes no context, as its callers validate a snapshot they already hold,
// so the reads run under the background context.
func InspectRuntimeObjects(conn *dbschema.DatabaseConnection, schemas []string) ([]Object, error) {
	return inspectRuntimeObjects(context.Background(), conn, schemas)
}

// inspectRuntimeObjects lists the objects the writer's catalog strategy
// reports for each schema of the scope; see [catalogObjects].
func inspectRuntimeObjects(ctx context.Context, conn *dbschema.DatabaseConnection, schemas []string) ([]Object, error) {
	return catalogObjects(ctx, cleanupCatalogOf(conn), conn, conn.Info().Dialect, runtimeObjectSchemas(conn, schemas))
}

// catalogObjects lists, through q, the objects strategy reports for each of
// schemas, as plan rows. It answers nil for a nil strategy, and refuses a
// kind the plan has no name for, naming dialect.
func catalogObjects(
	ctx context.Context, strategy dbreset.CleanupCatalog, q dbreset.Querier, dialect string, schemas []string,
) ([]Object, error) {
	if strategy == nil {
		return nil, nil
	}
	objects := make([]Object, 0)
	for _, schema := range schemas {
		listed, err := strategy.Objects(ctx, q, schema)
		if err != nil {
			return nil, err
		}
		for _, found := range listed {
			if !slices.Contains(objectTypes(), found.Kind) {
				return nil, fmt.Errorf("inspect schema before cleanup: the %s writer lists %s %q, which the plan cannot name",
					dialect, found.Kind, found.Name)
			}
			objects = append(objects, Object{
				Type:         found.Kind,
				Schema:       found.Schema,
				Table:        found.Table,
				Name:         found.Name,
				SelectorName: found.SelectorName,
				Implicit:     found.Implicit,
				Command:      found.Statement,
			})
		}
	}
	return objects, nil
}

// objectTypes returns every object kind a cleanup plan can name.
func objectTypes() []string {
	return []string{
		ObjectTypeAggregate, ObjectTypeCollation, ObjectTypeComposite, ObjectTypeDefaultPrivilege, ObjectTypeDomain,
		ObjectTypeEnum, ObjectTypeEvent, ObjectTypeForeignKey, ObjectTypeForeignTable, ObjectTypeFunction,
		ObjectTypeMaterializedView, ObjectTypeProcedure, ObjectTypeRange, ObjectTypeSequence, ObjectTypeSynonym,
		ObjectTypeTable, ObjectTypeTrigger, ObjectTypeType, ObjectTypeView, ObjectTypeSchema,
	}
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

// inspectViewDepths reads, from the writer's catalog strategy, how deep each
// view and materialized view of the connection's schema, its default schema
// when none is configured, reads through other views; see [catalogViewDepths].
func inspectViewDepths(
	ctx context.Context,
	conn *dbschema.DatabaseConnection,
	objects []Object,
) (map[executionObjectIdentity]int, error) {
	schema := strings.TrimSpace(conn.Info().Schema)
	if schema == "" {
		schema = defaultSchemaOf(conn.Info().Dialect)
	}
	return catalogViewDepths(ctx, cleanupCatalogOf(conn), conn, schema, objects)
}

// catalogViewDepths reads, through q, how deep each view of schema reads
// through other views, and keys the depth of each object that is one of them
// by its identity, an object naming no schema being in schema, so a scoped
// plan drops the deepest first. It answers nil for a nil strategy and for one
// that orders nothing.
func catalogViewDepths(
	ctx context.Context, strategy dbreset.CleanupCatalog, q dbreset.Querier, schema string, objects []Object,
) (map[executionObjectIdentity]int, error) {
	if strategy == nil {
		return nil, nil
	}
	found, err := strategy.ViewDepths(ctx, q, schema)
	if err != nil || found == nil {
		return nil, err
	}
	depths := make(map[viewIdentity]int, len(found))
	for _, view := range found {
		depths[viewIdentity{objectType: view.Kind, schema: schema, name: view.Name}] = view.Depth
	}
	executionDepths := make(map[executionObjectIdentity]int)
	for i := range objects {
		objectSchema := strings.TrimSpace(objects[i].Schema)
		if objectSchema == "" {
			objectSchema = schema
		}
		depth := depths[viewIdentity{
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

type viewIdentity struct {
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
func inspectRevisionTables(ctx context.Context, conn *dbschema.DatabaseConnection) ([]Object, error) {
	dialect := conn.Info().Dialect
	if !coverageFor(dialect).revisionTables {
		return nil, nil
	}
	if cleanupOf(dialect).RevisionProbe == "" {
		return inspectRevisionTablesByName(ctx, conn)
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

	objects := make([]Object, 0)
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

// ydbTableFinder is what the YDB revision-table probe needs of the writer: YDB
// has no catalog a query could ask, and the writer walks the scheme tree.
type ydbTableFinder interface {
	TablesNamed(ctx context.Context, names []string) ([]dbreset.Object, error)
}

// inspectYDBRevisionTables finds the migrator's tables in every directory the
// YDB writer's DropAllTables enters. The YDB reader leaves the default
// revision tables, their logs and the tag table out of every directory, and
// DropAllTables drops each of them with the rest. Measured on 26.2.1.14 after
// `migrations up`: `db drop-all --dry-run` reported 1 object for a database
// whose writer then dropped users, schema_migrations and
// schema_migrations_log.
func inspectRevisionTablesByName(ctx context.Context, conn *dbschema.DatabaseConnection) ([]Object, error) {
	finder, ok := conn.SchemaWriter().(ydbTableFinder)
	if !ok {
		return nil, fmt.Errorf("inspect cleanup revision tables: the %T writer cannot find tables by name", conn.SchemaWriter())
	}
	found, err := finder.TablesNamed(ctx, append(revisiontable.DefaultNames(), revisiontable.Tags))
	if err != nil {
		return nil, fmt.Errorf("inspect cleanup revision tables: %w", err)
	}
	objects := make([]Object, 0, len(found))
	for _, table := range found {
		objects = append(objects, Object{Type: ObjectTypeTable, Schema: table.Schema, Name: table.Name})
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
	probe := cleanupOf(dialect).RevisionProbe
	if probe == "" {
		return "", nil
	}
	if schema == "" {
		schema = defaultSchemaOf(dialect)
	}
	args := append([]any{schema}, namesAsArgs(names)...)
	return strings.Replace(probe, "{names}", revisionTablePlaceholders(dialect, 2, len(names)), 1), args
}

func namesAsArgs(names []string) []any {
	args := make([]any, len(names))
	for i, name := range names {
		args[i] = name
	}
	return args
}

func revisionTablePlaceholders(dialect string, start, count int) string {
	described, _ := platform.Lookup(dialect)
	parts := make([]string, count)
	for i := range parts {
		switch described.Placeholder {
		case descriptor.PlaceholderAtP:
			parts[i] = fmt.Sprintf("@p%d", start+i)
		case descriptor.PlaceholderDollar:
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
	// The Oracle writer's listing is already in its drop order, which the
	// plan's depths carry, so no kind goes first on its own.
	if coverageFor(dialect).writerListing {
		return 0
	}
	if cleanupOf(dialect).ViewsFirst {
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
	case ObjectTypeSchema:
		if cleanupOf(dialect).DropsDatabases {
			return "DROP DATABASE " + sqlident.Quote(dialect, object.Name)
		}
		return ""
	case ObjectTypeFunction:
		return dropRoutineCommand(dialect, "FUNCTION", name, object.Parameters)
	case ObjectTypeMaterializedView:
		return dropMaterializedViewCommand(dialect, name)
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
	clause := cleanupOf(dialect).ForeignKeyDrop
	if clause == "" {
		clause = "DROP CONSTRAINT"
	}
	return "ALTER TABLE " + table + " " + clause + " " + constraint
}

func dropRoutineCommand(dialect, keyword, name, parameters string) string {
	if cleanupOf(dialect).Restrict {
		return "DROP " + keyword + " IF EXISTS " + name + "(" + parameters + ") RESTRICT"
	}
	return "DROP " + keyword + " IF EXISTS " + name
}

func dropSequenceCommand(dialect, name string) string {
	if cleanupOf(dialect).Restrict {
		return "DROP SEQUENCE IF EXISTS " + name + " RESTRICT"
	}
	return "DROP SEQUENCE IF EXISTS " + name
}

func dropViewCommand(dialect, name string) string {
	return "DROP VIEW IF EXISTS " + name + dropSuffix(dialect)
}

// dropMaterializedViewCommand renders the drop a materialized view takes.
//
// ClickHouse has no DROP MATERIALIZED VIEW statement at all -- that spelling
// is a syntax error on the server, and DROP VIEW is what removes a
// materialized view together with the storage table it owns. Rendering the
// portable spelling there would put a statement in the plan that the scoped
// executor cannot run. The descriptor says so with Sync.
func dropMaterializedViewCommand(dialect, name string) string {
	if cleanupOf(dialect).Sync {
		return "DROP VIEW IF EXISTS " + name + " SYNC"
	}
	return "DROP MATERIALIZED VIEW IF EXISTS " + name + " RESTRICT"
}

func dropTableCommand(dialect, name string) string {
	return "DROP TABLE IF EXISTS " + name + dropSuffix(dialect)
}

// dropSuffix is what a DROP TABLE or DROP VIEW ends with on dialect.
func dropSuffix(dialect string) string {
	switch cleanup := cleanupOf(dialect); {
	case cleanup.Restrict:
		return " RESTRICT"
	case cleanup.Sync:
		return " SYNC"
	default:
		return ""
	}
}

func isCleanupTableType(tableType string) bool {
	switch strings.ToUpper(strings.TrimSpace(tableType)) {
	case "", "TABLE", "BASE TABLE":
		return true
	default:
		return false
	}
}

func isForeignKeyConstraint(constraint catalog.Constraint) bool {
	return strings.EqualFold(strings.TrimSpace(constraint.Type), "FOREIGN KEY")
}

// dependentRelationsQuery asks which relations are defined in terms of the
// selected ones. It reaches views and materialized views, which are the
// dependents a scoped cleanup meets: both store their definition in pg_rewrite,
// and pg_depend links that rule back to every relation the definition reads.
//
// Placeholders are built by the caller rather than passed as an array, because
// the two PostgreSQL-family drivers in use disagree about how a Go slice binds
// to ANY($1) and a scoped cleanup should not depend on that.
const dependentRelationsQuery = `
	SELECT DISTINCT source.relname, dependent.relname, dependent.relkind
	FROM pg_depend d
	JOIN pg_rewrite r ON r.oid = d.objid
	JOIN pg_class dependent ON dependent.oid = r.ev_class
	JOIN pg_class source ON source.oid = d.refobjid
	JOIN pg_namespace n ON n.oid = source.relnamespace
	WHERE n.nspname = current_schema()
	  AND dependent.oid <> source.oid
	  AND source.relname IN (%s)`

// refuseUnselectedDependents fails a scoped cleanup BEFORE any DROP runs when a
// selected object has a dependent the selection leaves behind.
//
// Without it the run reached the server, PostgreSQL answered
// `cannot drop table t because other objects depend on it (SQLSTATE 2BP01)`,
// and the whole transaction rolled back -- so the user was told an SQL
// dependency failed without being told WHICH object caused it or what to add to
// the selection. Enumerating the dependents themselves means querying the
// catalog by hand, which is the work the tool is for (stokaro/ptah#1704).
//
// Every offending pair is reported, not the first: the server stops at the
// first failure, so a user widening the selection one name at a time would
// discover the rest one run at a time.
//
// A dependent the selection ALREADY drops is not a problem -- the plan is
// ordered by dependency depth, so it goes first -- and is filtered out here
// rather than reported as one.
func refuseUnselectedDependents(
	ctx context.Context,
	conn *dbschema.DatabaseConnection,
	changes []Change,
) error {
	selected := make(map[string]bool, len(changes))
	names := make([]string, 0, len(changes))
	for _, change := range changes {
		if strings.TrimSpace(change.Name) == "" {
			continue
		}
		if !selected[change.Name] {
			names = append(names, change.Name)
		}
		selected[change.Name] = true
	}
	if len(names) == 0 {
		return nil
	}

	placeholders := make([]string, 0, len(names))
	args := make([]any, 0, len(names))
	for i, name := range names {
		placeholders = append(placeholders, fmt.Sprintf("$%d", i+1))
		args = append(args, name)
	}
	rows, err := conn.QueryContext(ctx, fmt.Sprintf(dependentRelationsQuery, strings.Join(placeholders, ", ")), args...)
	if err != nil {
		// The check is an improvement on the diagnostic, not a gate on the
		// cleanup: a catalog this query cannot read leaves the previous
		// behavior, where the server reports the dependency itself.
		return nil
	}
	defer rows.Close()

	var blocked []string
	for rows.Next() {
		var source, dependent, kind string
		if err := rows.Scan(&source, &dependent, &kind); err != nil {
			return nil
		}
		if selected[dependent] {
			continue
		}
		blocked = append(blocked, fmt.Sprintf("%s %q depends on %s", relationKindName(kind), dependent, source))
	}
	if err := rows.Err(); err != nil || len(blocked) == 0 {
		return nil
	}
	slices.Sort(blocked)
	return fmt.Errorf(
		"scoped cleanup would leave dependents behind: %s. "+
			"Add them to the selection so they are dropped together, or widen it to the whole schema",
		strings.Join(blocked, "; "))
}

// relationKindName spells a pg_class.relkind for a diagnostic.
func relationKindName(kind string) string {
	switch kind {
	case "v":
		return "view"
	case "m":
		return "materialized view"
	case "r", "p":
		return "table"
	default:
		return "object"
	}
}

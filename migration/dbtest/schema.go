package dbtest

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"time"

	"ptah.run/core/annotation"
	"ptah.run/core/goschema"
	"ptah.run/core/objectidentity"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/core/selectext"
	"ptah.run/core/sqlutil"
	"ptah.run/dbschema"
	"ptah.run/engine"
	"ptah.run/migration/planner"
	"ptah.run/migration/schemadiff"
	"ptah.run/migration/schemadiff/difftypes"
)

// SchemaOptions configures a single [RunSchemaTest] invocation.
type SchemaOptions struct {
	// Runtime selects required feature conversion and comparison services.
	Runtime engine.SchemaRuntime
	// Cases are the test cases to run, in order.
	Cases []Case
	// AllowExternalCommands authorizes [ExternalStep]. See
	// [Options.AllowExternalCommands]: the default refuses the run before any
	// database is provisioned.
	AllowExternalCommands bool
	// ExternalTimeout bounds each external step. Zero selects
	// [DefaultExternalTimeout].
	ExternalTimeout time.Duration
	// Parallelism bounds how many cases marked [Case.Parallel] run at once.
	// See [Options.Parallelism].
	Parallelism int
	// RootDir is a directory of Go entity annotations describing the desired
	// schema. It is parsed with goschema.ParseDir and converged through the live
	// diff and planner path before any case runs (once per ephemeral per-case
	// database, or once for a shared explicit database).
	//
	// Desired takes precedence when set, which is how callers supply a schema
	// resolved from a source RootDir cannot express -- a .sql or .hcl file.
	RootDir string
	// Desired is an already-resolved desired schema. When non-nil RootDir is
	// used only to name the source in errors.
	Desired *schemamodel.Database
	// SeedDir is the default directory of seed files for seed steps that omit
	// their own [SeedStep.Dir].
	SeedDir string
	// DBURL is an optional database URL to run the tests against. It must point at
	// a throwaway database, because tests mutate schema and data. When empty, an
	// ephemeral SQLite database is provisioned per case in a temporary directory
	// and removed afterwards.
	DBURL string
	// ReportKind labels the report header. Empty leaves it "SCHEMA", which is
	// what a schema test run is; a plan test run passes "PLAN", because a
	// header naming the wrong verb is the first thing a reader trusts and the
	// last thing they check (stokaro/ptah#1211).
	ReportKind string
	// ResolveSchema loads the desired state a `schema` block's url names. It
	// backs the `schema` block of a plan case.
	//
	// It resolves rather than applies, so the applying stays in one place: the
	// same convergence path every other schema step uses. Which URL schemes
	// exist, and how a desired state is loaded from one, belongs to the command
	// that knows about schema files (stokaro/ptah#1211). A case using the step
	// with no callback set fails, rather than silently skipping the state it
	// was meant to establish.
	ResolveSchema func(url string) (*schemamodel.Database, error)
	// ApplyPlan executes the saved plan file at url against the connected
	// database. It backs the `apply` block of a plan case, and is supplied by
	// the caller for the same reason.
	ApplyPlan func(ctx context.Context, conn *dbschema.DatabaseConnection, url string) error
}

// RunSchemaTest applies a desired schema — parsed from the Go annotations in
// opts.RootDir — to a fresh database and runs each case's exec and assert steps
// against it. Unlike [RunMigrationTest] there are no migrations: the desired
// schema is converged once per case before that case's steps run, so a
// migrate_to step is invalid and fails with an explanatory detail rather than
// being silently skipped.
//
// Isolation mirrors [RunMigrationTest]: with an empty DBURL each case runs
// against its own fresh ephemeral SQLite database; with an explicit DBURL all
// cases share that one throwaway database and the caller owns their isolation.
//
// A returned error indicates the run itself could not be set up (invalid cases,
// an unparseable or unrenderable desired schema, an unreachable database, or a
// desired schema that fails to apply), or the context was interrupted; ordinary
// assertion failures are captured in the report, so callers should inspect
// [Report.Failed].
func RunSchemaTest(ctx context.Context, opts SchemaOptions) (*Report, error) {
	if err := schemaext.RequireRuntime(ctx, opts.Runtime); err != nil {
		return nil, err
	}
	if err := validateCasesForRun(opts.Cases, opts.SeedDir); err != nil {
		return nil, fmt.Errorf("invalid test cases: %w", err)
	}
	if !opts.AllowExternalCommands {
		if err := refuseExternalSteps(opts.Cases); err != nil {
			return nil, err
		}
	}

	// A run with no desired schema at all is legitimate: a plan case builds its
	// own starting state from the source its `schema` block names, so there is
	// nothing to converge before the cases and nothing to parse. Parsing
	// opts.RootDir regardless walked the working directory and refused the run
	// over an annotation in some unrelated file (stokaro/ptah#1211).
	schema := opts.Desired
	if schema == nil && strings.TrimSpace(opts.RootDir) != "" {
		parsed, err := goschema.ParseDir(opts.Runtime.Annotations(), opts.RootDir)
		if err != nil {
			return nil, fmt.Errorf("parse desired schema from %s: %w", opts.RootDir, err)
		}
		schema = parsed
	}
	if schema != nil {
		if err := validateTestSchema(schema); err != nil {
			return nil, err
		}
	}

	// Provision the desired schema once per database — once per ephemeral
	// per-case database, or once for a shared explicit database — rather than
	// per case, so re-creating already-created objects never collides on the
	// shared-database path.
	provision := func(ctx context.Context, conn *dbschema.DatabaseConnection) error {
		if schema == nil {
			return nil
		}
		_, err := applyDesiredSchema(ctx, conn, schema, opts.Runtime)
		return err
	}
	run := func(ctx context.Context, conn *dbschema.DatabaseConnection, c Case) (CaseResult, error) {
		r := &runner{
			runtime:       opts.Runtime,
			conn:          conn,
			desiredSchema: schema,
			seedDir:       opts.SeedDir,
			resolveSchema: opts.ResolveSchema,
			applyPlan:     opts.ApplyPlan,

			externalTimeout: opts.ExternalTimeout,
		}
		r.migrateTo = rejectMigrateToInSchemaTest
		r.applySchema = r.runApplySchema
		return r.runCase(ctx, c)
	}
	kind := opts.ReportKind
	if kind == "" {
		kind = "SCHEMA"
	}
	return runCases(ctx, opts.DBURL, kind, opts.Cases, opts.Parallelism, provision, run)
}

func desiredSchemaForMigrationCases(annotations annotation.Set, rootDir string, cases []Case) (*schemamodel.Database, error) {
	if !casesUseStepKind(cases, stepKindApplySchema) {
		return nil, nil
	}
	if strings.TrimSpace(rootDir) == "" {
		return nil, fmt.Errorf("apply_schema requires a desired schema root directory")
	}
	schema, err := goschema.ParseDir(annotations, rootDir)
	if err != nil {
		return nil, fmt.Errorf("parse desired schema from %s: %w", rootDir, err)
	}
	if err := validateTestSchema(schema); err != nil {
		return nil, err
	}
	return schema, nil
}

func casesUseStepKind(cases []Case, want stepKind) bool {
	for i := range cases {
		for j := range cases[i].Steps {
			kind, _ := cases[i].Steps[j].kind()
			if kind == want {
				return true
			}
		}
	}
	return false
}

// applyDesiredSchema converges the connected throwaway database to schema using
// live catalog identifier semantics and server capabilities. It reports whether
// any DDL was required. Connection-aware splitting is necessary because some
// drivers execute only the first statement in a multi-statement string.
func applyDesiredSchema(
	ctx context.Context,
	conn *dbschema.DatabaseConnection,
	schema *schemamodel.Database,
	runtime engine.SchemaRuntime,
) (bool, error) {
	if err := schemaext.RequireRuntime(ctx, runtime); err != nil {
		return false, err
	}
	current, err := dbschema.ReadSchemaWithSchemasContext(ctx, conn, nil)
	if err != nil {
		return false, fmt.Errorf("inspect test database before applying desired schema: %w", err)
	}
	info := conn.Info()
	diff, err := schemadiff.CompareWithDatabase(ctx, conn, schema, current, nil, runtime)
	if err != nil {
		return false, fmt.Errorf("compare desired schema with test database: %w", err)
	}
	if err := preserveUnmanagedObjects(ctx, diff, info.Dialect, runtime.Selections()); err != nil {
		return false, err
	}
	if !diff.HasChanges() {
		return false, nil
	}

	sql, err := planner.GenerateSchemaDiffSQLWithOptions(
		ctx, runtime,
		diff,

		info.Dialect,
		planner.Options{Capabilities: info.Capabilities},
	)

	if err != nil {
		return false, fmt.Errorf("plan desired schema for dialect %q: %w", info.Dialect, err)
	}
	// One session, because a MySQL-family plan that replaces a trigger holds
	// LOCK TABLES across statements, and on a pool the next statement can wait
	// behind the lock another connection holds.
	err = conn.WithSessionOrCurrent(ctx, func(session *dbschema.DatabaseConnection) error {
		for _, stmt := range sqlutil.SplitStatementsForDialect(session.Info().Dialect, sql) {
			if strings.TrimSpace(stmt) == "" {
				continue
			}
			if _, err := session.ExecContext(ctx, stmt); err != nil {
				return fmt.Errorf("apply desired schema: %w", err)
			}
		}
		return nil
	})
	if err != nil {
		return false, err
	}
	return true, nil
}

// preserveUnmanagedObjects makes apply_schema additive. A migration test may
// already contain objects or object members that are absent from the desired Go
// schema; that live state is outside this step's ownership and must not be
// dropped. Desired additions and modifications are still applied, including
// replacements represented by matching remove/add pairs.
func preserveUnmanagedObjects(ctx context.Context, diff *difftypes.SchemaDiff, dialect string, selections selectext.Set) error {
	features, err := additiveFeatureChanges(ctx, diff, selections)
	if err != nil {
		return err
	}
	semantics := diff.EffectiveIdentifierSemantics(dialect)
	diff.TablesRemoved = nil
	diff.EnumsRemoved = nil
	enumDiffs := diff.EnumsModified[:0]
	for i := range diff.EnumsModified {
		enumDiff := diff.EnumsModified[i]
		enumDiff.ValuesRemoved = nil
		if len(enumDiff.ValuesAdded) > 0 {
			enumDiffs = append(enumDiffs, enumDiff)
		}
	}
	diff.EnumsModified = enumDiffs
	diff.IndexesRemoved = matchingIndexRefs(diff.IndexesRemoved, diff.IndexAdditions(), semantics)
	diff.FunctionsRemoved = nil
	diff.SequencesRemoved = nil
	diff.DomainsRemoved = nil
	diff.CompositeTypesRemoved = nil
	diff.ViewsRemoved = nil
	diff.MaterializedViewsRemoved = nil
	diff.TriggersRemoved = nil
	diff.RLSPoliciesRemoved = nil
	diff.RLSEnabledTablesRemoved = nil
	diff.RolesRemoved = nil
	diff.GrantsRemoved = nil
	diff.GrantOptionsRevoked = nil
	diff.ConstraintsRemoved = matchingConstraintRemovals(
		diff.ConstraintsRemoved,
		diff.ConstraintsAdded,
		semantics,
	)

	// Owners retain additions inside mixed feature changes. Other changes
	// are omitted when they report a removal; unclassified table changes
	// accompany a structural change.
	diff.FeatureChanges = features[0]
	tableDiffs := diff.TablesModified[:0]
	for i := range diff.TablesModified {
		tableDiff := diff.TablesModified[i]
		tableDiff.ColumnsRemoved = nil
		tableDiff.ConstraintsRemoved = matchingNames(
			tableDiff.ConstraintsRemoved,
			tableDiff.ConstraintsAdded,
			semantics.IndexIdentityKey,
		)
		tableDiff.FeatureChanges = features[i+1]
		if len(tableDiff.ColumnsAdded) > 0 ||
			len(tableDiff.ColumnsModified) > 0 ||
			len(tableDiff.ConstraintsAdded) > 0 ||
			len(tableDiff.ConstraintsRemoved) > 0 ||
			slices.ContainsFunc(tableDiff.FeatureChanges, declaresRemoval) {
			tableDiffs = append(tableDiffs, tableDiff)
		}
	}
	diff.TablesModified = tableDiffs
	return nil
}

// additiveFeatureChanges batches the entire diff so an executable provider
// receives one request across all tables. Placement stays with the host.
func additiveFeatureChanges(ctx context.Context, diff *difftypes.SchemaDiff, selections selectext.Set) ([][]schemaext.ChangeRecord, error) {
	groups := make([][]schemaext.ChangeRecord, len(diff.TablesModified)+1)
	groups[0] = diff.FeatureChanges
	for i, table := range diff.TablesModified {
		groups[i+1] = table.FeatureChanges
	}
	type key struct {
		subject objectidentity.Key
		kind    schemaext.Kind
	}
	placement := make(map[key]int)
	var changes []schemaext.ChangeRecord
	for index, group := range groups {
		for _, change := range group {
			if err := schemaext.ValidatePayload(change.Value); err != nil {
				return nil, err
			}
			identity := key{subject: change.Subject.Key(), kind: change.Value.Kind()}
			if earlier, found := placement[identity]; found && earlier != index {
				return nil, fmt.Errorf("%w: an additive change occurs in multiple scopes", schemaext.ErrDuplicate)
			}
			placement[identity] = index
			changes = append(changes, change)
		}
	}
	selected, err := selections.SelectAdditions(ctx, selectext.AdditiveRequest{Changes: changes})
	if err != nil {
		return nil, err
	}
	result := make([][]schemaext.ChangeRecord, len(groups))
	for _, change := range selected {
		index := placement[key{subject: change.Subject.Key(), kind: change.Value.Kind()}]
		result[index] = append(result[index], change)
	}
	return result, nil
}

// declaresRemoval reports a feature change whose owner answers whether it
// takes state away, so an additive apply can keep it on its own.
func declaresRemoval(change schemaext.ChangeRecord) bool {
	_, declares := change.Value.(schemaext.StateRemoval)
	return declares
}

func matchingNames(removed, added []string, key func(string) string) []string {
	matching := make([]string, 0, len(removed))
	for _, name := range removed {
		position := slices.IndexFunc(added, func(addedName string) bool {
			return key(addedName) == key(name)
		})
		if position >= 0 {
			matching = append(matching, added[position])
		}
	}
	return matching
}

func matchingIndexRefs(
	removed,
	added []difftypes.IndexRef,
	semantics identifier.Semantics,
) []difftypes.IndexRef {
	matching := make([]difftypes.IndexRef, 0, len(removed))
	for _, ref := range removed {
		position := slices.IndexFunc(added, func(addedRef difftypes.IndexRef) bool {
			return indexRefsEqual(ref, addedRef, semantics)
		})
		if position >= 0 {
			matching = append(matching, added[position])
		}
	}
	return matching
}

func indexRefsEqual(left, right difftypes.IndexRef, semantics identifier.Semantics) bool {
	return semantics.IndexIdentityKey(left.Name) == semantics.IndexIdentityKey(right.Name) &&
		semantics.QualifiedTableIdentityKey(left.TableName) ==
			semantics.QualifiedTableIdentityKey(right.TableName)
}

func matchingConstraintRemovals(
	removed difftypes.ConstraintRemovals,
	added difftypes.ConstraintAdditions,
	semantics identifier.Semantics,
) difftypes.ConstraintRemovals {
	matching := make(difftypes.ConstraintRemovals, 0, len(removed))
	for _, removal := range removed {
		position := slices.IndexFunc(added, func(addition difftypes.ConstraintAdditionInfo) bool {
			return constraintRefsEqual(removal, addition, semantics)
		})
		if position >= 0 {
			normalized := removal
			normalized.Name = added[position].Name
			normalized.TableName = added[position].TableName
			matching = append(matching, normalized)
		}
	}
	return matching
}

func constraintRefsEqual(
	removal difftypes.ConstraintRemovalInfo,
	addition difftypes.ConstraintAdditionInfo,
	semantics identifier.Semantics,
) bool {
	return semantics.IndexIdentityKey(addition.Name) == semantics.IndexIdentityKey(removal.Name) &&
		semantics.QualifiedTableIdentityKey(addition.TableName) ==
			semantics.QualifiedTableIdentityKey(removal.TableName)
}

func validateTestSchema(schema *schemamodel.Database) error {
	if len(schema.Roles) > 0 || len(schema.Grants) > 0 || len(schema.RevokedGrants) > 0 {
		return fmt.Errorf(
			"database tests do not support roles or grants because they can mutate cluster-scoped security state",
		)
	}
	return nil
}

// rejectMigrateToInSchemaTest is the migrate_to handler for schema tests. A
// schema test applies a desired schema directly and has no migration history to
// move between, so a migrate_to step is a mistake rather than a runnable action.
func rejectMigrateToInSchemaTest(context.Context, string) (passed bool, detail string) {
	return false, `migrate_to is not valid in a schema test; use "ptah migrations test"`
}

package dbtest

import (
	"context"
	"fmt"
	"slices"
	"strings"

	"github.com/stokaro/ptah/core/goschema"
	"github.com/stokaro/ptah/core/platform/identifier"
	"github.com/stokaro/ptah/dbschema"
	"github.com/stokaro/ptah/migration/migrator"
	"github.com/stokaro/ptah/migration/planner"
	"github.com/stokaro/ptah/migration/schemadiff"
	schemadifftypes "github.com/stokaro/ptah/migration/schemadiff/types"
)

// SchemaOptions configures a single [RunSchemaTest] invocation.
type SchemaOptions struct {
	// Cases are the test cases to run, in order.
	Cases []Case
	// RootDir is a directory of Go entity annotations describing the desired
	// schema. It is parsed with goschema.ParseDir and converged through the live
	// diff and planner path before any case runs (once per ephemeral per-case
	// database, or once for a shared explicit database).
	RootDir string
	// SeedDir is the default directory of seed files for seed steps that omit
	// their own [SeedStep.Dir].
	SeedDir string
	// DBURL is an optional database URL to run the tests against. It must point at
	// a throwaway database, because tests mutate schema and data. When empty, an
	// ephemeral SQLite database is provisioned per case in a temporary directory
	// and removed afterwards.
	DBURL string
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
	if err := validateCasesForRun(opts.Cases, opts.SeedDir); err != nil {
		return nil, fmt.Errorf("invalid test cases: %w", err)
	}

	schema, err := goschema.ParseDir(opts.RootDir)
	if err != nil {
		return nil, fmt.Errorf("parse desired schema from %s: %w", opts.RootDir, err)
	}
	if err := validateTestSchema(schema); err != nil {
		return nil, err
	}

	// Provision the desired schema once per database — once per ephemeral
	// per-case database, or once for a shared explicit database — rather than
	// per case, so re-creating already-created objects never collides on the
	// shared-database path.
	provision := func(ctx context.Context, conn *dbschema.DatabaseConnection) error {
		_, err := applyDesiredSchema(ctx, conn, schema)
		return err
	}
	run := func(ctx context.Context, conn *dbschema.DatabaseConnection, c Case) (CaseResult, error) {
		r := &runner{conn: conn, desiredSchema: schema, seedDir: opts.SeedDir}
		r.migrateTo = rejectMigrateToInSchemaTest
		r.applySchema = r.runApplySchema
		return r.runCase(ctx, c)
	}
	return runCases(ctx, opts.DBURL, "SCHEMA", opts.Cases, provision, run)
}

func desiredSchemaForMigrationCases(rootDir string, cases []Case) (*goschema.Database, error) {
	if !casesUseStepKind(cases, stepKindApplySchema) {
		return nil, nil
	}
	if strings.TrimSpace(rootDir) == "" {
		return nil, fmt.Errorf("apply_schema requires a desired schema root directory")
	}
	schema, err := goschema.ParseDir(rootDir)
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
	schema *goschema.Database,
) (bool, error) {
	current, err := dbschema.ReadSchemaWithSchemas(conn, nil)
	if err != nil {
		return false, fmt.Errorf("inspect test database before applying desired schema: %w", err)
	}
	info := conn.Info()
	diff, err := schemadiff.CompareWithDatabase(ctx, conn, schema, current, nil)
	if err != nil {
		return false, fmt.Errorf("compare desired schema with test database: %w", err)
	}
	preserveUnmanagedObjects(diff, info.Dialect)
	if !diff.HasChanges() {
		return false, nil
	}

	sql, err := planner.GenerateSchemaDiffSQLWithOptions(
		diff,
		schema,
		info.Dialect,
		planner.Options{Capabilities: info.Capabilities},
	)
	if err != nil {
		return false, fmt.Errorf("plan desired schema for dialect %q: %w", info.Dialect, err)
	}
	for _, stmt := range migrator.SplitSQLStatementsForConnection(conn, sql) {
		if strings.TrimSpace(stmt) == "" {
			continue
		}
		if _, err := conn.ExecContext(ctx, stmt); err != nil {
			return false, fmt.Errorf("apply desired schema: %w", err)
		}
	}
	return true, nil
}

// preserveUnmanagedObjects makes apply_schema additive. A migration test may
// already contain objects or object members that are absent from the desired Go
// schema; that live state is outside this step's ownership and must not be
// dropped. Desired additions and modifications are still applied, including
// replacements represented by matching remove/add pairs.
func preserveUnmanagedObjects(diff *schemadifftypes.SchemaDiff, dialect string) {
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
	diff.IndexesRemoved = matchingIndexRefs(diff.IndexesRemoved, diff.IndexesAdded, semantics)
	diff.ExtensionsRemoved = nil
	diff.FunctionsRemoved = nil
	diff.SequencesRemoved = nil
	diff.DomainsRemoved = nil
	diff.CompositeTypesRemoved = nil
	diff.RangesRemoved = matchingNames(diff.RangesRemoved, diff.RangesAdded, semantics.TableIdentityKey)
	diff.ViewsRemoved = nil
	diff.MaterializedViewsRemoved = nil
	diff.TriggersRemoved = nil
	diff.RLSPoliciesRemoved = nil
	diff.RLSEnabledTablesRemoved = nil
	diff.RolesRemoved = nil
	diff.GrantsRemoved = nil
	diff.GrantOptionsRevoked = nil
	diff.ConstraintsRemovedWithTables = matchingConstraintRemovals(
		diff.ConstraintsRemovedWithTables,
		diff.ConstraintsAddedWithTables,
		semantics,
	)
	diff.ConstraintsRemoved = constraintRemovalNames(diff.ConstraintsRemovedWithTables)

	tableDiffs := diff.TablesModified[:0]
	for i := range diff.TablesModified {
		tableDiff := diff.TablesModified[i]
		tableDiff.ColumnsRemoved = nil
		tableDiff.ConstraintsRemoved = matchingNames(
			tableDiff.ConstraintsRemoved,
			tableDiff.ConstraintsAdded,
			semantics.IndexIdentityKey,
		)
		if len(tableDiff.ColumnsAdded) > 0 ||
			len(tableDiff.ColumnsModified) > 0 ||
			len(tableDiff.ConstraintsAdded) > 0 ||
			len(tableDiff.ConstraintsRemoved) > 0 {
			tableDiffs = append(tableDiffs, tableDiff)
		}
	}
	diff.TablesModified = tableDiffs
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
	added []schemadifftypes.IndexRef,
	semantics identifier.Semantics,
) []schemadifftypes.IndexRef {
	matching := make([]schemadifftypes.IndexRef, 0, len(removed))
	for _, ref := range removed {
		position := slices.IndexFunc(added, func(addedRef schemadifftypes.IndexRef) bool {
			return indexRefsEqual(ref, addedRef, semantics)
		})
		if position >= 0 {
			matching = append(matching, added[position])
		}
	}
	return matching
}

func indexRefsEqual(left, right schemadifftypes.IndexRef, semantics identifier.Semantics) bool {
	return semantics.IndexIdentityKey(left.Name) == semantics.IndexIdentityKey(right.Name) &&
		semantics.QualifiedTableIdentityKey(left.TableName) ==
			semantics.QualifiedTableIdentityKey(right.TableName)
}

func matchingConstraintRemovals(
	removed []schemadifftypes.ConstraintRemovalInfo,
	added []schemadifftypes.ConstraintAdditionInfo,
	semantics identifier.Semantics,
) []schemadifftypes.ConstraintRemovalInfo {
	matching := make([]schemadifftypes.ConstraintRemovalInfo, 0, len(removed))
	for _, removal := range removed {
		position := slices.IndexFunc(added, func(addition schemadifftypes.ConstraintAdditionInfo) bool {
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
	removal schemadifftypes.ConstraintRemovalInfo,
	addition schemadifftypes.ConstraintAdditionInfo,
	semantics identifier.Semantics,
) bool {
	return semantics.IndexIdentityKey(addition.Name) == semantics.IndexIdentityKey(removal.Name) &&
		semantics.QualifiedTableIdentityKey(addition.TableName) ==
			semantics.QualifiedTableIdentityKey(removal.TableName)
}

func constraintRemovalNames(removals []schemadifftypes.ConstraintRemovalInfo) []string {
	names := make([]string, 0, len(removals))
	for _, removal := range removals {
		if !slices.Contains(names, removal.Name) {
			names = append(names, removal.Name)
		}
	}
	return names
}

func validateTestSchema(schema *goschema.Database) error {
	if len(schema.Roles) > 0 || len(schema.Grants) > 0 {
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

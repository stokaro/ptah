package schemadiff

import (
	"context"
	"fmt"
	"slices"

	"ptah.run/catalog"
	"ptah.run/config"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/core/schemapreparation"
	"ptah.run/internal/convert/goschematodb"
	"ptah.run/internal/schemaprep"
	"ptah.run/internal/sqlitevirtual"
	"ptah.run/migration/internal/identifiervalidation"
	"ptah.run/migration/schemadiff/difftypes"
	"ptah.run/migration/schemadiff/internal/compare"
)

// Compare compares ordinary relational state without target normalization.
// Feature state requires an explicit target through CompareWithDialect or
// CompareWithOptions. Failures and incomplete results return no diff.
func Compare(ctx context.Context, desired *schemamodel.Database, current *catalog.Database, runtime schemapreparation.Runtime) (*difftypes.SchemaDiff, error) {
	return CompareWithOptions(ctx, desired, current, nil, runtime)
}

// CompareWithDialect compares snapshots under an explicit target. Provider
// failures and incomplete results return no diff. The reporting variant retains
// partial changes together with structured knowledge limits.
func CompareWithDialect(ctx context.Context, desired *schemamodel.Database, current *catalog.Database, dialect string, runtime schemapreparation.Runtime) (*difftypes.SchemaDiff, error) {
	opts := config.DefaultCompareOptions()
	opts.Dialect = dialect
	return CompareWithOptions(ctx, desired, current, opts, runtime)
}

// CompareSchemas compares desired documents after the selected runtime converts
// the current document to its observed representation. Conversion preserves
// knowledge limits; it cannot establish facts that inspection did not supply.
// Both documents must be non-nil; otherwise ErrInvalidSchemaDiff is returned
// before invoking a conversion service.
func CompareSchemas(ctx context.Context, desired, current *schemamodel.Database, dialect string, runtime schemapreparation.Runtime) (*difftypes.SchemaDiff, error) {
	if err := schemaext.RequireRuntime(ctx, runtime); err != nil {
		return nil, err
	}
	if desired == nil || current == nil {
		return nil, fmt.Errorf("%w: comparison requires both desired schema documents", ptaherr.ErrInvalidSchemaDiff)
	}
	observed, err := goschematodb.ToDBSchema(ctx, current, dialect, runtime)
	if err != nil {
		return nil, err
	}
	return CompareWithDialect(ctx, desired, observed, dialect, runtime)
}

// CompareWithDatabaseInfo compares using caller-supplied database metadata.
// SQL Server callers should prefer CompareWithDatabase, which resolves the
// complete candidate identifier set under the live catalog collation.
//
// info.Dialect selects the comparison dialect, overriding opts.Dialect. A
// non-zero info.IdentifierSemantics snapshot must be valid, cover every
// compared identifier, and admit no target identifier collisions; a snapshot
// failing any of those checks is refused with an error satisfying
// errors.Is(err, ptaherr.ErrInvalidSchemaDiff). Offline comparison applies the
// same refusal to invalid caller-supplied snapshots.
//
// Unlike the pure entry points, this variant also validates the declaration
// against the target before comparing, and returns an error instead of a diff
// that plans a statement the server would refuse. Which rules apply is
// dialect-specific and grows with the dialects; a reserved PostgreSQL role
// name and SQLite's virtual-table rules are two of them. One of those checks
// is a promise in its own right: PTAH_SQLITE_ALLOW_VIRTUAL_TABLE_DROP is
// resolved on every SQLite comparison, so a malformed value is reported
// whether or not this comparison reaches a virtual table.
func CompareWithDatabaseInfo(
	ctx context.Context,
	desired *schemamodel.Database,
	database *catalog.Database,
	info catalog.ServerInfo,
	opts *config.CompareOptions,
	runtime TargetRuntime,
) (*difftypes.SchemaDiff, error) {
	return completeComparison(compareWithDatabaseInfoReportingUndecidedAdditions(ctx, desired, database, info, opts, runtime))
}

func compareWithDatabaseInfoReportingUndecidedAdditions(
	ctx context.Context,
	desired *schemamodel.Database,
	database *catalog.Database,
	info catalog.ServerInfo,
	opts *config.CompareOptions,
	runtime TargetRuntime,
) (*difftypes.SchemaDiff, Diagnostics, error) {
	if err := schemaext.RequireRuntime(ctx, runtime); err != nil {
		return nil, Diagnostics{}, err
	}
	if desired == nil || database == nil {
		return nil, Diagnostics{}, fmt.Errorf("%w: comparison requires desired and observed schemas", ptaherr.ErrInvalidSchemaDiff)
	}
	merged := copyComparisonOptions(opts)
	scoped, err := resolveComparisonScope(desired, database, info.Dialect, runtime)
	if err != nil {
		return nil, Diagnostics{}, err
	}
	desired, database = scoped.desired, scoped.current
	selected := scoped.target
	info.Dialect = selected.Name()
	merged.Dialect = info.Dialect
	// Resolved before any of the validations below can return, so a malformed
	// drop toggle is reported on every SQLite comparison rather than only the
	// ones that get far enough to classify a virtual table (stokaro/ptah#1028).
	if err := sqlitevirtual.ValidateToggle(info.Dialect); err != nil {
		return nil, Diagnostics{}, err
	}
	// A reserved PostgreSQL role is in neither Database.Roles nor
	// Database.RolesOutOfScope, so comparing it would read it as absent and
	// plan a CREATE ROLE the server always refuses. Refuse the declaration
	// here instead, before anything is compared (stokaro/ptah#1312).
	if err := validateDeclaredBeforeComparison(desired, database, info); err != nil {
		return nil, Diagnostics{}, &RefusalError{cause: err}
	}
	// A SQLite virtual table cannot appear on the desired side of any
	// comparison, so its absence there is not deletion intent and its presence
	// there is a different kind of object. Refuse both before anything is
	// compared, rather than planning a DROP the operator never asked for
	// (stokaro/ptah#1028).
	//
	// The caller's diff policy travels with the comparison because both halves
	// of that guard predict statements, and a caller that skips `drop_table`
	// deletes the predicted DROP again before anything is rendered. See
	// [config.CompareOptions.SkipTableDrops].
	virtualPolicy := sqlitevirtual.Policy{
		SkipDropTable:  merged.SkipTableDrops,
		SkipDropColumn: merged.SkipColumnDrops,
		SkipDropIndex:  merged.SkipIndexDrops,
	}
	if err := sqlitevirtual.ValidateComparison(info.Dialect, desired, database, virtualPolicy); err != nil {
		return nil, Diagnostics{}, err
	}
	desired = schemaprep.AssignDefaultForeignKeyNames(desired, info.Dialect)
	semantics := info.IdentifierSemantics.Normalize(info.Dialect)
	if !info.IdentifierSemantics.IsZero() &&
		!info.IdentifierSemantics.Equal(semantics) {
		return nil, Diagnostics{}, fmt.Errorf(
			"%w: invalid identifier semantics snapshot",
			ptaherr.ErrInvalidSchemaDiff,
		)
	}
	names := collectIdentifierNames(desired, database, semantics.DefaultSchema)
	if err := identifiervalidation.ValidateCoverage(semantics, names); err != nil {
		return nil, Diagnostics{}, err
	}
	if err := ValidateDesiredSchema(ctx, runtime, desired, info); err != nil {
		return nil, Diagnostics{}, err
	}
	if err := ValidateRolePasswordComparison(desired, database, selected); err != nil {
		return nil, Diagnostics{}, err
	}
	merged.IdentifierSemantics = &semantics
	// A connection to a whole MySQL-family server compares its databases as
	// well as what is in them. Only the connection says it is one: a server
	// read and a read of one database both describe tables, and only the
	// first describes the databases (stokaro/ptah#3789).
	merged.ServerSchemas = merged.ServerSchemas || info.WholeServer
	if merged.ServerSchemas && isMySQLFamilyComparison(info.Dialect) {
		if err := compare.RequireServerDatabases(desired); err != nil {
			return nil, Diagnostics{}, err
		}
	}
	diff, undecided, err := compareReportingUndecidedAdditions(ctx, desired, database, merged, info.Capabilities, info.DefaultIntSize, runtime)
	if err != nil {
		return nil, Diagnostics{}, err
	}
	// The half of the SQLite virtual-table guard that only the comparator can
	// answer. A table both sides name and describe differently is rebuilt by
	// the SQLite planner -- drop, recreate, copy -- which destroys a module's
	// storage as surely as a drop, and whether that will happen is this diff's
	// answer rather than anything the pre-comparison check could compute
	// without a second copy of these rules (stokaro/ptah#1028).
	if err := sqlitevirtual.ValidatePlannedChanges(
		info.Dialect, database, diff, virtualPolicy,
	); err != nil {
		return nil, Diagnostics{}, err
	}
	if err := compare.ValidateMySQLFunctionDefinerReplacements(
		desired,
		database,
		diff,
		info.Dialect,
		semantics,
	); err != nil {
		return nil, Diagnostics{}, err
	}
	return diff, undecided, nil
}

// copyComparisonOptions owns the values this entry point fills from target
// metadata, including the slice a downstream comparison may filter.
func copyComparisonOptions(opts *config.CompareOptions) *config.CompareOptions {
	merged := config.DefaultCompareOptions()
	if opts != nil {
		*merged = *opts
		merged.IgnoredExtensions = slices.Clone(opts.IgnoredExtensions)
	}
	return merged
}

package schemadiff

import (
	"context"
	"fmt"
	"slices"

	"ptah.run/catalog"
	"ptah.run/config"
	"ptah.run/core/difftypes"
	"ptah.run/core/platform"
	"ptah.run/core/platform/capability"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/core/schemapreparation"
	"ptah.run/core/schemaprojection"
	"ptah.run/internal/convert/goschematodb"
	"ptah.run/internal/schemaprep"
	"ptah.run/internal/sqlitevirtual"
	"ptah.run/migration/internal/identifiervalidation"
	"ptah.run/migration/schemadiff/internal/compare"
)

// Compare compares ordinary relational state without target normalization.
// Feature state requires an explicit target through CompareWithDialect or
// CompareWithOptions. With no target it assumes no target's capabilities, so
// what a capability gates, such as a comment or a role membership, is not
// compared. Failures and incomplete results return no diff.
//
// This entry point does not check who a replaced object runs as. It leaves
// [difftypes.SchemaDiff.ConnectedAccount] nil, so neither the routine definer
// rule nor the planner's view definer rule runs, even over a catalog a live
// read produced. To refuse a MySQL-family routine or view whose replacement
// would make the connected account its definer, compare through
// [CompareWithDatabase] or [CompareWithDatabaseInfo].
func Compare(ctx context.Context, desired *schemamodel.Database, current *catalog.Database, runtime schemapreparation.Runtime) (*difftypes.SchemaDiff, error) {
	return CompareWithOptions(ctx, desired, current, nil, runtime)
}

// CompareWithDialect compares snapshots under an explicit target. Provider
// failures and incomplete results return no diff. The reporting variant retains
// partial changes together with structured knowledge limits.
//
// It knows no connection, so it cannot tell which schema a read means by an
// unqualified name. A read of one MySQL-family database or one Oracle account
// names that database's or account's objects without it, and a declaration
// that names one of them, or a grant's object, with the database or account
// does not pair with the read here: the diff adds the declared object and
// removes the read one. Nor does a default that names the account's own
// sequence with the account, `app.seq.NEXTVAL` against the read's
// "SEQ"."NEXTVAL": the diff changes the column's default. A default that
// leaves the account out pairs here.
// [CompareWithDatabase] reads the connection's schema and pairs them all.
//
// This entry point does not check who a replaced object runs as. It leaves
// [difftypes.SchemaDiff.ConnectedAccount] nil, so neither the routine definer
// rule nor the planner's view definer rule runs, even over a catalog a live
// read produced. To refuse a MySQL-family routine or view whose replacement
// would make the connected account its definer, compare through
// [CompareWithDatabase] or [CompareWithDatabaseInfo].
func CompareWithDialect(ctx context.Context, desired *schemamodel.Database, current *catalog.Database, dialect string, runtime schemapreparation.Runtime) (*difftypes.SchemaDiff, error) {
	opts := config.DefaultCompareOptions()
	opts.Dialect = dialect
	return CompareWithOptions(ctx, desired, current, opts, runtime)
}

// DocumentRuntime selects comparison and offline CREATE prediction together.
// Transport adapters must preserve the separate operation contracts.
type DocumentRuntime interface {
	schemapreparation.Runtime
	schemaprojection.TableCreationService
	schemaprojection.ReadBackRuntime
}

// CompareSchemas compares desired documents after the selected runtime predicts
// the current document's CREATE result and converts it to Observed representation.
// Explicit source knowledge limits survive. Prediction is not live inspection.
// Both documents must be non-nil; otherwise ErrInvalidSchemaDiff is returned
// before invoking a conversion service.
func CompareSchemas(ctx context.Context, desired, current *schemamodel.Database, dialect string, runtime DocumentRuntime) (*difftypes.SchemaDiff, error) {
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
	return completeComparison(compareWithDatabaseInfoReportingUndecidedAdditions(ctx, desired, database, info, opts, runtime, nil))
}

// comparisonSemantics normalizes the identifier semantics a comparison's
// server snapshot carries for its dialect, refuses a snapshot whose semantics
// do not survive that, and checks every name the comparison meets against
// them.
func comparisonSemantics(desired *schemamodel.Database, database *catalog.Database, info catalog.ServerInfo) (identifier.Semantics, error) {
	semantics := info.IdentifierSemantics.Normalize(info.Dialect)
	if !info.IdentifierSemantics.IsZero() &&
		!info.IdentifierSemantics.Equal(semantics) {
		return identifier.Semantics{}, fmt.Errorf(
			"%w: invalid identifier semantics snapshot",
			ptaherr.ErrInvalidSchemaDiff,
		)
	}
	names := collectIdentifierNames(desired, database, semantics.DefaultSchema, info.Dialect)
	if err := identifiervalidation.ValidateCoverage(semantics, names); err != nil {
		return identifier.Semantics{}, err
	}
	return semantics, nil
}

// comparedDesired is desired as a comparison with database keys it. Either side
// missing is refused, and the names of desired are matched to the read's
// spelling of them; see [foldedNames].
func comparedDesired(desired *schemamodel.Database, database *catalog.Database, info catalog.ServerInfo) (*schemamodel.Database, error) {
	if desired == nil || database == nil {
		return nil, fmt.Errorf("%w: comparison requires desired and observed schemas", ptaherr.ErrInvalidSchemaDiff)
	}
	return foldedNames(desired, database, info.Dialect, info.Schema, info.NameFolding)
}

// foldedNames is desired with each database and table name it spells in
// another case than the read spells it, on a dialect whose servers each decide
// by a setting how they compare those names; see
// [schemaprep.RespellReadNames]. folding is how the server compares them, and
// session is the database a name written without one is in. On a server that
// folds names such a name is respelled before anything is keyed, and where no
// connection read the setting it is refused. Either side nil returns desired
// as it is, for the caller to refuse.
func foldedNames(
	desired *schemamodel.Database,
	database *catalog.Database,
	dialect, session string,
	folding catalog.NameFolding,
) (*schemamodel.Database, error) {
	if desired == nil || database == nil || !platform.NamesFoldBySetting(dialect) {
		return desired, nil
	}
	respelled, err := schemaprep.RespellReadNames(desired, schemaprep.ReadNamesOf(database, session, dialect), folding)
	if err != nil {
		return nil, &RefusalError{cause: err}
	}
	return respelled, nil
}

// compareWithDatabaseInfoReportingUndecidedAdditions is the comparison every
// entry point with caller-supplied server metadata runs. session is the
// connection's probe session, through which a target asks its server about
// the CHECKs the comparison pairs, and nil on a comparison with no
// connection.
func compareWithDatabaseInfoReportingUndecidedAdditions(
	ctx context.Context,
	desired *schemamodel.Database,
	database *catalog.Database,
	info catalog.ServerInfo,
	opts *config.CompareOptions,
	runtime TargetRuntime,
	session schemaext.ProbeSession,
) (*difftypes.SchemaDiff, Diagnostics, error) {
	if err := schemaext.RequireRuntime(ctx, runtime); err != nil {
		return nil, Diagnostics{}, err
	}
	desired, err := comparedDesired(desired, database, info)
	if err != nil {
		return nil, Diagnostics{}, err
	}
	merged := copyComparisonOptions(opts)
	scoped, err := resolveComparisonScope(desired, database, info.Dialect, info.IdentifierSemantics, runtime)
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
	rules, err := runtime.ComparisonRules(info.Dialect)
	if err != nil {
		return nil, Diagnostics{}, err
	}
	desired, err = heldCheckNames(ctx, desired, database, info.Dialect, info.IdentifierSemantics.Normalize(info.Dialect),
		info.Capabilities, rules.Checks, session)
	if err != nil {
		return nil, Diagnostics{}, err
	}
	semantics, err := comparisonSemantics(desired, database, info)
	if err != nil {
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
	if merged.ServerSchemas && comparesServerDatabases(info.Dialect) {
		if err := compare.RequireServerDatabases(desired); err != nil {
			return nil, Diagnostics{}, err
		}
	}
	diff, undecided, err := compareReportingUndecidedAdditions(ctx, desired, database, merged, info.Capabilities, info.DefaultIntSize, runtime, session)
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
	if err := recordDefiners(desired, database, info, merged, semantics, runtime, diff); err != nil {
		return nil, Diagnostics{}, err
	}
	// The planner refuses a database created beside one dropped whose name
	// differs only in case unless the diff says the server keeps the two
	// apart; see [difftypes.SchemaDiff.NameFolding].
	if platform.NamesFoldBySetting(info.Dialect) {
		diff.NameFolding = info.NameFolding
	}
	return diff, undecided, nil
}

// recordDefiners settles what a diff of a database read says about definers.
// A replayed current state holds the replay's account and definers, not the
// target's, so neither definer rule has anything to compare: the diff carries
// no account and no view definer, as a diff of two descriptions does. See
// [config.CompareOptions.ReplayedCurrent]. A read of the target refuses a
// routine whose definer the change would take over, and records the account
// a plan applied through this connection writes objects as. Only a comparison
// against a database read sets it, so a planner tells a read that did not
// record the account, which it refuses on, from a diff of two descriptions,
// which no connection applies.
func recordDefiners(
	desired *schemamodel.Database,
	database *catalog.Database,
	info catalog.ServerInfo,
	opts *config.CompareOptions,
	semantics identifier.Semantics,
	runtime TargetRuntime,
	diff *difftypes.SchemaDiff,
) error {
	if opts.ReplayedCurrent {
		for index := range diff.ViewsModified {
			diff.ViewsModified[index].PreviousDefiner = ""
		}
		return nil
	}
	// A comparison given no capability set assumes the dialect's preset, as
	// the comparison itself does.
	caps := info.Capabilities
	if caps.Len() == 0 {
		caps = capability.ForDialect(info.Dialect)
	}
	rules, err := runtime.ComparisonRules(info.Dialect)
	if err != nil {
		return err
	}
	if err := compare.ValidateRoutineDefinerReplacements(
		desired,
		database,
		diff,
		info.Dialect,
		caps,
		semantics,
		rules.Routines,
	); err != nil {
		return err
	}
	diff.ConnectedAccount = new(database.CurrentAccount)
	return nil
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

package shadow

import (
	"context"
	"fmt"
	"io/fs"
	"maps"

	"go.5x5.cz/ptah/config"
	"go.5x5.cz/ptah/core/goschema"
	"go.5x5.cz/ptah/core/platform/capability"
	"go.5x5.cz/ptah/core/platform/identifier"
	"go.5x5.cz/ptah/dbschema"
	"go.5x5.cz/ptah/internal/devlock"
	"go.5x5.cz/ptah/migration/internal/shadowdb"
	"go.5x5.cz/ptah/migration/migrator"
	"go.5x5.cz/ptah/migration/schemadiff"
)

// VerificationResult describes one failed shadow-database verification.
// Errors preserve the text form through [VerificationError.Error] while
// exposing mismatches to callers that need machine-readable diagnostics.
type VerificationResult struct {
	Stage string `json:"stage"`
	// Mismatches contains every structural mismatch in deterministic category and object order.
	Mismatches []Mismatch `json:"mismatches,omitempty"`
}

// Mismatch describes one schema mismatch found during shadow verification.
type Mismatch struct {
	Kind       string            `json:"kind"`
	Object     string            `json:"object,omitempty"`
	Table      string            `json:"table,omitempty"`
	Column     string            `json:"column,omitempty"`
	Constraint string            `json:"constraint,omitempty"`
	Changes    map[string]string `json:"changes,omitempty"`
	Message    string            `json:"message"`
}

// VerificationError wraps a structured shadow verification result.
type VerificationError struct {
	Result VerificationResult `json:"result"`
	Err    error              `json:"-"`
}

func (e *VerificationError) Error() string {
	if len(e.Result.Mismatches) > 0 {
		return "shadow check failed: " + e.Result.Mismatches[0].Message
	}
	return "shadow check failed: schema differs"
}

func (e *VerificationError) Unwrap() error {
	return e.Err
}

func newVerificationError(stage, kind, message string, err error) *VerificationError {
	if err != nil {
		message = fmt.Sprintf("%s: %v", message, err)
	}
	return newVerificationErrorWithDisplayMessage(stage, kind, message, err)
}

func newVerificationErrorWithDisplayMessage(
	stage,
	kind,
	message string,
	err error,
) *VerificationError {
	return &VerificationError{
		Result: VerificationResult{
			Stage: stage,
			Mismatches: []Mismatch{{
				Kind:    kind,
				Message: message,
			}},
		},
		Err: err,
	}
}

func targetConnectionRequiredError(
	target *dbschema.DatabaseConnection,
	message string,
) *VerificationError {
	if target != nil {
		return nil
	}
	return newVerificationError(
		"realm-check",
		"target_connection_required",
		message,
		nil,
	)
}

func validateConnection(
	ctx context.Context,
	target,
	shadowConn *dbschema.DatabaseConnection,
	dialect string,
	capabilities capability.Capabilities,
	targetRequiredMessage string,
) *VerificationError {
	if !shadowdb.SameDialect(dialect, shadowConn.Info().Dialect) {
		return newVerificationError(
			"dialect-check",
			"dialect_mismatch",
			fmt.Sprintf("shadow database dialect %q does not match target dialect %q", shadowConn.Info().Dialect, dialect),
			nil,
		)
	}
	if err := targetConnectionRequiredError(target, targetRequiredMessage); err != nil {
		return err
	}
	sameRealm, err := devlock.SameRealm(ctx, target, shadowConn)
	if err != nil {
		return newVerificationError(
			"realm-check",
			"realm_comparison_error",
			"compare target and shadow database realms",
			err,
		)
	}
	if sameRealm {
		return newVerificationError(
			"realm-check",
			"target_shadow_same_realm",
			"shadow database must be distinct from target database",
			nil,
		)
	}
	if capabilities != nil && !maps.Equal(capabilities, shadowConn.Info().Capabilities) {
		return newVerificationError(
			"capability-check",
			"capability_mismatch",
			fmt.Sprintf("shadow database capabilities do not match target %s capabilities", dialect),
			nil,
		)
	}
	return nil
}

// MigrationVerifyOptions configures [VerifyMigration].
type MigrationVerifyOptions struct {
	ShadowDatabaseURL   string
	TargetConnection    *dbschema.DatabaseConnection
	MigrationsDir       string
	MigrationsFS        fs.FS
	Dialect             string
	Capabilities        capability.Capabilities
	IdentifierSemantics identifier.Semantics
	Candidates          []Candidate
	Generated           *goschema.Database
	CompareOpts         *config.CompareOptions
	Schemas             []string
}

// Candidate is one migration under verification: a version, a name, and the
// two bodies the replay applies and reverses. The caller has planned it but not
// written it, which is the whole point of measuring it here.
type Candidate struct {
	Version int64
	Name    string
	UpSQL   string
	DownSQL string
}

// VerifyMigration measures a planned migration against a live disposable
// database before its files are written.
//
// The shadow database is dropped clean, the prior history is replayed into it,
// and the candidates are applied on top. The result is re-introspected and
// compared with the desired schema, so what is checked is what a server did
// rather than what the SQL was expected to mean. The candidates are then rolled
// back to the prior version and reapplied, because a down body that does not
// run is only discovered by running it.
//
// Failures are [VerificationError] values naming the stage that stopped and
// every mismatch found.
func VerifyMigration(ctx context.Context, opts MigrationVerifyOptions) error {
	database, err := shadowdb.Open(ctx, opts.ShadowDatabaseURL, "")
	if err != nil {
		return newVerificationError("connect", "connect_error", "connect to shadow database", err)
	}
	defer database.CloseAndWarn()
	conn := database.Connection()

	if err := validateConnection(
		ctx,
		opts.TargetConnection,
		conn,
		opts.Dialect,
		opts.Capabilities,
		"compare target and shadow database realms: target connection is required",
	); err != nil {
		return err
	}
	identifierSemanticsMatch, err := identifierSemanticsAgree(
		ctx,
		conn,
		opts.Dialect,
		opts.IdentifierSemantics,
	)
	if err != nil {
		return newVerificationError(
			"identifier-semantics-check",
			"identifier_semantics_resolution_error",
			"resolve shadow database identifier semantics",
			err,
		)
	}
	if !opts.IdentifierSemantics.IsZero() && !identifierSemanticsMatch {
		return newVerificationError(
			"identifier-semantics-check",
			"identifier_semantics_mismatch",
			fmt.Sprintf("shadow database identifier semantics do not match target %s catalog semantics", opts.Dialect),
			nil,
		)
	}

	if err := conn.SchemaWriter().DropAllTables(ctx); err != nil {
		return newVerificationError("drop-all", "drop_all_error", "drop all objects", err)
	}
	prior, err := shadowdb.LoadMigrations(opts.MigrationsFS, opts.MigrationsDir)
	if err != nil {
		return newVerificationError("load-prior", "load_prior_error", "load prior migrations", err)
	}

	migrations := make([]*migrator.Migration, 0, len(prior)+len(opts.Candidates))
	migrations = append(migrations, prior...)
	for _, candidate := range opts.Candidates {
		migrations = append(migrations,
			migrator.CreateMigrationFromSQL(candidate.Version, candidate.Name, candidate.UpSQL, candidate.DownSQL),
		)
	}

	mig := migrator.NewMigrator(conn, migrator.NewRegisteredMigrationProvider(migrations...))
	if err := mig.MigrateUp(ctx); err != nil {
		if description := shadowdb.DescribeReplayError(err); description != "" {
			return newVerificationError("replay", "replay_error", description, err)
		}
		return newVerificationError("replay", "replay_error", "replay migrations", err)
	}
	if err := assertSchemaMatches(ctx, conn, opts); err != nil {
		return err
	}

	previousVersion := latestMigrationVersion(prior)
	if err := mig.MigrateDownTo(ctx, previousVersion); err != nil {
		return newVerificationError("round-trip-down", "round_trip_down_error", "round-trip down", err)
	}
	if err := mig.MigrateTo(ctx, latestMigrationVersion(migrations)); err != nil {
		return newVerificationError("round-trip-up", "round_trip_up_error", "round-trip up", err)
	}
	return assertSchemaMatches(ctx, conn, opts)
}

func identifierSemanticsAgree(
	ctx context.Context,
	conn *dbschema.DatabaseConnection,
	dialect string,
	target identifier.Semantics,
) (bool, error) {
	if target.IsZero() {
		return true, nil
	}
	target = target.Normalize(dialect)
	names := make([]string, len(target.ResolvedNames))
	for position, resolved := range target.ResolvedNames {
		names[position] = resolved.Name
	}
	resolved, err := conn.ResolveIdentifierSemantics(ctx, names)
	if err != nil {
		return false, err
	}
	return target.Equal(resolved.Normalize(dialect)), nil
}

func latestMigrationVersion(migrations []*migrator.Migration) int64 {
	var latest int64
	for _, migration := range migrations {
		if migration.Version > latest {
			latest = migration.Version
		}
	}
	return latest
}

func assertSchemaMatches(
	ctx context.Context,
	conn *dbschema.DatabaseConnection,
	opts MigrationVerifyOptions,
) error {
	dbSchema, err := dbschema.ReadSchemaWithSchemas(conn, opts.Schemas)
	if err != nil {
		return newVerificationError("re-introspect", "re_introspect_error", "re-introspect shadow database", err)
	}

	diff, err := schemadiff.CompareWithDatabase(
		ctx,
		conn,
		opts.Generated,
		dbSchema,
		opts.CompareOpts,
	)
	if err != nil {
		return newVerificationError(
			"schema-match",
			"identifier_resolution_error",
			"resolve shadow database identifier semantics",
			err,
		)
	}
	if !diff.HasChanges() {
		return nil
	}
	return newSchemaMismatchError(diff)
}

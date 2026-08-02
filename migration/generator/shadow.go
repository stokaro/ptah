package generator

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"os"
	"regexp"
	"strings"

	"go.5x5.cz/ptah/config"
	"go.5x5.cz/ptah/core/goschema"
	"go.5x5.cz/ptah/core/platform"
	"go.5x5.cz/ptah/core/platform/capability"
	"go.5x5.cz/ptah/core/platform/identifier"
	"go.5x5.cz/ptah/dbschema"
	"go.5x5.cz/ptah/internal/devlock"
	"go.5x5.cz/ptah/migration/internal/shadowdb"
	"go.5x5.cz/ptah/migration/migrator"
	"go.5x5.cz/ptah/migration/schemadiff"
)

var missingColumnErrorRe = regexp.MustCompile(`column "([^"]+)" of relation "([^"]+)" does not exist`)

// ShadowVerificationResult describes one failed shadow-database verification.
// Errors preserve the text form through
// ShadowVerificationError.Error while exposing mismatches to callers that need
// machine-readable diagnostics.
type ShadowVerificationResult struct {
	Stage string `json:"stage"`
	// Mismatches contains every structural mismatch in deterministic category and object order.
	Mismatches []ShadowMismatch `json:"mismatches,omitempty"`
}

// ShadowMismatch describes one schema mismatch found during shadow
// verification.
type ShadowMismatch struct {
	Kind       string            `json:"kind"`
	Object     string            `json:"object,omitempty"`
	Table      string            `json:"table,omitempty"`
	Column     string            `json:"column,omitempty"`
	Constraint string            `json:"constraint,omitempty"`
	Changes    map[string]string `json:"changes,omitempty"`
	Message    string            `json:"message"`
}

// ShadowVerificationError wraps a structured shadow verification result.
type ShadowVerificationError struct {
	Result ShadowVerificationResult `json:"result"`
	Err    error                    `json:"-"`
}

func (e *ShadowVerificationError) Error() string {
	if len(e.Result.Mismatches) > 0 {
		return "shadow check failed: " + e.Result.Mismatches[0].Message
	}
	return "shadow check failed: schema differs"
}

func (e *ShadowVerificationError) Unwrap() error {
	return e.Err
}

func newShadowVerificationError(stage, kind, message string, err error) *ShadowVerificationError {
	if err != nil {
		message = fmt.Sprintf("%s: %v", message, err)
	}
	return &ShadowVerificationError{
		Result: ShadowVerificationResult{
			Stage: stage,
			Mismatches: []ShadowMismatch{{
				Kind:    kind,
				Message: message,
			}},
		},
		Err: err,
	}
}

type shadowMigrationOptions struct {
	DatabaseURL         string
	TargetConnection    *dbschema.DatabaseConnection
	MigrationsDir       string
	Dialect             string
	Capabilities        capability.Capabilities
	IdentifierSemantics identifier.Semantics
	Candidates          []shadowCandidate
	Generated           *goschema.Database
	CompareOpts         *config.CompareOptions
	Schemas             []string
}

type shadowCandidate struct {
	Version int64
	Name    string
	UpSQL   string
	DownSQL string
}

func verifyShadowMigration(ctx context.Context, opts shadowMigrationOptions) error {
	database, err := shadowdb.Open(ctx, opts.DatabaseURL, "")
	if err != nil {
		return newShadowVerificationError("connect", "connect_error", "connect to shadow database", err)
	}
	defer database.CloseAndWarn()
	conn := database.Connection()

	if !sameDialect(opts.Dialect, conn.Info().Dialect) {
		return newShadowVerificationError(
			"dialect-check",
			"dialect_mismatch",
			fmt.Sprintf("shadow database dialect %q does not match target dialect %q", conn.Info().Dialect, opts.Dialect),
			nil,
		)
	}
	if opts.TargetConnection == nil {
		return newShadowVerificationError(
			"realm-check",
			"target_connection_required",
			"compare target and shadow database realms: target connection is required",
			nil,
		)
	}
	sameRealm, err := devlock.SameRealm(ctx, opts.TargetConnection, conn)
	if err != nil {
		return newShadowVerificationError(
			"realm-check",
			"realm_comparison_error",
			"compare target and shadow database realms",
			err,
		)
	}
	if sameRealm {
		return newShadowVerificationError(
			"realm-check",
			"target_shadow_same_realm",
			"shadow database must be distinct from target database",
			nil,
		)
	}
	if opts.Capabilities != nil && !maps.Equal(opts.Capabilities, conn.Info().Capabilities) {
		return newShadowVerificationError(
			"capability-check",
			"capability_mismatch",
			fmt.Sprintf("shadow database capabilities do not match target %s capabilities", opts.Dialect),
			nil,
		)
	}
	identifierSemanticsMatch, err := shadowIdentifierSemanticsMatch(
		ctx,
		conn,
		opts.Dialect,
		opts.IdentifierSemantics,
	)
	if err != nil {
		return newShadowVerificationError(
			"identifier-semantics-check",
			"identifier_semantics_resolution_error",
			"resolve shadow database identifier semantics",
			err,
		)
	}
	if !opts.IdentifierSemantics.IsZero() && !identifierSemanticsMatch {
		return newShadowVerificationError(
			"identifier-semantics-check",
			"identifier_semantics_mismatch",
			fmt.Sprintf("shadow database identifier semantics do not match target %s catalog semantics", opts.Dialect),
			nil,
		)
	}

	if err := conn.SchemaWriter().DropAllTables(ctx); err != nil {
		return newShadowVerificationError("drop-all", "drop_all_error", "drop all objects", err)
	}
	prior, err := loadPriorMigrations(opts.MigrationsDir)
	if err != nil {
		return newShadowVerificationError("load-prior", "load_prior_error", "load prior migrations", err)
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
		if description := describeReplayError(err); description != "" {
			return newShadowVerificationError("replay", "replay_error", description, err)
		}
		return newShadowVerificationError("replay", "replay_error", "replay migrations", err)
	}
	if err := assertShadowSchemaMatches(ctx, conn, opts); err != nil {
		return err
	}

	previousVersion := latestMigrationVersion(prior)
	if err := mig.MigrateDownTo(ctx, previousVersion); err != nil {
		return newShadowVerificationError("round-trip-down", "round_trip_down_error", "round-trip down", err)
	}
	if err := mig.MigrateTo(ctx, latestMigrationVersion(migrations)); err != nil {
		return newShadowVerificationError("round-trip-up", "round_trip_up_error", "round-trip up", err)
	}
	return assertShadowSchemaMatches(ctx, conn, opts)
}

func shadowIdentifierSemanticsMatch(
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
	shadow, err := conn.ResolveIdentifierSemantics(ctx, names)
	if err != nil {
		return false, err
	}
	return target.Equal(shadow.Normalize(dialect)), nil
}

func describeReplayError(err error) string {
	match := missingColumnErrorRe.FindStringSubmatch(err.Error())
	if match == nil {
		return ""
	}
	return fmt.Sprintf("missing column %s.%s", match[2], match[1])
}

func sameDialect(left, right string) bool {
	return platform.NormalizeDialect(left) == platform.NormalizeDialect(right)
}

func loadPriorMigrations(dir string, opts ...migrator.FSProviderOption) ([]*migrator.Migration, error) {
	if strings.TrimSpace(dir) == "" {
		return nil, nil
	}
	if _, err := os.Stat(dir); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, nil
		}
		return nil, err
	}

	provider, err := migrator.NewFSMigrationProvider(os.DirFS(dir), opts...)
	if err != nil {
		return nil, err
	}
	migrations := provider.Migrations()
	out := make([]*migrator.Migration, len(migrations))
	copy(out, migrations)
	return out, nil
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

func assertShadowSchemaMatches(
	ctx context.Context,
	conn *dbschema.DatabaseConnection,
	opts shadowMigrationOptions,
) error {
	dbSchema, err := dbschema.ReadSchemaWithSchemas(conn, opts.Schemas)
	if err != nil {
		return newShadowVerificationError("re-introspect", "re_introspect_error", "re-introspect shadow database", err)
	}

	diff, err := schemadiff.CompareWithDatabase(
		ctx,
		conn,
		opts.Generated,
		dbSchema,
		opts.CompareOpts,
	)
	if err != nil {
		return newShadowVerificationError(
			"schema-match",
			"identifier_resolution_error",
			"resolve shadow database identifier semantics",
			err,
		)
	}
	if !diff.HasChanges() {
		return nil
	}
	return &ShadowVerificationError{Result: ShadowVerificationResult{
		Stage:      "schema-match",
		Mismatches: collectShadowMismatches(diff),
	}}
}

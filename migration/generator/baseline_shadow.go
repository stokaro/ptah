package generator

import (
	"context"
	"fmt"
	"io/fs"
	"strings"
	"time"

	"go.5x5.cz/ptah/config"
	"go.5x5.cz/ptah/core/platform/capability"
	"go.5x5.cz/ptah/dbschema"
	dbschematypes "go.5x5.cz/ptah/dbschema/types"
	"go.5x5.cz/ptah/internal/convert/dbschematogo"
	"go.5x5.cz/ptah/internal/sqlitevirtual"
	"go.5x5.cz/ptah/migration/migrator"
	"go.5x5.cz/ptah/migration/schemadiff"
)

// BaselineShadowVerifyOptions configures shadow verification before metadata
// baselining.
type BaselineShadowVerifyOptions struct {
	ShadowDatabaseURL string
	TargetConn        *dbschema.DatabaseConnection
	MigrationsDir     string
	// MigrationsFS is the immutable migration history to replay. When nil,
	// MigrationsDir is opened for compatibility with existing embedders.
	MigrationsFS    fs.FS
	Version         int64
	Dialect         string
	Capabilities    capability.Capabilities
	CompareOptions  *config.CompareOptions
	Schemas         []string
	ProviderOptions []migrator.FSProviderOption
	ConnectTimeout  time.Duration
}

// VerifyBaselineShadow replays migrations up to Version on the shadow database
// and compares the resulting schema with the target database. Failures support
// errors.As with [ShadowVerificationError], including the complete structured
// mismatch list for schema drift. For SQLite, malformed
// PTAH_SQLITE_ALLOW_VIRTUAL_TABLE_DROP configuration is refused before target
// validation, shadow connection, or replay.
func VerifyBaselineShadow(ctx context.Context, opts BaselineShadowVerifyOptions) error {
	dialect := opts.Dialect
	if opts.TargetConn != nil {
		dialect = opts.TargetConn.Info().Dialect
	}
	if err := sqlitevirtual.ValidateToggle(dialect); err != nil {
		return baselineShadowError(
			"configuration",
			"invalid_sqlite_virtual_table_drop_toggle",
			"validate SQLite virtual-table drop toggle",
			err,
		)
	}
	if err := targetConnectionRequiredError(opts.TargetConn, "target database connection is required"); err != nil {
		return wrapBaselineShadowError(err)
	}
	connectCtx, cancelConnect := baselineShadowConnectContext(ctx, opts.ConnectTimeout)
	shadowConn, err := dbschema.ConnectToDatabase(connectCtx, opts.ShadowDatabaseURL)
	cancelConnect()
	if err != nil {
		return baselineShadowError("connect", "connect_error", "connect to shadow database", err)
	}
	defer dbschema.CloseAndWarn(shadowConn)

	if err := validateShadowConnection(
		ctx,
		opts.TargetConn,
		shadowConn,
		opts.Dialect,
		opts.Capabilities,
		"target database connection is required",
	); err != nil {
		return wrapBaselineShadowError(err)
	}

	targetSchema, err := validateBaselineTargetIdentifierSemantics(
		ctx,
		shadowConn,
		opts,
	)
	if err != nil {
		return err
	}
	if err := shadowConn.SchemaWriter().DropAllTables(ctx); err != nil {
		return baselineShadowError("drop-all", "drop_all_error", "drop all objects", err)
	}
	if err := resetBaselineShadowSchemas(ctx, shadowConn, opts.Schemas); err != nil {
		return err
	}

	migrations, err := loadPriorMigrationsFS(opts.MigrationsFS, opts.MigrationsDir, opts.ProviderOptions...)
	if err != nil {
		return baselineShadowError("load-prior", "load_prior_error", "load migrations", err)
	}
	migrations = migrationsAtOrBelow(migrations, opts.Version)
	if len(migrations) == 0 {
		return baselineShadowError(
			"load-prior",
			"no_migrations",
			fmt.Sprintf("no migrations found at or below version %d", opts.Version),
			nil,
		)
	}

	mig := migrator.NewMigrator(shadowConn, migrator.NewRegisteredMigrationProvider(migrations...))
	if err := mig.MigrateUp(ctx); err != nil {
		if description := describeReplayError(err); description != "" {
			return baselineShadowErrorWithDisplayMessage("replay", "replay_error", description, err)
		}
		return baselineShadowError("replay", "replay_error", "replay migrations", err)
	}
	if err := dropBaselineShadowMetadata(ctx, shadowConn, mig.MigrationsTableIdentifier()); err != nil {
		return err
	}

	shadowSchema, err := dbschema.ReadSchemaWithSchemas(shadowConn, opts.Schemas)
	if err != nil {
		return baselineShadowError("re-introspect", "re_introspect_error", "read shadow schema", err)
	}
	diff, err := schemadiff.CompareWithDatabase(
		ctx,
		opts.TargetConn,
		dbschematogo.ConvertDBSchemaToGoSchema(shadowSchema),
		targetSchema,
		opts.CompareOptions,
	)
	if err != nil {
		return baselineShadowError(
			"schema-match",
			"identifier_resolution_error",
			"resolve target identifier semantics",
			err,
		)
	}
	identifierSemanticsMatch, err := shadowIdentifierSemanticsMatch(
		ctx,
		shadowConn,
		opts.Dialect,
		*diff.IdentifierSemantics,
	)
	if err != nil {
		return baselineShadowError(
			"identifier-semantics-check",
			"identifier_semantics_resolution_error",
			"resolve replayed shadow identifier semantics",
			err,
		)
	}
	if !identifierSemanticsMatch {
		return baselineShadowError(
			"identifier-semantics-check",
			"identifier_semantics_mismatch",
			fmt.Sprintf("replayed shadow identifier semantics do not match target %s catalog semantics", opts.Dialect),
			nil,
		)
	}
	if !diff.HasChanges() {
		return nil
	}
	return wrapBaselineShadowError(newShadowSchemaMismatchError(diff))
}

func baselineShadowError(stage, kind, message string, err error) error {
	return wrapBaselineShadowError(newShadowVerificationError(stage, kind, message, err))
}

func baselineShadowErrorWithDisplayMessage(stage, kind, message string, err error) error {
	return wrapBaselineShadowError(newShadowVerificationErrorWithDisplayMessage(stage, kind, message, err))
}

func wrapBaselineShadowError(err *ShadowVerificationError) error {
	return fmt.Errorf("baseline %w", err)
}

func validateBaselineTargetIdentifierSemantics(
	ctx context.Context,
	shadowConn *dbschema.DatabaseConnection,
	opts BaselineShadowVerifyOptions,
) (*dbschematypes.DBSchema, error) {
	targetSchema, err := dbschema.ReadSchemaWithSchemas(opts.TargetConn, opts.Schemas)
	if err != nil {
		return nil, baselineShadowError("target-introspect", "target_introspection_error", "read target schema", err)
	}
	targetGenerated := dbschematogo.ConvertDBSchemaToGoSchema(targetSchema)
	targetDiff, err := schemadiff.CompareWithDatabase(
		ctx,
		opts.TargetConn,
		targetGenerated,
		targetSchema,
		opts.CompareOptions,
	)
	if err != nil {
		return nil, baselineShadowError(
			"identifier-semantics-check",
			"target_identifier_semantics_resolution_error",
			"resolve target identifier semantics",
			err,
		)
	}
	identifierSemanticsMatch, err := shadowIdentifierSemanticsMatch(
		ctx,
		shadowConn,
		opts.Dialect,
		*targetDiff.IdentifierSemantics,
	)
	if err != nil {
		return nil, baselineShadowError(
			"identifier-semantics-check",
			"identifier_semantics_resolution_error",
			"resolve shadow identifier semantics",
			err,
		)
	}
	if !identifierSemanticsMatch {
		return nil, baselineShadowError(
			"identifier-semantics-check",
			"identifier_semantics_mismatch",
			fmt.Sprintf(
				"shadow database identifier semantics do not match target %s catalog semantics",
				opts.Dialect,
			),
			nil,
		)
	}
	return targetSchema, nil
}

func baselineShadowConnectContext(ctx context.Context, timeout time.Duration) (context.Context, context.CancelFunc) {
	if timeout <= 0 {
		return context.WithCancel(ctx)
	}
	return context.WithTimeout(ctx, timeout)
}

func migrationsAtOrBelow(migrations []*migrator.Migration, version int64) []*migrator.Migration {
	out := make([]*migrator.Migration, 0, len(migrations))
	for _, migration := range migrations {
		if migration.Version <= version {
			out = append(out, migration)
		}
	}
	return out
}

func resetBaselineShadowSchemas(ctx context.Context, conn *dbschema.DatabaseConnection, schemas []string) error {
	if conn.Info().Dialect != "postgres" {
		return nil
	}
	for _, schema := range schemas {
		if schema == "" || schema == "public" {
			continue
		}
		_, err := conn.ExecContext(ctx, "DROP SCHEMA IF EXISTS "+quoteBaselinePostgresIdentifier(schema)+" CASCADE")
		if err != nil {
			return baselineShadowError(
				"reset-schemas",
				"schema_reset_error",
				fmt.Sprintf("drop schema %q", schema),
				err,
			)
		}
	}
	return nil
}

func dropBaselineShadowMetadata(ctx context.Context, conn *dbschema.DatabaseConnection, tableIdentifier string) error {
	_, err := conn.ExecContext(ctx, "DROP TABLE IF EXISTS "+tableIdentifier)
	if err != nil {
		return baselineShadowError("drop-metadata", "drop_metadata_error", "drop metadata table", err)
	}
	return nil
}

func quoteBaselinePostgresIdentifier(value string) string {
	return `"` + strings.ReplaceAll(value, `"`, `""`) + `"`
}

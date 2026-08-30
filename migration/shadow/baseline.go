package shadow

import (
	"context"
	"fmt"
	"io/fs"
	"time"

	"go.5x5.cz/ptah/catalog"
	"go.5x5.cz/ptah/config"
	"go.5x5.cz/ptah/core/platform/capability"
	"go.5x5.cz/ptah/dbschema"
	"go.5x5.cz/ptah/internal/convert/dbschematogo"
	"go.5x5.cz/ptah/internal/sqlitevirtual"
	"go.5x5.cz/ptah/migration/internal/shadowdb"
	"go.5x5.cz/ptah/migration/migrator"
	"go.5x5.cz/ptah/migration/schemadiff"
)

// BaselineVerifyOptions configures shadow verification before metadata
// baselining.
type BaselineVerifyOptions struct {
	// ShadowDatabaseURL is an ephemeral database the verification drops clean
	// and replays the history into. Its contents are discarded, and its live
	// realm must be distinct from TargetConn's.
	ShadowDatabaseURL string
	// TargetConn is the already-open database whose metadata would be
	// baselined. It is introspected and compared against, never written, and
	// it is required.
	TargetConn *dbschema.DatabaseConnection
	// MigrationsDir names the migration directory in messages and is opened
	// as the history when MigrationsFS is nil.
	MigrationsDir string
	// MigrationsFS is the immutable migration history to replay. When nil,
	// MigrationsDir is opened for compatibility with existing embedders.
	MigrationsFS fs.FS
	// Version is the version being baselined: every migration at or below it
	// is replayed, and finding none fails at the load-prior stage.
	Version int64
	// Dialect is the target dialect the shadow database must match. The two
	// spellings are compared normalized, so an alias matches its canonical
	// name.
	Dialect string
	// Capabilities, when non-nil, must equal the shadow connection's resolved
	// capabilities exactly; a difference fails at the capability-check stage.
	// Nil skips the check.
	Capabilities capability.Capabilities
	// CompareOptions tunes the schema comparison; nil selects
	// config.DefaultCompareOptions.
	CompareOptions *config.CompareOptions
	// Schemas scopes both introspections to the named schemas; empty reads
	// the connection's default scope. On PostgreSQL the named non-public
	// schemas are also dropped from the shadow database before the replay, so
	// a migration that creates one starts from nothing.
	Schemas []string
	// ProviderOptions are passed to the migration provider (e.g. dir format).
	ProviderOptions []migrator.FSProviderOption
	// ConnectTimeout bounds the shadow database connection attempt; zero or
	// negative means no bound beyond the caller's context.
	ConnectTimeout time.Duration
}

// VerifyBaseline replays migrations up to Version on the shadow database
// and compares the resulting schema with the target database. Failures support
// errors.As with [VerificationError], including the complete structured
// mismatch list for schema drift. For SQLite, malformed
// PTAH_SQLITE_ALLOW_VIRTUAL_TABLE_DROP configuration is refused before target
// validation, shadow connection, or replay.
func VerifyBaseline(ctx context.Context, opts BaselineVerifyOptions) error {
	dialect := opts.Dialect
	if opts.TargetConn != nil {
		dialect = opts.TargetConn.Info().Dialect
	}
	if err := sqlitevirtual.ValidateToggle(dialect); err != nil {
		return baselineError(
			"configuration",
			"invalid_sqlite_virtual_table_drop_toggle",
			"validate SQLite virtual-table drop toggle",
			err,
		)
	}
	if err := targetConnectionRequiredError(opts.TargetConn, "target database connection is required"); err != nil {
		return wrapBaselineError(err)
	}
	connectCtx, cancelConnect := connectContext(ctx, opts.ConnectTimeout)
	shadowConn, err := dbschema.ConnectToDatabase(connectCtx, opts.ShadowDatabaseURL)
	cancelConnect()
	if err != nil {
		return baselineError("connect", "connect_error", "connect to shadow database", err)
	}
	defer dbschema.CloseAndWarn(shadowConn)

	if err := validateConnection(
		ctx,
		opts.TargetConn,
		shadowConn,
		opts.Dialect,
		opts.Capabilities,
		"target database connection is required",
	); err != nil {
		return wrapBaselineError(err)
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
		return baselineError("drop-all", "drop_all_error", "drop all objects", err)
	}
	if err := shadowdb.ResetSchemas(ctx, shadowConn, opts.Schemas); err != nil {
		return baselineErrorWithDisplayMessage("reset-schemas", "schema_reset_error", err.Error(), err)
	}

	migrations, err := shadowdb.LoadMigrations(opts.MigrationsFS, opts.MigrationsDir, opts.ProviderOptions...)
	if err != nil {
		return baselineError("load-prior", "load_prior_error", "load migrations", err)
	}
	migrations = migrationsAtOrBelow(migrations, opts.Version)
	if len(migrations) == 0 {
		return baselineError(
			"load-prior",
			"no_migrations",
			fmt.Sprintf("no migrations found at or below version %d", opts.Version),
			nil,
		)
	}

	mig := migrator.NewMigrator(shadowConn, migrator.NewRegisteredMigrationProvider(migrations...))
	if err := mig.MigrateUp(ctx); err != nil {
		if description := shadowdb.DescribeReplayError(err); description != "" {
			return baselineErrorWithDisplayMessage("replay", "replay_error", description, err)
		}
		return baselineError("replay", "replay_error", "replay migrations", err)
	}
	if err := shadowdb.DropMigrationMetadata(ctx, shadowConn, mig.MigrationsTableIdentifier()); err != nil {
		return baselineErrorWithDisplayMessage("drop-metadata", "drop_metadata_error", err.Error(), err)
	}

	shadowSchema, err := dbschema.ReadSchemaWithSchemasContext(ctx, shadowConn, opts.Schemas)
	if err != nil {
		return baselineError("re-introspect", "re_introspect_error", "read shadow schema", err)
	}
	diff, err := schemadiff.CompareWithDatabase(
		ctx,
		opts.TargetConn,
		dbschematogo.ConvertDBSchemaToGoSchema(shadowSchema),
		targetSchema,
		opts.CompareOptions,
	)
	if err != nil {
		return baselineError(
			"schema-match",
			"identifier_resolution_error",
			"resolve target identifier semantics",
			err,
		)
	}
	identifierSemanticsMatch, err := identifierSemanticsAgree(
		ctx,
		shadowConn,
		opts.Dialect,
		*diff.IdentifierSemantics,
	)
	if err != nil {
		return baselineError(
			"identifier-semantics-check",
			"identifier_semantics_resolution_error",
			"resolve replayed shadow identifier semantics",
			err,
		)
	}
	if !identifierSemanticsMatch {
		return baselineError(
			"identifier-semantics-check",
			"identifier_semantics_mismatch",
			fmt.Sprintf("replayed shadow identifier semantics do not match target %s catalog semantics", opts.Dialect),
			nil,
		)
	}
	if !diff.HasChanges() {
		return nil
	}
	return wrapBaselineError(newSchemaMismatchError(diff))
}

func baselineError(stage, kind, message string, err error) error {
	return wrapBaselineError(newVerificationError(stage, kind, message, err))
}

func baselineErrorWithDisplayMessage(stage, kind, message string, err error) error {
	return wrapBaselineError(newVerificationErrorWithDisplayMessage(stage, kind, message, err))
}

func wrapBaselineError(err *VerificationError) error {
	return fmt.Errorf("baseline %w", err)
}

func validateBaselineTargetIdentifierSemantics(
	ctx context.Context,
	shadowConn *dbschema.DatabaseConnection,
	opts BaselineVerifyOptions,
) (*catalog.Database, error) {
	targetSchema, err := dbschema.ReadSchemaWithSchemasContext(ctx, opts.TargetConn, opts.Schemas)
	if err != nil {
		return nil, baselineError("target-introspect", "target_introspection_error", "read target schema", err)
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
		return nil, baselineError(
			"identifier-semantics-check",
			"target_identifier_semantics_resolution_error",
			"resolve target identifier semantics",
			err,
		)
	}
	identifierSemanticsMatch, err := identifierSemanticsAgree(
		ctx,
		shadowConn,
		opts.Dialect,
		*targetDiff.IdentifierSemantics,
	)
	if err != nil {
		return nil, baselineError(
			"identifier-semantics-check",
			"identifier_semantics_resolution_error",
			"resolve shadow identifier semantics",
			err,
		)
	}
	if !identifierSemanticsMatch {
		return nil, baselineError(
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

func connectContext(ctx context.Context, timeout time.Duration) (context.Context, context.CancelFunc) {
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

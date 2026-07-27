package generator

import (
	"context"
	"fmt"
	"time"

	"github.com/stokaro/ptah/core/goschema"
	"github.com/stokaro/ptah/core/platform/capability"
	"github.com/stokaro/ptah/dbschema"
	dbschematypes "github.com/stokaro/ptah/dbschema/types"
	"github.com/stokaro/ptah/internal/convert/dbschematogo"
	"github.com/stokaro/ptah/migration/migrator"
	"github.com/stokaro/ptah/migration/schemadiff"
)

// GenerateCheckpoint renders a full cumulative schema as a checkpoint migration
// body pair. The up body creates every object in dependency order; the down
// body drops them in reverse.
//
// It diffs the schema against an empty database, so it is deterministic and
// needs no live database connection. Callers obtain the schema either by
// introspecting a database that has the whole migration directory applied (and
// converting the result with internal/convert/dbschematogo) or from Go
// entities / schema files. An empty schema yields empty up and down bodies.
func GenerateCheckpoint(schema *goschema.Database, dialect string) (upSQL, downSQL string, err error) {
	if schema == nil {
		return "", "", fmt.Errorf("checkpoint schema is required")
	}

	empty := &dbschematypes.DBSchema{}
	diff := schemadiff.CompareWithDialect(schema, empty, dialect)
	spec, _, err := buildGeneratedMigrationSpec(generatedMigrationSpecOptions{
		Diff:         diff,
		Generated:    schema,
		DBSchema:     empty,
		Dialect:      dialect,
		Capabilities: capability.ForDialect(dialect),
	})
	if err != nil {
		return "", "", fmt.Errorf("generate checkpoint: %w", err)
	}
	return spec.UpSQL, spec.DownSQL, nil
}

// WriteCheckpointFiles writes a checkpoint migration pair
// (NNNNNNNNNN_description.checkpoint.up.sql and .checkpoint.down.sql) into
// outputDir at the given version and rewrites the directory's ptah.sum so the
// new files are integrity-protected. It refuses to overwrite existing files —
// the caller resolves a version above the existing history — and returns the
// two written paths.
func WriteCheckpointFiles(outputDir string, version int64, description, upSQL, downSQL string) (upPath, downPath string, err error) {
	return writeMigrationPair(outputDir, version, description, upSQL, downSQL, "checkpoint", migrator.GenerateCheckpointMigrationFileName)
}

// CheckpointFromShadowOptions configures GenerateCheckpointFromShadow.
type CheckpointFromShadowOptions struct {
	// ShadowDatabaseURL is an ephemeral database the generator drops clean and
	// replays the migration directory into. Its contents are discarded.
	ShadowDatabaseURL string
	// MigrationsDir is the directory whose entire history is replayed.
	MigrationsDir string
	// Dialect, when set, must match the shadow database dialect. When empty the
	// shadow database's dialect is used.
	Dialect string
	// Schemas restricts introspection to the listed schemas where supported.
	Schemas []string
	// ProviderOptions are passed to the migration provider (e.g. dir format).
	ProviderOptions []migrator.FSProviderOption
	// ConnectTimeout bounds the shadow database connection attempt.
	ConnectTimeout time.Duration
}

// GenerateCheckpointFromShadow replays the entire migration directory on a fresh
// shadow database, introspects the resulting cumulative schema, and renders it
// as a checkpoint migration body pair (up creates everything, down drops it).
// The migration directory is the source of truth, so no target database is
// needed. The shadow database is dropped clean before the replay and its
// migration metadata is removed before introspection.
func GenerateCheckpointFromShadow(ctx context.Context, opts CheckpointFromShadowOptions) (upSQL, downSQL string, err error) {
	connectCtx, cancelConnect := baselineShadowConnectContext(ctx, opts.ConnectTimeout)
	shadowConn, err := dbschema.ConnectToDatabase(connectCtx, opts.ShadowDatabaseURL)
	cancelConnect()
	if err != nil {
		return "", "", fmt.Errorf("checkpoint generation failed: connect to shadow database: %w", err)
	}
	defer dbschema.CloseAndWarn(shadowConn)

	return generateCheckpointFromConn(ctx, shadowConn, opts)
}

// generateCheckpointFromConn holds the database-facing core of checkpoint
// generation so it can be exercised against any connection, including an
// in-memory one, without a live server.
func generateCheckpointFromConn(ctx context.Context, shadowConn *dbschema.DatabaseConnection, opts CheckpointFromShadowOptions) (upSQL, downSQL string, err error) {
	dialect := opts.Dialect
	if dialect == "" {
		dialect = shadowConn.Info().Dialect
	} else if !sameDialect(dialect, shadowConn.Info().Dialect) {
		return "", "", fmt.Errorf("checkpoint generation failed: shadow database dialect %q does not match target dialect %q", shadowConn.Info().Dialect, dialect)
	}

	if err := shadowConn.SchemaWriter().DropAllTables(ctx); err != nil {
		return "", "", fmt.Errorf("checkpoint generation failed: drop all objects: %w", err)
	}
	if err := resetBaselineShadowSchemas(ctx, shadowConn, opts.Schemas); err != nil {
		return "", "", err
	}

	migrations, err := loadPriorMigrations(opts.MigrationsDir, opts.ProviderOptions...)
	if err != nil {
		return "", "", fmt.Errorf("checkpoint generation failed: load migrations: %w", err)
	}
	if len(migrations) == 0 {
		return "", "", fmt.Errorf("checkpoint generation failed: no migrations found in %s", opts.MigrationsDir)
	}

	mig := migrator.NewMigrator(shadowConn, migrator.NewRegisteredMigrationProvider(migrations...))
	if err := mig.MigrateUp(ctx); err != nil {
		if description := describeReplayError(err); description != "" {
			return "", "", fmt.Errorf("checkpoint generation failed: %s", description)
		}
		return "", "", fmt.Errorf("checkpoint generation failed: replay migrations: %w", err)
	}
	if err := dropBaselineShadowMetadata(ctx, shadowConn, mig.MigrationsTableIdentifier()); err != nil {
		return "", "", err
	}

	shadowSchema, err := dbschema.ReadSchemaWithSchemas(shadowConn, opts.Schemas)
	if err != nil {
		return "", "", fmt.Errorf("checkpoint generation failed: read shadow schema: %w", err)
	}
	return GenerateCheckpoint(dbschematogo.ConvertDBSchemaToGoSchema(shadowSchema), dialect)
}

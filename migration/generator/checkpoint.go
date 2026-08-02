package generator

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/stokaro/ptah/core/goschema"
	"github.com/stokaro/ptah/core/platform/capability"
	"github.com/stokaro/ptah/dbschema"
	dbschematypes "github.com/stokaro/ptah/dbschema/types"
	"github.com/stokaro/ptah/internal/convert/dbschematogo"
	"github.com/stokaro/ptah/internal/migratesum"
	"github.com/stokaro/ptah/migration/migrator"
	"github.com/stokaro/ptah/migration/schemadiff"
	schemadifftypes "github.com/stokaro/ptah/migration/schemadiff/types"
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
	return GenerateCheckpointWithDatabaseInfo(schema, dbschematypes.DBInfo{
		Dialect:      dialect,
		Capabilities: capability.ForDialect(dialect),
	})
}

// GenerateCheckpointWithDatabaseInfo renders a full cumulative schema using
// caller-supplied dialect, capability, and identifier metadata. SQL Server
// callers should prefer GenerateCheckpointWithDatabase so the complete
// candidate identifier set is resolved under the live catalog collation.
func GenerateCheckpointWithDatabaseInfo(
	schema *goschema.Database,
	info dbschematypes.DBInfo,
) (upSQL, downSQL string, err error) {
	if schema == nil {
		return "", "", fmt.Errorf("checkpoint schema is required")
	}

	empty := &dbschematypes.DBSchema{}
	diff, err := schemadiff.CompareWithDatabaseInfo(schema, empty, info, nil)
	if err != nil {
		return "", "", fmt.Errorf("generate checkpoint: %w", err)
	}
	return generateCheckpointFromDiff(schema, empty, info, diff)
}

// GenerateCheckpointWithDatabase renders a checkpoint after resolving the
// schema's finite identifier set against the connected catalog.
func GenerateCheckpointWithDatabase(
	ctx context.Context,
	conn *dbschema.DatabaseConnection,
	schema *goschema.Database,
) (upSQL, downSQL string, err error) {
	if schema == nil {
		return "", "", fmt.Errorf("checkpoint schema is required")
	}
	empty := &dbschematypes.DBSchema{}
	diff, err := schemadiff.CompareWithDatabase(ctx, conn, schema, empty, nil)
	if err != nil {
		return "", "", fmt.Errorf("generate checkpoint: %w", err)
	}
	return generateCheckpointFromDiff(schema, empty, conn.Info(), diff)
}

func generateCheckpointFromDiff(
	schema *goschema.Database,
	empty *dbschematypes.DBSchema,
	info dbschematypes.DBInfo,
	diff *schemadifftypes.SchemaDiff,
) (upSQL, downSQL string, err error) {
	capabilities := info.Capabilities
	if capabilities == nil {
		capabilities = capability.ForDialect(info.Dialect)
	}
	spec, _, err := buildGeneratedMigrationSpec(generatedMigrationSpecOptions{
		Diff:         diff,
		Generated:    schema,
		DBSchema:     empty,
		Dialect:      info.Dialect,
		Capabilities: capabilities,
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

// AtlasCheckpointDirective is the file directive that marks an Atlas-format
// migration as a checkpoint.
//
// Measured against Atlas: the directive is honored only as the file's FIRST
// line — the same text further down is ordinary comment content. The reader
// enforces that (see migration/migrator/atlas_checkpoint.go), so the writer
// must place it first and must not prepend a provenance header before it.
const AtlasCheckpointDirective = "-- atlas:checkpoint"

// ResolveAtlasCheckpointVersion returns the version an Atlas-format checkpoint
// written into outputDir should carry: the current UTC timestamp in Atlas's
// 20060102150405 layout, bumped past the newest migration at the TOP LEVEL of
// outputDir.
//
// This mirrors the version policy Atlas itself was measured to use and the one
// `migrate new --dir-format atlas` already applies, rather than the ptah
// format's "newest + 1" counter.
//
// The scan is deliberately shallow, matching Atlas's own reader, which does not
// recurse. Ptah's reader and its checkpoint replay DO recurse, so this value
// alone does not guarantee the checkpoint sorts above everything its body
// covers: a nested migration dated after the timestamp still outranks it, and a
// fresh database would then run the checkpoint and replay that migration on top
// of it. Callers writing into a directory Ptah will read must take the maximum
// of this and the newest version from a recursive walk — see
// resolveCheckpointVersion in cmd/migratecheckpoint. The signature is kept free
// of that bound on purpose; supplying it is the caller's job.
func ResolveAtlasCheckpointVersion(outputDir string) int64 {
	return nextAvailableAtlasMigrationVersion(outputDir, nextAtlasMigrationVersion())
}

// WriteAtlasCheckpointFile writes an Atlas-format checkpoint migration
// (<version>_<description>.sql whose first line is [AtlasCheckpointDirective])
// into outputDir and rewrites the directory's atlas.sum so the new file is
// integrity-protected. It returns the written path.
//
// Unlike the ptah two-file convention there is no down body: the Atlas format
// is up-only, and measured Atlas checkpoints are a single file. Callers that
// need a reversible checkpoint must use [WriteCheckpointFiles].
//
// The file is created exclusively. A collision means the resolved version is
// not above the existing history after all, which would produce a checkpoint
// that does not cover it, so the write fails rather than silently choosing a
// different version.
//
// atlas.sum is written unconditionally. This does not check whether outputDir
// also carries a ptah.sum; a directory holding both integrity files is
// ambiguous to `--dir-format auto`, so callers that accept a user-chosen
// directory should reject that combination first, as
// `ptah migrations checkpoint` does.
func WriteAtlasCheckpointFile(outputDir string, version int64, description, upSQL string) (path string, err error) {
	if version <= 0 {
		return "", fmt.Errorf("checkpoint version must be greater than zero, got %d", version)
	}
	if err := ensureMigrationOutputDir(outputDir); err != nil {
		return "", fmt.Errorf("failed to create output directory: %w", err)
	}

	name, contents := AtlasCheckpointArtifact(version, description, upSQL)
	filePath := filepath.Join(outputDir, name)
	if err := writeNewMigrationFile(filePath, contents); err != nil {
		if errors.Is(err, os.ErrExist) {
			return "", fmt.Errorf("checkpoint file %s already exists", filePath)
		}
		return "", fmt.Errorf("failed to write atlas checkpoint file: %w", err)
	}
	if _, err := migratesum.WriteWithFormat(outputDir, migrator.MigrationDirFormatAtlas); err != nil {
		// The checkpoint is only safe to leave behind once atlas.sum covers it;
		// an uncovered file makes the whole directory fail verification.
		_ = os.Remove(filePath)
		return "", fmt.Errorf("failed to write atlas checkpoint checksum: %w", err)
	}
	return filePath, nil
}

// AtlasCheckpointArtifact returns the file name and contents that
// [WriteAtlasCheckpointFile] writes for the given version, description and
// cumulative up body, without touching the filesystem. Dry runs render it, and
// the writer uses it too, so the previewed artifact is the written one.
//
// The name is <version>_<description>.sql; Atlas was measured to write
// <version>_checkpoint.sql for an unnamed checkpoint, so an empty description
// falls back to "checkpoint" rather than to the bare <version>.sql that
// `migrate new` uses. The contents are the measured Atlas layout: the
// directive on the first line, a blank separator line, then the SQL.
func AtlasCheckpointArtifact(version int64, description, upSQL string) (name, contents string) {
	stem := atlasEmptyMigrationName(description)
	if stem == "" {
		stem = "checkpoint"
	}
	name = fmt.Sprintf("%d_%s.sql", version, stem)

	body := strings.Trim(upSQL, "\n")
	if body == "" {
		return name, AtlasCheckpointDirective + "\n"
	}
	return name, AtlasCheckpointDirective + "\n\n" + body + "\n"
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
	if opts.Dialect != "" && !sameDialect(opts.Dialect, shadowConn.Info().Dialect) {
		return "", "", fmt.Errorf(
			"checkpoint generation failed: shadow database dialect %q does not match target dialect %q",
			shadowConn.Info().Dialect,
			opts.Dialect,
		)
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
	return GenerateCheckpointWithDatabase(
		ctx,
		shadowConn,
		dbschematogo.ConvertDBSchemaToGoSchema(shadowSchema),
	)
}

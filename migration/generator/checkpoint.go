package generator

import (
	"context"
	"fmt"
	"io/fs"
	"strings"
	"time"

	"go.5x5.cz/ptah/catalog"
	"go.5x5.cz/ptah/core/platform/capability"
	"go.5x5.cz/ptah/core/schemamodel"
	"go.5x5.cz/ptah/dbschema"
	"go.5x5.cz/ptah/internal/atlasmigrate"
	"go.5x5.cz/ptah/internal/atlasurl"
	"go.5x5.cz/ptah/internal/convert/dbschematogo"
	"go.5x5.cz/ptah/internal/sqlitevirtual"
	"go.5x5.cz/ptah/migration/internal/shadowdb"
	"go.5x5.cz/ptah/migration/migrationfile"
	"go.5x5.cz/ptah/migration/migrator"
	"go.5x5.cz/ptah/migration/schemadiff"
	"go.5x5.cz/ptah/migration/schemadiff/difftypes"
)

// generateCheckpoint renders a full cumulative schema as a checkpoint migration
// body pair. The up body creates every object in dependency order; the down
// body drops them in reverse.
//
// It diffs the schema against an empty database, so it is deterministic and
// needs no live database connection. Callers obtain the schema either by
// introspecting a database that has the whole migration directory applied (and
// converting the result with atlascompat.DBSchemaToGoSchema) or from Go
// entities / schema files. An empty schema yields empty up and down bodies.
func generateCheckpoint(schema *schemamodel.Database, dialect string) (upSQL, downSQL string, err error) {
	return generateCheckpointWithDatabaseInfo(schema, catalog.ServerInfo{
		Dialect:      dialect,
		Capabilities: capability.ForDialect(dialect),
	})
}

// generateCheckpointWithDatabaseInfo renders a full cumulative schema using
// caller-supplied dialect, capability, and identifier metadata. It resolves
// identifiers only from what info carries; checkpoints rendered from a live
// database go through generateCheckpointWithDatabaseQualified instead, which
// resolves the complete candidate identifier set under the live catalog
// collation — the distinction that matters on SQL Server.
func generateCheckpointWithDatabaseInfo(
	schema *schemamodel.Database,
	info catalog.ServerInfo,
) (upSQL, downSQL string, err error) {
	if schema == nil {
		return "", "", fmt.Errorf("checkpoint schema is required")
	}

	empty := &catalog.Database{}
	diff, err := schemadiff.CompareWithDatabaseInfo(schema, empty, info, nil)
	if err != nil {
		return "", "", fmt.Errorf("generate checkpoint: %w", err)
	}
	return generateCheckpointFromDiff(schema, empty, info, diff, "")
}

// generateCheckpointWithDatabaseQualified renders a checkpoint after resolving
// the schema's finite identifier set against the connected catalog, under the
// schema qualifier the shadow-replay entry point carries (empty for none).
func generateCheckpointWithDatabaseQualified(
	ctx context.Context,
	conn *dbschema.DatabaseConnection,
	schema *schemamodel.Database,
	qualifier string,
) (upSQL, downSQL string, err error) {
	if schema == nil {
		return "", "", fmt.Errorf("checkpoint schema is required")
	}
	empty := &catalog.Database{}
	diff, err := schemadiff.CompareWithDatabase(ctx, conn, schema, empty, nil)
	if err != nil {
		return "", "", fmt.Errorf("generate checkpoint: %w", err)
	}
	return generateCheckpointFromDiff(schema, empty, conn.Info(), diff, qualifier)
}

func generateCheckpointFromDiff(
	schema *schemamodel.Database,
	empty *catalog.Database,
	info catalog.ServerInfo,
	diff *difftypes.SchemaDiff,
	qualifierValue string,
) (upSQL, downSQL string, err error) {
	capabilities := info.Capabilities
	if capabilities == nil {
		capabilities = capability.ForDialect(info.Dialect)
	}
	qualifier, err := atlasmigrate.ParseQualifier(qualifierValue)
	if err != nil {
		return "", "", err
	}
	plan, err := PlanBidirectionalSchemaDiff(BidirectionalSchemaPlanOptions{
		Diff:          diff,
		DesiredSchema: schema,
		CurrentSchema: empty,
		Dialect:       info.Dialect,
		Capabilities:  capabilities,
		Policy: BidirectionalPlanPolicy{
			Create: ConcurrentIndexAutomatic,
			Drop:   ConcurrentIndexDisabled,
		},
	})
	if err != nil {
		return "", "", fmt.Errorf("generate checkpoint: %w", err)
	}
	spec, _, err := buildGeneratedMigrationSpec(generatedMigrationSpecOptions{
		Plan:      plan,
		Qualifier: qualifier,
	})
	if err != nil {
		return "", "", fmt.Errorf("generate checkpoint: %w", err)
	}
	return spec.UpSQL, spec.DownSQL, nil
}

// writeCheckpointFiles writes a checkpoint migration pair
// (NNNNNNNNNN_description.checkpoint.up.sql and .checkpoint.down.sql) into
// outputDir at the given version and rewrites the directory's ptah.sum so the
// new files are integrity-protected. It refuses to overwrite existing files —
// the caller resolves a version above the existing history — and returns the
// two written paths.
func writeCheckpointFiles(outputDir string, version int64, description, upSQL, downSQL string) (upPath, downPath string, err error) {
	return WriteCheckpointFilesWithOptions(outputDir, version, description, upSQL, downSQL, CheckpointWriteOptions{})
}

// CheckpointWriteOptions supplies the state a checkpoint writer must preserve
// while it adds the checkpoint and refreshes the directory integrity file.
type CheckpointWriteOptions struct {
	// AuthorizedMigrationsFS is the immutable migration history that produced
	// the checkpoint body. When non-nil, the writer refuses if the bound output
	// directory no longer contains exactly that history before publication.
	AuthorizedMigrationsFS fs.FS
}

// WriteCheckpointFilesWithOptions writes a reversible checkpoint migration
// pair (NNNNNNNNNN_description.checkpoint.up.sql and .checkpoint.down.sql)
// into outputDir at the given version and rewrites the directory's ptah.sum so
// the new pair is integrity-protected. It refuses to overwrite existing files
// -- the caller resolves a version above the existing history -- and returns
// the two written paths. A down half that cannot be created withdraws the up
// half, so a failure never leaves a migration with no rollback.
//
// When opts.AuthorizedMigrationsFS is set, publication is bound to the history
// that produced the checkpoint body: a bound output directory that does not
// match the authorized state fails with [ErrMigrationDirectoryChanged]
// (errors.Is) before the checkpoint is created, or the checkpoint is withdrawn
// before the sum is published, so a mismatch never leaves a half-published
// checkpoint behind. The refreshed ptah.sum is computed from the authorized
// state plus the new pair rather than from a reopened live directory, so a
// concurrent edit cannot be legitimized by the new checksum.
//
// The whole transaction -- verification, both files, and the sum -- runs
// through one rooted binding of outputDir (stokaro/ptah#1118).
func WriteCheckpointFilesWithOptions(
	outputDir string,
	version int64,
	description, upSQL, downSQL string,
	opts CheckpointWriteOptions,
) (upPath, downPath string, err error) {
	return writeMigrationPairAuthorized(
		outputDir,
		version,
		description,
		upSQL,
		downSQL,
		"checkpoint",
		migrationfile.CheckpointFileName,
		opts.AuthorizedMigrationsFS,
	)
}

// atlasCheckpointDirective is the file directive that marks an Atlas-format
// migration as a checkpoint.
//
// Measured against Atlas: the directive is honored only as the file's FIRST
// line — the same text further down is ordinary comment content. The reader
// enforces that (see migration/migrator/atlas_checkpoint.go), so the writer
// must place it first and must not prepend a provenance header before it.
const atlasCheckpointDirective = "-- atlas:checkpoint"

// ResolveAtlasCheckpointVersion returns the version an Atlas-format checkpoint
// written into outputDir should carry: the current UTC timestamp in Atlas's
// 20060102150405 layout, bumped past the newest migration at the TOP LEVEL of
// outputDir.
//
// The timestamp is the version policy Atlas itself was measured to use. The
// bump on top of it is this function's own, and is what separates a checkpoint
// from an ordinary migration: `migrate new --dir-format atlas` deliberately
// does NOT bump (stokaro/ptah#938), because a new migration may sort below a
// future-dated neighbor, while a checkpoint may not sort below the history it
// squashes.
//
// It returns 0 when no version outranks the directory -- a directory whose
// newest migration already sits at the largest value an Atlas file name can
// carry. Callers take the maximum of this and their own recursive bound anyway,
// so 0 loses that comparison and the caller reports the exhaustion.
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

// writeAtlasCheckpointFile writes an Atlas-format checkpoint migration
// (<version>_<description>.sql whose first line is [atlasCheckpointDirective])
// into outputDir and rewrites the directory's atlas.sum so the new file is
// integrity-protected. It returns the written path.
//
// Unlike the ptah two-file convention there is no down body: the Atlas format
// is up-only, and measured Atlas checkpoints are a single file. Callers that
// need a reversible checkpoint must use [WriteCheckpointFilesWithOptions].
//
// The file is created exclusively. A collision means the resolved version is
// not above the existing history after all, which would produce a checkpoint
// that does not cover it, so the write fails rather than silently choosing a
// different version.
//
// atlas.sum is committed conditionally on the checksum state this call
// observed, so a concurrent writer that replaced it is reported rather than
// overwritten. This does not check whether outputDir also carries a ptah.sum; a
// directory holding both integrity files is ambiguous to `--dir-format auto`, so
// callers that accept a user-chosen directory should reject that combination
// first, as `ptah migrations checkpoint` does.
//
// The whole transaction runs through one binding of outputDir, so the file and
// the checksum are committed to the same filesystem object even if the pathname
// is replaced mid-call (stokaro/ptah#1118).
func writeAtlasCheckpointFile(outputDir string, version int64, description, upSQL string) (path string, err error) {
	return WriteAtlasCheckpointFileWithOptions(outputDir, version, description, upSQL, CheckpointWriteOptions{})
}

// WriteAtlasCheckpointFileWithOptions writes an Atlas-format checkpoint --
// the exact file name and contents [AtlasCheckpointArtifact] renders, so a
// preview cannot drift from what is written -- into outputDir and rewrites the
// directory's atlas.sum so the new file is integrity-protected. The Atlas
// convention is up-only; a caller that needs a reversible checkpoint uses
// [WriteCheckpointFilesWithOptions]. A version of zero or less is refused, and
// the file is created exclusively, so a name collision fails rather than
// silently choosing a different version.
//
// When opts.AuthorizedMigrationsFS is set, publication is bound to the history
// that produced the checkpoint body: a bound output directory that does not
// match the authorized state fails with [ErrMigrationDirectoryChanged]
// (errors.Is) before the checkpoint is created, or the checkpoint is withdrawn
// before atlas.sum is published. The whole transaction runs through one rooted
// binding of outputDir (stokaro/ptah#1118).
func WriteAtlasCheckpointFileWithOptions(
	outputDir string,
	version int64,
	description, upSQL string,
	opts CheckpointWriteOptions,
) (path string, err error) {
	if version <= 0 {
		return "", fmt.Errorf("checkpoint version must be greater than zero, got %d", version)
	}
	return writeRootedAtlasCheckpoint(outputDir, version, description, upSQL, opts.AuthorizedMigrationsFS)
}

// AtlasCheckpointArtifact returns the file name and contents that
// [WriteAtlasCheckpointFileWithOptions] writes for the given version,
// description and cumulative up body, without touching the filesystem. Dry
// runs render it, and the writer uses it too, so the previewed artifact is
// the written one.
//
// The name is <version>_<description>.sql; Atlas was measured to write
// <version>_checkpoint.sql for an unnamed checkpoint, so an empty description
// falls back to "checkpoint" rather than to the bare <version>.sql that
// `migrate new` uses. The contents are the measured Atlas layout: the
// directive on the first line, a blank separator line, then the SQL.
func AtlasCheckpointArtifact(version int64, description, upSQL string) (name, contents string) {
	stem := atlasCheckpointNameStem(description)
	if stem == "" {
		stem = "checkpoint"
	}
	name = fmt.Sprintf("%d_%s.sql", version, stem)

	body := strings.Trim(upSQL, "\n")
	if body == "" {
		return name, atlasCheckpointDirective + "\n"
	}
	return name, atlasCheckpointDirective + "\n\n" + body + "\n"
}

// CheckpointFromShadowOptions configures GenerateCheckpointFromShadow.
type CheckpointFromShadowOptions struct {
	// ShadowDatabaseURL is an ephemeral database the generator drops clean and
	// replays the migration directory into. Its contents are discarded.
	ShadowDatabaseURL string
	// MigrationsDir is the directory whose entire history is replayed.
	MigrationsDir string
	// MigrationsFS is the immutable migration history to replay. When nil,
	// MigrationsDir is opened for compatibility with existing embedders.
	MigrationsFS fs.FS
	// Dialect, when set, must match the shadow database dialect. When empty the
	// shadow database's dialect is used.
	Dialect string
	// Schemas restricts introspection to the listed schemas where supported.
	Schemas []string
	// ProviderOptions are passed to the migration provider (e.g. dir format).
	ProviderOptions []migrator.FSProviderOption
	// ConnectTimeout bounds the shadow database connection attempt.
	ConnectTimeout time.Duration
	// MigrationLockTimeout bounds how long the replay waits for the shadow
	// database's migration advisory lock. Zero waits indefinitely, which is the
	// migrator's own default. It only has an effect on dialects that implement
	// advisory locking.
	MigrationLockTimeout time.Duration
	// SchemaQualifier, when non-empty, prefixes every object the checkpoint
	// creates with a schema qualifier, exactly as the same-named option does on
	// a generated migration. It applies to a single-schema checkpoint only, and
	// an unqualifiable statement kind is refused rather than emitted unqualified.
	SchemaQualifier string
}

// GenerateCheckpointFromShadow replays the entire migration directory on a fresh
// shadow database, introspects the resulting cumulative schema, and renders it
// as a checkpoint migration body pair (up creates everything, down drops it).
// The migration directory is the source of truth, so no target database is
// needed. The shadow database is dropped clean before the replay and its
// migration metadata is removed before introspection. For a SQLite shadow,
// malformed PTAH_SQLITE_ALLOW_VIRTUAL_TABLE_DROP configuration is refused
// before the shadow connection or replay.
//
// The checkpoint is rendered through the shadow connection, so the complete
// candidate identifier set is resolved under the shadow catalog's collation
// rather than under conservative offline rules — on SQL Server that is what
// lets case-variant identifiers coexist in one checkpoint.
func GenerateCheckpointFromShadow(ctx context.Context, opts CheckpointFromShadowOptions) (upSQL, downSQL string, err error) {
	dialect := opts.Dialect
	if resolvedDialect, dialectErr := atlasurl.DialectFromURL(opts.ShadowDatabaseURL); dialectErr == nil {
		dialect = resolvedDialect
	}
	if err := sqlitevirtual.ValidateToggle(dialect); err != nil {
		return "", "", fmt.Errorf(
			"checkpoint generation failed: validate SQLite virtual-table drop toggle: %w",
			err,
		)
	}
	connectCtx, cancelConnect := connectContext(ctx, opts.ConnectTimeout)
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
	if opts.Dialect != "" && !shadowdb.SameDialect(opts.Dialect, shadowConn.Info().Dialect) {
		return "", "", fmt.Errorf(
			"checkpoint generation failed: shadow database dialect %q does not match target dialect %q",
			shadowConn.Info().Dialect,
			opts.Dialect,
		)
	}

	if err := shadowConn.SchemaWriter().DropAllTables(ctx); err != nil {
		return "", "", fmt.Errorf("checkpoint generation failed: drop all objects: %w", err)
	}
	if err := shadowdb.ResetSchemas(ctx, shadowConn, opts.Schemas); err != nil {
		return "", "", fmt.Errorf("checkpoint generation failed: %w", err)
	}

	migrations, err := shadowdb.LoadMigrations(opts.MigrationsFS, opts.MigrationsDir, opts.ProviderOptions...)
	if err != nil {
		return "", "", fmt.Errorf("checkpoint generation failed: load migrations: %w", err)
	}
	if len(migrations) == 0 {
		return "", "", fmt.Errorf("checkpoint generation failed: no migrations found in %s", opts.MigrationsDir)
	}

	mig := migrator.NewMigrator(shadowConn, migrator.NewRegisteredMigrationProvider(migrations...)).
		WithMigrationLockTimeout(opts.MigrationLockTimeout)
	if err := mig.MigrateUp(ctx); err != nil {
		if description := shadowdb.DescribeReplayError(err); description != "" {
			return "", "", fmt.Errorf("checkpoint generation failed: %s", description)
		}
		return "", "", fmt.Errorf("checkpoint generation failed: replay migrations: %w", err)
	}
	if err := shadowdb.DropMigrationMetadata(ctx, shadowConn, mig.MigrationsTableIdentifier()); err != nil {
		return "", "", fmt.Errorf("checkpoint generation failed: %w", err)
	}

	shadowSchema, err := dbschema.ReadSchemaWithSchemasContext(ctx, shadowConn, opts.Schemas)
	if err != nil {
		return "", "", fmt.Errorf("checkpoint generation failed: read shadow schema: %w", err)
	}
	return generateCheckpointWithDatabaseQualified(
		ctx,
		shadowConn,
		dbschematogo.ConvertDBSchemaToGoSchema(shadowSchema),
		opts.SchemaQualifier,
	)
}

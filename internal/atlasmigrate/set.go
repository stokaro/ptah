package atlasmigrate

import (
	"context"
	"fmt"
	"io/fs"
	"strings"

	"ptah.run/dbschema"
	"ptah.run/migration/migrationfile"
	"ptah.run/migration/migrator"
)

// SetOptions configures a migrate set operation.
type SetOptions struct {
	Dir             string
	FS              fs.FS
	AtlasEnv        string
	RevisionsSchema string
	// MigrationLockName names the session advisory lock the revision write
	// takes. Empty keeps the migrator's default lock name.
	MigrationLockName string
	// RevisionsTable overrides the revision table name. Empty uses the
	// revision-format default.
	RevisionsTable string
	// DirFormat selects the migration directory layout. Empty keeps the Atlas
	// layout, preserving the Atlas-compatible caller's behavior.
	DirFormat migrationfile.DirFormat
	// RevisionFormat selects the revision table layout. Empty keeps the Atlas
	// layout, preserving the Atlas-compatible caller's behavior.
	RevisionFormat migrator.RevisionTableFormat
	// RevisionVersions maps converted execution-order keys to exact revision
	// identities. The map may include squashed history so existing rows remain
	// readable; only migrations present in FS own pending work.
	RevisionVersions map[int64]string

	// RevisionChecksums carries source atlas.sum h1 hashes for converted
	// layouts; see [ApplyOptions.RevisionChecksums] (stokaro/ptah#1209).
	RevisionChecksums map[int64]string
	// RevisionTypes preserves source-format metadata for converted migrations.
	// A manually set row combines the supplied type with the manually-set bit.
	RevisionTypes map[int64]migrator.AtlasRevisionType
	// RepeatableVersions marks converted execution-order keys whose source
	// migrations are repeatable. The role remains separate from exact identity:
	// an empty exact identity can also belong to an ordinary Flyway V.sql file.
	RepeatableVersions []int64
	// RevisionVersionComparator orders retired exact identities according to
	// the source format. Nil or an ambiguous result makes metadata movement fail
	// closed instead of guessing from opaque identity bytes.
	RevisionVersionComparator migrator.AtlasRevisionVersionComparator
	// VerbatimDescriptions records and reports each revision under the
	// description the community binary gives it (see
	// [migrator.WithAtlasVerbatimDescriptions]). ptah-compat sets it; the
	// native `ptah migrations set` keeps Ptah's description.
	VerbatimDescriptions bool
	// TextOrder orders the versions of the set as text, as the community
	// binary does, so `set 2` keeps 10 and removes v1 (see
	// [migrator.Migrator.WithAtlasStringLinearity]). ptah-compat sets it; the
	// native `ptah migrations set` keeps the numeric order.
	TextOrder bool
	// Version, when not empty, names the target as the directory's file names
	// spell it, such as v1, and replaces the version argument. Set reads it
	// with [migrator.Migrator.ResolveVersion] once the directory is read, so a
	// number no file carries is refused. The native `ptah migrations set`
	// passes its --version flag here; ptah-compat resolves the version itself.
	Version string
}

// Set moves revision metadata to version without executing migration SQL.
func Set(
	ctx context.Context,
	conn *dbschema.DatabaseConnection,
	version int64,
	opts SetOptions,
) (migrator.AtlasRevisionSetResult, error) {
	if conn == nil {
		return migrator.AtlasRevisionSetResult{}, fmt.Errorf("migrate set requires database connection")
	}
	if strings.TrimSpace(opts.Dir) == "" {
		return migrator.AtlasRevisionSetResult{}, fmt.Errorf("migrate set requires migration directory")
	}
	if opts.FS == nil {
		return migrator.AtlasRevisionSetResult{}, fmt.Errorf("migrate set requires migration filesystem")
	}
	dirFormat := opts.DirFormat
	if dirFormat == "" {
		dirFormat = migrationfile.DirFormatAtlas
	}
	revisionFormat := opts.RevisionFormat
	if revisionFormat == "" {
		revisionFormat = migrator.RevisionTableFormatAtlas
	}

	providerOptions := []migrator.FSProviderOption{
		migrator.WithMigrationDirFormat(dirFormat),
		migrator.WithAtlasTemplateData(migrationfile.AtlasTemplateData{Env: opts.AtlasEnv}),
		migrator.WithAtlasRevisionVersions(opts.RevisionVersions),
		migrator.WithAtlasRevisionChecksums(opts.RevisionChecksums),
		migrator.WithAtlasRevisionTypes(opts.RevisionTypes),
		migrator.WithAtlasRepeatableVersions(opts.RepeatableVersions),
	}
	if opts.VerbatimDescriptions {
		providerOptions = append(providerOptions, migrator.WithAtlasVerbatimDescriptions())
	}
	mig, err := migrator.NewFSMigrator(conn, opts.FS, providerOptions...)
	if err != nil {
		return migrator.AtlasRevisionSetResult{}, fmt.Errorf("error registering migrations: %w", err)
	}
	mig = mig.WithMigrationsTable(opts.RevisionsSchema, opts.RevisionsTable).
		WithRevisionTableFormat(revisionFormat).
		WithMigrationLockName(opts.MigrationLockName).
		WithAtlasRevisionVersionComparator(opts.RevisionVersionComparator)
	if opts.TextOrder {
		mig = mig.WithAtlasStringLinearity()
	}
	if opts.Version != "" {
		if version, err = mig.ResolveVersion(opts.Version); err != nil {
			return migrator.AtlasRevisionSetResult{}, fmt.Errorf("invalid migration version %q: %w", opts.Version, err)
		}
	}

	result, err := mig.SetRevision(ctx, version)
	if err != nil {
		return migrator.AtlasRevisionSetResult{}, err
	}
	return result, nil
}

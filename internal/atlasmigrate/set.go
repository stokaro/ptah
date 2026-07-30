package atlasmigrate

import (
	"context"
	"fmt"
	"io/fs"
	"strings"

	"github.com/stokaro/ptah/dbschema"
	"github.com/stokaro/ptah/migration/migrator"
)

// SetOptions configures a migrate set operation.
type SetOptions struct {
	Dir             string
	FS              fs.FS
	AtlasEnv        string
	RevisionsSchema string
	// RevisionsTable overrides the revision table name. Empty uses the
	// revision-format default.
	RevisionsTable string
	// DirFormat selects the migration directory layout. Empty keeps the Atlas
	// layout, preserving the Atlas-compatible caller's behavior.
	DirFormat migrator.MigrationDirFormat
	// RevisionFormat selects the revision table layout. Empty keeps the Atlas
	// layout, preserving the Atlas-compatible caller's behavior.
	RevisionFormat migrator.RevisionTableFormat
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
		dirFormat = migrator.MigrationDirFormatAtlas
	}
	revisionFormat := opts.RevisionFormat
	if revisionFormat == "" {
		revisionFormat = migrator.RevisionTableFormatAtlas
	}

	mig, err := migrator.NewFSMigrator(
		conn,
		opts.FS,
		migrator.WithMigrationDirFormat(dirFormat),
		migrator.WithAtlasTemplateData(migrator.AtlasTemplateData{Env: opts.AtlasEnv}),
	)
	if err != nil {
		return migrator.AtlasRevisionSetResult{}, fmt.Errorf("error registering migrations: %w", err)
	}
	mig = mig.WithMigrationsTable(opts.RevisionsSchema, opts.RevisionsTable).
		WithRevisionTableFormat(revisionFormat)

	result, err := mig.SetRevision(ctx, version)
	if err != nil {
		return migrator.AtlasRevisionSetResult{}, err
	}
	return result, nil
}

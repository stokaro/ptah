package atlasmigrate

import (
	"context"
	"fmt"
	"io/fs"

	"go.5x5.cz/ptah/dbschema"
	"go.5x5.cz/ptah/migration/migrator"
)

type StatusOptions struct {
	Dir             string
	FS              fs.FS
	AtlasEnv        string
	RevisionsSchema string
	// RevisionVersions maps converted numeric order keys to exact revision
	// identities. A full mapping may include baseline-squashed history; only
	// migrations present in FS become pending work.
	RevisionVersions map[int64]string

	// RevisionChecksums carries source atlas.sum h1 hashes for converted
	// layouts; see [ApplyOptions.RevisionChecksums] (stokaro/ptah#1209).
	RevisionChecksums map[int64]string
}

type StatusResult struct {
	Status           *migrator.MigrationStatus
	AppliedRevisions []migrator.MigrationRevision
}

func Status(ctx context.Context, conn *dbschema.DatabaseConnection, opts StatusOptions) (StatusResult, error) {
	if conn == nil {
		return StatusResult{}, fmt.Errorf("migrate status requires database connection")
	}
	if opts.Dir == "" {
		return StatusResult{}, fmt.Errorf("migrate status requires migration directory")
	}
	if opts.FS == nil {
		return StatusResult{}, fmt.Errorf("migrate status requires migration filesystem")
	}
	mig, err := migrator.NewFSMigrator(
		conn,
		opts.FS,
		migrator.WithMigrationDirFormat(migrator.MigrationDirFormatAtlas),
		migrator.WithAtlasTemplateData(migrator.AtlasTemplateData{Env: opts.AtlasEnv}),
		migrator.WithAtlasRevisionVersions(opts.RevisionVersions),
		migrator.WithAtlasRevisionChecksums(opts.RevisionChecksums),
	)
	if err != nil {
		return StatusResult{}, fmt.Errorf("error registering migrations: %w", err)
	}
	mig = mig.WithMigrationsTable(opts.RevisionsSchema, "").
		WithRevisionTableFormat(migrator.RevisionTableFormatAtlas)
	snapshot, err := mig.GetMigrationStatusSnapshot(ctx)
	if err != nil {
		return StatusResult{}, fmt.Errorf("error getting migration status: %w", err)
	}
	return StatusResult{
		Status:           snapshot.Status,
		AppliedRevisions: snapshot.Revisions,
	}, nil
}

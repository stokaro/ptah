package atlasmigrate

import (
	"context"
	"fmt"
	"io/fs"

	"ptah.run/dbschema"
	"ptah.run/migration/migrationfile"
	"ptah.run/migration/migrator"
)

type StatusOptions struct {
	Dir             string
	FS              fs.FS
	AtlasEnv        string
	RevisionsSchema string
	// MigrationsEngine names the storage engine the revision table is created
	// with. Status reads revisions, and reading them initializes the metadata,
	// so this path creates the table too (stokaro/ptah#2234).
	MigrationsEngine string
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
		migrator.WithMigrationDirFormat(migrationfile.DirFormatAtlas),
		migrator.WithAtlasTemplateData(migrationfile.AtlasTemplateData{Env: opts.AtlasEnv}),
		migrator.WithAtlasRevisionVersions(opts.RevisionVersions),
		migrator.WithAtlasRevisionChecksums(opts.RevisionChecksums),
	)
	if err != nil {
		return StatusResult{}, fmt.Errorf("error registering migrations: %w", err)
	}
	mig = mig.WithMigrationsTable(opts.RevisionsSchema, "").
		WithMigrationsEngine(opts.MigrationsEngine).
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

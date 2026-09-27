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
	// with. Status creates no table, but an engine the revision table cannot
	// have is refused here as it is on apply (stokaro/ptah#2234).
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
	// A status is a read: the pinned community binary v1.3.0 creates nothing
	// on `migrate status`, measured on MySQL 8.4.11 and PostgreSQL 18
	// (stokaro/ptah#3881). With the writer in dry-run mode the migrator
	// inspects the revision table instead of creating it, and an absent table
	// reads as no revisions. Without it, the status of an empty database
	// leaves a revision table behind, with the schema or database that holds
	// it.
	writer := conn.SchemaWriter()
	restore := writer.IsDryRun()
	writer.SetDryRun(true)
	defer writer.SetDryRun(restore)
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

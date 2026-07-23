package atlasmigrate

import (
	"context"
	"fmt"
	"os"

	"github.com/stokaro/ptah/dbschema"
	"github.com/stokaro/ptah/migration/migrator"
)

type StatusOptions struct {
	Dir             string
	AtlasEnv        string
	RevisionsSchema string
}

type StatusResult struct {
	Status *migrator.MigrationStatus
}

func Status(ctx context.Context, conn *dbschema.DatabaseConnection, opts StatusOptions) (StatusResult, error) {
	if conn == nil {
		return StatusResult{}, fmt.Errorf("migrate status requires database connection")
	}
	if opts.Dir == "" {
		return StatusResult{}, fmt.Errorf("migrate status requires migration directory")
	}
	mig, err := migrator.NewFSMigrator(
		conn,
		os.DirFS(opts.Dir),
		migrator.WithMigrationDirFormat(migrator.MigrationDirFormatAtlas),
		migrator.WithAtlasTemplateData(migrator.AtlasTemplateData{Env: opts.AtlasEnv}),
	)
	if err != nil {
		return StatusResult{}, fmt.Errorf("error registering migrations: %w", err)
	}
	mig = mig.WithMigrationsTable(opts.RevisionsSchema, "").
		WithRevisionTableFormat(migrator.RevisionTableFormatAtlas)
	status, err := mig.GetMigrationStatus(ctx)
	if err != nil {
		return StatusResult{}, fmt.Errorf("error getting migration status: %w", err)
	}
	return StatusResult{Status: status}, nil
}

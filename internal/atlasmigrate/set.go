package atlasmigrate

import (
	"context"
	"fmt"
	"io/fs"
	"os"
	"strings"

	"github.com/stokaro/ptah/dbschema"
	"github.com/stokaro/ptah/migration/migrator"
)

// SetOptions configures an Atlas migrate set operation.
type SetOptions struct {
	Dir             string
	FS              fs.FS
	AtlasEnv        string
	RevisionsSchema string
}

// Set moves Atlas revision metadata to version without executing migration SQL.
func Set(
	ctx context.Context,
	conn *dbschema.DatabaseConnection,
	version int64,
	opts SetOptions,
) (migrator.AtlasRevisionSetResult, error) {
	if conn == nil {
		return migrator.AtlasRevisionSetResult{}, fmt.Errorf("migrate set requires database connection")
	}
	if strings.TrimSpace(opts.Dir) == "" && opts.FS == nil {
		return migrator.AtlasRevisionSetResult{}, fmt.Errorf("migrate set requires migration directory")
	}

	migrationFS := opts.FS
	if migrationFS == nil {
		migrationFS = os.DirFS(opts.Dir)
	}
	mig, err := migrator.NewFSMigrator(
		conn,
		migrationFS,
		migrator.WithMigrationDirFormat(migrator.MigrationDirFormatAtlas),
		migrator.WithAtlasTemplateData(migrator.AtlasTemplateData{Env: opts.AtlasEnv}),
	)
	if err != nil {
		return migrator.AtlasRevisionSetResult{}, fmt.Errorf("error registering migrations: %w", err)
	}
	mig = mig.WithMigrationsTable(opts.RevisionsSchema, "").
		WithRevisionTableFormat(migrator.RevisionTableFormatAtlas)

	result, err := mig.SetAtlasRevision(ctx, version)
	if err != nil {
		return migrator.AtlasRevisionSetResult{}, err
	}
	return result, nil
}

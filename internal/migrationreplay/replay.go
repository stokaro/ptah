// Package migrationreplay replays migration directories on disposable dev
// databases for Atlas-compatible validation and lint workflows.
package migrationreplay

import (
	"context"
	"fmt"
	"io/fs"
	"net/url"
	"os"
	"strings"

	"github.com/stokaro/ptah/dbschema"
	"github.com/stokaro/ptah/internal/migrationsnapshot"
	"github.com/stokaro/ptah/migration/migrator"
)

// Options configures a migration replay run.
type Options struct {
	Dir       string
	DirFormat migrator.MigrationDirFormat
	DevURL    string
	// FS supplies an immutable migration snapshot. When nil, Replay opens Dir.
	FS                fs.FS
	AtlasTemplateData any
}

// Replay connects to the configured dev database and replays the migration
// directory against it.
func Replay(ctx context.Context, opts Options) error {
	devURL := strings.TrimSpace(opts.DevURL)
	if devURL == "" {
		return nil
	}
	if isDockerURL(devURL) {
		return fmt.Errorf("docker --dev-url values are accepted by Atlas, but Ptah requires a directly connectable dev database URL for migration SQL replay")
	}

	sourceFS := opts.FS
	if sourceFS == nil {
		sourceFS = os.DirFS(opts.Dir)
	}
	snapshot, err := migrationsnapshot.Capture(sourceFS)
	if err != nil {
		return fmt.Errorf("capture migration directory: %w", err)
	}
	conn, err := dbschema.ConnectToDatabase(ctx, devURL)
	if err != nil {
		return fmt.Errorf("error connecting to dev database: %w", err)
	}
	defer dbschema.CloseAndWarn(conn)

	return replayOnConnection(ctx, conn, snapshot, opts.DirFormat, opts.AtlasTemplateData)
}

// ReplayOnConnection replays the migration directory on an already-open dev
// database connection.
func ReplayOnConnection(
	ctx context.Context,
	conn *dbschema.DatabaseConnection,
	dir string,
	dirFormat migrator.MigrationDirFormat,
) error {
	snapshot, err := migrationsnapshot.Capture(os.DirFS(dir))
	if err != nil {
		return fmt.Errorf("capture migration directory: %w", err)
	}
	return replayOnConnection(ctx, conn, snapshot, dirFormat, nil)
}

func replayOnConnection(
	ctx context.Context,
	conn *dbschema.DatabaseConnection,
	fsys fs.FS,
	dirFormat migrator.MigrationDirFormat,
	atlasTemplateData any,
) error {
	provider, err := migrator.NewFSMigrationProvider(
		fsys,
		migrator.WithMigrationDirFormat(dirFormat),
		migrator.WithAtlasTemplateData(atlasTemplateData),
	)
	if err != nil {
		return fmt.Errorf("load migration directory: %w", err)
	}
	migrations := provider.Migrations()
	if err := conn.SchemaWriter().DropAllTables(ctx); err != nil {
		return fmt.Errorf("clean dev database: %w", err)
	}
	for _, migration := range migrations {
		if err := migration.Up(ctx, conn); err != nil {
			return fmt.Errorf("replay migration %d on dev database: %w", migration.Version, err)
		}
	}
	return nil
}

func isDockerURL(rawURL string) bool {
	parsed, err := url.Parse(rawURL)
	return err == nil && parsed.Scheme == "docker"
}

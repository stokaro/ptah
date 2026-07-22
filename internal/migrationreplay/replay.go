// Package migrationreplay replays migration directories on disposable dev
// databases for Atlas-compatible validation and lint workflows.
package migrationreplay

import (
	"context"
	"fmt"
	"net/url"
	"os"
	"strings"

	"github.com/stokaro/ptah/dbschema"
	"github.com/stokaro/ptah/migration/migrator"
)

// Options configures a migration replay run.
type Options struct {
	Dir       string
	DirFormat migrator.MigrationDirFormat
	DevURL    string
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

	conn, err := dbschema.ConnectToDatabase(ctx, devURL)
	if err != nil {
		return fmt.Errorf("error connecting to dev database: %w", err)
	}
	defer dbschema.CloseAndWarn(conn)

	return ReplayOnConnection(ctx, conn, opts.Dir, opts.DirFormat)
}

// ReplayOnConnection replays the migration directory on an already-open dev
// database connection.
func ReplayOnConnection(
	ctx context.Context,
	conn *dbschema.DatabaseConnection,
	dir string,
	dirFormat migrator.MigrationDirFormat,
) error {
	if err := conn.SchemaWriter().DropAllTables(); err != nil {
		return fmt.Errorf("clean dev database: %w", err)
	}
	provider, err := migrator.NewFSMigrationProvider(
		os.DirFS(dir),
		migrator.WithMigrationDirFormat(dirFormat),
	)
	if err != nil {
		return fmt.Errorf("load migration directory: %w", err)
	}
	for _, migration := range provider.Migrations() {
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

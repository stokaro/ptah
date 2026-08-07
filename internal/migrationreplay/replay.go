// Package migrationreplay replays migration directories on disposable dev
// databases for Atlas-compatible validation and lint workflows.
package migrationreplay

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"net/url"
	"os"
	"strings"
	"time"

	"go.5x5.cz/ptah/core/platform"
	"go.5x5.cz/ptah/dbschema"
	"go.5x5.cz/ptah/internal/devclean"
	"go.5x5.cz/ptah/internal/devlock"
	"go.5x5.cz/ptah/internal/migrationsnapshot"
	"go.5x5.cz/ptah/migration/migrator"
)

const failedReplayCleanupTimeout = 30 * time.Second

// Options configures a migration replay run.
type Options struct {
	Dir       string
	DirFormat migrator.MigrationDirFormat
	DevURL    string
	// FS supplies an immutable migration snapshot. When nil, Replay opens Dir.
	FS                fs.FS
	AtlasTemplateData any
	// ObserveVersion, when set, runs before each migration is replayed, with the
	// connection bound to the schema state that migration starts from. It is how
	// a caller reads the before-state of one version without replaying the
	// directory once per version. An error from it aborts the replay.
	ObserveVersion func(ctx context.Context, version int64, conn *dbschema.DatabaseConnection) error
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

	return replayOnConnection(
		ctx,
		conn,
		snapshot,
		opts.DirFormat,
		opts.AtlasTemplateData,
		replayHooks{observeVersion: opts.ObserveVersion},
	)
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
	return ReplaySnapshotOnConnection(ctx, conn, snapshot, dirFormat)
}

// WithReplayedDirectory replays one migration directory, invokes consume while
// the replayed database is bound to the same physical session, and cleans the
// database realm before returning.
func WithReplayedDirectory(
	ctx context.Context,
	conn *dbschema.DatabaseConnection,
	dir string,
	dirFormat migrator.MigrationDirFormat,
	consume func(*dbschema.DatabaseConnection) error,
) error {
	snapshot, err := migrationsnapshot.Capture(os.DirFS(dir))
	if err != nil {
		return fmt.Errorf("capture migration directory: %w", err)
	}
	return WithReplayedSnapshot(ctx, conn, snapshot, dirFormat, consume)
}

// ReplaySnapshotOnConnection replays one immutable migration filesystem on an
// already-open dev database connection.
func ReplaySnapshotOnConnection(
	ctx context.Context,
	conn *dbschema.DatabaseConnection,
	snapshot fs.FS,
	dirFormat migrator.MigrationDirFormat,
) error {
	snapshot, err := migrationsnapshot.Capture(snapshot)
	if err != nil {
		return fmt.Errorf("capture migration snapshot: %w", err)
	}
	return replayOnConnection(ctx, conn, snapshot, dirFormat, nil, replayHooks{})
}

// WithReplayedSnapshot replays one immutable migration filesystem, invokes
// consume while the replayed database is bound to the same physical session,
// and cleans the database realm before returning.
func WithReplayedSnapshot(
	ctx context.Context,
	conn *dbschema.DatabaseConnection,
	snapshot fs.FS,
	dirFormat migrator.MigrationDirFormat,
	consume func(*dbschema.DatabaseConnection) error,
) error {
	if consume == nil {
		return fmt.Errorf("consume replayed database callback is nil")
	}
	snapshot, err := migrationsnapshot.Capture(snapshot)
	if err != nil {
		return fmt.Errorf("capture migration snapshot: %w", err)
	}
	return replayOnConnection(ctx, conn, snapshot, dirFormat, nil, replayHooks{consume: consume})
}

// WithReplayedSnapshotLocked performs the same replay as
// [WithReplayedSnapshot] while relying on the caller to hold the dev database
// realm lock across a larger operation.
func WithReplayedSnapshotLocked(
	ctx context.Context,
	conn *dbschema.DatabaseConnection,
	snapshot fs.FS,
	dirFormat migrator.MigrationDirFormat,
	consume func(*dbschema.DatabaseConnection) error,
) error {
	if consume == nil {
		return fmt.Errorf("consume replayed database callback is nil")
	}
	snapshot, err := migrationsnapshot.Capture(snapshot)
	if err != nil {
		return fmt.Errorf("capture migration snapshot: %w", err)
	}
	return replayOnLockedConnection(ctx, conn, snapshot, dirFormat, nil, replayHooks{consume: consume})
}

// replayHooks are the optional callbacks a replay runs alongside the
// migrations. The zero value replays and observes nothing.
type replayHooks struct {
	// observeVersion runs before each migration, with the connection bound to
	// the state that migration starts from.
	observeVersion func(context.Context, int64, *dbschema.DatabaseConnection) error
	// consume runs once after every migration, before the realm is cleaned.
	consume func(*dbschema.DatabaseConnection) error
}

func replayOnConnection(
	ctx context.Context,
	conn *dbschema.DatabaseConnection,
	fsys fs.FS,
	dirFormat migrator.MigrationDirFormat,
	atlasTemplateData any,
	hooks replayHooks,
) (resultErr error) {
	if conn == nil {
		return fmt.Errorf("replay migrations: nil database connection")
	}
	lock, err := devlock.Acquire(ctx, conn, 0)
	if err != nil {
		return err
	}
	defer func() {
		resultErr = errors.Join(resultErr, lock.Release())
	}()
	return replayOnLockedConnection(ctx, conn, fsys, dirFormat, atlasTemplateData, hooks)
}

func replayOnLockedConnection(
	ctx context.Context,
	conn *dbschema.DatabaseConnection,
	fsys fs.FS,
	dirFormat migrator.MigrationDirFormat,
	atlasTemplateData any,
	hooks replayHooks,
) error {
	if conn == nil {
		return fmt.Errorf("replay migrations: nil database connection")
	}
	provider, err := migrator.NewFSMigrationProvider(
		fsys,
		migrator.WithMigrationDirFormat(dirFormat),
		migrator.WithAtlasTemplateData(atlasTemplateData),
		migrator.WithStatementValidator(devclean.NewReplayGuard(conn.Info())),
	)
	if err != nil {
		return fmt.Errorf("load migration directory: %w", err)
	}
	migrations := provider.Migrations()
	return conn.WithSession(ctx, func(replayConn *dbschema.DatabaseConnection) (resultErr error) {
		restoreSession, err := captureReplaySessionState(ctx, replayConn)
		if err != nil {
			return err
		}
		defer func() {
			restoreCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), failedReplayCleanupTimeout)
			defer cancel()
			resultErr = errors.Join(resultErr, restoreSession(restoreCtx))
		}()
		return replayMigrations(
			ctx,
			replayConn,
			migrations,
			hooks,
		)
	})
}

func replayMigrations(
	ctx context.Context,
	conn *dbschema.DatabaseConnection,
	migrations []*migrator.Migration,
	hooks replayHooks,
) (resultErr error) {
	if err := ctx.Err(); err != nil {
		return err
	}
	replaySucceeded := false
	defer func() {
		cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), failedReplayCleanupTimeout)
		defer cancel()
		if cleanupErr := devclean.DatabaseRealm(cleanupCtx, conn); cleanupErr != nil {
			label := "clean dev database after replay"
			if !replaySucceeded {
				label = "clean dev database after failed replay"
			}
			resultErr = errors.Join(resultErr, fmt.Errorf("%s: %w", label, cleanupErr))
		}
	}()
	if err := devclean.DatabaseRealm(ctx, conn); err != nil {
		return fmt.Errorf("clean dev database: %w", err)
	}
	for _, migration := range migrations {
		if hooks.observeVersion != nil {
			if err := hooks.observeVersion(ctx, migration.Version, conn); err != nil {
				return fmt.Errorf("observe dev database before migration %d: %w", migration.Version, err)
			}
		}
		if err := migration.UpForReplay(ctx, conn); err != nil {
			return fmt.Errorf("replay migration %d on dev database: %w", migration.Version, err)
		}
	}
	if hooks.consume != nil {
		if err := hooks.consume(conn); err != nil {
			return err
		}
	}
	replaySucceeded = true
	return nil
}

func captureReplaySessionState(
	ctx context.Context,
	conn *dbschema.DatabaseConnection,
) (func(context.Context) error, error) {
	if platform.NormalizeDialect(conn.Info().Dialect) != platform.SQLite {
		return func(context.Context) error { return nil }, nil
	}

	var foreignKeysEnabled bool
	if err := conn.QueryRowContext(ctx, "PRAGMA foreign_keys").Scan(&foreignKeysEnabled); err != nil {
		return nil, fmt.Errorf("capture SQLite foreign key state before migration replay: %w", err)
	}
	return func(restoreCtx context.Context) error {
		statement := "PRAGMA foreign_keys = OFF"
		if foreignKeysEnabled {
			statement = "PRAGMA foreign_keys = ON"
		}
		if _, err := conn.ExecContext(restoreCtx, statement); err != nil {
			return fmt.Errorf("restore SQLite foreign key state after migration replay: %w", err)
		}
		return nil
	}, nil
}

func isDockerURL(rawURL string) bool {
	parsed, err := url.Parse(rawURL)
	return err == nil && parsed.Scheme == "docker"
}

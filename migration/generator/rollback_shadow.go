package generator

import (
	"context"
	"fmt"
	"io/fs"
	"time"

	"go.5x5.cz/ptah/dbschema"
	"go.5x5.cz/ptah/internal/atlasurl"
	"go.5x5.cz/ptah/internal/devlock"
	"go.5x5.cz/ptah/migration/migrator"
)

// RollbackFromShadowOptions configures VerifyRollbackFromShadow.
type RollbackFromShadowOptions struct {
	// TargetConnection is the already-open database the verified rollback
	// would eventually modify. Its live realm must be distinct from the shadow
	// database.
	TargetConnection *dbschema.DatabaseConnection
	// ShadowDatabaseURL is an ephemeral database the verification drops clean
	// and replays the migration directory into. Its contents are discarded.
	ShadowDatabaseURL string
	// FS is the migration filesystem whose rollback plan is verified.
	FS fs.FS
	// CurrentVersion is the target database's current migration version. The
	// shadow database is migrated up to it before the rollback is replayed.
	CurrentVersion int64
	// TargetVersion is the version the rollback plan lands on.
	TargetVersion int64
	// ProviderOptions are passed to the migration provider (e.g. dir format).
	ProviderOptions []migrator.FSProviderOption
	// ConnectTimeout bounds the shadow database connection attempt.
	ConnectTimeout time.Duration
}

// VerifyRollbackFromShadow replays a rollback plan on a disposable shadow
// database before the target database is touched: the shadow database is
// dropped clean, migrated up to the target's current version, and then
// migrated down to the requested target version. Any failure aborts with the
// target untouched.
//
// The replay assumes a linear history: every migration at or below
// CurrentVersion is applied during the up phase, so a target database with a
// non-linear applied set is approximated by its full linear prefix.
func VerifyRollbackFromShadow(ctx context.Context, opts RollbackFromShadowOptions) error {
	if opts.ShadowDatabaseURL == "" {
		return fmt.Errorf("rollback verification failed: a shadow database URL is required")
	}
	if opts.TargetConnection == nil {
		return fmt.Errorf("rollback verification failed: a target database connection is required")
	}
	if opts.FS == nil {
		return fmt.Errorf("rollback verification failed: a migration filesystem is required")
	}
	sameDatabase, err := atlasurl.SameDatabase(
		opts.TargetConnection.Info().URL,
		opts.ShadowDatabaseURL,
	)
	if err != nil {
		return fmt.Errorf("rollback verification failed: compare target and shadow databases: %w", err)
	}
	if sameDatabase {
		return fmt.Errorf("rollback verification failed: shadow database must be distinct from target database")
	}

	connectCtx, cancelConnect := baselineShadowConnectContext(ctx, opts.ConnectTimeout)
	shadowConn, err := dbschema.ConnectToDatabase(connectCtx, opts.ShadowDatabaseURL)
	cancelConnect()
	if err != nil {
		return fmt.Errorf("rollback verification failed: connect to shadow database: %w", err)
	}
	defer dbschema.CloseAndWarn(shadowConn)

	if !sameDialect(opts.TargetConnection.Info().Dialect, shadowConn.Info().Dialect) {
		return fmt.Errorf(
			"rollback verification failed: shadow database dialect %q does not match target dialect %q",
			shadowConn.Info().Dialect,
			opts.TargetConnection.Info().Dialect,
		)
	}
	sameDatabase, err = devlock.SameRealm(ctx, opts.TargetConnection, shadowConn)
	if err != nil {
		return fmt.Errorf("rollback verification failed: compare live target and shadow database realms: %w", err)
	}
	if sameDatabase {
		return fmt.Errorf("rollback verification failed: shadow database must be distinct from target database")
	}

	if err := shadowConn.SchemaWriter().DropAllTables(ctx); err != nil {
		return fmt.Errorf("rollback verification failed: drop all objects: %w", err)
	}

	mig, err := migrator.NewFSMigrator(shadowConn, opts.FS, opts.ProviderOptions...)
	if err != nil {
		return fmt.Errorf("rollback verification failed: register migrations: %w", err)
	}
	if err := mig.MigrateTo(ctx, opts.CurrentVersion); err != nil {
		if description := describeReplayError(err); description != "" {
			return fmt.Errorf("rollback verification failed: replay migrations: %s", description)
		}
		return fmt.Errorf("rollback verification failed: replay migrations: %w", err)
	}
	if err := mig.MigrateDownTo(ctx, opts.TargetVersion); err != nil {
		return fmt.Errorf("rollback verification failed: roll back to version %d on shadow database: %w", opts.TargetVersion, err)
	}
	return nil
}

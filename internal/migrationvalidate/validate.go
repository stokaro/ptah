// Package migrationvalidate implements migration-directory validation runtime.
package migrationvalidate

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"

	"github.com/stokaro/ptah/dbschema"
	"github.com/stokaro/ptah/internal/migratesum"
	"github.com/stokaro/ptah/migration/migrator"
)

// Options configures a migration validation run.
type Options struct {
	Dir       string
	DirFormat migrator.MigrationDirFormat
	DevURL    string
}

// Result is the validated integrity result plus optional dev-database replay
// metadata.
type Result struct {
	Integrity       *migratesum.Result
	DevSQLValidated bool
}

// Validate verifies the migration directory integrity first. When DevURL is
// set and the integrity check passes, it also replays the migration directory
// against that dev database to validate SQL execution semantics.
func Validate(ctx context.Context, opts Options) (Result, error) {
	integrity, err := migratesum.VerifyDirWithFormat(opts.Dir, opts.DirFormat)
	if err != nil {
		return Result{}, err
	}

	result := Result{
		Integrity: integrity,
	}
	if !integrity.OK() || opts.DevURL == "" {
		return result, nil
	}

	conn, err := dbschema.ConnectToDatabase(ctx, opts.DevURL)
	if err != nil {
		return result, fmt.Errorf("error connecting to dev database: %w", err)
	}
	defer dbschema.CloseAndWarn(conn)

	if err := replayMigrations(ctx, conn, opts.Dir, migrationFormatForSum(integrity), revisionFormatForSum(integrity)); err != nil {
		return result, fmt.Errorf("error validating migration SQL on dev database: %w", err)
	}
	result.DevSQLValidated = true
	return result, nil
}

func replayMigrations(
	ctx context.Context,
	conn *dbschema.DatabaseConnection,
	dir string,
	dirFormat migrator.MigrationDirFormat,
	revisionFormat migrator.RevisionTableFormat,
) error {
	mig, err := migrator.NewFSMigrator(
		conn,
		os.DirFS(dir),
		migrator.WithMigrationDirFormat(dirFormat),
	)
	if err != nil {
		return fmt.Errorf("error registering migrations: %w", err)
	}
	mig = mig.WithRevisionTableFormat(revisionFormat).
		WithLogger(slog.New(slog.NewTextHandler(io.Discard, nil)))
	return mig.MigrateUp(ctx)
}

func migrationFormatForSum(result *migratesum.Result) migrator.MigrationDirFormat {
	if result != nil && result.SumFileName == migratesum.AtlasFileName {
		return migrator.MigrationDirFormatAtlas
	}
	return migrator.MigrationDirFormatPtah
}

func revisionFormatForSum(result *migratesum.Result) migrator.RevisionTableFormat {
	if result != nil && result.SumFileName == migratesum.AtlasFileName {
		return migrator.RevisionTableFormatAtlas
	}
	return migrator.RevisionTableFormatPtah
}

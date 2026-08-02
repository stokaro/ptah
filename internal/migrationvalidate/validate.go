// Package migrationvalidate implements migration-directory validation runtime.
package migrationvalidate

import (
	"context"
	"fmt"

	"go.5x5.cz/ptah/internal/migratesum"
	"go.5x5.cz/ptah/internal/migrationreplay"
	"go.5x5.cz/ptah/migration/migrator"
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

	if err := migrationreplay.Replay(ctx, migrationreplay.Options{
		Dir:       opts.Dir,
		DirFormat: migrationFormatForSum(integrity),
		DevURL:    opts.DevURL,
	}); err != nil {
		return result, fmt.Errorf("error validating migration SQL on dev database: %w", err)
	}
	result.DevSQLValidated = true
	return result, nil
}

func migrationFormatForSum(result *migratesum.Result) migrator.MigrationDirFormat {
	if result != nil && result.SumFileName == migratesum.AtlasFileName {
		return migrator.MigrationDirFormatAtlas
	}
	return migrator.MigrationDirFormatPtah
}

// Package migrationvalidate implements migration-directory validation runtime.
package migrationvalidate

import (
	"context"
	"fmt"
	"io/fs"

	"go.5x5.cz/ptah/internal/migratesum"
	"go.5x5.cz/ptah/internal/migrationreplay"
	"go.5x5.cz/ptah/migration/migrator"
)

// Options configures a migration validation run.
type Options struct {
	// Dir names the directory being validated. When FS is nil it is also the
	// path the directory is read from; when FS is set it is display only, so a
	// caller that resolved an `oci://` reference can keep the reference in the
	// diagnostics rather than a temporary path nobody typed.
	Dir string
	// FS supplies an already-resolved immutable migration snapshot. When nil,
	// Validate reads Dir from the local filesystem.
	//
	// It exists because the integrity question is about a set of bytes rather
	// than about a path: `migrations validate --dir oci://…` resolves the
	// reference through the same puller the executing verbs use and then has
	// nothing to stat (stokaro/ptah#1499). Routing the resolved filesystem
	// through here rather than materializing it to a temporary directory keeps
	// the verified bytes the pulled bytes, with no second read in between.
	FS        fs.FS
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
	integrity, err := verify(opts)
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
		FS:        opts.FS,
		DirFormat: migrationFormatForSum(integrity),
		DevURL:    opts.DevURL,
	}); err != nil {
		return result, fmt.Errorf("error validating migration SQL on dev database: %w", err)
	}
	result.DevSQLValidated = true
	return result, nil
}

// verify reads the integrity file from the supplied snapshot, falling back to
// Dir on the local filesystem when no snapshot was supplied.
func verify(opts Options) (*migratesum.Result, error) {
	if opts.FS == nil {
		return migratesum.VerifyDirWithFormat(opts.Dir, opts.DirFormat)
	}
	return migratesum.VerifyWithFormat(opts.FS, opts.DirFormat)
}

func migrationFormatForSum(result *migratesum.Result) migrator.MigrationDirFormat {
	if result != nil && result.SumFileName == migratesum.AtlasFileName {
		return migrator.MigrationDirFormatAtlas
	}
	return migrator.MigrationDirFormatPtah
}

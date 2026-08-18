// Package migrateread holds the flag surface and source resolution shared by
// the migration-directory commands that only READ the directory and print what
// they found: `ptah migrations ls` and `ptah migrations show` (#1618).
//
// Neither connects to a database and neither executes any of the SQL it reads,
// which is what places them outside the always-on integrity class that covers
// every verb executing migration SQL. They take the opt-in `--verify-sum`
// instead, for the same reason `ptah migrations status` does: these are the
// verbs an operator reaches for while diagnosing a directory that has already
// drifted, and a default gate would refuse to describe the very thing being
// investigated.
package migrateread

import (
	"context"
	"fmt"
	"io"
	"io/fs"

	"github.com/spf13/pflag"

	"go.5x5.cz/ptah/cmd/internal/dbcli"
	"go.5x5.cz/ptah/cmd/internal/migrationsource"
	"go.5x5.cz/ptah/internal/migrationintegrity"
	"go.5x5.cz/ptah/migration/migrator"
)

// Options are the flags common to every read-only migration-directory command.
type Options struct {
	MigrationsDir string
	DirFormat     string
	VerifySum     bool
	PlainHTTP     bool
}

// RegisterFlags installs the shared read flags on a command's flag set.
//
// verifySumLead is the verb's own half of the `--verify-sum` help: what the
// flag ADDS on this verb. The shared qualifier about what a sum can prove at
// all is appended by [migrationsource.VerifySumUsage], which is what keeps the
// promise identical on every verb that offers the flag.
func RegisterFlags(flags *pflag.FlagSet, opts *Options, verifySumLead string) {
	flags.StringVar(
		&opts.MigrationsDir,
		"migrations-dir",
		"./migrations",
		"Local directory or oci:// reference containing migration files",
	)
	flags.StringVar(
		&opts.DirFormat,
		"dir-format",
		string(migrator.MigrationDirFormatAuto),
		"Migration directory format: auto, ptah, or atlas",
	)
	flags.BoolVar(&opts.VerifySum, "verify-sum", false, migrationsource.VerifySumUsage(verifySumLead))
	dbcli.RegisterPlainHTTPFlag(flags, &opts.PlainHTTP)
}

// Directory is one resolved migration directory and the migration files it
// holds, in version order.
type Directory struct {
	// FS is the immutable snapshot the files were discovered in, and the one a
	// caller reading a migration body must read it from: for a registry
	// artifact there is no local path to open instead.
	FS fs.FS
	// Display is the stable name of what was read: the lexical local path, or
	// the registry reference an oci:// source was pulled from.
	Display string
	// Format is the layout the files were parsed under, resolved from the
	// requested format and — for a registry artifact — from the format the
	// artifact records.
	Format migrator.MigrationDirFormat
	// Files are every migration file the directory holds, ordered by version
	// and then by path, which is the order [migrator.DiscoverMigrationFiles]
	// establishes.
	Files []migrator.MigrationFile
}

// Resolve resolves the named directory, discovers the migration files it holds
// and, under `--verify-sum`, verifies the directory against the integrity file
// it carries before any of it is printed.
//
// notice receives the provenance qualifier a verification over a movable
// registry tag cannot carry on its own; pass the command's standard error so a
// caller parsing standard output is unaffected.
func (o *Options) Resolve(ctx context.Context, notice io.Writer) (Directory, error) {
	requested, err := migrator.ParseMigrationDirFormat(o.DirFormat)
	if err != nil {
		return Directory{}, err
	}
	source, err := migrationsource.Resolve(ctx, o.MigrationsDir, migrationsource.Options{
		DirFormat: requested,
		PlainHTTP: o.PlainHTTP,
	})
	if err != nil {
		return Directory{}, err
	}
	files, err := migrator.DiscoverMigrationFiles(source.FileSystem, source.DirFormat)
	if err != nil {
		return Directory{}, err
	}
	// The gate runs when it was asked for AND the directory holds something to
	// assert about.
	//
	// A directory holding no migration files at all carries no integrity file
	// either, and demanding one there would refuse the first thing anyone does
	// with a migration directory: list it before anything has been written into
	// it. There is no claim to make about an empty directory and none is made —
	// the gate asserts that the files about to be printed are the ones a
	// recorded sum covers, and zero files are covered by every sum equally.
	if o.VerifySum && len(files) > 0 {
		if err := verify(notice, source, source.DirFormat); err != nil {
			return Directory{}, err
		}
	}
	return Directory{
		FS:      source.FileSystem,
		Display: source.Display,
		Format:  source.DirFormat,
		Files:   files,
	}, nil
}

// verify checks the directory against the integrity file it carries and states
// what that check is worth when the bytes came from a movable registry tag.
//
// The escape-hatch policy is resolved here rather than at the command's own
// boundary for the same reason the gate is conditional: a run that was never
// going to check anything has no configuration to refuse. These verbs run a
// single gate, so there is no second one that could observe a different value.
func verify(
	notice io.Writer,
	source migrationsource.Source,
	format migrator.MigrationDirFormat,
) error {
	policy, err := migrationintegrity.Resolve()
	if err != nil {
		return err
	}
	verified, err := migrationintegrity.GateWithPolicy(
		notice,
		source.FileSystem,
		format,
		policy,
		migrationintegrity.Options{RequireSum: true},
	)
	if err != nil {
		return err
	}
	warning := migrationsource.MutableTagSumWarning(source, verified)
	if warning == "" {
		return nil
	}
	if _, err := fmt.Fprintf(notice, "Warning: %s\n", warning); err != nil {
		return fmt.Errorf("write provenance qualifier: %w", err)
	}
	return nil
}

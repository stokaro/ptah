// Package migratecheckpoint implements `ptah migrations checkpoint`, which
// squashes a migration directory's history into a cumulative-schema checkpoint.
package migratecheckpoint

import (
	"fmt"
	"io"
	"os"
	"strconv"

	"github.com/spf13/cobra"

	"github.com/stokaro/ptah/cmd/internal/cmdutil"
	"github.com/stokaro/ptah/cmd/internal/dbcli"
	"github.com/stokaro/ptah/migration/generator"
	"github.com/stokaro/ptah/migration/migrator"
)

const (
	migrationsFlag  = "migrations-dir"
	shadowDBFlag    = "shadow-db"
	dialectFlag     = "dialect"
	descriptionFlag = "description"
	versionFlag     = "version"
	dryRunFlag      = "dry-run"
	dirFormatFlag   = "dir-format"
)

type options struct {
	migrationsDir  string
	shadowDB       string
	dialect        string
	description    string
	version        string
	dryRun         bool
	dirFormat      string
	schemas        string
	connectTimeout string
}

// NewMigrateCheckpointCommand returns the `checkpoint` command.
func NewMigrateCheckpointCommand() *cobra.Command {
	opts := options{}
	cmd := &cobra.Command{
		Use:   "checkpoint",
		Short: "Squash migration history into a cumulative-schema checkpoint",
		Long: `Write a checkpoint migration that captures the full cumulative schema at the
current version.

The entire migration directory is replayed on a fresh shadow database, the
resulting schema is introspected, and a checkpoint migration is written. A fresh
database then bootstraps from the newest checkpoint instead of replaying all
history, while an already-migrated database ignores it.

--dir-format selects the checkpoint convention:

  ptah   a reversible pair, NNNNNNNNNN_description.checkpoint.up.sql and
         .checkpoint.down.sql, with ptah.sum refreshed (default)
  atlas  a single up-only file, <version>_description.sql whose first line is
         the "-- atlas:checkpoint" directive, with atlas.sum refreshed`,
		RunE: func(cmd *cobra.Command, args []string) error {
			return migrateCheckpointCommand(cmd, args, &opts)
		},
	}
	registerFlags(cmd, &opts)
	cmdutil.ConfigureCommand(cmd)
	return cmd
}

func registerFlags(cmd *cobra.Command, opts *options) {
	flags := cmd.Flags()
	flags.StringVar(&opts.migrationsDir, migrationsFlag, "./migrations", "Directory containing migration files")
	flags.StringVar(&opts.shadowDB, shadowDBFlag, "", "Ephemeral shadow database URL the directory is replayed into (required)")
	flags.StringVar(&opts.dialect, dialectFlag, "", "Database dialect; inferred from the shadow database when omitted")
	flags.StringVar(&opts.description, descriptionFlag, "checkpoint", "Checkpoint description used in the file name")
	flags.StringVar(&opts.version, versionFlag, "", "Checkpoint version; defaults to one above the newest migration")
	flags.BoolVar(&opts.dryRun, dryRunFlag, false, "Print the checkpoint SQL instead of writing files")
	flags.StringVar(&opts.dirFormat, dirFormatFlag, string(migrator.MigrationDirFormatPtah), "Migration directory format")
	dbcli.RegisterSchemasFlag(flags, &opts.schemas)
	dbcli.RegisterConnectTimeoutFlag(flags, &opts.connectTimeout)
}

func migrateCheckpointCommand(cmd *cobra.Command, _ []string, opts *options) error {
	ctx := cmd.Context()
	out := cmd.OutOrStdout()

	if opts.shadowDB == "" {
		return cmdutil.Fail(cmd, fmt.Errorf("a shadow database URL is required (--%s)", shadowDBFlag))
	}
	if opts.migrationsDir == "" {
		return cmdutil.Fail(cmd, fmt.Errorf("migrations directory is required"))
	}

	dirFormat, err := migrator.ParseMigrationDirFormat(opts.dirFormat)
	if err != nil {
		return cmdutil.Fail(cmd, err)
	}
	// Checkpoints are written in the ptah two-file convention
	// (NNNNNNNNNN_name.checkpoint.up.sql / .down.sql plus ptah.sum) or in the
	// Atlas convention (<version>_name.sql carrying the `-- atlas:checkpoint`
	// directive, plus atlas.sum). "auto" selects neither: it is a READ-side
	// probe, and writing under it would have to guess which convention and
	// which integrity file the directory wants, leaving a mixed-format
	// directory with a stale sum. Refuse it up front rather than reporting
	// success and corrupting the directory.
	if dirFormat == migrator.MigrationDirFormatAuto {
		return cmdutil.Fail(cmd, fmt.Errorf(
			"checkpoint cannot write under --%s=%s: name the target convention with %s or %s",
			dirFormatFlag, migrator.MigrationDirFormatAuto,
			migrator.MigrationDirFormatPtah, migrator.MigrationDirFormatAtlas,
		))
	}
	connectTimeout, err := dbcli.ParseConnectTimeout(opts.connectTimeout)
	if err != nil {
		return cmdutil.Fail(cmd, err)
	}
	version, err := resolveCheckpointVersion(opts.version, opts.migrationsDir, dirFormat)
	if err != nil {
		return cmdutil.Fail(cmd, err)
	}

	upSQL, downSQL, err := generator.GenerateCheckpointFromShadow(ctx, generator.CheckpointFromShadowOptions{
		ShadowDatabaseURL: opts.shadowDB,
		MigrationsDir:     opts.migrationsDir,
		Dialect:           opts.dialect,
		Schemas:           dbcli.ParseSchemas(opts.schemas),
		ProviderOptions:   []migrator.FSProviderOption{migrator.WithMigrationDirFormat(dirFormat)},
		ConnectTimeout:    connectTimeout,
	})
	if err != nil {
		return err
	}

	if dirFormat == migrator.MigrationDirFormatAtlas {
		return writeAtlasCheckpoint(cmd, out, opts, version, upSQL)
	}

	if opts.dryRun {
		upName := migrator.GenerateCheckpointMigrationFileName(version, opts.description, "up")
		downName := migrator.GenerateCheckpointMigrationFileName(version, opts.description, "down")
		fmt.Fprintf(out, "-- checkpoint version %d (dry run, no files written)\n\n-- %s\n%s\n\n-- %s\n%s\n",
			version, upName, upSQL, downName, downSQL)
		return nil
	}

	upPath, downPath, err := generator.WriteCheckpointFiles(opts.migrationsDir, version, opts.description, upSQL, downSQL)
	if err != nil {
		return cmdutil.Fail(cmd, err)
	}
	fmt.Fprintf(out, "Wrote checkpoint version %d:\n  %s\n  %s\n", version, upPath, downPath)
	return nil
}

// writeAtlasCheckpoint emits the Atlas single-file checkpoint convention. The
// down body the generator produced is deliberately discarded: the Atlas format
// is up-only and measured Atlas checkpoints have no down file, so writing one
// would put a file in the directory that Atlas cannot read.
func writeAtlasCheckpoint(cmd *cobra.Command, out io.Writer, opts *options, version int64, upSQL string) error {
	if opts.dryRun {
		name, contents := generator.AtlasCheckpointArtifact(version, opts.description, upSQL)
		fmt.Fprintf(out, "-- checkpoint version %d (dry run, no files written)\n\n-- %s\n%s", version, name, contents)
		return nil
	}

	path, err := generator.WriteAtlasCheckpointFile(opts.migrationsDir, version, opts.description, upSQL)
	if err != nil {
		return cmdutil.Fail(cmd, err)
	}
	fmt.Fprintf(out, "Wrote checkpoint version %d:\n  %s\n", version, path)
	return nil
}

// maxMigrationVersion is the largest value the 10-digit NNNNNNNNNN file-name
// prefix can hold. A larger version would produce a file name the migration
// parser cannot recognize, so the checkpoint would be silently invisible.
const maxMigrationVersion = 9999999999

// ptahVersionWidth is the digit count of a ptah migration file-name prefix.
// Atlas auto-detection deliberately refuses a suffixless Atlas name whose
// version is exactly this wide, because it cannot be told apart from a ptah
// name. An Atlas checkpoint at such a version is therefore invisible to any
// reader that has not been told the format explicitly — the same silent-
// invisibility failure maxMigrationVersion guards on the ptah side.
const ptahVersionWidth = 10

// resolveCheckpointVersion returns the checkpoint version.
//
// Without --version the default depends on the target convention. The ptah
// format counts: one above the newest migration. The Atlas format timestamps,
// as Atlas itself was measured to do, bumped past the newest migration when a
// directory already holds a future-dated one. Either way the checkpoint sorts
// after and covers the whole existing history.
//
// An explicit --version must be a positive value that fits the format's
// file-name shape and is above every existing migration — otherwise the
// checkpoint would overwrite history, collide with an ordinary migration, or
// fail to squash it — so those are rejected up front rather than producing a
// directory that only breaks on a later command.
func resolveCheckpointVersion(explicit, migrationsDir string, dirFormat migrator.MigrationDirFormat) (int64, error) {
	files, err := migrator.DiscoverMigrationFiles(os.DirFS(migrationsDir), dirFormat)
	if err != nil {
		return 0, fmt.Errorf("failed to scan migrations directory: %w", err)
	}
	var latest int64
	for _, file := range files {
		if file.Version > latest {
			latest = file.Version
		}
	}

	atlas := dirFormat == migrator.MigrationDirFormatAtlas

	if explicit == "" {
		if atlas {
			return generator.ResolveAtlasCheckpointVersion(migrationsDir), nil
		}
		return latest + 1, nil
	}

	version, err := strconv.ParseInt(explicit, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("invalid --%s value %q: %w", versionFlag, explicit, err)
	}
	if version <= 0 {
		return 0, fmt.Errorf("invalid --%s value %d: must be a positive version", versionFlag, version)
	}
	if atlas {
		if len(explicit) == ptahVersionWidth {
			return 0, fmt.Errorf(
				"invalid --%s value %s: an Atlas checkpoint version of exactly %d digits is indistinguishable from a ptah migration name and is skipped by format auto-detection",
				versionFlag, explicit, ptahVersionWidth,
			)
		}
	} else if version > maxMigrationVersion {
		return 0, fmt.Errorf("invalid --%s value %d: exceeds the maximum migration version %d", versionFlag, version, int64(maxMigrationVersion))
	}
	if version <= latest {
		return 0, fmt.Errorf("invalid --%s value %d: must be above the newest existing migration version %d so the checkpoint covers the whole history", versionFlag, version, latest)
	}
	return version, nil
}

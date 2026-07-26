// Package migratecheckpoint implements `ptah migrations checkpoint`, which
// squashes a migration directory's history into a cumulative-schema checkpoint.
package migratecheckpoint

import (
	"fmt"
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
resulting schema is introspected, and a checkpoint migration pair
(NNNNNNNNNN_description.checkpoint.up.sql / .down.sql) is written and ptah.sum is
refreshed. A fresh database then bootstraps from the newest checkpoint instead
of replaying all history, while an already-migrated database ignores it.`,
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
	// Checkpoint files and their integrity entry are written only in the ptah
	// two-file convention (NNNNNNNNNN_name.checkpoint.up.sql / .down.sql plus
	// ptah.sum). Writing them into a directory read as another format would
	// leave a mixed-format directory with a stale integrity file, so refuse any
	// non-ptah format up front rather than reporting success and corrupting it.
	if dirFormat != migrator.MigrationDirFormatPtah {
		return cmdutil.Fail(cmd, fmt.Errorf("checkpoint supports only the %q migration directory format, not %q", migrator.MigrationDirFormatPtah, dirFormat))
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

// maxMigrationVersion is the largest value the 10-digit NNNNNNNNNN file-name
// prefix can hold. A larger version would produce a file name the migration
// parser cannot recognize, so the checkpoint would be silently invisible.
const maxMigrationVersion = 9999999999

// resolveCheckpointVersion returns the checkpoint version. Without --version it
// is one above the newest migration so the checkpoint sorts after and covers
// the whole existing history. An explicit --version must be a positive value
// that fits the file-name width and is above every existing migration —
// otherwise the checkpoint would overwrite history, collide with an ordinary
// migration, or fail to squash it — so those are rejected up front rather than
// producing a directory that only breaks on a later command.
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

	if explicit == "" {
		return latest + 1, nil
	}

	version, err := strconv.ParseInt(explicit, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("invalid --%s value %q: %w", versionFlag, explicit, err)
	}
	if version <= 0 {
		return 0, fmt.Errorf("invalid --%s value %d: must be a positive version", versionFlag, version)
	}
	if version > maxMigrationVersion {
		return 0, fmt.Errorf("invalid --%s value %d: exceeds the maximum migration version %d", versionFlag, version, int64(maxMigrationVersion))
	}
	if version <= latest {
		return 0, fmt.Errorf("invalid --%s value %d: must be above the newest existing migration version %d so the checkpoint covers the whole history", versionFlag, version, latest)
	}
	return version, nil
}

// Package migratedata implements `ptah migrations data`, which generates an
// ordinary migration from the drift between declarative reference/seed data
// (//migrator:schema:data annotations) and a live database.
package migratedata

import (
	"errors"
	"fmt"
	"os"
	"strconv"

	"github.com/spf13/cobra"

	"github.com/stokaro/ptah/cmd/internal/cmdutil"
	"github.com/stokaro/ptah/cmd/internal/dbcli"
	"github.com/stokaro/ptah/dbschema"
	"github.com/stokaro/ptah/internal/datamigrate"
	"github.com/stokaro/ptah/migration/generator"
	"github.com/stokaro/ptah/migration/migrator"
)

const (
	rootDirFlag     = "root-dir"
	dbURLFlag       = "db-url"
	migrationsFlag  = "migrations-dir"
	versionFlag     = "version"
	descriptionFlag = "description"
	dryRunFlag      = "dry-run"
)

type options struct {
	rootDir        string
	dbURL          string
	migrationsDir  string
	version        string
	description    string
	dryRun         bool
	schemas        string
	connectTimeout string
}

// NewMigrateDataCommand returns the `data` command.
func NewMigrateDataCommand() *cobra.Command {
	opts := options{}
	cmd := &cobra.Command{
		Use:   "data",
		Short: "Generate a migration from declarative reference/seed data drift",
		Long: `Generate an ordinary migration from the drift between declarative
reference/seed data and a live database.

The Go sources under --root-dir are scanned for //migrator:schema:data
annotations. For every declared table its desired rows are loaded from the
referenced YAML file and diffed against the live rows in --db-url, and the
combined difference is written as an ordinary migration pair
(NNNNNNNNNN_description.up.sql / .down.sql) with ptah.sum refreshed. The up
body inserts, updates, and deletes rows to reach the desired state; the down
body reverses it. When nothing has drifted, no files are written.

The generated migration is meant to be reviewed before it is applied, and it is
applied through the normal migration path, so this command performs no
safety/risk gating of its own: destructive UPDATE/DELETE volume gating and
protected-table guards are a deferred follow-up.`,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return migrateDataCommand(cmd, &opts)
		},
	}
	registerFlags(cmd, &opts)
	cmdutil.ConfigureCommand(cmd)
	return cmd
}

func registerFlags(cmd *cobra.Command, opts *options) {
	flags := cmd.Flags()
	flags.StringVar(&opts.rootDir, rootDirFlag, "", "Directory of Go sources carrying //migrator:schema:data annotations (required)")
	flags.StringVar(&opts.dbURL, dbURLFlag, "", "Live database URL to diff the desired data against (required)")
	flags.StringVar(&opts.migrationsDir, migrationsFlag, "./migrations", "Directory the generated migration pair is written to")
	flags.StringVar(&opts.version, versionFlag, "", "Migration version; defaults to one above the newest migration")
	flags.StringVar(&opts.description, descriptionFlag, "data", "Migration description used in the file name")
	flags.BoolVar(&opts.dryRun, dryRunFlag, false, "Print the migration SQL instead of writing files")
	dbcli.RegisterSchemasFlag(flags, &opts.schemas)
	dbcli.RegisterConnectTimeoutFlag(flags, &opts.connectTimeout)
}

func migrateDataCommand(cmd *cobra.Command, opts *options) error {
	ctx := cmd.Context()
	out := cmd.OutOrStdout()

	if opts.rootDir == "" {
		return cmdutil.Fail(cmd, fmt.Errorf("a Go annotations directory is required (--%s)", rootDirFlag))
	}
	if opts.dbURL == "" {
		return cmdutil.Fail(cmd, fmt.Errorf("a database URL is required (--%s)", dbURLFlag))
	}
	if opts.migrationsDir == "" {
		return cmdutil.Fail(cmd, fmt.Errorf("migrations directory is required (--%s)", migrationsFlag))
	}

	connectTimeout, err := dbcli.ParseConnectTimeout(opts.connectTimeout)
	if err != nil {
		return cmdutil.Fail(cmd, err)
	}
	version, err := resolveVersion(opts.version, opts.migrationsDir)
	if err != nil {
		return cmdutil.Fail(cmd, err)
	}

	connectCtx, cancel := dbcli.ConnectContext(ctx, connectTimeout)
	conn, err := dbschema.ConnectToDatabase(connectCtx, opts.dbURL)
	cancel()
	if err != nil {
		return cmdutil.Fail(cmd, fmt.Errorf("connect to database: %w", err))
	}
	defer dbschema.CloseAndWarn(conn)

	upSQL, downSQL, err := datamigrate.Generate(ctx, conn, datamigrate.Options{RootDir: opts.rootDir})
	if err != nil {
		return cmdutil.Fail(cmd, err)
	}

	if upSQL == "" {
		fmt.Fprintln(out, "no data changes")
		return nil
	}

	if opts.dryRun {
		upName := migrator.GenerateMigrationFileName(version, opts.description, "up")
		downName := migrator.GenerateMigrationFileName(version, opts.description, "down")
		fmt.Fprintf(out, "-- data migration version %d (dry run, no files written)\n\n-- %s\n%s\n\n-- %s\n%s\n",
			version, upName, upSQL, downName, downSQL)
		return nil
	}

	upPath, downPath, err := generator.WriteDataMigrationFiles(opts.migrationsDir, version, opts.description, upSQL, downSQL)
	if err != nil {
		return cmdutil.Fail(cmd, err)
	}
	fmt.Fprintf(out, "Wrote data migration version %d:\n  %s\n  %s\n", version, upPath, downPath)
	return nil
}

// maxMigrationVersion is the largest value the 10-digit NNNNNNNNNN file-name
// prefix can hold. A larger version would produce a file name the migration
// parser cannot recognize, so the migration would be silently invisible.
const maxMigrationVersion = 9999999999

// resolveVersion returns the version for the generated migration. Without
// --version it is one above the newest migration so the data migration sorts
// after the whole existing history. An explicit --version must be a positive
// value that fits the file-name width and is above every existing migration —
// otherwise it would collide with or precede an existing migration — so those
// are rejected up front. It mirrors the checkpoint command's resolver, but
// treats a not-yet-created migrations directory as an empty history (version 1)
// because a data migration may be the first file written to a fresh project.
func resolveVersion(explicit, migrationsDir string) (int64, error) {
	latest, err := latestMigrationVersion(migrationsDir)
	if err != nil {
		return 0, err
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
		return 0, fmt.Errorf("invalid --%s value %d: must be above the newest existing migration version %d", versionFlag, version, latest)
	}
	return version, nil
}

// latestMigrationVersion returns the highest version among the ptah-format
// migrations in migrationsDir, or 0 when the directory does not yet exist.
func latestMigrationVersion(migrationsDir string) (int64, error) {
	info, err := os.Stat(migrationsDir)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return 0, nil
		}
		return 0, fmt.Errorf("failed to scan migrations directory: %w", err)
	}
	if !info.IsDir() {
		return 0, fmt.Errorf("%q exists and is not a directory", migrationsDir)
	}

	files, err := migrator.DiscoverMigrationFiles(os.DirFS(migrationsDir), migrator.MigrationDirFormatPtah)
	if err != nil {
		return 0, fmt.Errorf("failed to scan migrations directory: %w", err)
	}
	var latest int64
	for _, file := range files {
		if file.Version > latest {
			latest = file.Version
		}
	}
	return latest, nil
}

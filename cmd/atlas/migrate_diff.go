package atlas

import (
	"fmt"
	"strings"

	"github.com/spf13/cobra"

	"github.com/stokaro/ptah/cmd/internal/cmdutil"
	"github.com/stokaro/ptah/cmd/internal/dbcli"
	"github.com/stokaro/ptah/dbschema"
	"github.com/stokaro/ptah/internal/atlasargs"
	"github.com/stokaro/ptah/internal/atlasmigrate"
	"github.com/stokaro/ptah/internal/atlasreport"
	"github.com/stokaro/ptah/internal/pathguard"
	"github.com/stokaro/ptah/migration/migrator"
)

type atlasMigrateDiffOptions struct {
	toURLs      []string
	devURL      string
	dirURL      string
	dirFormat   string
	format      string
	lockTimeout string
}

func newAtlasMigrateDiffCommand() *cobra.Command {
	opts := atlasMigrateDiffOptions{}
	cmd := &cobra.Command{
		Use:   "diff [name]",
		Short: "Compute migration diff against a desired schema",
		Long: `Atlas OSS ` + "`atlas migrate diff`" + ` command path.

Drops all tables in the --dev-url database, replays the local migration
directory on it, compares the resulting state to local --to schema files, and
writes a new Atlas-style single-file migration plus atlas.sum when changes are
found. Use a disposable dev database. This implementation currently supports
local file:// migration directories and local .hcl, .yaml, .yml, or .sql schema
files. Database URLs, env:// URLs, schema filters, lock flags other than
--lock-timeout, and Docker dev databases remain explicit follow-up gaps.`,
		Args: cobra.MaximumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			name := "migration"
			if len(args) == 1 {
				name = args[0]
			}
			return runAtlasMigrateDiff(cmd, opts, name)
		},
	}
	flags := cmd.Flags()
	flags.StringArrayVar(&opts.toURLs, "to", nil, "Desired schema target URL")
	flags.StringVar(&opts.devURL, "dev-url", "", "Dev database URL used to replay migrations and compute the diff")
	flags.StringVar(&opts.dirURL, "dir", "file://migrations", "Migration directory URL")
	flags.StringVar(&opts.dirFormat, "dir-format", "atlas", "Migration directory format; only atlas is implemented")
	flags.StringVar(&opts.format, "format", "", "Atlas Go template output format")
	flags.StringArray("schema", nil, "Schemas to diff when database URLs are used")
	flags.StringVar(&opts.lockTimeout, "lock-timeout", "", "Timeout for acquiring Atlas migration directory locks")
	cmdutil.ConfigureCommandArgs(cmd, nil)
	return cmd
}

func runAtlasMigrateDiff(cmd *cobra.Command, opts atlasMigrateDiffOptions, name string) error {
	if err := validateAtlasMigrateDiffOptions(cmd, opts); err != nil {
		return cmdutil.Fail(cmd, err)
	}
	format := atlasreport.NormalizeMigrateDiffFormat(opts.format)
	if err := atlasreport.ValidateSchemaDiffTemplate(format); err != nil {
		return cmdutil.Fail(cmd, err)
	}
	lockTimeout, err := migrator.ParseMigrationLockTimeout(opts.lockTimeout)
	if err != nil {
		return cmdutil.Fail(cmd, err)
	}

	migrationsDir, err := atlasargs.LocalDirValue(opts.dirURL)
	if err != nil {
		return cmdutil.Fail(cmd, fmt.Errorf("--dir %q: %w", opts.dirURL, err))
	}
	migrationsDir, err = pathguard.ResolveCLIPath(migrationsDir)
	if err != nil {
		return cmdutil.Fail(cmd, fmt.Errorf("resolve migration directory: %w", err))
	}

	connectCtx, cancel := dbcli.ConnectContext(cmd.Context(), dbcli.DefaultConnectTimeout)
	defer cancel()
	conn, err := dbschema.ConnectToDatabase(connectCtx, opts.devURL)
	if err != nil {
		return cmdutil.Fail(cmd, fmt.Errorf("connect to --dev-url: %w", err))
	}
	defer dbschema.CloseAndWarn(conn)

	diffResult, err := atlasmigrate.GenerateDiff(cmd.Context(), conn, atlasmigrate.DiffOptions{
		Dir:         migrationsDir,
		ToURLs:      opts.toURLs,
		Name:        name,
		Format:      format,
		LockTimeout: lockTimeout,
	})
	if err != nil {
		return cmdutil.Fail(cmd, err)
	}
	if diffResult.Synced {
		fmt.Fprintln(cmd.OutOrStdout(), "The migration directory is synced with the desired state, no changes to be made")
		return nil
	}
	fmt.Fprintf(cmd.OutOrStdout(), "Created migration file: %s\n", diffResult.MigrationPath)
	fmt.Fprintf(cmd.OutOrStdout(), "Updated migration checksum: %s\n", diffResult.SumPath)
	return nil
}

func validateAtlasMigrateDiffOptions(cmd *cobra.Command, opts atlasMigrateDiffOptions) error {
	if len(opts.toURLs) == 0 {
		return fmt.Errorf("--to is required")
	}
	if strings.TrimSpace(opts.devURL) == "" {
		return fmt.Errorf("--dev-url is required")
	}
	dirFormat := strings.ToLower(strings.TrimSpace(opts.dirFormat))
	if dirFormat != "" && dirFormat != string(migrator.MigrationDirFormatAtlas) {
		return fmt.Errorf("atlas migrate diff currently writes Atlas-format migration directories only")
	}
	if values, err := cmd.Flags().GetStringArray("schema"); err == nil && len(values) > 0 {
		return fmt.Errorf("atlas migrate diff accepts --schema, but Ptah only supports local schema files for this command yet")
	}
	if strings.HasPrefix(strings.TrimSpace(opts.devURL), "docker://") {
		return fmt.Errorf("atlas migrate diff accepts docker --dev-url values, but Ptah requires a directly connectable dev database URL")
	}
	return ensureLocalSchemaURLs("--to", opts.toURLs)
}

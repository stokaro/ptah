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
	"github.com/stokaro/ptah/internal/atlasschema"
	"github.com/stokaro/ptah/internal/pathguard"
	"github.com/stokaro/ptah/migration/migrator"
)

type atlasMigrateDiffOptions struct {
	toURLs      []string
	devURL      string
	envName     string
	dirURL      string
	dirFormat   string
	format      string
	schemas     []string
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
files. Use --schema to limit the comparison to selected schema names. Database
URLs, env:// URLs, lock flags other than --lock-timeout, concurrent index
migration-file metadata, and Docker dev databases remain explicit follow-up
gaps. When --env is set, the selected atlas.hcl env can provide schema.src,
dev, migration.dir, format.migrate.diff, and supported non-concurrent diff
policy values.`,
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
	dbcli.RegisterEnvFlag(flags, &opts.envName)
	flags.StringVar(&opts.dirURL, "dir", "file://migrations", "Migration directory URL")
	flags.StringVar(&opts.dirFormat, "dir-format", "atlas", "Migration directory format; only atlas is implemented")
	flags.StringVar(&opts.format, "format", "", "Atlas Go template output format")
	flags.StringArrayVar(&opts.schemas, "schema", nil, "Schemas to diff")
	flags.StringVar(&opts.lockTimeout, "lock-timeout", "", "Timeout for acquiring Atlas migration directory locks")
	cmdutil.ConfigureCommandArgs(cmd, nil)
	return cmd
}

func runAtlasMigrateDiff(cmd *cobra.Command, opts atlasMigrateDiffOptions, name string) error {
	policy := atlasschema.DiffPolicy{}
	projectCfg, loaded, err := loadOptionalAtlasProjectConfigForCommand(cmd, opts.envName)
	if needsAtlasMigrateDiffConfig(cmd) {
		projectCfg, loaded, err = loadRequiredAtlasProjectConfigForCommand(cmd, opts.envName)
	}
	if err != nil {
		return cmdutil.Fail(cmd, err)
	}
	if loaded {
		opts.toURLs = effectiveStringArray(cmd, "to", opts.toURLs, projectCfg.SchemaSources)
		opts.devURL = dbcli.EffectiveString(cmd, "dev-url", opts.devURL, projectCfg.DevURL)
		opts.dirURL = dbcli.EffectiveString(cmd, "dir", opts.dirURL, projectCfg.Migration.Dir)
		opts.format = dbcli.EffectiveString(cmd, "format", opts.format, projectCfg.Format.Migrate.Diff)
		policy, err = atlasDiffPolicy(projectCfg)
		if err != nil {
			return cmdutil.Fail(cmd, err)
		}
		if policy.ConcurrentIndexCreate {
			return cmdutil.Fail(cmd, fmt.Errorf("atlas.hcl diff.concurrent_index.create is not supported by migrate diff yet"))
		}
	}
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
		Schemas:     opts.schemas,
		LockTimeout: lockTimeout,
		Policy:      policy,
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

func needsAtlasMigrateDiffConfig(cmd *cobra.Command) bool {
	return !cmd.Flags().Changed("to") ||
		!cmd.Flags().Changed("dev-url")
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
	if strings.HasPrefix(strings.TrimSpace(opts.devURL), "docker://") {
		return fmt.Errorf("atlas migrate diff accepts docker --dev-url values, but Ptah requires a directly connectable dev database URL")
	}
	return ensureLocalSchemaURLs("--to", opts.toURLs)
}

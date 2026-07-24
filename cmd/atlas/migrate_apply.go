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
	"github.com/stokaro/ptah/internal/atlasmigratereport"
	"github.com/stokaro/ptah/internal/atlasreport"
	"github.com/stokaro/ptah/internal/pathguard"
	"github.com/stokaro/ptah/migration/migrator"
)

type atlasMigrateApplyOptions struct {
	url             string
	dir             string
	dryRun          bool
	txMode          string
	execOrder       string
	allowDirty      bool
	baseline        string
	revisionsSchema string
	lockTimeout     string
	format          string
}

func newAtlasMigrateApplyCommand() *cobra.Command {
	opts := atlasMigrateApplyOptions{
		txMode:    string(migrator.MigrationTxModeFile),
		execOrder: string(migrator.ExecOrderLinear),
	}
	cmd := &cobra.Command{
		Use:   "apply [flags] [amount]",
		Short: "Apply pending migrations",
		Long: `Apply pending Atlas migrations to the target database.

By default, all pending migrations are applied. The optional amount argument
limits the run to the first N pending migrations.

Native Ptah equivalent: ptah migrations up.`,
		RunE: func(cmd *cobra.Command, args []string) error {
			return runAtlasMigrateApply(cmd, opts, args)
		},
	}

	flags := cmd.Flags()
	flags.StringVarP(&opts.url, "url", "u", "", "Database URL to apply migrations to")
	flags.StringVar(&opts.dir, "dir", "", "Migration directory URL")
	flags.BoolVar(&opts.dryRun, "dry-run", false, "Show migrations without applying them")
	flags.StringVar(&opts.txMode, "tx-mode", opts.txMode, "Transaction mode: file, all, or none")
	flags.StringVar(&opts.execOrder, "exec-order", opts.execOrder, "Execution order: linear, linear-skip, or non-linear")
	flags.BoolVar(&opts.allowDirty, "allow-dirty", false, "Allow applying migrations when the revision table is dirty")
	flags.StringVar(&opts.baseline, "baseline", "", "Baseline version to mark applied before running pending migrations")
	flags.StringVar(&opts.revisionsSchema, "revisions-schema", "", "Schema for the Atlas revisions table")
	flags.StringVar(&opts.lockTimeout, "lock-timeout", "", "Timeout for acquiring the migration lock, such as 10s or 2m")
	flags.StringVar(&opts.format, "format", "", "Atlas Go template output format")

	cmdutil.ConfigureCommandArgs(cmd, atlasMigrateApplyArgs)
	return cmd
}

func atlasMigrateApplyArgs(cmd *cobra.Command, args []string) error {
	_, err := atlasmigrate.ParseApplyAmount(args)
	if err != nil {
		return cmdutil.Fail(cmd, err)
	}
	return nil
}

func runAtlasMigrateApply(cmd *cobra.Command, opts atlasMigrateApplyOptions, args []string) error {
	formatOutput := cmd.Flags().Changed("format")
	projectCfg, loaded, err := loadOptionalAtlasProjectConfigForCommand(cmd)
	if needsAtlasMigrateApplyConfig(cmd) {
		projectCfg, loaded, err = loadRequiredAtlasProjectConfigForCommand(cmd)
	}
	if err != nil {
		return err
	}
	if loaded {
		opts.url = dbcli.EffectiveString(cmd, "url", opts.url, projectCfg.DatabaseURL)
		opts.dir = dbcli.EffectiveString(cmd, "dir", opts.dir, projectCfg.Migration.Dir)
		opts.txMode = dbcli.EffectiveString(cmd, "tx-mode", opts.txMode, projectCfg.Migration.TxMode)
		opts.execOrder = dbcli.EffectiveString(cmd, "exec-order", opts.execOrder, projectCfg.Migration.ExecOrder)
		opts.revisionsSchema = dbcli.EffectiveString(cmd, "revisions-schema", opts.revisionsSchema, projectCfg.Migration.RevisionsSchema)
		opts.lockTimeout = dbcli.EffectiveString(cmd, "lock-timeout", opts.lockTimeout, projectCfg.Migration.LockTimeout)
		opts.format = dbcli.EffectiveString(cmd, "format", opts.format, projectCfg.Format.Migrate.Apply)
		formatOutput = formatOutput || projectCfg.Format.Migrate.Apply != ""
	}
	if loaded && !cmd.Flags().Changed("dir") && projectCfg.Migration.Dir != "" {
		opts.dir, err = atlasProjectConfigLocalDir(cmd, opts.dir)
		if err != nil {
			return fmt.Errorf("atlas migrate apply --dir: %w", err)
		}
	}
	if formatOutput && strings.TrimSpace(opts.format) == "" {
		return fmt.Errorf("--format must not be empty")
	}
	if formatOutput {
		if err := atlasreport.ValidateMigrateApplyTemplate(opts.format); err != nil {
			return err
		}
	}
	if opts.url == "" {
		return fmt.Errorf("database URL is required")
	}
	if opts.dir == "" {
		return fmt.Errorf("migrations directory is required")
	}

	dir, err := atlasargs.LocalDirValue(opts.dir)
	if err != nil {
		return fmt.Errorf("atlas migrate apply --dir: %w", err)
	}
	dir, err = pathguard.ResolveCLIPath(dir)
	if err != nil {
		return fmt.Errorf("invalid migration directory: %w", err)
	}

	amount, err := atlasmigrate.ParseApplyAmount(args)
	if err != nil {
		return err
	}
	baselineVersion, err := atlasmigrate.ParseMigrationVersionFlag("baseline", opts.baseline)
	if err != nil {
		return err
	}

	txMode, err := migrator.ParseMigrationTxMode(opts.txMode)
	if err != nil {
		return err
	}
	execOrder, err := migrator.ParseExecOrder(opts.execOrder)
	if err != nil {
		return err
	}
	migrationLockTimeout, err := migrator.ParseMigrationLockTimeout(opts.lockTimeout)
	if err != nil {
		return err
	}

	conn, err := dbschema.ConnectToDatabase(cmd.Context(), opts.url)
	if err != nil {
		return fmt.Errorf("error connecting to database: %w", err)
	}
	defer dbschema.CloseAndWarn(conn)

	plan, err := atlasmigrate.PrepareApply(cmd.Context(), conn, atlasmigrate.ApplyOptions{
		Dir:                  dir,
		DryRun:               opts.dryRun,
		ExecOrder:            execOrder,
		TxMode:               txMode,
		RevisionsSchema:      opts.revisionsSchema,
		MigrationLockTimeout: migrationLockTimeout,
		Amount:               amount,
		AllowDirty:           opts.allowDirty,
		BaselineVersion:      baselineVersion,
	})
	if err != nil {
		return err
	}

	out := cmd.OutOrStdout()
	if opts.dryRun && !formatOutput {
		fmt.Fprintln(out, "Dry run mode: no changes will be made.")
	}
	if opts.dryRun && baselineVersion > 0 && !formatOutput {
		fmt.Fprintf(out, "Would baseline migrations at version %d.\n", baselineVersion)
	}
	if plan.Noop() {
		result, err := plan.Execute(cmd.Context())
		if err != nil {
			return err
		}
		if formatOutput {
			return writeAtlasMigrateApplyFormat(cmd, opts, dir, conn, result)
		}
		fmt.Fprintln(out, "No migration files to execute.")
		return nil
	}
	if len(plan.SelectedVersions) > 0 && !formatOutput {
		fmt.Fprintf(out, "Migrating to version %d from %d pending migrations.\n",
			plan.SelectedVersions[len(plan.SelectedVersions)-1],
			len(plan.SelectedVersions),
		)
	}

	result, err := plan.Execute(cmd.Context())
	if err != nil {
		if formatOutput && result.ApplyError != nil {
			writeErr := writeAtlasMigrateApplyFormat(cmd, opts, dir, conn, result)
			if writeErr != nil {
				return fmt.Errorf("%w; additionally failed to write --format output: %v", err, writeErr)
			}
		}
		return err
	}

	if opts.dryRun {
		if formatOutput {
			return writeAtlasMigrateApplyFormat(cmd, opts, dir, conn, result)
		}
		fmt.Fprintf(out, "Would have applied %d migrations.\n", len(plan.SelectedVersions))
		return nil
	}
	if formatOutput {
		return writeAtlasMigrateApplyFormat(cmd, opts, dir, conn, result)
	}
	fmt.Fprintf(out, "Migration complete. Current version: %d\n", result.FinalStatus.CurrentVersion)
	return nil
}

func needsAtlasMigrateApplyConfig(cmd *cobra.Command) bool {
	return !cmd.Flags().Changed("url") ||
		!cmd.Flags().Changed("dir")
}

func writeAtlasMigrateApplyFormat(
	cmd *cobra.Command,
	opts atlasMigrateApplyOptions,
	resolvedDir string,
	conn *dbschema.DatabaseConnection,
	result atlasmigrate.ApplyResult,
) error {
	return atlasmigratereport.WriteApplyFormat(cmd.OutOrStdout(), opts.format, atlasmigratereport.ApplyFormatOptions{
		Conn:        conn,
		ResolvedDir: resolvedDir,
		Dir:         opts.dir,
		URL:         opts.url,
		Result:      result,
	})
}

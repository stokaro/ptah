package atlas

import (
	"errors"
	"fmt"
	"io/fs"
	"strings"

	"github.com/spf13/cobra"

	"github.com/stokaro/ptah/cmd/internal/cmdutil"
	"github.com/stokaro/ptah/cmd/internal/dbcli"
	"github.com/stokaro/ptah/cmd/migratevalidate"
	"github.com/stokaro/ptah/config/projectconfig"
	"github.com/stokaro/ptah/dbschema"
	"github.com/stokaro/ptah/internal/atlasargs"
	"github.com/stokaro/ptah/internal/atlasmigrate"
	"github.com/stokaro/ptah/internal/atlasmigrateimport"
	"github.com/stokaro/ptah/internal/atlasmigratereport"
	"github.com/stokaro/ptah/internal/atlasreport"
	"github.com/stokaro/ptah/internal/migratesum"
	"github.com/stokaro/ptah/migration/migrator"
)

type atlasMigrateApplyOptions struct {
	url             string
	dir             string
	dirFormat       string
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
		dirFormat: atlasDirFormatDefault,
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

func runAtlasMigrateApply(
	cmd *cobra.Command,
	opts atlasMigrateApplyOptions,
	args []string,
) (runErr error) {
	formatOutput := cmd.Flags().Changed("format")
	projectDirFormatPresent := false
	mode := ignoreMissingEnvSelection
	if needsAtlasMigrateApplyConfig(cmd) {
		mode = reportMissingEnvSelection
	}
	project, loaded, err := openAtlasProjectForCommand(cmd, mode)
	if err != nil {
		return err
	}
	defer closeAtlasProject(&project, &runErr)
	projectCfg := project.Config
	if loaded {
		opts.url = dbcli.EffectiveString(
			cmd,
			"url",
			opts.url,
			projectCfg.StringValue(projectconfig.StringDatabaseURL),
		)
		opts.dir = dbcli.EffectiveString(
			cmd,
			"dir",
			opts.dir,
			projectCfg.StringValue(projectconfig.StringMigrationDir),
		)
		dirFormat := projectCfg.StringValue(projectconfig.StringMigrationFormat)
		projectDirFormatPresent = dirFormat.Present
		if dirFormat.Present {
			opts.dirFormat = dirFormat.Value
		}
		opts.txMode = dbcli.EffectiveString(
			cmd,
			"tx-mode",
			opts.txMode,
			projectCfg.StringValue(projectconfig.StringMigrationTxMode),
		)
		opts.execOrder = dbcli.EffectiveString(
			cmd,
			"exec-order",
			opts.execOrder,
			projectCfg.StringValue(projectconfig.StringMigrationExecOrder),
		)
		opts.revisionsSchema = dbcli.EffectiveString(
			cmd,
			"revisions-schema",
			opts.revisionsSchema,
			projectCfg.StringValue(projectconfig.StringMigrationRevisionsSchema),
		)
		opts.lockTimeout = dbcli.EffectiveString(
			cmd,
			"lock-timeout",
			opts.lockTimeout,
			projectCfg.StringValue(projectconfig.StringMigrationLockTimeout),
		)
		formatValue := projectCfg.StringValue(projectconfig.StringFormatMigrateApply)
		opts.format = dbcli.EffectiveString(cmd, "format", opts.format, formatValue)
		formatOutput = formatOutput || formatValue.Present
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

	var localDir atlasargs.LocalDir
	if loaded &&
		!cmd.Flags().Changed("dir") &&
		projectCfg.StringValue(projectconfig.StringMigrationDir).Present {
		localDir, err = project.localDirWithQuery(opts.dir)
	} else {
		localDir, err = atlasargs.ParseLocalDir(opts.dir)
	}
	if err != nil {
		return fmt.Errorf("atlas migrate apply --dir: %w", err)
	}
	if projectDirFormatPresent && opts.dirFormat == "" {
		if _, queryOverridesProjectFormat := localDir.Query["format"]; !queryOverridesProjectFormat {
			return fmt.Errorf("atlas migrate apply --dir: migration directory format must not be empty")
		}
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

	// Resolve the directory format once: the filesystem that gets executed and
	// the format the integrity gate reasons about must be the same decision,
	// not two computations that happen to agree (#970).
	resolvedDirFormat, err := atlasmigrate.ResolveApplyDirFormat(opts.dirFormat, localDir.Query)
	if err != nil {
		return fmt.Errorf("atlas migrate apply --dir: %w", err)
	}

	source, err := project.openLocal(localDir)
	if err != nil {
		return fmt.Errorf("atlas migrate apply --dir: %w", err)
	}
	dir := source.Display()
	migrationFS, err := atlasmigrate.ResolveApplySourceForFormat(
		source.FS(),
		dir,
		resolvedDirFormat,
	)
	closeErr := source.Close()
	if err != nil {
		return errors.Join(
			fmt.Errorf("atlas migrate apply --dir: %w", err),
			closeErr,
		)
	}
	if closeErr != nil {
		return fmt.Errorf("atlas migrate apply --dir: close migrations directory: %w", closeErr)
	}

	// Integrity gate (#955, #970): a native Atlas directory must carry an
	// atlas.sum and verify against it before anything is read for execution,
	// matching official Atlas, which refuses both a tampered and an unhashed
	// directory before applying a single migration. This runs before the
	// database connection is even opened, so neither a tampered file
	// (including a tampered checkpoint) nor an unhashed directory can execute
	// or create the target database.
	if err := verifyAtlasApplyChecksum(cmd, migrationFS, resolvedDirFormat); err != nil {
		return err
	}

	conn, err := dbschema.ConnectToDatabase(cmd.Context(), opts.url)
	if err != nil {
		return fmt.Errorf("error connecting to database: %w", err)
	}
	defer dbschema.CloseAndWarn(conn)

	plan, err := atlasmigrate.PrepareApply(cmd.Context(), conn, atlasmigrate.ApplyOptions{
		Dir:                  dir,
		FS:                   migrationFS,
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
			return writeAtlasMigrateApplyFormat(cmd, opts, migrationFS, conn, result)
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
			writeErr := writeAtlasMigrateApplyFormat(cmd, opts, migrationFS, conn, result)
			if writeErr != nil {
				return fmt.Errorf("%w; additionally failed to write --format output: %v", err, writeErr)
			}
		}
		return err
	}

	if opts.dryRun {
		if formatOutput {
			return writeAtlasMigrateApplyFormat(cmd, opts, migrationFS, conn, result)
		}
		fmt.Fprintf(out, "Would have applied %d migrations.\n", len(plan.SelectedVersions))
		return nil
	}
	if formatOutput {
		return writeAtlasMigrateApplyFormat(cmd, opts, migrationFS, conn, result)
	}
	fmt.Fprintf(out, "Migration complete. Current version: %d\n", result.FinalStatus.CurrentVersion)
	return nil
}

// verifyAtlasApplyChecksum enforces the atlas.sum integrity gate on the
// captured migration filesystem of a native Atlas directory: a missing
// atlas.sum and a failed verification both refuse the whole apply, with output
// byte-identical to `migrate validate` on the same directory.
//
// A directory read through ?format= is converted in memory and carries no
// Atlas integrity file, so it is not gated here. That is a known divergence
// from Atlas CE, which does gate converted directories; see
// verifyAtlasApplyChecksum's callers' tests and stokaro/ptah#973.
func verifyAtlasApplyChecksum(cmd *cobra.Command, fsys fs.FS, format atlasmigrateimport.Format) error {
	if !atlasmigrate.ReadsNativeAtlasDir(format) {
		return nil
	}
	result, hashed, err := migratesum.VerifyHashed(fsys, migrator.MigrationDirFormatAtlas)
	switch {
	case errors.Is(err, migratesum.ErrSumFileMalformed):
		// A malformed atlas.sum has no entry-level mismatch to point at; the
		// validate surface reports it as a plain checksum mismatch.
		return migratevalidate.FailAtlasChecksumMismatch(cmd, nil)
	case err != nil:
		return err
	case !hashed:
		return failUnhashedAtlasApplyDir(cmd, fsys)
	case !result.OK():
		return migratevalidate.FailAtlasChecksumMismatch(cmd, result.FirstMismatch())
	}
	return nil
}

// failUnhashedAtlasApplyDir refuses a directory that carries no atlas.sum,
// unless it holds no SQL file at all.
//
// Measured against Atlas CE v1.2.0: the checksum gate fires on the presence of
// any *.sql file, not on parseable versioned migrations — an unhashed directory
// holding only `foo.sql` is refused, while an empty directory and one holding
// only non-SQL files (a README, a .gitkeep) report "No migration files to
// execute" and exit 0. A CI bootstrap that creates an empty migrations
// directory must keep working (#970).
func failUnhashedAtlasApplyDir(cmd *cobra.Command, fsys fs.FS) error {
	sqlFiles, err := fs.Glob(fsys, "*.sql")
	if err != nil {
		return fmt.Errorf("scan migration directory for SQL files: %w", err)
	}
	if len(sqlFiles) == 0 {
		return nil
	}
	return migratevalidate.FailAtlasChecksumFileNotFound(cmd)
}

func needsAtlasMigrateApplyConfig(cmd *cobra.Command) bool {
	return !cmd.Flags().Changed("url") ||
		!cmd.Flags().Changed("dir")
}

func writeAtlasMigrateApplyFormat(
	cmd *cobra.Command,
	opts atlasMigrateApplyOptions,
	migrationFS fs.FS,
	conn *dbschema.DatabaseConnection,
	result atlasmigrate.ApplyResult,
) error {
	return atlasmigratereport.WriteApplyFormat(cmd.OutOrStdout(), opts.format, atlasmigratereport.ApplyFormatOptions{
		Conn:   conn,
		FS:     migrationFS,
		Dir:    opts.dir,
		URL:    opts.url,
		Result: result,
	})
}

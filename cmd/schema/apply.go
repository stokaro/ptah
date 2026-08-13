package schema

import (
	"context"
	"fmt"
	"io"
	"os"
	"slices"
	"strings"

	"github.com/spf13/cobra"

	"go.5x5.cz/ptah/cmd/internal/cmdflags"
	"go.5x5.cz/ptah/cmd/internal/cmdutil"
	"go.5x5.cz/ptah/cmd/internal/dbcli"
	"go.5x5.cz/ptah/cmd/internal/editor"
	"go.5x5.cz/ptah/config/projectconfig"
	"go.5x5.cz/ptah/core/goschema"
	"go.5x5.cz/ptah/dbschema"
	"go.5x5.cz/ptah/internal/atlasfilter"
	"go.5x5.cz/ptah/internal/atlasschema"
	"go.5x5.cz/ptah/internal/atlassource"
	"go.5x5.cz/ptah/internal/atlasurl"
	"go.5x5.cz/ptah/internal/schemafile"
	"go.5x5.cz/ptah/internal/schemaload"
	"go.5x5.cz/ptah/internal/sqlitevirtual"
	"go.5x5.cz/ptah/migration/diffpolicy"
	"go.5x5.cz/ptah/migration/migrator"
)

const (
	applyDBURLFlag       = "db-url"
	applyRootDirFlag     = "root-dir"
	applySchemaFileFlag  = "schema-file"
	applyToFlag          = "to"
	applyDevURLFlag      = "dev-url"
	applyDryRunFlag      = "dry-run"
	applyAutoApproveFlag = "auto-approve"
	applyEditFlag        = "edit"
	applyTxModeFlag      = "tx-mode"
	applyLockTimeoutFlag = "lock-timeout"
	applyIncludeFlag     = "include"
	applyExcludeFlag     = "exclude"
	applyPlanFlag        = "plan"
)

type schemaApplyOptions struct {
	dbURL          string
	rootDirs       []string
	schemaFiles    []string
	toURLs         []string
	devURL         string
	dryRun         bool
	autoApprove    bool
	edit           bool
	txMode         string
	lockTimeout    string
	schemas        string
	include        []string
	exclude        []string
	planPath       string
	connectTimeout string
	configPath     string
	envName        string
}

func newSchemaApplyCommand() *cobra.Command {
	opts := schemaApplyOptions{}
	cmd := &cobra.Command{
		Use:   "apply",
		Short: "Apply a desired schema directly to a database",
		Long: `Apply a desired schema directly to the --db-url database, without migration
files: the live schema is compared with the desired state, the resulting SQL
plan is shown, and after confirmation the plan is executed.

The desired state comes from Go annotations (--root-dir), native schema files
(--schema-file, repeatable; sources merge into one composite schema), or --to
source URLs (a database URL whose live schema becomes the desired state, or an
Atlas-format migration directory replayed on the required --dev-url dev
database).

Safety semantics: a session advisory lock serializes concurrent applies
against one target (--lock-timeout bounds the wait); when --dev-url is set the
exact ordered plan is rehearsed on the dev database first and a failed
rehearsal refuses the apply with the target unchanged; the plan must be
confirmed interactively unless --auto-approve is set; --dry-run prints the
plan without applying. With --edit the planned SQL opens in $VISUAL or
$EDITOR before confirmation, and the edited SQL is what gets applied. With
--plan <path>, a pre-approved plan file saved by "ptah schema plan" is
executed instead of re-planning, after verifying the database still matches
the plan's source fingerprint. --schemas and --include positively select what
both comparison sides see; --exclude subtracts from the result. An --include
selection that matches neither the target nor the desired state refuses the
apply rather than reporting a synced schema for work that did not happen.`,
		SilenceErrors: true,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return runSchemaApply(cmd, opts)
		},
	}
	flags := cmd.Flags()
	flags.StringVar(&opts.dbURL, applyDBURLFlag, "", "Database URL the schema is applied to (required)")
	flags.StringArrayVar(&opts.rootDirs, applyRootDirFlag, nil, "Root directory to scan for Go entities (repeatable; multiple sources merge into one composite schema)")
	flags.StringArrayVar(&opts.schemaFiles, applySchemaFileFlag, nil, "YAML, HCL, or SQL schema file describing the desired state (repeatable)")
	flags.StringArrayVar(&opts.toURLs, applyToFlag, nil, "Desired schema source URL: a database URL or an Atlas-format migration directory (repeatable)")
	flags.StringVar(&opts.devURL, applyDevURLFlag, "", "Dev database URL the plan is rehearsed on before touching the target; also replays migration-directory --to sources")
	flags.BoolVar(&opts.dryRun, applyDryRunFlag, false, "Show planned changes without applying them")
	flags.BoolVar(&opts.autoApprove, applyAutoApproveFlag, false, "Skip interactive approval")
	flags.BoolVar(&opts.edit, applyEditFlag, false, "Open the planned SQL in $VISUAL or $EDITOR before confirmation")
	flags.StringVar(&opts.txMode, applyTxModeFlag, "", "Transaction mode: all, file, or none (default file)")
	flags.StringVar(&opts.lockTimeout, applyLockTimeoutFlag, "", "Timeout for acquiring the schema apply lock, such as 10s (empty waits indefinitely)")
	dbcli.RegisterURLScopedSchemasFlag(flags, &opts.schemas)
	flags.StringArrayVar(&opts.include, applyIncludeFlag, nil, "Schema objects to include in the apply (Atlas-style selectors)")
	flags.StringArrayVar(&opts.exclude, applyExcludeFlag, nil, "Schema objects to exclude from the apply (Atlas-style selectors)")
	flags.StringVar(&opts.planPath, applyPlanFlag, "", "Pre-approved plan file saved by `ptah schema plan`; executed after fingerprint verification")
	dbcli.RegisterConnectTimeoutFlag(flags, &opts.connectTimeout)
	dbcli.RegisterConfigFlag(flags, &opts.configPath)
	dbcli.RegisterEnvFlag(flags, &opts.envName)
	if err := cmdflags.DisableEnvBinding(flags, applyAutoApproveFlag); err != nil {
		panic(err)
	}
	cmd.MarkFlagsMutuallyExclusive(applyToFlag, applyRootDirFlag)
	cmd.MarkFlagsMutuallyExclusive(applyToFlag, applySchemaFileFlag)
	cmdutil.ConfigureCommandArgs(cmd, cmdutil.NoPositionalArgs)
	return cmd
}

// nativeDiffPolicy maps the native project diff policy onto the shared
// schema-apply planning policy.
func nativeDiffPolicy(cfg projectconfig.Config) atlasschema.DiffPolicy {
	return atlasschema.DiffPolicy{
		SkipDropTable:         slices.Contains(cfg.Diff.SkipChangeKinds(), diffpolicy.DropTable),
		ConcurrentIndexCreate: cfg.Diff.ConcurrentIndexCreate(),
		ConcurrentIndexDrop:   cfg.Diff.ConcurrentIndexDrop(),
	}
}

func runSchemaApply(cmd *cobra.Command, opts schemaApplyOptions) error {
	projectCfg, err := dbcli.LoadProjectConfig(cmd, opts.configPath)
	if err != nil {
		return cmdutil.Fail(cmd, err)
	}
	opts.dbURL = dbcli.EffectiveString(
		cmd,
		applyDBURLFlag,
		opts.dbURL,
		projectCfg.StringValue(projectconfig.StringDatabaseURL),
	)
	opts.devURL = dbcli.EffectiveString(
		cmd,
		applyDevURLFlag,
		opts.devURL,
		projectCfg.StringValue(projectconfig.StringDevURL),
	)
	policy := nativeDiffPolicy(projectCfg)

	if strings.TrimSpace(opts.dbURL) == "" {
		return cmdutil.Fail(cmd, fmt.Errorf("database URL is required"))
	}
	if dialect, dialectErr := atlasurl.DialectFromURL(opts.dbURL); dialectErr == nil {
		if err := sqlitevirtual.ValidateToggle(dialect); err != nil {
			return cmdutil.Fail(cmd, err)
		}
	}
	if strings.TrimSpace(opts.planPath) != "" {
		return runSchemaApplyPlanFile(cmd, opts)
	}
	if len(opts.rootDirs) == 0 && len(opts.schemaFiles) == 0 && len(opts.toURLs) == 0 {
		return cmdutil.Fail(cmd, fmt.Errorf(
			"a desired schema source is required: pass --%s and/or --%s, or --%s",
			applyRootDirFlag, applySchemaFileFlag, applyToFlag))
	}
	if err := atlasfilter.ValidateIncludeSelectors(opts.include); err != nil {
		return cmdutil.Fail(cmd, err)
	}
	if len(opts.toURLs) > 0 {
		// Classification rejects unsupported schemes and source conflicts, and
		// the dev-database requirement is checked here, before the target
		// database is contacted.
		set, err := atlassource.ClassifySet("--"+applyToFlag, opts.toURLs, atlassource.ProjectEnv{})
		if err != nil {
			return cmdutil.Fail(cmd, err)
		}
		if err := set.EnsureDevDatabase(opts.devURL); err != nil {
			return cmdutil.Fail(cmd, err)
		}
	}
	txMode, err := migrator.ParseMigrationTxMode(opts.txMode)
	if err != nil {
		return cmdutil.Fail(cmd, err)
	}
	lockTimeout, err := atlasschema.ParseApplyLockTimeout(opts.lockTimeout)
	if err != nil {
		return cmdutil.Fail(cmd, err)
	}
	connectTimeout, err := dbcli.ParseConnectTimeout(
		dbcli.EffectiveString(
			cmd,
			dbcli.ConnectTimeoutFlagName,
			opts.connectTimeout,
			projectCfg.StringValue(projectconfig.StringMigrationConnectTimeout),
		))
	if err != nil {
		return cmdutil.Fail(cmd, err)
	}

	connectCtx, cancel := dbcli.ConnectContext(cmd.Context(), connectTimeout)
	defer cancel()
	conn, err := dbschema.ConnectToDatabase(connectCtx, opts.dbURL)
	if err != nil {
		return cmdutil.Fail(cmd, fmt.Errorf("connect to --%s: %w", applyDBURLFlag, err))
	}
	defer dbschema.CloseAndWarn(conn)

	var desired *goschema.Database
	if len(opts.rootDirs) > 0 || len(opts.schemaFiles) > 0 {
		desired, err = schemaload.LoadContext(cmd.Context(), schemaload.Options{
			RootDirs:    opts.rootDirs,
			SchemaFiles: opts.schemaFiles,
			Dialect:     conn.Info().Dialect,
		})
		if err != nil {
			return cmdutil.Fail(cmd, err)
		}
	}

	// The apply lock is held across inspection, planning, simulation,
	// confirmation, and execution, so the plan cannot go stale between
	// planning and applying. The deferred release covers every exit path.
	applyLock, err := atlasschema.AcquireApplyLock(cmd.Context(), conn, "", lockTimeout)
	if err != nil {
		return cmdutil.Fail(cmd, err)
	}
	defer releaseSchemaApplyLock(cmd, applyLock)
	noteSchemaApplyLockUnsupported(cmd, opts.lockTimeout, applyLock, conn.Info().Dialect)

	plan, err := atlasschema.PrepareApply(cmd.Context(), conn, atlasschema.ApplyRuntimeOptions{
		DevURL:      opts.devURL,
		ToURLs:      opts.toURLs,
		Desired:     desired,
		Exclude:     opts.exclude,
		Schemas:     dbcli.ParseSchemas(opts.schemas),
		Include:     opts.include,
		Policy:      policy,
		TxMode:      txMode,
		DryRun:      opts.dryRun,
		Diagnostics: cmd.ErrOrStderr(),
	})
	if err != nil {
		return cmdutil.Fail(cmd, err)
	}
	if !plan.HasChanges() {
		fmt.Fprintln(cmd.OutOrStdout(), "Schema is synced, no changes to be made.")
		return nil
	}

	sqlText := plan.SQL()
	statements := plan.Statements()
	if opts.edit {
		edited, err := editSchemaApplySQL(cmd.Context(), sqlText)
		if err != nil {
			return cmdutil.Fail(cmd, err)
		}
		sqlText = edited
		statements = atlasschema.SplitApplyStatements(sqlText, conn.Info().Dialect)
	}
	printSchemaApplyPlan(cmd.OutOrStdout(), sqlText)
	if opts.dryRun {
		return nil
	}
	if err := validateSchemaApplyConcurrentIndexPolicy(txMode, conn, statements); err != nil {
		return cmdutil.Fail(cmd, err)
	}
	// The dev database rehearses the exact ordered statements that would be
	// applied — including edited SQL — and a failed rehearsal refuses the
	// apply before the target is touched.
	if err := plan.SimulateOnDev(cmd.Context(), atlasschema.SimulateOptions{
		DevURL:      opts.devURL,
		TargetURL:   opts.dbURL,
		DesiredURLs: opts.toURLs,
		Statements:  statements,
	}); err != nil {
		return cmdutil.Fail(cmd, err)
	}

	ok, err := confirmSchemaApply(cmd, opts)
	if err != nil {
		return cmdutil.Fail(cmd, err)
	}
	if !ok {
		return nil
	}

	if opts.edit {
		// The edited SQL replaces the prepared plan as the executable payload.
		conn.SchemaWriter().SetDryRun(false)
		if err := atlasschema.ApplySQL(cmd.Context(), conn, txMode, sqlText); err != nil {
			return cmdutil.Fail(cmd, fmt.Errorf("apply schema changes: %w", err))
		}
	} else if err := plan.Execute(cmd.Context()); err != nil {
		return cmdutil.Fail(cmd, fmt.Errorf("apply schema changes: %w", err))
	}
	fmt.Fprintln(cmd.OutOrStdout(), "Schema apply completed successfully.")
	return nil
}

// runSchemaApplyPlanFile executes a pre-approved plan file saved by
// `ptah schema plan` instead of re-planning. The plan's source fingerprint is
// verified against the live database first: a drifted target refuses to
// execute, which is the entire value of a pre-approved plan.
func runSchemaApplyPlanFile(cmd *cobra.Command, opts schemaApplyOptions) error {
	if err := validateSchemaApplyPlanOptions(cmd); err != nil {
		return cmdutil.Fail(cmd, err)
	}
	txMode, err := migrator.ParseMigrationTxMode(opts.txMode)
	if err != nil {
		return cmdutil.Fail(cmd, err)
	}
	lockTimeout, err := atlasschema.ParseApplyLockTimeout(opts.lockTimeout)
	if err != nil {
		return cmdutil.Fail(cmd, err)
	}
	path, err := schemafile.LocalFilePath(opts.planPath)
	if err != nil {
		return cmdutil.Fail(cmd, fmt.Errorf("--%s %q: %w", applyPlanFlag, opts.planPath, err))
	}
	plan, err := atlasschema.ReadPlanFile(path)
	if err != nil {
		return cmdutil.Fail(cmd, err)
	}
	connectTimeout, err := dbcli.ParseConnectTimeout(opts.connectTimeout)
	if err != nil {
		return cmdutil.Fail(cmd, err)
	}

	connectCtx, cancel := dbcli.ConnectContext(cmd.Context(), connectTimeout)
	defer cancel()
	conn, err := dbschema.ConnectToDatabase(connectCtx, opts.dbURL)
	if err != nil {
		return cmdutil.Fail(cmd, fmt.Errorf("connect to --%s: %w", applyDBURLFlag, err))
	}
	defer dbschema.CloseAndWarn(conn)

	// The fingerprint verification is the serialized target inspection of the
	// pre-approved plan path, so the lock is held before it and released on
	// every exit path.
	applyLock, err := atlasschema.AcquireApplyLock(cmd.Context(), conn, "", lockTimeout)
	if err != nil {
		return cmdutil.Fail(cmd, err)
	}
	defer releaseSchemaApplyLock(cmd, applyLock)
	noteSchemaApplyLockUnsupported(cmd, opts.lockTimeout, applyLock, conn.Info().Dialect)

	if err := atlasschema.VerifyPlanTarget(conn, plan); err != nil {
		return cmdutil.Fail(cmd, err)
	}

	printSchemaApplyPlan(cmd.OutOrStdout(), plan.SQL())
	if opts.dryRun {
		return nil
	}
	if err := validateSchemaApplyConcurrentIndexPolicy(txMode, conn, plan.StatementSQL()); err != nil {
		return cmdutil.Fail(cmd, err)
	}
	ok, err := confirmSchemaApply(cmd, opts)
	if err != nil {
		return cmdutil.Fail(cmd, err)
	}
	if !ok {
		return nil
	}

	conn.SchemaWriter().SetDryRun(false)
	if err := atlasschema.ApplySQL(cmd.Context(), conn, txMode, plan.SQL()); err != nil {
		return cmdutil.Fail(cmd, fmt.Errorf("apply schema changes: %w", err))
	}
	fmt.Fprintln(cmd.OutOrStdout(), "Schema apply completed successfully.")
	return nil
}

// validateSchemaApplyPlanOptions rejects flags that would recompute or
// reshape the pre-approved plan: the plan file already fixes the desired
// state, the exclude patterns, and the exact SQL that was reviewed.
func validateSchemaApplyPlanOptions(cmd *cobra.Command) error {
	conflicts := []struct {
		flag   string
		reason string
	}{
		{applyToFlag, "the plan file already fixes the desired state"},
		{applyRootDirFlag, "the plan file already fixes the desired state"},
		{applySchemaFileFlag, "the plan file already fixes the desired state"},
		{applyDevURLFlag, "the plan is already computed; there is nothing to re-plan on a dev database"},
		{applyExcludeFlag, "the plan file records the exclude patterns it was computed with"},
		{applyEditFlag, "a pre-approved plan must execute exactly as reviewed; recompute the plan with `ptah schema plan` instead"},
		{dbcli.SchemasFlagName, "the plan file already fixes the planned schema objects"},
		{applyIncludeFlag, "the plan file already fixes the planned schema objects"},
	}
	for _, conflict := range conflicts {
		if cmd.Flags().Changed(conflict.flag) {
			return fmt.Errorf("ptah schema apply --%s cannot be combined with --%s: %s", applyPlanFlag, conflict.flag, conflict.reason)
		}
	}
	return nil
}

func validateSchemaApplyConcurrentIndexPolicy(
	txMode migrator.MigrationTxMode,
	conn *dbschema.DatabaseConnection,
	statements []string,
) error {
	if len(statements) == 0 {
		return nil
	}
	if conn.Info().Dialect != "postgres" && conn.Info().Dialect != "postgresql" {
		return nil
	}
	if txMode == migrator.MigrationTxModeNone {
		return nil
	}
	for _, statement := range statements {
		if strings.Contains(strings.ToUpper(statement), "CREATE INDEX CONCURRENTLY") {
			return fmt.Errorf("the concurrent-index diff policy requires --tx-mode none for schema apply")
		}
	}
	return nil
}

// editSchemaApplySQL round-trips the planned SQL through the operator's
// editor ($VISUAL, then $EDITOR) via a temporary file and returns the edited
// text, which replaces the prepared plan for display, policy validation, and
// execution.
func editSchemaApplySQL(ctx context.Context, sqlText string) (string, error) {
	file, err := os.CreateTemp("", "ptah-schema-apply-*.sql")
	if err != nil {
		return "", fmt.Errorf("create schema apply edit file: %w", err)
	}
	path := file.Name()
	defer os.Remove(path)
	if _, err := file.WriteString(sqlText); err != nil {
		_ = file.Close()
		return "", fmt.Errorf("write schema apply edit file: %w", err)
	}
	if err := file.Close(); err != nil {
		return "", fmt.Errorf("close schema apply edit file: %w", err)
	}
	if err := editor.Open(ctx, "", path); err != nil {
		return "", err
	}
	edited, err := os.ReadFile(path)
	if err != nil {
		return "", fmt.Errorf("read edited schema apply SQL: %w", err)
	}
	return string(edited), nil
}

func printSchemaApplyPlan(out io.Writer, sqlText string) {
	fmt.Fprintln(out, "Planned schema changes:")
	fmt.Fprintln(out, strings.TrimSpace(sqlText))
}

func confirmSchemaApply(cmd *cobra.Command, opts schemaApplyOptions) (bool, error) {
	if opts.autoApprove {
		fmt.Fprintln(cmd.OutOrStdout(), "Auto-approval enabled; applying schema changes.")
		return true, nil
	}
	prompt := cmd.OutOrStdout()
	fmt.Fprint(prompt, "Apply these schema changes? Type 'YES' to confirm: ")
	var confirmation string
	if _, err := fmt.Fscan(cmd.InOrStdin(), &confirmation); err != nil {
		return false, fmt.Errorf("read schema apply confirmation: %w", err)
	}
	if confirmation != "YES" {
		fmt.Fprintln(prompt, "Schema apply canceled.")
		return false, nil
	}
	fmt.Fprintln(prompt)
	return true, nil
}

// releaseSchemaApplyLock releases the schema apply lock on every exit path.
// Release runs on its own bounded background context, so it also works when
// the command context has already been canceled.
func releaseSchemaApplyLock(cmd *cobra.Command, lock *atlasschema.ApplyLock) {
	if err := lock.Release(); err != nil {
		fmt.Fprintf(cmd.ErrOrStderr(), "warning: failed to release schema apply lock: %v\n", err)
	}
}

// noteSchemaApplyLockUnsupported surfaces the capability decision for
// dialects without advisory-lock semantics: an explicitly requested
// --lock-timeout is ignored and the apply proceeds without a database lock.
func noteSchemaApplyLockUnsupported(
	cmd *cobra.Command,
	requestedTimeout string,
	lock *atlasschema.ApplyLock,
	dialect string,
) {
	if strings.TrimSpace(requestedTimeout) == "" || lock.Supported() {
		return
	}
	fmt.Fprintf(cmd.ErrOrStderr(),
		"note: schema apply locking is not supported for dialect %q; --%s is ignored and the apply proceeds without a database lock\n",
		dialect, applyLockTimeoutFlag)
}

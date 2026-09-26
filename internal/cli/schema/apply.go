package schema

import (
	"context"
	"fmt"
	"io"
	"os"
	"slices"
	"strings"
	"time"

	"github.com/spf13/cobra"

	"ptah.run/config/projectconfig"
	"ptah.run/core/schemamodel"
	"ptah.run/dbschema"
	"ptah.run/internal/atlasfilter"
	"ptah.run/internal/atlasschema"
	"ptah.run/internal/atlassource"
	"ptah.run/internal/atlasurl"
	"ptah.run/internal/cli/internal/cmdflags"
	"ptah.run/internal/cli/internal/cmdutil"
	"ptah.run/internal/cli/internal/dbcli"
	"ptah.run/internal/cli/internal/editor"
	"ptah.run/internal/cli/internal/migrateflags"
	"ptah.run/internal/cli/internal/schemaroot"
	"ptah.run/internal/devdocker"
	"ptah.run/internal/schemafile"
	"ptah.run/internal/schemaload"
	"ptah.run/internal/sqlitevirtual"
	"ptah.run/migration/diffpolicy"
	"ptah.run/migration/migrator"
)

const (
	applyDBURLFlag           = "db-url"
	applyRootDirFlag         = "root-dir"
	applySchemaFileFlag      = "schema-file"
	applyToFlag              = "to"
	applyDevURLFlag          = "dev-url"
	applyDryRunFlag          = "dry-run"
	applyAutoApproveFlag     = "auto-approve"
	applyEditFlag            = "edit"
	applyTxModeFlag          = "tx-mode"
	applyLockTimeoutFlag     = "lock-timeout"
	applyIncludeFlag         = "include"
	applyExcludeFlag         = "exclude"
	applyProtectedTableFlag  = "protected-table"
	applyPlanFlag            = "plan"
	applyRequireApprovalFlag = "require-approval"
	applyJSONFlag            = "json"
)

type schemaApplyOptions struct {
	dbURL           string
	rootDirs        []string
	schemaFiles     []string
	toURLs          []string
	devURL          string
	dryRun          bool
	autoApprove     bool
	edit            bool
	txMode          string
	lockTimeout     string
	schemas         string
	include         []string
	exclude         []string
	protectedTables []string
	planPath        string
	requireApproval bool
	allowedSigners  string
	approvalSigner  string
	jsonOutput      bool
	plainHTTP       bool
	connectTimeout  string
	configPath      string
	envName         string
	// devServerDisposable is what [devdocker.DisposableServerDeclared]
	// resolved for this run.
	devServerDisposable bool
}

// schemaApplyLockSession runs one apply with the lock held. The callback is
// handed the pinned session and nothing else: whether that session carries a
// real database lock is settled from --db-url before the connection is opened,
// so no caller reads the lock back here.
type schemaApplyLockSession func(
	context.Context,
	*dbschema.DatabaseConnection,
	string,
	time.Duration,
	func(*dbschema.DatabaseConnection) error,
) (runErr, releaseErr error)

func withSchemaApplyLockSession(
	ctx context.Context,
	conn *dbschema.DatabaseConnection,
	name string,
	timeout time.Duration,
	use func(*dbschema.DatabaseConnection) error,
) (runErr, releaseErr error) {
	return atlasschema.WithApplyLockSession(
		ctx,
		conn,
		name,
		timeout,
		func(session *dbschema.DatabaseConnection, _ *atlasschema.ApplyLock) error {
			return use(session)
		},
	)
}

func newSchemaApplyCommand() *cobra.Command {
	opts := schemaApplyOptions{}
	cmd := &cobra.Command{
		Use:   "apply",
		Short: "Apply a desired schema directly to a database",
		Long: `Apply a desired schema directly to the --db-url database, without migration
files: the live schema is compared with the desired schema, the resulting SQL
plan is shown, and after confirmation the plan is executed.

The desired schema comes from native schema files or OCI artifacts
(--schema-file, repeatable), Go annotations (--root-dir, repeatable), or --to
source URLs (a database URL whose live schema becomes the desired schema, or an
Atlas-format migration directory replayed on the required --dev-url dev
database).

Safety semantics: a session advisory lock serializes concurrent applies
against one target (--lock-timeout bounds the wait, and the command refuses a
typed --lock-timeout against a dialect that takes no such lock rather than
applying unlocked with it); when --dev-url is set the exact ordered plan is
rehearsed on the dev database first and a failed rehearsal refuses the apply
with the target unchanged; the plan must be confirmed interactively unless
--auto-approve is set; --dry-run prints the plan without applying, and asks
for the same lock, so it is refused on the same targets. With --edit the
planned SQL opens in $VISUAL or $EDITOR before confirmation, and the edited
SQL is what gets applied. With --plan <path>, a pre-approved plan file saved
by "ptah schema plan" is executed instead of re-planning, after verifying the
database still matches the plan's source fingerprint. --plan refuses
--protected-table, typed or set in the environment: the fence is read when the
plan is computed, so pass it to "ptah schema plan", which refuses to save a
plan that would change a fenced table. --schemas and --include
positively select what both comparison sides see; --exclude subtracts from the
result. An --include selection that matches neither the target nor the desired
schema refuses the apply rather than reporting a synced schema for work that
did not happen.

--json prints one versioned JSON document on standard output, on success and
on failure: the outcome, the statements the run listed, the plan file's name
and digest with --plan, and a typed refusal code when the apply refused before
anything reached the database. Everything written for a person, including the
confirmation prompt, goes to standard error.`,
		SilenceErrors: true,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return runSchemaApply(cmd, opts)
		},
	}
	flags := cmd.Flags()
	flags.StringVar(&opts.dbURL, applyDBURLFlag, "", "Database URL the schema is applied to (required)")
	flags.StringArrayVar(&opts.rootDirs, applyRootDirFlag, nil, "Root directory to scan for Go entities (repeatable; multiple sources merge into one composite schema)")
	flags.StringArrayVar(&opts.schemaFiles, applySchemaFileFlag, nil, "SQL, YAML, HCL, DBML, or OCI desired-schema source (repeatable)")
	flags.StringArrayVar(&opts.toURLs, applyToFlag, nil, "Desired schema source URL: a database URL or an Atlas-format migration directory (repeatable)")
	flags.StringVar(&opts.devURL, applyDevURLFlag, "", "Dev database URL the plan is rehearsed on before touching the target; also replays migration-directory --to sources")
	flags.BoolVar(&opts.dryRun, applyDryRunFlag, false, "Show planned changes without applying them")
	flags.BoolVar(&opts.autoApprove, applyAutoApproveFlag, false, "Skip interactive approval")
	flags.BoolVar(&opts.edit, applyEditFlag, false, "Open the planned SQL in $VISUAL or $EDITOR before confirmation")
	flags.StringVar(&opts.txMode, applyTxModeFlag, "", "Transaction mode: all, file, or none (default file)")
	flags.StringVar(&opts.lockTimeout, applyLockTimeoutFlag, "",
		"Timeout for acquiring the schema apply lock, such as 10s (empty waits indefinitely); refused when passed here against a dialect that takes no lock")
	dbcli.RegisterURLScopedSchemasFlag(flags, &opts.schemas)
	flags.StringArrayVar(&opts.include, applyIncludeFlag, nil, "Schema objects to include in the apply (Atlas-style selectors)")
	flags.StringArrayVar(&opts.exclude, applyExcludeFlag, nil, "Schema objects to exclude from the apply (Atlas-style selectors)")
	flags.StringArrayVar(&opts.protectedTables, applyProtectedTableFlag, nil,
		"Declared row set this apply refuses to change, by table or schema.table; repeat to add more. There is no override. "+
			"Refused with --plan: fence the plan with \"ptah schema plan --protected-table\" instead")
	flags.StringVar(&opts.planPath, applyPlanFlag, "", "Pre-approved plan file saved by `ptah schema plan`; executed after fingerprint verification")
	flags.BoolVar(&opts.requireApproval, applyRequireApprovalFlag, false,
		"Refuse to execute a --plan that does not carry a verified approval")
	flags.StringVar(&opts.allowedSigners, approvalAllowedSignersFlag, "",
		"OpenSSH allowed_signers file listing approvers (default: ./.ptah/allowed_signers)")
	flags.StringVar(&opts.approvalSigner, approvalSignerFlag, "",
		"Require the approval to belong to this principal")
	flags.BoolVar(&opts.jsonOutput, applyJSONFlag, false,
		"Print the run's result as one versioned JSON document, on success and on failure")
	dbcli.RegisterPlainHTTPFlag(flags, &opts.plainHTTP)
	dbcli.RegisterConnectTimeoutFlag(flags, &opts.connectTimeout)
	dbcli.RegisterConfigFlag(flags, &opts.configPath)
	dbcli.RegisterEnvFlag(flags, &opts.envName)
	// Approval is the one thing an environment must not be able to grant. Every
	// other flag on every native verb binds to a PTAH_* variable, which is what
	// lets a container be configured entirely through env -- and is exactly why
	// this one does not: a variable set once in a shell profile, a CI job or a
	// ConfigMap would turn every later `schema apply` in that environment into
	// an unattended one. It has to be typed, or passed in a command line
	// somebody wrote (stokaro/ptah#852).
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
		SkipDropTable:                 slices.Contains(cfg.Diff.SkipChangeKinds(), diffpolicy.DropTable),
		ConcurrentIndexCreate:         cfg.Diff.ConcurrentIndexCreate(),
		ConcurrentIndexDrop:           cfg.Diff.ConcurrentIndexDrop(),
		ConcurrentIndexCreateDisabled: cfg.Diff.ConcurrentIndexCreateDisabled(),
		OnlineAlter:                   cfg.Diff.OnlineAlterRequested(),
	}
}

func runSchemaApply(cmd *cobra.Command, opts schemaApplyOptions) error {
	return runSchemaApplyWithLockSession(cmd, opts, withSchemaApplyLockSession)
}

func runSchemaApplyWithLockSession(
	cmd *cobra.Command,
	opts schemaApplyOptions,
	lockSession schemaApplyLockSession,
) error {
	run := &applyRun{human: opts.humanOutput(cmd)}
	outcome, err := applySchema(cmd, opts, lockSession, run)
	if err == nil && outcome == atlasschema.ApplyOutcomeApplied {
		fmt.Fprintln(run.human, "Schema apply completed successfully.")
	}
	if opts.jsonOutput {
		run.evidence.Completed = outcome
		run.evidence.Err = err
		if writeErr := writeReport(cmd.OutOrStdout(), atlasschema.NewApplyReport(run.evidence)); writeErr != nil && err == nil {
			err = writeErr
		}
	}
	if err != nil {
		return cmdutil.Fail(cmd, err)
	}
	return nil
}

// applyRun is where one apply writes for a person, and the evidence its --json
// document reports.
type applyRun struct {
	human    io.Writer
	evidence atlasschema.ApplyEvidence
}

// applySchema runs one apply and says how it ended. The outcome is empty when
// the error is not nil.
func applySchema(
	cmd *cobra.Command,
	opts schemaApplyOptions,
	lockSession schemaApplyLockSession,
	run *applyRun,
) (atlasschema.ApplyOutcome, error) {
	// Resolved before the project file is read, so a malformed declaration
	// fails every apply and not only one whose desired state is a migration
	// directory.
	devServerDisposable, err := devdocker.DisposableServerDeclared()
	if err != nil {
		return "", err
	}
	opts.devServerDisposable = devServerDisposable
	if err := sqlitevirtual.ValidateExplicitURLToggle(opts.dbURL); err != nil {
		return "", err
	}
	projectCfg, err := dbcli.LoadProjectConfig(cmd, opts.configPath)
	if err != nil {
		return "", err
	}
	schemaSourceEnv, err := dbcli.SchemaSourceProjectEnv(cmd, projectCfg)
	if err != nil {
		return "", err
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

	if strings.TrimSpace(opts.dbURL) == "" {
		return "", fmt.Errorf("database URL is required")
	}
	if dialect, dialectErr := atlasurl.DialectFromURL(opts.dbURL); dialectErr == nil {
		if err := sqlitevirtual.ValidateToggle(dialect); err != nil {
			return "", err
		}
		if err := decideSchemaApplyLockRequest(cmd, opts.dbURL, dialect); err != nil {
			return "", err
		}
	}
	if strings.TrimSpace(opts.planPath) != "" {
		return applySchemaPlanFile(cmd, opts, lockSession, run)
	}
	if len(opts.rootDirs) == 0 && len(opts.schemaFiles) == 0 && len(opts.toURLs) == 0 {
		return "", fmt.Errorf(
			"a desired schema source is required: pass --%s and/or --%s, or --%s",
			applyRootDirFlag, applySchemaFileFlag, applyToFlag)
	}
	if err := atlasfilter.ValidateIncludeSelectors(opts.include); err != nil {
		return "", err
	}
	if len(opts.toURLs) > 0 {
		// Classification rejects unsupported schemes and source conflicts, and
		// the dev-database requirement is checked here, before the target
		// database is contacted.
		set, err := atlassource.ClassifySet("--"+applyToFlag, opts.toURLs, atlassource.ProjectEnv{})
		if err != nil {
			return "", err
		}
		if err := set.EnsureDevDatabase(opts.devURL); err != nil {
			return "", err
		}
	}
	txMode, err := migrateflags.ParseMigrationTxMode(opts.txMode)
	if err != nil {
		return "", err
	}
	lockTimeout, err := atlasschema.ParseApplyLockTimeout(opts.lockTimeout)
	if err != nil {
		return "", err
	}
	connectTimeout, err := dbcli.ParseConnectTimeout(
		dbcli.EffectiveString(
			cmd,
			dbcli.ConnectTimeoutFlagName,
			opts.connectTimeout,
			projectCfg.StringValue(projectconfig.StringMigrationConnectTimeout),
		))
	if err != nil {
		return "", err
	}

	var desired *schemamodel.Database
	loadOptions := schemaload.Options{
		RootDirs:        opts.rootDirs,
		SchemaFiles:     opts.schemaFiles,
		ProjectEnv:      schemaSourceEnv,
		EnvSelectorFlag: dbcli.SchemaSourceEnvSelectorFlag(cmd),
		PlainHTTP:       opts.plainHTTP,
	}
	if len(opts.rootDirs) > 0 || len(opts.schemaFiles) > 0 {
		declaredVars, varsErr := dbcli.DeclaredVars(cmd)
		if varsErr != nil {
			return "", varsErr
		}
		loadOptions.Vars = declaredVars
	}
	// What an artifact needs of its reader is decided before the target is
	// opened. A layer this build does not know refuses the whole artifact, and
	// a refusal after connecting has already opened the database with whatever
	// the credentials allow (ADR 0019, stokaro/ptah#3291). Everything else
	// reads against the target's dialect and is loaded below.
	resolved, isArtifact, err := schemaload.ResolveBeforeConnect(cmd.Context(), loadOptions)
	if err != nil {
		return "", err
	}
	if isArtifact {
		desired = resolved.Database
	}

	connectCtx, cancel := dbcli.ConnectContext(cmd.Context(), connectTimeout)
	defer cancel()
	conn, err := dbschema.ConnectToDatabase(connectCtx, opts.dbURL)
	if err != nil {
		return "", fmt.Errorf("connect to --%s: %w", applyDBURLFlag, err)
	}
	defer dbschema.CloseAndWarn(conn)

	if err := decideSchemaApplyLockForConnection(cmd, opts.dbURL, conn); err != nil {
		return "", err
	}

	if desired == nil && (len(opts.rootDirs) > 0 || len(opts.schemaFiles) > 0) {
		loadOptions.Dialect = conn.Info().Dialect
		desired, err = schemaload.LoadContext(cmd.Context(), loadOptions)
		if err != nil {
			return "", err
		}
	}

	// Lock ownership and every authoritative target action share one physical
	// session. If that session disappears, the target operation fails with it;
	// no pooled connection can continue the DDL after losing the lock.
	var outcome atlasschema.ApplyOutcome
	runErr, releaseErr := lockSession(
		cmd.Context(),
		conn,
		"",
		lockTimeout,
		func(session *dbschema.DatabaseConnection) error {
			var applyErr error
			outcome, applyErr = runSchemaApplyOnLockedSession(cmd, opts, session, desired, projectCfg, txMode, run)
			return applyErr
		},
	)
	warnSchemaApplyLockRelease(cmd, releaseErr)
	if runErr != nil {
		return "", runErr
	}
	return outcome, nil
}

func runSchemaApplyOnLockedSession(
	cmd *cobra.Command,
	opts schemaApplyOptions,
	conn *dbschema.DatabaseConnection,
	desired *schemamodel.Database,
	projectCfg projectconfig.Config,
	txMode migrator.MigrationTxMode,
	run *applyRun,
) (atlasschema.ApplyOutcome, error) {
	plan, err := atlasschema.PrepareApply(cmd.Context(), conn, atlasschema.ApplyRuntimeOptions{
		ProjectRoot:         schemaroot.Of(opts.rootDirs),
		DevURL:              opts.devURL,
		DevServerDisposable: opts.devServerDisposable,
		ToURLs:              opts.toURLs,
		Desired:             desired,
		Exclude:             opts.exclude,
		Schemas:             dbcli.ParseSchemas(opts.schemas),
		Include:             opts.include,
		Policy:              nativeDiffPolicy(projectCfg),
		ProtectedTables:     opts.protectedTables,
		TxMode:              txMode,
		DryRun:              opts.dryRun,
		Diagnostics:         cmd.ErrOrStderr(),
	})
	if err != nil {
		return "", err
	}
	if !plan.HasChanges() {
		fmt.Fprintln(run.human, "Schema is synced, no changes to be made.")
		return atlasschema.ApplyOutcomeNoChanges, nil
	}

	sqlText := plan.SQL()
	statements := plan.Statements()
	if opts.edit {
		edited, err := editSchemaApplySQL(cmd.Context(), sqlText)
		if err != nil {
			return "", err
		}
		sqlText = edited
		statements = atlasschema.SplitApplyStatements(sqlText, conn.Info().Dialect)
	}
	printSchemaApplyPlan(run.human, sqlText)
	run.evidence.Statements = statements
	if opts.dryRun {
		return atlasschema.ApplyOutcomeDryRun, nil
	}
	info := conn.Info()
	if err := atlasschema.PreflightApplyTransaction(info.Dialect, info.Capabilities, txMode, statements); err != nil {
		return "", err
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
		return "", err
	}

	ok, err := confirmSchemaApply(cmd, opts, run.human)
	if err != nil {
		return "", err
	}
	if !ok {
		return atlasschema.ApplyOutcomeCanceled, nil
	}

	run.evidence.Dispatched = true
	if opts.edit {
		// The edited SQL replaces the prepared plan as the executable payload.
		conn.SchemaWriter().SetDryRun(false)
		if err := atlasschema.ApplySQL(cmd.Context(), conn, txMode, sqlText); err != nil {
			return "", fmt.Errorf("apply schema changes: %w", err)
		}
	} else if err := plan.Execute(cmd.Context()); err != nil {
		return "", fmt.Errorf("apply schema changes: %w", err)
	}
	return atlasschema.ApplyOutcomeApplied, nil
}

// applySchemaPlanFile runs one apply of a saved plan file and says how it
// ended. The outcome is empty when the error is not nil.
func applySchemaPlanFile(
	cmd *cobra.Command,
	opts schemaApplyOptions,
	lockSession schemaApplyLockSession,
	run *applyRun,
) (atlasschema.ApplyOutcome, error) {
	if err := validateSchemaApplyPlanOptions(cmd); err != nil {
		return "", err
	}
	txMode, err := migrateflags.ParseMigrationTxMode(opts.txMode)
	if err != nil {
		return "", err
	}
	lockTimeout, err := atlasschema.ParseApplyLockTimeout(opts.lockTimeout)
	if err != nil {
		return "", err
	}
	path, err := schemafile.LocalFilePath(opts.planPath)
	if err != nil {
		return "", fmt.Errorf("--%s %q: %w", applyPlanFlag, opts.planPath, err)
	}
	// The approval gate runs on the resolved path, before the plan is parsed
	// and long before the database is contacted: a plan nobody approved must
	// not reach a connection, and refusing after a connect would leave the
	// operator wondering what already ran (stokaro/ptah#1857).
	if opts.requireApproval {
		if err := requirePlanApproval(cmd, path, opts.allowedSigners, opts.approvalSigner); err != nil {
			return "", err
		}
	}
	plan, planDigest, err := atlasschema.ReadPlanFileDigest(path)
	if err != nil {
		return "", err
	}
	run.evidence.PlanName = plan.Name
	run.evidence.PlanDigest = planDigest
	// The plan's `-- atlas:txmode` header is part of what was reviewed, so it
	// decides the transaction mode together with --tx-mode, under the rule
	// ptah-compat applies to the same file. It is resolved before the
	// connection, so a refused combination touches no database.
	txMode, err = atlasschema.ResolvePlanTxMode(txMode, path, plan.SQL())
	if err != nil {
		return "", err
	}
	connectTimeout, err := dbcli.ParseConnectTimeout(opts.connectTimeout)
	if err != nil {
		return "", err
	}

	connectCtx, cancel := dbcli.ConnectContext(cmd.Context(), connectTimeout)
	defer cancel()
	conn, err := dbschema.ConnectToDatabase(connectCtx, opts.dbURL)
	if err != nil {
		return "", fmt.Errorf("connect to --%s: %w", applyDBURLFlag, err)
	}
	defer dbschema.CloseAndWarn(conn)

	if err := decideSchemaApplyLockForConnection(cmd, opts.dbURL, conn); err != nil {
		return "", err
	}

	// Fingerprint verification and execution share the session that owns the
	// lock, so no pooled connection can continue after that lock is lost.
	var outcome atlasschema.ApplyOutcome
	runErr, releaseErr := lockSession(
		cmd.Context(),
		conn,
		"",
		lockTimeout,
		func(session *dbschema.DatabaseConnection) error {
			var applyErr error
			outcome, applyErr = runSchemaApplyPlanFileOnLockedSession(cmd, opts, session, plan, txMode, run)
			return applyErr
		},
	)
	warnSchemaApplyLockRelease(cmd, releaseErr)
	if runErr != nil {
		return "", runErr
	}
	return outcome, nil
}

func runSchemaApplyPlanFileOnLockedSession(
	cmd *cobra.Command,
	opts schemaApplyOptions,
	conn *dbschema.DatabaseConnection,
	plan atlasschema.PlanFile,
	txMode migrator.MigrationTxMode,
	run *applyRun,
) (atlasschema.ApplyOutcome, error) {
	if err := atlasschema.VerifyPlanTarget(cmd.Context(), conn, plan); err != nil {
		return "", err
	}

	printSchemaApplyPlan(run.human, plan.SQL())
	run.evidence.Statements = plan.StatementSQL()
	if opts.dryRun {
		return atlasschema.ApplyOutcomeDryRun, nil
	}
	info := conn.Info()
	if err := atlasschema.PreflightApplyTransaction(
		info.Dialect, info.Capabilities, txMode, plan.StatementSQL(),
	); err != nil {
		return "", err
	}
	ok, err := confirmSchemaApply(cmd, opts, run.human)
	if err != nil {
		return "", err
	}
	if !ok {
		return atlasschema.ApplyOutcomeCanceled, nil
	}

	conn.SchemaWriter().SetDryRun(false)
	run.evidence.Dispatched = true
	if err := atlasschema.ApplySQL(cmd.Context(), conn, txMode, plan.SQL()); err != nil {
		return "", fmt.Errorf("apply schema changes: %w", err)
	}
	return atlasschema.ApplyOutcomeApplied, nil
}

// validateSchemaApplyPlanOptions rejects flags that would recompute or
// reshape the pre-approved plan: the plan file already fixes the desired
// schema, the exclude patterns, and the exact SQL that was reviewed.
//
// It also rejects the protected-table fence, which this path cannot decide.
// The computed path refuses a fenced row set only where its own statements
// change it, and a plan file records neither that nor anything it could be
// derived from: managed_rows names the row sets the plan read, and a table the
// plan creates has its rows inserted without a read. A fence accepted here
// would promise a check nothing makes.
//
// Changed is also set for a value a PTAH_* variable supplied, so an exported
// fence is refused as well, and the refusal names the variable rather than a
// flag the command line never carried.
func validateSchemaApplyPlanOptions(cmd *cobra.Command) error {
	conflicts := []struct {
		flag   string
		reason string
	}{
		{applyToFlag, "the plan file already fixes the desired schema"},
		{applyRootDirFlag, "the plan file already fixes the desired schema"},
		{applySchemaFileFlag, "the plan file already fixes the desired schema"},
		{applyDevURLFlag, "the plan is already computed; there is nothing to re-plan on a dev database"},
		{applyExcludeFlag, "the plan file records the exclude patterns it was computed with"},
		{applyEditFlag, "a pre-approved plan must execute exactly as reviewed; recompute the plan with `ptah schema plan` instead"},
		{dbcli.SchemasFlagName, "the plan file already fixes the planned schema objects"},
		{applyIncludeFlag, "the plan file already fixes the planned schema objects"},
		{applyProtectedTableFlag, "the fence is read when the plan is computed, and the plan file does not record " +
			"which declared row sets its statements change; pass it to `ptah schema plan --protected-table`, " +
			"which refuses to save a plan that would change a fenced table, and apply the plan without it"},
	}
	flags := cmd.Flags()
	for _, conflict := range conflicts {
		if !flags.Changed(conflict.flag) {
			continue
		}
		source := "--" + conflict.flag
		if envName, fromEnv := cmdflags.AppliedEnvName(flags, conflict.flag); fromEnv {
			source = envName
		}
		return fmt.Errorf("ptah schema apply --%s cannot be combined with %s: %s", applyPlanFlag, source, conflict.reason)
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

func confirmSchemaApply(cmd *cobra.Command, opts schemaApplyOptions, prompt io.Writer) (bool, error) {
	if opts.autoApprove {
		fmt.Fprintln(prompt, "Auto-approval enabled; applying schema changes.")
		return true, nil
	}
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

// warnSchemaApplyLockRelease reports cleanup separately from the operation.
// The session wrapper has already discarded the physical connection, so a
// failed release cannot leave it in the pool with a lock still attached.
func warnSchemaApplyLockRelease(cmd *cobra.Command, err error) {
	if err != nil {
		fmt.Fprintf(cmd.ErrOrStderr(), "warning: failed to release schema apply lock: %v\n", err)
	}
}

// decideSchemaApplyLockRequest answers a lock timeout aimed at a target whose
// dialect has no session advisory lock. A typed --lock-timeout is refused; a
// value that reached the flag from PTAH_LOCK_TIMEOUT gets a note on stderr and
// the apply goes on unlocked. A dialect that locks takes either spelling.
//
// A bound on a wait the target never makes is an instruction with nowhere to
// go, and taking it silently is what stokaro/ptah#3411 reports.
//
// The caller decides twice, because the dialect a URL names is not always the
// one it reaches. From --db-url the answer lands before the connection, which
// is what lets a sqlite:// refusal arrive without creating the file. From the
// connected dialect it lands after, which is the only place a PostgreSQL-wire
// server can be told apart from PostgreSQL; see
// [decideSchemaApplyLockForConnection].
//
// Only the command line refuses, because PTAH_LOCK_TIMEOUT is not this
// command's variable. `ptah migrations up` and `ptah migrations down` register
// a --lock-timeout of their own, which the same binding fills, and there it
// bounds each migration's statement lock rather than a wait for a session
// advisory lock. An operator who exports the variable for a versioned workflow
// has said nothing about this apply, and "remove PTAH_LOCK_TIMEOUT" would tell
// them to break the configuration it does belong to. A typed --lock-timeout is
// addressed to this command and to nothing else, which is the reading
// [cmdflags.SetOnCommandLine] exists for.
//
// Presence decides, not the value: `--lock-timeout ""` asks for an unbounded
// wait, which is equally a wait this target never makes, so reading the string
// would take that spelling in silence.
func decideSchemaApplyLockRequest(cmd *cobra.Command, dbURL, dialect string) error {
	// A docker:// URL names a dev engine Ptah would start, not a target that
	// exists. DialectFromURL reads the engine out of it, ConnectToDatabase has
	// no arm for the scheme, and an operator who passed one to --db-url has to
	// read that refusal rather than one about a lock on a database they never
	// named.
	if atlasurl.IsDockerURL(dbURL) {
		return nil
	}
	flags := cmd.Flags()
	if envName, fromEnv := cmdflags.AppliedEnvName(flags, applyLockTimeoutFlag); fromEnv {
		noteSchemaApplyLockIgnored(cmd, envName, dialect)
		return nil
	}
	if !cmdflags.SetOnCommandLine(flags, applyLockTimeoutFlag) {
		return nil
	}
	return atlasschema.EnsureApplyLockSupported("--"+applyLockTimeoutFlag, dialect)
}

// decideSchemaApplyLockForConnection repeats the decision against the dialect
// the server reported, which is the authoritative one.
//
// A PostgreSQL-wire URL does not name its product. [dbschema.ConnectToDatabase]
// reads the server's own banner and answers cockroachdb, yugabytedb or spanner
// for a server that names itself, so `postgres://` reaches a target that may
// have no session advisory lock. Deciding from the URL alone applies unlocked
// under an explicit --lock-timeout, against CockroachDB and Spanner.
//
// It runs only where the server disagrees with the URL, so the note an
// environment-set timeout earns is written once rather than per decision. A URL
// whose dialect did not resolve was never decided, so it is decided here.
func decideSchemaApplyLockForConnection(
	cmd *cobra.Command,
	dbURL string,
	conn *dbschema.DatabaseConnection,
) error {
	connected := conn.Info().Dialect
	if urlDialect, err := atlasurl.DialectFromURL(dbURL); err == nil && urlDialect == connected {
		return nil
	}
	return decideSchemaApplyLockRequest(cmd, dbURL, connected)
}

// noteSchemaApplyLockIgnored reports an environment-set lock timeout that this
// target drops. The variable is shared with the versioned commands, so the note
// says which spelling refuses instead of leaving the operator to find out by
// typing it.
func noteSchemaApplyLockIgnored(cmd *cobra.Command, envName, dialect string) {
	if atlasschema.ApplyLockSupported(dialect) {
		return
	}
	fmt.Fprintf(cmd.ErrOrStderr(),
		"note: %s is ignored here: dialect %q has no schema apply lock, so this apply runs unlocked. "+
			"The variable also sets --%s on \"ptah migrations up\", so only a typed --%s refuses\n",
		envName, dialect, applyLockTimeoutFlag, applyLockTimeoutFlag)
}

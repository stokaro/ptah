package atlas

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"slices"
	"strings"
	"time"

	"github.com/spf13/cobra"

	"go.5x5.cz/ptah/cmd/internal/cmdflags"
	"go.5x5.cz/ptah/cmd/internal/cmdutil"
	"go.5x5.cz/ptah/cmd/internal/dbcli"
	"go.5x5.cz/ptah/config/projectconfig"
	"go.5x5.cz/ptah/core/goschema"
	"go.5x5.cz/ptah/core/platform"
	"go.5x5.cz/ptah/dbschema"
	"go.5x5.cz/ptah/internal/atlascompatpolicy"
	"go.5x5.cz/ptah/internal/atlasfilter"
	"go.5x5.cz/ptah/internal/atlasreport"
	"go.5x5.cz/ptah/internal/atlasschema"
	"go.5x5.cz/ptah/internal/atlassource"
	"go.5x5.cz/ptah/internal/envbool"
	"go.5x5.cz/ptah/internal/schemafile"
	migrationlint "go.5x5.cz/ptah/migration/lint"
	"go.5x5.cz/ptah/migration/migrator"
)

type atlasSchemaApplyOptions struct {
	url         string
	filePaths   []string
	toURLs      []string
	devURL      string
	dryRun      bool
	autoApprove bool
	format      string
	exclude     []string
	txMode      string
	schemas     []string
	include     []string
	planURL     string
	lockTimeout string
	lock        atlasLockOptions
	edit        bool
	skipLint    bool
	policy      atlascompatpolicy.Policy
	// formatOutput is derived at run time: true when --format was passed or
	// atlas.hcl provides format.schema.apply.
	formatOutput bool
	// lintPolicy is the atlas.hcl lint block, resolved at run time. It decides
	// both which rules the plan is linted against and whether there is a lint
	// pass at all.
	lintPolicy projectconfig.LintConfig
}

type atlasSchemaApplyDisplayError struct {
	text string
	err  error
}

func (e atlasSchemaApplyDisplayError) Error() string { return e.text }
func (e atlasSchemaApplyDisplayError) Unwrap() error { return e.err }

func displayAtlasSchemaApplyError(err error) error {
	const hclContext = "load --to schema: parse HCL schema: "

	message, found := strings.CutPrefix(err.Error(), hclContext)
	if !found {
		return err
	}
	return atlasSchemaApplyDisplayError{text: message, err: err}
}

func newAtlasSchemaApplyCommand(policy atlascompatpolicy.Policy) *cobra.Command {
	opts := atlasSchemaApplyOptions{
		policy: policy,
		// The pinned community binary accepts lint policy in atlas.hcl but
		// does not run a plan-lint pass during schema apply. Full compatibility
		// retains Ptah's Pro-like pre-apply lint gate. Strict mode has no such
		// pass, so it refuses an authored apply policy before work instead of
		// silently dropping the operator's safety contract.
		skipLint: policy.IsStrictCE(),
	}
	cmd := &cobra.Command{
		Use:   "apply",
		Short: "Apply a desired schema to a database",
		Long: `Atlas OSS ` + "`atlas schema apply`" + ` command path.

Compares a live database from --url with the --to desired state and applies the
generated schema changes directly to the target database. --to accepts local
file:// schema files with .hcl, .yaml, .yml, or .sql extensions, one directly
connectable database URL whose live schema becomes the desired state, one
migration directory (a file:// directory containing atlas.sum) replayed on the
required --dev-url dev database, or one env://<attribute> reference (src,
schema.src, url, dev, migration.dir) resolved through the evaluated atlas.hcl
env. All --to values must be one source kind; unsupported schemes such as
atlas:// fail before the target database is touched. When --env is set, the
selected atlas.hcl env can provide url, schema.src, dev, exclude, schema.mode,
format.schema.apply, and supported diff policy values. With --edit the planned
SQL opens in $VISUAL or $EDITOR before the plan is shown and approved, and the
edited SQL is what gets applied. With --plan file://<path>, a pre-approved
local plan file is executed instead of re-planning; both the Atlas
` + "`.plan.hcl`" + ` format and Ptah's native ` + "`.plan.json`" + ` format are accepted, detected
by content. A JSON plan is verified against its recorded source fingerprint
(a drifted target refuses as stale) and may run without --to. An Atlas-format
plan requires --to, exactly as Atlas does: its hashes are
Atlas-computed and Ptah cannot recompute them, so the plan is verified
semantically instead — the plan is replayed on a dev database (--dev-url, or
an ephemeral SQLite dev database for SQLite targets) starting from the
target's current schema, and the reached state must equal the --to desired
state before the target is touched. After every --plan apply with a desired
state available, the end state is verified again on the target and a mismatch
fails loudly; registry plan URLs are not supported. A session advisory lock
serializes concurrent applies against one target on PostgreSQL, MySQL,
MariaDB, and SQL Server; --lock-timeout bounds how long acquisition waits
(empty waits indefinitely), and dialects without advisory locks proceed
unlocked with a note. A --to that is not already a live database — a schema
file, a schema directory, a migration directory, or an env:// reference to one
of those — requires --dev-url, exactly as ` + "`schema inspect`" + ` and ` + "`schema diff`" + `
do; a database --to needs none. Before the target is touched, --dev-url
rehearses the exact ordered plan on the dev database: the dev database is
reset, the target's current schema is recreated on it, and a failed rehearsal
refuses the apply with the target unchanged. The rehearsal runs under --dry-run
too, so a dry run cannot report a plan the real apply refuses.
--schema and --include positively select
what both comparison sides see: --schema names define the schema universe,
--include selectors pick top-level resources inside it, and --exclude plus
env.schema.mode subtract from the result. A selected object that depends on
an unselected object refuses the plan with an explicit diagnostic instead of
emitting incomplete SQL. An --include selection that matches neither the
target nor the desired state refuses the apply: there is nothing to apply, and
reporting a synced schema would claim success for work that did not happen.

When the selected atlas.hcl env declares a ` + "`lint`" + ` policy, the planned
SQL is linted against exactly the rules that policy names before anything is
executed, and a finding the policy rates as an error refuses the apply.
--skip-lint runs the apply without that check. A project with no lint policy
has no lint pass to skip, so --skip-lint changes nothing there.`,
		RunE: func(cmd *cobra.Command, _ []string) error {
			if opts.policy.IsStrictCE() && cmd.Flags().Changed("plan") {
				return failAtlasCommunityGate(cmd, "atlas schema apply --plan")
			}
			if opts.policy.IsStrictCE() && cmd.Flags().Changed("include") {
				return failAtlasCommunityGate(cmd, "atlas schema apply --include")
			}
			return runAtlasSchemaApply(cmd, opts)
		},
	}
	if policy.IsStrictCE() {
		cmd.Long = strictAtlasSchemaApplyLong()
	}
	flags := cmd.Flags()
	flags.StringVarP(&opts.url, "url", "u", "", "Database URL to apply to")
	flags.StringArrayVar(&opts.toURLs, "to", nil, "Desired schema target URL")
	flags.StringArrayVarP(&opts.filePaths, atlasFileFlagName, atlasFileFlagShorthand, nil, "File or directory containing HCL or SQL files")
	flags.StringVar(&opts.devURL, "dev-url", "", "Dev database URL used by Atlas for planning")
	flags.BoolVar(&opts.dryRun, "dry-run", false, "Show planned changes without applying them")
	flags.BoolVar(&opts.autoApprove, "auto-approve", false, "Skip interactive approval")
	flags.StringVar(&opts.format, "format", "", "Atlas Go template output format")
	flags.StringArrayVar(&opts.exclude, "exclude", nil, "Schema objects to exclude from apply")
	flags.StringVar(&opts.txMode, "tx-mode", "", "Transaction mode: all, file, or none")
	registerAtlasSchemaFlag(flags, &opts.schemas, "Schemas to apply when database URLs are used")
	flags.StringArrayVar(&opts.include, "include", nil, "Schema objects to include in apply")
	flags.StringVar(&opts.planURL, "plan", "", "URL to a pre-planned migration (e.g., file://<name>"+atlasschema.PlanFileSuffixHCL+" or file://<name>"+atlasschema.PlanFileSuffix+")")
	flags.BoolVar(&opts.edit, "edit", false, "Open the generated SQL in an editor")
	flags.StringVar(&opts.lockTimeout, "lock-timeout", "", "Timeout for acquiring the database lock")
	if !policy.IsStrictCE() {
		registerAtlasLockNameFlag(flags, &opts.lock)
		registerAtlasSkipLockFlag(flags, &opts.lock)
		flags.BoolVar(&opts.skipLint, "skip-lint", false, "Skip linting the planned migration")
	}
	if err := cmdflags.DisableEnvBinding(flags, "auto-approve"); err != nil {
		panic(err)
	}
	if err := flags.MarkHidden(atlasFileFlagName); err != nil {
		panic(err)
	}
	cmd.MarkFlagsMutuallyExclusive(atlasFileFlagName, "to")
	// --dry-run and --auto-approve contradict each other: one asks for the plan
	// and no execution, the other for execution with no prompt. The pinned
	// community binary v1.3.0 refuses the pair at exit 1 while Ptah printed the
	// plan at exit 0 (stokaro/ptah#1231 case 5). Nothing is lost by refusing:
	// --auto-approve had no effect on a run that executes nothing, so the pair
	// never reached a behavior that --dry-run alone does not have.
	//
	// The question is what the operator typed, which is why this is not
	// cmd.MarkFlagsMutuallyExclusive. That reads pflag's Changed bit, and
	// Ptah's environment binding sets Changed when it applies PTAH_DRY_RUN, so
	// the group refused `--auto-approve` alone whenever that variable was
	// exported -- a command line the pinned binary accepts at exit 0, refused
	// here because of a variable that binary does not have, with a message
	// naming a flag absent from the command. PTAH_DRY_RUN is a documented Ptah
	// capability on this surface and compatibility does not remove it: with the
	// variable set and --auto-approve typed the run does what --dry-run alone
	// does, printing the plan and applying nothing. The wording below is
	// cobra's own, which is where that binary's identical sentence comes from.
	cmd.PreRunE = func(cmd *cobra.Command, _ []string) error {
		return cmdflags.MutuallyExclusiveOnCommandLine(cmd.Flags(), "dry-run", "auto-approve")
	}
	cmdutil.ConfigureCommandArgs(cmd, cmdutil.NoPositionalArgsHint("name the database with -u/--url and the desired schema with --to"))
	return cmd
}

func strictAtlasSchemaApplyLong() string {
	return `Atlas OSS ` + "`atlas schema apply`" + ` command path.

Compares the live database from --url with the --to desired state and applies
the generated changes. A local desired schema or migration directory requires
--dev-url so the plan can be rehearsed before the target is touched. --dry-run
prints the plan without applying it, and --auto-approve skips the prompt.

Strict compatibility exposes only the pinned Community Edition command and
flag surface. The Pro-only --plan execution path is community-gated, and the
default ptah-compat policy retains Ptah's complete plan, lint, and lock
capabilities.`
}

func runAtlasSchemaApply(cmd *cobra.Command, opts atlasSchemaApplyOptions) error {
	formatOutput := cmd.Flags().Changed("format")
	policy := atlasschema.DiffPolicy{}
	mode := ignoreMissingEnvSelection
	if needsAtlasSchemaApplyConfig(cmd) {
		mode = reportMissingEnvSelection
	}
	projectCfg, loaded, err := loadAtlasProjectConfigForCommand(cmd, mode)
	if err != nil {
		return cmdutil.Fail(cmd, err)
	}
	if loaded {
		if err := opts.policy.ValidateSchemaApplyConfig(projectCfg); err != nil {
			return cmdutil.Fail(cmd, err)
		}
		opts.url = dbcli.EffectiveString(
			cmd,
			"url",
			opts.url,
			projectCfg.StringValue(projectconfig.StringDatabaseURL),
		)
		opts.devURL = dbcli.EffectiveString(
			cmd,
			"dev-url",
			opts.devURL,
			projectCfg.StringValue(projectconfig.StringDevURL),
		)
		opts.toURLs = effectiveStringArray(cmd, "to", opts.toURLs, projectCfg.SchemaSources)
		opts.exclude = effectiveAtlasExclude(cmd, opts.exclude, projectCfg)
		formatValue := projectCfg.StringValue(projectconfig.StringFormatSchemaApply)
		opts.format = dbcli.EffectiveString(cmd, "format", opts.format, formatValue)
		formatOutput = formatOutput || formatValue.Present
		policy, err = atlasDiffPolicy(projectCfg)
		if err != nil {
			return cmdutil.Fail(cmd, err)
		}
		opts.lintPolicy = projectCfg.Lint
	}
	opts.formatOutput = formatOutput
	if strings.TrimSpace(opts.planURL) != "" {
		return runAtlasSchemaApplyPlanFile(cmd, opts)
	}
	if loaded && !cmd.Flags().Changed("to") && !cmd.Flags().Changed(atlasFileFlagName) && len(projectCfg.SchemaSources) > 0 {
		opts.toURLs, err = atlasProjectConfigSchemaURLs(cmd, opts.toURLs)
		if err != nil {
			return cmdutil.Fail(cmd, fmt.Errorf("atlas.hcl schema.src: %w", err))
		}
	}
	if loaded && !cmd.Flags().Changed("to") && !cmd.Flags().Changed(atlasFileFlagName) &&
		atlasExternalSchemaConfigured(projectCfg) {
		opts.toURLs = []string{"env://src"}
	}
	if cmd.Flags().Changed(atlasFileFlagName) {
		opts.toURLs = opts.filePaths
	}
	if formatOutput && strings.TrimSpace(opts.format) == "" {
		return cmdutil.Fail(cmd, fmt.Errorf("--format must not be empty"))
	}
	if formatOutput {
		if err := atlasreport.ValidateSchemaApplyTemplate(opts.format); err != nil {
			return cmdutil.Fail(cmd, err)
		}
	}

	projectEnv := atlassource.ProjectEnv{}
	if loaded {
		projectEnv, err = atlasSourceProjectEnv(cmd, projectCfg)
		if err != nil {
			return cmdutil.Fail(cmd, err)
		}
	}
	if err := validateAtlasSchemaApplyOptions(cmd, opts, projectEnv); err != nil {
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
	lockRequest, err := resolveAtlasLockRequest(cmd, opts.lock)
	if err != nil {
		return cmdutil.Fail(cmd, err)
	}
	// The dev-database requirement lands after the flag-shape checks above and
	// before the target database is contacted, which is the order the community
	// binary reports in: an unparseable --tx-mode or --lock-timeout is named
	// there even when --dev-url is also missing.
	if err := ensureAtlasSchemaApplyDevURL(opts, projectEnv); err != nil {
		return cmdutil.Fail(cmd, err)
	}

	connectCtx, cancel := dbcli.ConnectContext(cmd.Context(), dbcli.DefaultConnectTimeout)
	defer cancel()
	conn, err := dbschema.ConnectToDatabase(connectCtx, opts.url)
	if err != nil {
		return cmdutil.Fail(cmd, fmt.Errorf("connect to --url: %w", err))
	}
	defer dbschema.CloseAndWarn(conn)

	// The apply lock is held across inspection, planning, simulation,
	// confirmation, and execution, so the plan cannot go stale between
	// planning and applying. The deferred release covers every exit path.
	applyLock, err := acquireAtlasSchemaApplyLock(cmd, conn, lockRequest, lockTimeout)
	if err != nil {
		return cmdutil.Fail(cmd, err)
	}
	defer releaseAtlasSchemaApplyLock(cmd, applyLock)
	noteAtlasSchemaApplyLockUnsupported(cmd, opts.lockTimeout, opts.lock, applyLock, conn.Info().Dialect)

	schemaVars, err := atlasVarFlagValues(cmd)
	if err != nil {
		return cmdutil.Fail(cmd, err)
	}
	plan, err := atlasschema.PrepareApply(cmd.Context(), conn, atlasschema.ApplyRuntimeOptions{
		DevURL:      opts.devURL,
		ToURLs:      opts.toURLs,
		Exclude:     opts.exclude,
		Schemas:     opts.schemas,
		Include:     opts.include,
		Policy:      policy,
		TxMode:      txMode,
		DryRun:      opts.dryRun,
		ProjectEnv:  projectEnv,
		Diagnostics: cmd.ErrOrStderr(),

		// Atlas-compatible surface: a schema file written for another tool
		// must not be refused over a name this parser does not model.
		IgnoreUnknownHCLNames:     opts.policy.IgnoreUnknownHCLNames(),
		ValidateDesiredSchema:     opts.policy.ValidateDesiredSchema,
		ValidateCurrentSchema:     opts.policy.ValidateInspectedSchema,
		ValidateMigrationSource:   opts.policy.ValidateMigrationSource,
		ValidateLocalSchemaSource: opts.policy.ValidateLocalSchemaSource,
		Vars:                      schemaVars,
	})
	if err != nil {
		// The pinned community binary v1.3.0 reports the HCL diagnostic itself
		// for this command. The loader's two context wrappers are useful on the
		// native surface, but they are extra bytes on the compatibility boundary.
		// Strip only that measured pair so every unrelated apply error retains
		// its existing context (stokaro/ptah#1235 cell 9.13).
		return cmdutil.Fail(cmd, displayAtlasSchemaApplyError(err))
	}
	if !plan.HasChanges() {
		if formatOutput {
			return writeAtlasSchemaApplyFormat(cmd, opts, conn.Info().Dialect, plan.Statements())
		}
		// No trailing period, and only on this verb. The pinned community binary
		// v1.3.0 writes `Schema is synced, no changes to be made\n` here -- 40
		// bytes against Ptah's 41, read back with xxd and wc -c from an unpiped
		// second `schema apply --auto-approve` over a synced SQLite database --
		// while its `schema diff` answer, `Schemas are synced, no changes to be
		// made.`, does carry one and already matches (stokaro/ptah#1235 9.4).
		// The native `ptah schema apply` sentence is untouched: no parity is owed
		// there and it is not this surface.
		fmt.Fprintln(cmd.OutOrStdout(), "Schema is synced, no changes to be made")
		return nil
	}

	sqlText := plan.SQL()
	statements := plan.Statements()
	if opts.edit {
		edited, err := editAtlasSchemaApplySQL(cmd.Context(), sqlText)
		if err != nil {
			return cmdutil.Fail(cmd, err)
		}
		sqlText = edited
		statements = atlasschema.SplitApplyStatements(sqlText, conn.Info().Dialect)
	}
	formattedPlan := ""
	if formatOutput {
		var err error
		formattedPlan, err = renderAtlasSchemaApplyFormat(opts, conn.Info().Dialect, statements)
		if err != nil {
			return cmdutil.Fail(cmd, err)
		}
		fmt.Fprint(cmd.OutOrStdout(), formattedPlan)
	} else {
		printAtlasSchemaApplyPlan(cmd.OutOrStdout(), sqlText)
	}
	// The lint verdict covers the statements that would run, edits included,
	// and lands before the --dry-run exit so a dry run reports the same refusal
	// the real apply would.
	if err := lintAtlasSchemaApplyPlan(opts, conn.Info().Dialect, statements); err != nil {
		return cmdutil.Fail(cmd, err)
	}
	// Both gates that can refuse this plan sit ABOVE the --dry-run exit, for
	// the same reason the lint verdict does: a dry run exists so a plan can be
	// checked before it is committed to, and a dry run that exits 0 on a plan
	// the real apply refuses turns a CI gate into a false green. The cheap
	// local policy check keeps running first, so the error a user sees when
	// both would fail does not depend on whether --dry-run was passed.
	if err := validateAtlasSchemaApplyDiffPolicy(txMode, conn, statements); err != nil {
		return cmdutil.Fail(cmd, err)
	}
	// The dev database rehearses the exact ordered statements that would be
	// applied — including edited SQL — and a failed rehearsal refuses the
	// apply before the target is touched.
	if err := plan.SimulateOnDev(cmd.Context(), atlasschema.SimulateOptions{
		DevURL:      opts.devURL,
		TargetURL:   opts.url,
		DesiredURLs: opts.toURLs,
		Statements:  statements,
	}); err != nil {
		return cmdutil.Fail(cmd, err)
	}
	if opts.dryRun {
		return nil
	}

	ok, err := confirmAtlasSchemaApply(cmd, opts, formattedPlan)
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
	if formatOutput {
		return nil
	}
	fmt.Fprintln(cmd.OutOrStdout(), "Schema apply completed successfully.")
	return nil
}

func needsAtlasSchemaApplyConfig(cmd *cobra.Command) bool {
	if !cmd.Flags().Changed("url") {
		return true
	}
	// A pre-approved plan file fixes the desired state, so no schema source is
	// needed from flags or atlas.hcl.
	if cmd.Flags().Changed("plan") {
		return false
	}
	return !cmd.Flags().Changed("to") && !cmd.Flags().Changed(atlasFileFlagName)
}

// runAtlasSchemaApplyPlanFile executes a pre-approved local plan file instead
// of re-planning. A native JSON plan is verified against its recorded source
// fingerprint: a drifted target refuses to execute, which is the entire value
// of a pre-approved plan. An Atlas-format `.plan.hcl` plan carries hashes
// Ptah cannot recompute, so it is verified semantically against the required
// --to desired state instead: the plan is rehearsed on a dev database from
// the target's current schema and must reach exactly the desired state.
// Whenever a desired state is available, the end state is verified again on
// the target after the apply.
func runAtlasSchemaApplyPlanFile(cmd *cobra.Command, opts atlasSchemaApplyOptions) error {
	if opts.formatOutput && strings.TrimSpace(opts.format) == "" {
		return cmdutil.Fail(cmd, fmt.Errorf("--format must not be empty"))
	}
	if opts.formatOutput {
		if err := atlasreport.ValidateSchemaApplyTemplate(opts.format); err != nil {
			return cmdutil.Fail(cmd, err)
		}
	}
	if err := validateAtlasSchemaApplyPlanOptions(cmd, opts); err != nil {
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
	lockRequest, err := resolveAtlasLockRequest(cmd, opts.lock)
	if err != nil {
		return cmdutil.Fail(cmd, err)
	}
	path, err := atlasSchemaApplyPlanFilePath(opts.planURL)
	if err != nil {
		return cmdutil.Fail(cmd, err)
	}
	plan, planFormat, err := atlasschema.ReadPlanDocument(path)
	if err != nil {
		return cmdutil.Fail(cmd, err)
	}
	// Atlas requires the desired state to verify a plan file; the Atlas plan
	// format has nothing else to verify against, so the compat tree mirrors
	// Atlas's contract and error for it.
	if planFormat == atlasschema.PlanFormatHCL && len(opts.toURLs) == 0 {
		return cmdutil.Fail(cmd, fmt.Errorf("the flag %q is required to verify the provided plan", "to"))
	}
	if len(opts.toURLs) > 0 {
		if err := ensureLocalSchemaURLs("--to", opts.toURLs); err != nil {
			return cmdutil.Fail(cmd, err)
		}
	}

	connectCtx, cancel := dbcli.ConnectContext(cmd.Context(), dbcli.DefaultConnectTimeout)
	defer cancel()
	conn, err := dbschema.ConnectToDatabase(connectCtx, opts.url)
	if err != nil {
		return cmdutil.Fail(cmd, fmt.Errorf("connect to --url: %w", err))
	}
	defer dbschema.CloseAndWarn(conn)

	schemaVars, err := atlasVarFlagValues(cmd)
	if err != nil {
		return cmdutil.Fail(cmd, err)
	}
	var desired *goschema.Database
	if len(opts.toURLs) > 0 {
		schemaScope, schemaScopeFlag := schemafile.ScopeFromURLs(opts.devURL, opts.url, "url")
		desired, err = schemafile.LoadAll(opts.toURLs, schemafile.Options{
			Dialect:               conn.Info().Dialect,
			IgnoreUnknownHCLNames: opts.policy.IgnoreUnknownHCLNames(),
			SchemaScope:           schemaScope,
			SchemaScopeFlag:       schemaScopeFlag,
			Vars:                  schemaVars,
		})
		if err != nil {
			return cmdutil.Fail(cmd, fmt.Errorf("load --to schema: %w", err))
		}
		if err := opts.policy.ValidateDesiredSchema(desired); err != nil {
			return cmdutil.Fail(cmd, fmt.Errorf("load --to schema: %w", err))
		}
	}

	// The plan verification is the serialized target inspection of the
	// pre-approved plan path, so the lock is held before it and released on
	// every exit path.
	applyLock, err := acquireAtlasSchemaApplyLock(cmd, conn, lockRequest, lockTimeout)
	if err != nil {
		return cmdutil.Fail(cmd, err)
	}
	defer releaseAtlasSchemaApplyLock(cmd, applyLock)
	noteAtlasSchemaApplyLockUnsupported(cmd, opts.lockTimeout, opts.lock, applyLock, conn.Info().Dialect)

	// A JSON plan always carries a fingerprint contract; a Ptah-written
	// `.plan.hcl` carries one too (round-trip). Foreign Atlas hashes cannot be
	// recomputed locally, so those plans rely on the rehearsal gate. The
	// fingerprint shape is not a security boundary — the derivation is public
	// — so it only ever adds a check, never removes one.
	if planFormat == atlasschema.PlanFormatJSON || atlasschema.IsNativeFingerprint(plan.FromFingerprint) {
		if err := atlasschema.VerifyPlanTarget(conn, plan); err != nil {
			return cmdutil.Fail(cmd, err)
		}
	}

	// One statement list is derived here and used for everything that follows
	// — display, policy validation, rehearsal, and execution — so the list
	// that gets verified is exactly the list that runs. Splitting with the
	// connection dialect matters for the MySQL family, whose backslash escapes
	// change where a string literal ends.
	statements := atlasschema.SplitApplyStatements(plan.SQL(), conn.Info().Dialect)
	formattedPlan := ""
	if opts.formatOutput {
		formattedPlan, err = renderAtlasSchemaApplyFormat(opts, conn.Info().Dialect, statements)
		if err != nil {
			return cmdutil.Fail(cmd, err)
		}
		fmt.Fprint(cmd.OutOrStdout(), formattedPlan)
	} else {
		printAtlasSchemaApplyPlan(cmd.OutOrStdout(), plan.SQL())
	}
	// A pre-approved plan file is still SQL this command is about to run
	// against the target, so the project's lint policy applies to it too.
	// Exempting it would make --skip-lint inert on the one path an operator is
	// most likely to be scripting.
	if err := lintAtlasSchemaApplyPlan(opts, conn.Info().Dialect, statements); err != nil {
		return cmdutil.Fail(cmd, err)
	}
	if err := validateAtlasSchemaApplyDiffPolicy(txMode, conn, statements); err != nil {
		return cmdutil.Fail(cmd, err)
	}
	// The rehearsal runs on --dry-run too: a dry run is how an operator
	// test-drives a plan, and it would be useless if verifying a foreign plan
	// required committing to apply it.
	if err := rehearseAtlasSchemaApplyPlan(cmd, conn, rehearsePlanParams{
		policy:      rehearseWhenUnverified,
		format:      planFormat,
		statements:  statements,
		desired:     desired,
		exclude:     plan.Exclude,
		txMode:      txMode,
		devURL:      opts.devURL,
		targetURL:   opts.url,
		desiredURLs: opts.toURLs,
	}); err != nil {
		return cmdutil.Fail(cmd, err)
	}
	if opts.dryRun {
		return nil
	}

	ok, err := confirmAtlasSchemaApply(cmd, opts, formattedPlan)
	if err != nil {
		return cmdutil.Fail(cmd, err)
	}
	if !ok {
		return nil
	}

	conn.SchemaWriter().SetDryRun(false)
	if err := atlasschema.ApplyStatements(cmd.Context(), conn, txMode, statements); err != nil {
		return cmdutil.Fail(cmd, fmt.Errorf("apply schema changes: %w", err))
	}
	// The semantic end-state verification mirrors Atlas: always on whenever a
	// desired state is available, with no flag to disable it.
	if desired != nil {
		if err := atlasschema.VerifyAppliedPlanState(cmd.Context(), conn, desired, plan.Exclude); err != nil {
			return cmdutil.Fail(cmd, err)
		}
	}
	if opts.formatOutput {
		return nil
	}
	fmt.Fprintln(cmd.OutOrStdout(), "Schema apply completed successfully.")
	return nil
}

// rehearsePlanParams carries the plan-file rehearsal inputs derived from the
// loaded plan document and from the command's URLs.
//
// The URLs travel in the struct rather than in a command-options argument so
// that `schema apply --plan` and `schema plan validate` reach the rehearsal
// through one function. The rehearsal is the only gate standing between a
// foreign plan file and a dev database, so a second call site that could drift
// from this one is not worth the convenience.
type rehearsePlanParams struct {
	// policy selects whether a plan whose fingerprint already verified may
	// skip the replay.
	policy     planRehearsalPolicy
	format     atlasschema.PlanFormat
	statements []string
	desired    *goschema.Database
	exclude    []string
	txMode     migrator.MigrationTxMode
	// devURL is the operator-supplied dev database, empty when none was given.
	devURL string
	// targetURL is the database the plan applies to; the simulation refuses a
	// dev URL that resolves to it.
	targetURL string
	// desiredURLs are the --to sources, refused as dev databases for the same
	// reason.
	desiredURLs []string
}

// planRehearsalPolicy selects how hard the dev-database replay is required.
type planRehearsalPolicy uint8

const (
	// rehearseAlways replays every plan format. `schema plan validate` uses
	// it: the verb has no other effect, so a skipped replay would silently
	// answer a narrower question than the one it was asked. A plan can carry a
	// matching from-fingerprint and still not reach --to, and for a validate
	// verb that is the interesting failure.
	//
	// It is deliberately the zero value: a rehearsePlanParams literal that
	// forgets to set policy then gets the STRONGER verification. The other
	// order fails open, and the thing being skipped is the only gate between a
	// foreign plan file and a dev database.
	rehearseAlways planRehearsalPolicy = iota
	// rehearseWhenUnverified replays only what the fingerprint contract cannot
	// already vouch for. `schema apply --plan` uses it: a native JSON plan
	// whose source fingerprint matched this exact database, applied by the
	// operator to their own target, gains nothing from a dev replay.
	rehearseWhenUnverified
)

// planRehearsalDecision is what the dev-database policy resolved to for one
// plan apply.
type planRehearsalDecision struct {
	// skip reports that no rehearsal runs.
	skip bool
	// devURL is the dev database to rehearse on; empty with ephemeral set
	// means the caller must create a throwaway SQLite dev database.
	devURL string
	// ephemeral requests a throwaway SQLite dev database.
	ephemeral bool
}

// resolveAtlasSchemaApplyPlanRehearsal decides how a plan file gets rehearsed.
// The plan format decides, never the fingerprint: the `sha256:` shape is
// public and forgeable, so it must not be able to switch a verification off.
//
//   - An Atlas-format plan is always rehearsed. Its hashes are Atlas-computed
//     and unverifiable locally, so the replay is the only from-state gate.
//     SQLite targets get a throwaway dev database for free (it is just a temp
//     file); every other dialect requires --dev-url, which every measured
//     Atlas invocation passes anyway.
//   - A native JSON plan already passed its fingerprint check, so under
//     rehearseWhenUnverified it is rehearsed only when a dev database was
//     requested explicitly. Under rehearseAlways it is rehearsed like any
//     other plan.
func resolveAtlasSchemaApplyPlanRehearsal(
	policy planRehearsalPolicy,
	format atlasschema.PlanFormat,
	dialect string,
	devURL string,
	desired *goschema.Database,
) (planRehearsalDecision, error) {
	if desired == nil {
		// Without a desired state there is nothing to verify the replay
		// against, so there is no rehearsal to run.
		return planRehearsalDecision{skip: true}, nil
	}
	devURL = strings.TrimSpace(devURL)
	if devURL != "" {
		return planRehearsalDecision{devURL: devURL}, nil
	}
	if policy == rehearseWhenUnverified && format != atlasschema.PlanFormatHCL {
		// A native JSON plan without a dev database is not rehearsed, so the
		// escape lint — which only runs in front of a dev-database replay —
		// does not see it either. That is deliberate, not an oversight: a JSON
		// plan is Ptah-authored, its source fingerprint has already been
		// verified against this exact database, and its statements are applied
		// to the operator's own target, which is the one database the operator
		// is unambiguously entitled to change. The lint exists to protect a
		// dev database from a foreign document; neither half of that applies
		// here. Do not "fix" this by linting the target apply: it would refuse
		// legitimate operator-authored SQL with no security gain.
		return planRehearsalDecision{skip: true}, nil
	}
	if platform.NormalizeDialect(dialect) == platform.SQLite {
		return planRehearsalDecision{ephemeral: true}, nil
	}
	if policy == rehearseAlways {
		return planRehearsalDecision{}, fmt.Errorf(
			"verifying a plan file requires a dev database: the plan is verified by replaying it on a dev "+
				"database and comparing the reached state with --to; pass --dev-url with a %s dev database URL",
			dialect)
	}
	return planRehearsalDecision{}, fmt.Errorf(
		"verifying an Atlas plan file requires a dev database: the plan's from/to hashes are Atlas-computed "+
			"and Ptah cannot recompute them, so the plan is verified by replaying it on a dev database and "+
			"comparing the reached state with --to; pass --dev-url with a %s dev database URL",
		dialect)
}

// rehearseAtlasSchemaApplyPlan runs the pre-apply semantic verification of a
// plan file when the policy calls for it.
func rehearseAtlasSchemaApplyPlan(
	cmd *cobra.Command,
	conn *dbschema.DatabaseConnection,
	params rehearsePlanParams,
) error {
	decision, err := resolveAtlasSchemaApplyPlanRehearsal(
		params.policy, params.format, conn.Info().Dialect, params.devURL, params.desired)
	if err != nil {
		return err
	}
	if decision.skip {
		return nil
	}
	devURL := decision.devURL
	if decision.ephemeral {
		ephemeralURL, cleanup, err := atlasschema.NewEphemeralSQLiteDev()
		if err != nil {
			return err
		}
		defer cleanup()
		devURL = ephemeralURL
	}
	return atlasschema.RehearsePlanStatements(cmd.Context(), conn, params.statements, params.desired, atlasschema.PlanRehearsalOptions{
		DevURL:      devURL,
		TargetURL:   params.targetURL,
		DesiredURLs: params.desiredURLs,
		Exclude:     params.exclude,
		TxMode:      params.txMode,
	})
}

// validateAtlasSchemaApplyPlanOptions rejects flags that would recompute or
// reshape the pre-approved plan: the plan file already fixes the exclude
// patterns, the planned schema objects, and the exact SQL that was reviewed.
// --to and --dev-url combine with --plan the way Atlas
// combines them: --to names the desired state the plan is verified against
// and --dev-url hosts the pre-apply rehearsal.
func validateAtlasSchemaApplyPlanOptions(cmd *cobra.Command, opts atlasSchemaApplyOptions) error {
	if strings.TrimSpace(opts.url) == "" {
		return fmt.Errorf("--url is required")
	}
	conflicts := []struct {
		flag   string
		reason string
	}{
		{atlasFileFlagName, "the plan file already fixes the desired state; name the verification desired state with --to"},
		{"exclude", "the plan file records the exclude patterns it was computed with"},
		{"edit", "a pre-approved plan must execute exactly as reviewed; recompute the plan with `schema plan` instead"},
		{atlasSchemaFlagName, "the plan file already fixes the planned schema objects"},
		{"include", "the plan file already fixes the planned schema objects"},
	}
	for _, conflict := range conflicts {
		if cmd.Flags().Changed(conflict.flag) {
			return fmt.Errorf("atlas schema apply --plan cannot be combined with --%s: %s", conflict.flag, conflict.reason)
		}
	}
	if cmd.Flags().Changed("dev-url") && len(opts.toURLs) == 0 {
		return fmt.Errorf("atlas schema apply --plan with --dev-url requires --to: the rehearsal verifies the plan against the desired schema state")
	}
	return nil
}

// atlasSchemaApplyPlanFilePath resolves a --plan URL to a local plan-file
// path. Registry URLs (for example atlas://repo/plans/name) are rejected:
// Ptah has no plan registry, and the open replacement is a local plan file.
func atlasSchemaApplyPlanFilePath(raw string) (string, error) {
	trimmed := strings.TrimSpace(raw)
	if strings.Contains(trimmed, "://") && !strings.HasPrefix(trimmed, "file://") {
		return "", fmt.Errorf("atlas schema apply accepts registry plan URLs like %q, but Ptah has no plan registry; "+
			"pass a local plan file saved by `schema plan` as --plan file://<path>", raw)
	}
	path, err := schemafile.LocalFilePath(trimmed)
	if err != nil {
		return "", fmt.Errorf("--plan %q: %w", raw, err)
	}
	return path, nil
}

func confirmAtlasSchemaApply(cmd *cobra.Command, opts atlasSchemaApplyOptions, formattedPlan string) (bool, error) {
	if opts.autoApprove {
		if !opts.formatOutput {
			fmt.Fprintln(cmd.OutOrStdout(), "Auto-approval enabled; applying schema changes.")
		}
		return true, nil
	}
	if opts.formatOutput && !strings.HasSuffix(formattedPlan, "\n") {
		fmt.Fprintln(cmd.OutOrStdout())
	}
	return promptAtlasSchemaApplyConfirmation(cmd.OutOrStdout(), cmd.InOrStdin())
}

// lintAtlasSchemaApplyPlan refuses a plan the project's lint policy rates as an
// error, unless --skip-lint was passed.
//
// The pass exists only where a policy does. Atlas CE's `schema apply` neither
// lints nor registers --skip-lint (measured on the pinned community binary), so
// a project without a `lint` block keeps byte-identical CE behavior and
// --skip-lint has nothing to skip there. Where the block is present, the same
// severities that decide `migrate lint` decide this, which is the point: one
// policy, applied wherever SQL is about to reach a database.
func lintAtlasSchemaApplyPlan(opts atlasSchemaApplyOptions, dialect string, statements []string) error {
	if opts.skipLint || len(statements) == 0 {
		return nil
	}
	findings, err := atlasschema.LintPlan(statements, atlasSchemaApplyLintOptions(opts, dialect))
	if err != nil {
		return err
	}
	if len(findings) == 0 {
		return nil
	}
	return fmt.Errorf(
		"the planned changes are refused by the atlas.hcl lint policy:%s\nreview the plan, or rerun with --skip-lint to apply it anyway",
		atlasschema.FormatPlanLintFindings(findings))
}

func atlasSchemaApplyLintOptions(opts atlasSchemaApplyOptions, dialect string) atlasschema.PlanLintOptions {
	rules := make(map[string]migrationlint.RuleConfig, len(opts.lintPolicy.RuleConfigs))
	for code, rule := range opts.lintPolicy.RuleConfigs {
		rules[code] = migrationlint.RuleConfig{
			Severity: migrationlint.Severity(rule.Severity),
			Exclude:  slices.Clone(rule.Exclude),
		}
	}
	return atlasschema.PlanLintOptions{
		Dialect:     dialect,
		RuleConfigs: rules,
		Disabled:    slices.Clone(opts.lintPolicy.DisabledRules),
	}
}

func validateAtlasSchemaApplyDiffPolicy(
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
		if setting := concurrentIndexPolicySetting(statement); setting != "" {
			return fmt.Errorf("atlas.hcl %s requires --tx-mode none for schema apply", setting)
		}
	}
	return nil
}

// concurrentIndexPolicySetting names the diff policy that produced a statement
// PostgreSQL refuses to run inside a transaction block, or "" when the
// statement is safe to wrap.
//
// The UNIQUE spelling is listed explicitly: a unique index renders as
// CREATE UNIQUE INDEX CONCURRENTLY, which a substring test for
// "CREATE INDEX CONCURRENTLY" does not see.
func concurrentIndexPolicySetting(statement string) string {
	upper := strings.ToUpper(statement)
	switch {
	case strings.Contains(upper, "CREATE INDEX CONCURRENTLY"),
		strings.Contains(upper, "CREATE UNIQUE INDEX CONCURRENTLY"):
		return "diff.concurrent_index.create"
	case strings.Contains(upper, "DROP INDEX CONCURRENTLY"):
		return "diff.concurrent_index.drop"
	default:
		return ""
	}
}

// editAtlasSchemaApplySQL round-trips the planned SQL through the operator's
// editor ($VISUAL, then $EDITOR) via a temporary file and returns the edited
// text, which replaces the prepared plan for display, policy validation, and
// execution.
func editAtlasSchemaApplySQL(ctx context.Context, sqlText string) (string, error) {
	return editAtlasSQL(ctx, "schema apply", sqlText)
}

func effectiveStringArray(cmd *cobra.Command, flagName string, flagValues, configValues []string) []string {
	if cmd.Flags().Changed(flagName) || len(configValues) == 0 {
		return flagValues
	}
	return slices.Clone(configValues)
}

func validateAtlasSchemaApplyOptions(
	cmd *cobra.Command,
	opts atlasSchemaApplyOptions,
	projectEnv atlassource.ProjectEnv,
) error {
	if strings.TrimSpace(opts.url) == "" {
		return fmt.Errorf("--url is required")
	}
	if len(opts.toURLs) == 0 {
		return fmt.Errorf("--to is required")
	}
	// Malformed or unsupported --include selectors fail before the target
	// database is contacted.
	if err := atlasfilter.ValidateIncludeSelectors(opts.include); err != nil {
		return err
	}
	// Classification rejects unsupported schemes and source conflicts, and the
	// dev-database requirement is checked here, before the target database is
	// contacted.
	set, err := atlassource.ClassifySet("--to", opts.toURLs, projectEnv)
	if err != nil {
		return err
	}
	return set.EnsureDevDatabase(opts.devURL)
}

// applyWithoutDevURLEnvVar restores planning a non-database desired state with
// no dev database at all.
//
// The default refuses it, because the community binary refuses it: `schema
// apply --to file://… --dry-run` and `--auto-approve` both fail there with
// `--dev-url cannot be empty`, while this binary planned AND applied. The
// capability is not deleted with the default, per AGENTS.md — compatibility
// never removes a capability — and it is an environment variable rather than a
// flag because the conformance cli-surface tier asserts flag parity, so a flag
// that binary does not register would break the promise this surface exists to
// keep.
//
// Native `ptah schema apply` never consults this: it has no parity contract and
// still plans a file desired state against the target without a dev database.
const applyWithoutDevURLEnvVar = "PTAH_ATLAS_APPLY_WITHOUT_DEV_URL"

// ensureAtlasSchemaApplyDevURL requires --dev-url when the desired state is not
// already a live database.
//
// The rule is scoped, not universal, and the scope is measured: with a database
// URL as --to and no --dev-url the community binary exits 0, so a database
// desired state stays exempt here. `schema inspect` and `schema diff` on this
// binary already refuse their non-database sources for want of a dev database
// (internal/atlasschema/inspect_source.go); `schema apply` was the sibling that
// never got the gate, and it is the one that modifies the target.
//
// A migration directory keeps its own longer diagnostic from
// [atlassource.Set.EnsureDevDatabase], which names why the replay needs a dev
// database. Both exit 1; this one only speaks for the sources that had no rule.
func ensureAtlasSchemaApplyDevURL(
	opts atlasSchemaApplyOptions,
	projectEnv atlassource.ProjectEnv,
) error {
	// Resolved before the --dev-url shortcut, so a run that supplies a dev
	// database still refuses a malformed value. That is the whole of a healthy
	// pipeline: the variable exists for the runs that have no dev database, and
	// validating it only there would let a typo survive every other run.
	// See stokaro/ptah#1334.
	allowed, err := atlasApplyWithoutDevURLAllowed()
	if err != nil {
		return err
	}
	if strings.TrimSpace(opts.devURL) != "" {
		return nil
	}
	set, err := atlassource.ClassifySet("--to", opts.toURLs, projectEnv)
	if err != nil || set.Kind == atlassource.KindDatabase || len(set.Sources) == 0 {
		// A classification error was already refused by
		// validateAtlasSchemaApplyOptions with its own diagnostic; nothing here
		// can improve on it.
		return nil //nolint:nilerr // the caller already refused on this error
	}
	if allowed {
		return nil
	}
	return errors.New("--dev-url cannot be empty")
}

// atlasApplyWithoutDevURL is the declaration of the variable, made once, on the
// verb that owns it. See [go.5x5.cz/ptah/internal/envbool].
var atlasApplyWithoutDevURL = envbool.New(applyWithoutDevURLEnvVar, false)

// atlasApplyWithoutDevURLAllowed reports whether a non-database desired state
// may be applied with no dev database. Unset keeps the refusal and a valid false
// spelling keeps it too; an empty or unparsable value is a configuration error
// (stokaro/ptah#1334).
func atlasApplyWithoutDevURLAllowed() (bool, error) {
	return atlasApplyWithoutDevURL.Resolve()
}

func printAtlasSchemaApplyPlan(out io.Writer, sqlText string) {
	fmt.Fprintln(out, "Planned schema changes:")
	fmt.Fprintln(out, strings.TrimSpace(sqlText))
}

func writeAtlasSchemaApplyFormat(
	cmd *cobra.Command,
	opts atlasSchemaApplyOptions,
	driver string,
	statements []string,
) error {
	rendered, err := renderAtlasSchemaApplyFormat(opts, driver, statements)
	if err != nil {
		return err
	}
	_, err = io.WriteString(cmd.OutOrStdout(), rendered)
	return err
}

func renderAtlasSchemaApplyFormat(opts atlasSchemaApplyOptions, driver string, statements []string) (string, error) {
	report := atlasreport.NewSchemaApply(atlasreport.SchemaApplyOptions{
		Driver:     driver,
		URL:        opts.url,
		DryRun:     opts.dryRun,
		Statements: statements,
	})
	var out bytes.Buffer
	if err := atlasreport.WriteSchemaApply(&out, opts.format, report); err != nil {
		return "", err
	}
	return out.String(), nil
}

// acquireAtlasSchemaApplyLock takes the schema apply lock the request selects,
// or takes nothing at all when --skip-lock asked for no lock.
//
// A skipped acquisition returns a nil lock rather than a held no-op one, so
// the difference between "this dialect cannot lock" and "the caller declined
// to lock" stays visible to everything downstream: a nil lock reports no name
// and releases as a no-op.
func acquireAtlasSchemaApplyLock(
	cmd *cobra.Command,
	conn *dbschema.DatabaseConnection,
	request atlasLockRequest,
	timeout time.Duration,
) (*atlasschema.ApplyLock, error) {
	if request.Skip {
		return nil, nil
	}
	return atlasschema.AcquireApplyLock(cmd.Context(), conn, request.Name, timeout)
}

// releaseAtlasSchemaApplyLock releases the schema apply lock on every exit
// path. Release runs on its own bounded background context, so it also works
// when the command context has already been canceled.
func releaseAtlasSchemaApplyLock(cmd *cobra.Command, lock *atlasschema.ApplyLock) {
	if err := lock.Release(); err != nil {
		fmt.Fprintf(cmd.ErrOrStderr(), "warning: failed to release schema apply lock: %v\n", err)
	}
}

// noteAtlasSchemaApplyLockUnsupported surfaces the capability decision for
// dialects without advisory-lock semantics: an explicitly requested
// --lock-timeout or --lock-name is ignored and the apply proceeds without a
// database lock. The note goes to stderr so --format output on stdout stays
// machine-clean.
//
// --lock-name gets its own wording because the two flags fail differently: an
// ignored timeout only means "no wait", while an ignored name means the run is
// not coordinating with whatever else holds that name. The name printed is
// read back from the acquired lock, so it is the name the machinery resolved
// rather than the flag string.
//
// --skip-lock produces no note in either wording. It leaves a nil lock behind,
// and a caller who asked for no lock does not need to be told the dialect has
// none.
func noteAtlasSchemaApplyLockUnsupported(
	cmd *cobra.Command,
	requestedTimeout string,
	lockOpts atlasLockOptions,
	lock *atlasschema.ApplyLock,
	dialect string,
) {
	if lock == nil || lock.Supported() {
		return
	}
	if strings.TrimSpace(lockOpts.name) != "" {
		fmt.Fprintf(cmd.ErrOrStderr(),
			"note: schema apply locking is not supported for dialect %q; the advisory lock %q is not acquired and the apply proceeds without a database lock\n",
			dialect, lock.Name())
		return
	}
	if strings.TrimSpace(requestedTimeout) == "" {
		return
	}
	fmt.Fprintf(cmd.ErrOrStderr(),
		"note: schema apply locking is not supported for dialect %q; --lock-timeout is ignored and the apply proceeds without a database lock\n",
		dialect)
}

func promptAtlasSchemaApplyConfirmation(prompt io.Writer, input io.Reader) (bool, error) {
	fmt.Fprint(prompt, "Apply these schema changes? Type 'YES' to confirm: ")
	var confirmation string
	if _, err := fmt.Fscan(input, &confirmation); err != nil {
		return false, fmt.Errorf("read schema apply confirmation: %w", err)
	}
	if confirmation != "YES" {
		fmt.Fprintln(prompt, "Schema apply canceled.")
		return false, nil
	}
	fmt.Fprintln(prompt)
	return true, nil
}

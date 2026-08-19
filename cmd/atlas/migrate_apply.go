package atlas

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net/url"
	"strconv"
	"strings"

	"github.com/spf13/cobra"

	"go.5x5.cz/ptah/cmd/internal/cmdutil"
	"go.5x5.cz/ptah/cmd/internal/dbcli"
	"go.5x5.cz/ptah/cmd/internal/exitcode"
	"go.5x5.cz/ptah/config/projectconfig"
	"go.5x5.cz/ptah/dbschema"
	"go.5x5.cz/ptah/internal/atlasargs"
	"go.5x5.cz/ptah/internal/atlascompatpolicy"
	"go.5x5.cz/ptah/internal/atlasmigrate"
	"go.5x5.cz/ptah/internal/atlasmigrateimport"
	"go.5x5.cz/ptah/internal/atlasmigratereport"
	"go.5x5.cz/ptah/internal/atlasreport"
	"go.5x5.cz/ptah/internal/dblock"
	"go.5x5.cz/ptah/internal/envbool"
	"go.5x5.cz/ptah/migration/migrator"
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
	toVersion       string
	revisionsSchema string
	lockTimeout     string
	lock            atlasLockOptions
	format          string
}

type atlasMigrateApplyRunOptions struct {
	amount          uint64
	baselineVersion int64
	baselineLabel   string
	baselineSource  atlasMigrateApplyOperandSource
	toVersion       int64
	toVersionSource atlasMigrateApplyOperandSource
	lockRequest     atlasLockRequest
	skipChecks      bool
}

type atlasMigrateApplyOperandSource uint8

const atlasMigrateApplyOperandExplicit atlasMigrateApplyOperandSource = 1

func newAtlasMigrateApplyCommand(policy atlascompatpolicy.Policy) *cobra.Command {
	opts := atlasMigrateApplyOptions{
		// `--dir` defaults to the same directory `migrate status` and
		// `migrate set` already default to (stokaro/ptah#1241 item 2).
		// Measured on the pinned community binary v1.3.0: in a directory
		// holding a hashed ./migrations, `migrate apply --url sqlite://local.db`
		// with no --dir exits 0 and applies, where Ptah exited 1 with
		// `migrations directory is required`. Its own --help documents
		// `(default "file://migrations")` for this flag.
		//
		// The default is a default, never a fallback: --dir naming a directory
		// that is not there still fails rather than quietly reading
		// ./migrations, which is what keeps a typo visible.
		dir:       atlasDefaultMigrationDirURL,
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
			return runAtlasMigrateApply(cmd, policy, opts, args)
		},
	}

	flags := cmd.Flags()
	flags.StringVarP(&opts.url, "url", "u", "", "Database URL to apply migrations to")
	flags.StringVar(&opts.dir, "dir", opts.dir, "Migration directory URL")
	flags.BoolVar(&opts.dryRun, "dry-run", false, "Show migrations without applying them")
	flags.StringVar(&opts.txMode, "tx-mode", opts.txMode, "Transaction mode: file, all, or none")
	flags.StringVar(&opts.execOrder, "exec-order", opts.execOrder, "Execution order: linear, linear-skip, or non-linear")
	// The Atlas meaning: the connected SCHEMA is not empty, adopt it anyway.
	// Native `ptah migrations up` registers the same spelling for the other
	// kind of dirty -- a revision ROW left behind by a body that failed
	// part-way -- and the two share no meaning. See the note at that
	// declaration in cmd/migrateup/migrateup.go, and stokaro/ptah#1253.
	flags.BoolVar(&opts.allowDirty, "allow-dirty", false, "Allow applying migrations when the revision table is dirty")
	flags.StringVar(&opts.baseline, "baseline", "", "Baseline version to mark applied before running pending migrations")
	// The wider Atlas distribution documents --to-version as a string
	// ("migrate to this version, if set"). The pinned CE binary does not
	// register it, so strict CE mode omits it while the complete compatibility
	// surface retains the documented extension.
	flags.StringVar(&opts.revisionsSchema, "revisions-schema", "", "Schema for the Atlas revisions table")
	flags.StringVar(&opts.lockTimeout, "lock-timeout", "", "Timeout for acquiring the migration lock, such as 10s or 2m")
	if !policy.IsStrictCE() {
		flags.StringVar(&opts.toVersion, "to-version", "", "Migrate to this version, if set")
		registerAtlasLockNameFlag(flags, &opts.lock)
		registerAtlasSkipLockFlag(flags, &opts.lock)
	}
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

// resolveAtlasMigrateApplyDirFormat resolves the layout this apply reads, and
// reports the `--dir` query keys it took no meaning from.
//
// The format is resolved ONCE for the whole run: the filesystem that gets
// executed and the format the integrity gate reasons about must be the same
// decision, not two computations that happen to agree (#970).
//
// The report follows the resolution rather than preceding it, so a run refused
// for `?format=totally-bogus` prints that refusal alone; see
// [reportIgnoredDirQuery] for the two rules that fix its position.
func resolveAtlasMigrateApplyDirFormat(
	cmd *cobra.Command,
	configured string,
	query url.Values,
) (atlasmigrateimport.Format, error) {
	format, err := atlasmigrate.ResolveApplyDirFormat(configured, query)
	if err != nil {
		// `migrate apply` registers no --dir-format on either binary, so the
		// query is the only spelling that can carry a rejected value here and
		// the blame is fixed. See migrate_dir_format_error.go for the shared
		// display adaptation.
		return "", atlasDirFormatError("apply", "--dir", err)
	}
	if err := reportIgnoredDirQuery(cmd.ErrOrStderr(), "apply", query); err != nil {
		return "", err
	}
	return format, nil
}

func runAtlasMigrateApply(
	cmd *cobra.Command,
	policy atlascompatpolicy.Policy,
	opts atlasMigrateApplyOptions,
	args []string,
) (runErr error) {
	mode := ignoreMissingEnvSelection
	if needsAtlasMigrateApplyConfig(cmd) {
		mode = reportMissingEnvSelection
	}
	projects, loaded, err := openAtlasProjectsForCommand(cmd, mode)
	if err != nil {
		return err
	}
	defer closeAtlasProjectSet(&projects, &runErr)

	amount, err := atlasmigrate.ParseApplyAmount(args)
	if err != nil {
		return err
	}
	lockRequest, err := resolveAtlasLockRequest(cmd, opts.lock)
	if err != nil {
		return err
	}
	skipChecks, err := resolveAtlasApplySkipChecks(cmd)
	if err != nil {
		return err
	}
	runOpts := atlasMigrateApplyRunOptions{
		amount:          amount,
		baselineSource:  atlasMigrateApplyFlagSource(cmd, "baseline"),
		toVersionSource: atlasMigrateApplyFlagSource(cmd, "to-version"),
		lockRequest:     lockRequest,
		skipChecks:      skipChecks,
	}

	targets := projects.projects
	if !loaded {
		targets = []atlasProject{{}}
	}
	out := cmd.OutOrStdout()
	var targetOutput bytes.Buffer
	cmd.SetOut(io.MultiWriter(out, &targetOutput))
	defer cmd.SetOut(out)
	for index, project := range targets {
		formatted, err := runAtlasMigrateApplyTarget(cmd, policy, project, opts, runOpts)
		if err != nil {
			return err
		}
		if formatted && index < len(targets)-1 && atlasMigrateApplyOutputNeedsSeparator(targetOutput.Bytes()) {
			if _, err := fmt.Fprintln(out); err != nil {
				return fmt.Errorf("write atlas migrate apply report separator: %w", err)
			}
		}
		targetOutput.Reset()
	}
	return nil
}

func atlasMigrateApplyFlagSource(cmd *cobra.Command, name string) atlasMigrateApplyOperandSource {
	flag := cmd.Flags().Lookup(name)
	if flag != nil && flag.Changed {
		return atlasMigrateApplyOperandExplicit
	}
	return 0
}

func runAtlasMigrateApplyTarget(
	cmd *cobra.Command,
	policy atlascompatpolicy.Policy,
	project atlasProject,
	opts atlasMigrateApplyOptions,
	runOpts atlasMigrateApplyRunOptions,
) (bool, error) {
	opts, formatOutput := resolveAtlasMigrateApplyProjectOptions(cmd, project, opts)
	loaded := project.root != nil
	projectCfg := project.Config
	if formatOutput && strings.TrimSpace(opts.format) == "" {
		return formatOutput, fmt.Errorf("--format must not be empty")
	}
	if formatOutput {
		if err := atlasreport.ValidateMigrateApplyTemplate(opts.format); err != nil {
			return formatOutput, err
		}
	}
	// Singular, and the only verb of the family that answers this way. See
	// cmd/atlas/compat_url_diagnostic.go for the measured per-verb table; this
	// check tests the value rather than cobra's Changed bit because the pinned
	// binary answers `--url ""` here exactly as it answers an absent one.
	if opts.url == "" {
		return formatOutput, atlasRequiredURLError(atlasRequiredURLSingular)
	}
	if opts.dir == "" {
		return formatOutput, fmt.Errorf("migrations directory is required")
	}

	var (
		localDir atlasargs.LocalDir
		err      error
	)
	if loaded &&
		!cmd.Flags().Changed("dir") &&
		projectCfg.StringValue(projectconfig.StringMigrationDir).Present {
		localDir, err = project.resolveProjectMigrationDir(opts.dir)
	} else {
		localDir, err = atlasargs.ParseLocalDir(opts.dir)
	}
	if err != nil {
		return formatOutput, fmt.Errorf("atlas migrate apply --dir: %w", err)
	}
	// An atlas.hcl `migration { format = "" }` selects the native Atlas layout
	// rather than being refused. `migrate apply` registers no --dir-format flag,
	// so atlas.hcl is the only way to reach an empty configured format here, and
	// measured against the pinned community binary v1.3.0 a hashed directory
	// under that config applies cleanly and exits 0 (stokaro/ptah#990 item 1).
	// [resolveAtlasMigrateApplyDirFormat] below maps the empty value to the
	// Atlas format, the same as the empty --dir-format the other verbs accept.

	txMode, err := migrator.ParseMigrationTxMode(opts.txMode)
	if err != nil {
		return formatOutput, err
	}
	execOrder, err := migrator.ParseExecOrder(opts.execOrder)
	if err != nil {
		return formatOutput, err
	}
	migrationLockTimeout, err := migrator.ParseMigrationLockTimeout(opts.lockTimeout)
	if err != nil {
		return formatOutput, err
	}

	resolvedDirFormat, err := resolveAtlasMigrateApplyDirFormat(cmd, opts.dirFormat, localDir.Query)
	if err != nil {
		return formatOutput, err
	}
	if err := resolveAtlasMigrateApplyNumericVersions(
		resolvedDirFormat,
		opts,
		&runOpts,
	); err != nil {
		return formatOutput, err
	}

	source, err := project.openLocal(localDir)
	if err != nil {
		return formatOutput, fmt.Errorf("atlas migrate apply --dir: %w", err)
	}
	dir := source.Display()
	captured, err := atlasmigrate.CaptureApplySource(source.FS(), resolvedDirFormat)
	closeErr := source.Close()
	if err != nil {
		return formatOutput, errors.Join(
			fmt.Errorf("atlas migrate apply --dir: %w", err),
			closeErr,
		)
	}
	if closeErr != nil {
		return formatOutput, fmt.Errorf("atlas migrate apply --dir: close migrations directory: %w", closeErr)
	}

	// Integrity gate (#955, #970, #973): the SOURCE directory must carry an
	// atlas.sum and verify against it before anything is read for execution,
	// matching official Atlas, which refuses both a tampered and an unhashed
	// directory before applying a single migration.
	//
	// Ordering is measured, not incidental. The gate runs on the captured
	// source, before the source layout is parsed, because that is where Atlas
	// CE runs it: an unhashed Goose directory whose .sql has no `-- +goose Up`
	// directive is refused with "checksum file not found", not with a
	// conversion error. It also runs before the database connection is opened,
	// so neither a tampered file (including a tampered checkpoint) nor an
	// unhashed directory can execute or create the target database — CE emits
	// the checksum refusal even when --url is unreachable.
	if err := verifyAtlasApplyChecksum(cmd, captured, resolvedDirFormat); err != nil {
		return formatOutput, err
	}
	if err := policy.ValidateMigrationSourceForURL(captured, opts.url); err != nil {
		return formatOutput, err
	}

	migrationFS, err := atlasmigrate.ConvertApplySource(captured, dir, resolvedDirFormat)
	if err != nil {
		return formatOutput, fmt.Errorf("atlas migrate apply --dir: %w", err)
	}
	if err := resolveAtlasMigrateApplyFlywayVersions(
		captured,
		resolvedDirFormat,
		opts,
		&runOpts,
	); err != nil {
		return formatOutput, err
	}

	if err := atlasDatabaseURLDiagnostic(opts.url); err != nil {
		return formatOutput, err
	}
	conn, err := dbschema.ConnectToDatabase(cmd.Context(), opts.url)
	if err != nil {
		return formatOutput, fmt.Errorf("error connecting to database: %w", err)
	}
	defer dbschema.CloseAndWarn(conn)

	linearity, err := flywayLinearityOperands(captured, resolvedDirFormat)
	if err != nil {
		return formatOutput, fmt.Errorf("atlas migrate apply --dir: %w", err)
	}

	cleanScope, plan, err := inspectThenPrepareApply(cmd.Context(), conn, atlasmigrate.ApplyOptions{
		Dir:                      dir,
		FS:                       migrationFS,
		DryRun:                   opts.dryRun,
		ExecOrder:                execOrder,
		OutOfOrderExempt:         linearity.outOfOrderExempt,
		SourceVersions:           linearity.sourceVersions,
		RevisionVersions:         linearity.sourceVersions,
		RevisionChecksums:        linearity.sourceChecksums,
		RevisionTypes:            linearity.revisionTypes,
		RepeatableVersions:       linearity.repeatableVersions,
		TxMode:                   txMode,
		RevisionsSchema:          applyAtlasRevisionsSchemaDefault(opts.revisionsSchema, opts.url),
		MigrationLockTimeout:     migrationLockTimeout,
		MigrationLockName:        runOpts.lockRequest.Name,
		SkipMigrationLock:        runOpts.lockRequest.Skip,
		Amount:                   runOpts.amount,
		ToVersion:                runOpts.toVersion,
		AllowDirty:               opts.allowDirty,
		DiscardRolledBackFailure: true,
		BaselineVersion:          runOpts.baselineVersion,
		SkipChecks:               runOpts.skipChecks,
	})
	if err != nil {
		return formatOutput, remapAtlasExactIdentityError(err, linearity.sourceVersions)
	}
	if err := runAtlasMigrateApplyRefusals(cmd.Context(), atlasMigrateApplyRefusalOperands{
		conn:            conn,
		plan:            plan,
		captured:        captured,
		dirFormat:       resolvedDirFormat,
		linearity:       linearity,
		execOrder:       execOrder,
		allowDirty:      opts.allowDirty,
		baselineVersion: runOpts.baselineVersion,
		cleanScope:      cleanScope,
	}); err != nil {
		return formatOutput, err
	}

	noteAtlasMigrateApplyLockUnsupported(cmd, opts.lock, plan, conn.Info().Dialect)

	out := cmd.OutOrStdout()
	emitApplyStart := func([]int64, []string) {}
	if !formatOutput {
		emitApplyStart = func(selectedVersions []int64, selectedKeys []string) {
			emitAtlasMigrateApplyStart(
				out,
				opts,
				runOpts.baselineVersion,
				runOpts.baselineLabel,
				selectedVersions,
				selectedKeys,
			)
		}
	}
	result, err := plan.ExecuteWithPreflight(
		cmd.Context(),
		func(_ context.Context, lockedPlan migrator.MigrationPlan) error {
			emitApplyStart(lockedPlan.Versions, lockedPlan.VersionKeys)
			return nil
		},
	)
	if err != nil {
		if formatOutput && result.ApplyError != nil {
			writeErr := writeAtlasMigrateApplyFormat(cmd, opts, migrationFS, conn, result)
			if writeErr != nil {
				return formatOutput, fmt.Errorf("%w; additionally failed to write --format output: %v", err, writeErr)
			}
			// The structured report already carries the execution error. Atlas
			// leaves stderr empty in this mode, so return a coded failure that
			// bypasses the generic command diagnostic without losing exit status.
			return formatOutput, exitcode.New(atlasErrorExitCode, err)
		}
		return formatOutput, atlasMigrateApplyExactIdentityError(
			err,
			result.SelectedVersions,
			result.SelectedKeys,
			result.Migrations,
		)
	}
	if handled, err := finishAtlasMigrateApplyFreshNoop(cmd, opts, migrationFS, conn, result); handled || err != nil {
		return formatOutput, err
	}

	if opts.dryRun {
		emitAtlasMigrateApplyDeferredChecks(cmd, result.ChecksDeferred, result.ChecksDeferredKeys)
		if formatOutput {
			return formatOutput, writeAtlasMigrateApplyFormat(cmd, opts, migrationFS, conn, result)
		}
		fmt.Fprintf(out, "Would have applied %d migrations.\n", len(result.SelectedVersions))
		return formatOutput, nil
	}
	if formatOutput {
		return formatOutput, writeAtlasMigrateApplyFormat(cmd, opts, migrationFS, conn, result)
	}
	fmt.Fprintf(out, "Migration complete. Current version: %s\n", atlasApplyCurrentVersionLabel(result.FinalStatus))
	return formatOutput, nil
}

func resolveAtlasMigrateApplyProjectOptions(
	cmd *cobra.Command,
	project atlasProject,
	opts atlasMigrateApplyOptions,
) (atlasMigrateApplyOptions, bool) {
	formatOutput := cmd.Flags().Changed("format")
	if project.root == nil {
		return opts, formatOutput
	}
	projectCfg := project.Config
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
	// atlas.hcl `migration { baseline }` is the config spelling of --baseline,
	// and the flag still wins when it was passed. Measured on the pinned
	// community binary v1.3.0 with `migrate apply --env local --dry-run`
	// against a hashed two-migration directory, exit codes read directly from
	// unpiped invocations:
	//
	//	no baseline                        -> 0  "2 migrations in total"   (control)
	//	baseline = "20260719010000"        -> 0  "from 20260719010000
	//	                                          (1 migrations in total)"
	//
	// ptah-compat answered "2 pending migrations" to BOTH before this line, at
	// exit 0, with a warning that the attribute has no effect. The control is
	// what makes the pair discriminating: the two binaries agree when no
	// baseline is configured, so the divergence belongs to the attribute.
	opts.baseline = dbcli.EffectiveString(
		cmd,
		"baseline",
		opts.baseline,
		projectCfg.StringValue(projectconfig.StringMigrationBaseline),
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
	return opts, formatOutput || formatValue.Present
}

func atlasMigrateApplyExactIdentityError(
	err error,
	versions []int64,
	keys []string,
	migrations []*migrator.Migration,
) error {
	return remapAtlasExactIdentityError(
		err,
		atlasMigrateApplyRevisionIdentities(versions, keys, migrations),
	)
}

func atlasMigrateApplyOutputNeedsSeparator(output []byte) bool {
	trimmed := bytes.TrimLeft(output, " \t\r")
	return len(trimmed) > 0 && !bytes.HasSuffix(trimmed, []byte{'\n'})
}

// noteAtlasMigrateApplyLockUnsupported surfaces the capability decision behind
// an explicitly named migration lock on a dialect that has no advisory locks:
// the name is accepted and then has nothing to name, so the run says so rather
// than letting `--lock-name` read as serialization it did not get.
//
// It fires only when --lock-name was passed, so output for every existing
// invocation is byte-identical. --skip-lock never reaches here: it is mutually
// exclusive with --lock-name, and a caller who asked for no lock does not need
// to be told the dialect has none.
//
// The name comes from the prepared plan's migrator rather than from the flag
// value, so the note reports the lock the machinery resolved.
func noteAtlasMigrateApplyLockUnsupported(
	cmd *cobra.Command,
	lockOpts atlasLockOptions,
	plan atlasmigrate.ApplyPlan,
	dialect string,
) {
	if strings.TrimSpace(lockOpts.name) == "" || plan.MigrationLockSkipped() {
		return
	}
	if dblock.Supported(dialect) {
		return
	}
	fmt.Fprintf(cmd.ErrOrStderr(),
		"note: migration locking is not supported for dialect %q; the advisory lock %q is not acquired and the migrations run without a database lock\n",
		dialect, plan.MigrationLockName())
}

// emitAtlasMigrateApplyDeferredChecks names the pre-migration checks a dry run
// validated but did not evaluate, so a preview never drops a guard silently.
//
// It writes to stderr on purpose. Stdout carries the machine-readable --format
// document, and Atlas emits no field for this, so putting the note there would
// invent one and corrupt a caller's parse.
func emitAtlasMigrateApplyDeferredChecks(cmd *cobra.Command, versions []int64, keys []string) {
	if len(versions) == 0 {
		return
	}
	labels := make([]string, 0, len(versions))
	for index := range versions {
		labels = append(labels, atlasApplyVersionKeyAt(versions, keys, index))
	}
	noun := "migrations"
	if len(versions) == 1 {
		noun = "migration"
	}
	fmt.Fprintf(
		cmd.ErrOrStderr(),
		"Deferred pre-migration checks for %d %s (%s): a dry run does not create the state they assert on, so they are evaluated on apply.\n",
		len(versions),
		noun,
		strings.Join(labels, ", "),
	)
}

func finishAtlasMigrateApplyFreshNoop(
	cmd *cobra.Command,
	opts atlasMigrateApplyOptions,
	migrationFS fs.FS,
	conn *dbschema.DatabaseConnection,
	result atlasmigrate.ApplyResult,
) (bool, error) {
	if len(result.SelectedVersions) != 0 || result.Applied {
		return false, nil
	}
	if opts.format != "" {
		return true, writeAtlasMigrateApplyFormat(cmd, opts, migrationFS, conn, result)
	}
	// No trailing period: the pinned community binary v1.3.0 writes
	// `No migration files to execute\n` on stdout for the same run, 30 bytes,
	// verified byte for byte with xxd on 2026-08-08 against a directory both
	// binaries had already applied. Ptah wrote 31 (stokaro/ptah#1235 finding
	// 9.3). The report model in internal/atlasreport already spells it without
	// the period, so this line was also the odd one out inside Ptah.
	fmt.Fprintln(cmd.OutOrStdout(), "No migration files to execute")
	return true, nil
}

func emitAtlasMigrateApplyStart(
	out io.Writer,
	opts atlasMigrateApplyOptions,
	baselineVersion int64,
	baselineLabel string,
	selectedVersions []int64,
	selectedKeys []string,
) {
	if opts.dryRun {
		fmt.Fprintln(out, "Dry run mode: no changes will be made.")
	}
	if opts.dryRun && baselineVersion > 0 {
		fmt.Fprintf(out, "Would baseline migrations at version %s.\n", baselineLabel)
	}
	if len(selectedVersions) > 0 {
		fmt.Fprintf(out, "Migrating to version %s from %d pending migrations.\n",
			atlasApplyVersionKeyAt(selectedVersions, selectedKeys, len(selectedVersions)-1),
			len(selectedVersions),
		)
	}
}

func newAtlasMigrateApplyVersionTokens(
	captured fs.FS,
	format atlasmigrateimport.Format,
) (flywayVersionTokens, error) {
	covered, err := atlasmigrateimport.FlywayCoveredSourceVersions(captured, format)
	if err != nil {
		return flywayVersionTokens{}, err
	}
	return newFlywayVersionTokens(covered), nil
}

func resolveAtlasMigrateApplyNumericVersions(
	format atlasmigrateimport.Format,
	opts atlasMigrateApplyOptions,
	runOpts *atlasMigrateApplyRunOptions,
) error {
	// Native Atlas and every numeric-prefix foreign layout validate version
	// operands before touching the directory. Preserve that precedence. Flyway
	// is the one exception because the opaque token can only be resolved from
	// the captured source itself.
	if format == atlasmigrateimport.FormatFlyway {
		return nil
	}
	var err error
	runOpts.baselineVersion, runOpts.baselineLabel, err = resolveAtlasMigrateApplyVersion(
		"baseline",
		opts.baseline,
		runOpts.baselineSource,
		flywayVersionTokens{},
	)
	if err != nil {
		return err
	}
	runOpts.toVersion, _, err = resolveAtlasMigrateApplyVersion(
		"to-version",
		opts.toVersion,
		runOpts.toVersionSource,
		flywayVersionTokens{},
	)
	return err
}

func resolveAtlasMigrateApplyFlywayVersions(
	captured fs.FS,
	format atlasmigrateimport.Format,
	opts atlasMigrateApplyOptions,
	runOpts *atlasMigrateApplyRunOptions,
) error {
	if format != atlasmigrateimport.FormatFlyway {
		return nil
	}
	versionTokens, err := newAtlasMigrateApplyVersionTokens(captured, format)
	if err != nil {
		return fmt.Errorf("atlas migrate apply --dir: %w", err)
	}
	runOpts.baselineVersion, runOpts.baselineLabel, err = resolveAtlasMigrateApplyVersion(
		"baseline",
		opts.baseline,
		runOpts.baselineSource,
		versionTokens,
	)
	if err != nil {
		return err
	}
	runOpts.toVersion, _, err = resolveAtlasMigrateApplyVersion(
		"to-version",
		opts.toVersion,
		runOpts.toVersionSource,
		versionTokens,
	)
	return err
}

func resolveAtlasMigrateApplyVersion(
	name,
	value string,
	source atlasMigrateApplyOperandSource,
	tokens flywayVersionTokens,
) (int64, string, error) {
	if value == "" && (source != atlasMigrateApplyOperandExplicit || !tokens.translates()) {
		return 0, "", nil
	}
	if tokens.translates() {
		version, ok := tokens.resolve(value)
		if !ok {
			if name == "baseline" {
				return 0, "", fmt.Errorf("baseline version %q not found", value)
			}
			return 0, "", fmt.Errorf("target version %q was not found in the migration provider", value)
		}
		return version, value, nil
	}
	version, err := atlasmigrate.ParseMigrationVersionFlag(name, value)
	if err != nil {
		return 0, "", err
	}
	return version, strconv.FormatInt(version, 10), nil
}

func atlasApplyVersionKeyAt(versions []int64, keys []string, index int) string {
	if index < len(keys) {
		return keys[index]
	}
	return strconv.FormatInt(versions[index], 10)
}

func atlasApplyCurrentVersionLabel(status *migrator.MigrationStatus) string {
	if status.CurrentVersionKeySet {
		return atlasExactIdentityLabel(status.CurrentVersionKey)
	}
	return strconv.FormatInt(status.CurrentVersion, 10)
}

// verifyAtlasApplyChecksum enforces the atlas.sum integrity gate on the
// captured SOURCE migration filesystem: a missing atlas.sum and a failed
// verification both refuse the whole apply, with output byte-identical to
// `migrate validate` on the same directory.
//
// fsys is the source snapshot, never the converted one. A directory read
// through ?format= is rebuilt in memory as up-only Atlas migrations and has no
// integrity file by construction, but the directory it was read FROM carries
// atlas.sum next to its own migrations — that is the file Atlas CE writes for
// it and verifies against, and the mismatch line it prints names the source
// file (`L2: 1_init.up.sql was edited`, not a converted name).
//
// The two branches differ only in which files the sum covers, so they are two
// verifiers rather than one: a native directory is hashed by Ptah's Atlas-format
// hasher, while a converted one is hashed over the per-format file set Atlas CE
// selects, which for golang-migrate excludes the down file and for Flyway drops
// undo files, squashes baselines, and reaches into subdirectories (#984).
//
// The native branch lives in migrate_integrity_gate.go, shared verbatim with
// `migrate status` and `migrate set`, which read native Atlas directories only
// (#974). Apply keeps the dispatcher because it is the one verb that can also
// reach a converted directory.
func verifyAtlasApplyChecksum(cmd *cobra.Command, fsys fs.FS, format atlasmigrateimport.Format) error {
	if atlasmigrate.ReadsNativeAtlasDir(format) {
		return verifyNativeAtlasDirChecksum(cmd, fsys)
	}
	return verifyConvertedAtlasApplyChecksum(cmd, fsys, format)
}

// verifyConvertedAtlasApplyChecksum verifies a directory laid out in a foreign
// tool's convention against the atlas.sum that directory carries, over exactly
// the file set Atlas CE covers for that layout.
//
// It is [verifyCoveredAtlasDirChecksum] under the apply-side policy: an
// unhashed directory whose covered set is non-empty is refused, because apply
// would otherwise execute migrations nothing verified. `migrate import` shares
// the same verifier under the other policy; see migrate_integrity_gate.go.
func verifyConvertedAtlasApplyChecksum(
	cmd *cobra.Command,
	fsys fs.FS,
	format atlasmigrateimport.Format,
) error {
	return verifyCoveredAtlasDirChecksum(cmd, fsys, format, requireAtlasSum)
}

// applySkipChecksEnvVar gates pre-migration checks on the compat apply path.
//
// The name is not invented here. Native `ptah migrations up` registers a
// --skip-checks flag, and Ptah's generic flag/environment twin convention
// (cmd/internal/cmdflags) already binds that flag to PTAH_SKIP_CHECKS. This
// command reads the same variable so one name means one thing across both
// binaries.
const applySkipChecksEnvVar = "PTAH_SKIP_CHECKS"

// atlasApplySkipChecksFromEnv resolves the pre-migration check bypass for
// `ptah-compat migrate apply` from the environment instead of from a flag.
//
// WHY AN ENVIRONMENT VARIABLE AND NOT A FLAG. Measured, not assumed: Atlas CE
// v1.2.0 rejects `migrate apply --skip-checks` with `unknown flag:
// --skip-checks`, byte-identical to the refusal for a nonsense sibling
// (`--skip-chxxxx`), so it is genuinely unregistered rather than registered and
// community-gated. Atlas's own help surface does not register it on
// `migrate apply` either — across that whole surface `--skip-checks` appears
// only on `migrate down`. Registering it here would therefore put a non-Atlas
// flag on the compat surface and break the conformance cli-surface tier, which
// asserts flag parity against the pinned CE binary. An environment variable is
// invisible to the help surface, which is exactly why it is the sanctioned gate
// for capabilities with no Atlas spelling (precedent:
// PTAH_ALLOW_EXTERNAL_SCHEMA).
//
// WHY THE CAPABILITY IS EXPOSED AT ALL. The checks this bypasses are not only
// Atlas txtar checks.sql sections; they are also `-- +ptah check` directives, a
// Ptah-only construct Atlas has no counterpart for and therefore no oracle
// behavior to reproduce. Before this, a directory carrying those directives was
// enforceable through ptah-compat with no escape hatch of any kind, while
// native `ptah migrations up --skip-checks` had one — a Pro-shaped capability
// reachable only from the native verb, which is what stokaro/ptah#951 exists to
// close.
//
// An invalid value is a hard error rather than a silent false. A typo in a CI
// environment file must not read as "checks enforced" when the operator
// believes they are bypassed; the same choice is already made for the
// PTAH_<FLAG> fallbacks on `migrate down`.
func atlasApplySkipChecksFromEnv() (bool, error) {
	return applySkipChecks.Resolve()
}

// applySkipChecks is the declaration of the variable, made once, on the verb
// that owns it. See [go.5x5.cz/ptah/internal/envbool].
//
// It carries the one change stokaro/ptah#1334 makes to this reader: an
// explicitly EMPTY value used to read as unset here, which folded a
// `PTAH_SKIP_CHECKS=` left behind by a broken shell expansion into "checks
// enforced" without a word. Absence still selects the default; a present value
// has to parse.
// It is [go.5x5.cz/ptah/internal/envbool.Gated]: bypassing `-- +ptah check`
// directives is a Ptah-only capability the pinned community binary has no
// spelling for, so strict compatibility must refuse an enabled value rather
// than run a migration set the oracle would have enforced.
var applySkipChecks = envbool.New(applySkipChecksEnvVar, false, envbool.Gated)

// resolveAtlasApplySkipChecks resolves the bypass and announces an active one.
//
// The warning is not optional decoration. A flag leaves a trace in the command
// line an operator can read back; an environment variable does not, so a run
// whose safety gate is off has to say so on its own.
func resolveAtlasApplySkipChecks(cmd *cobra.Command) (bool, error) {
	skip, err := atlasApplySkipChecksFromEnv()
	if err != nil {
		return false, err
	}
	if skip {
		fmt.Fprintf(cmd.ErrOrStderr(),
			"warning: %s is set; pre-migration checks are bypassed for this run\n",
			applySkipChecksEnvVar,
		)
	}
	return skip, nil
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
	if result.ErrorText != "" {
		result.ErrorText = atlasMigrateApplyExactIdentityError(
			errors.New(result.ErrorText),
			result.SelectedVersions,
			result.SelectedKeys,
			result.Migrations,
		).Error()
	}
	return atlasmigratereport.WriteApplyFormat(cmd.OutOrStdout(), opts.format, atlasmigratereport.ApplyFormatOptions{
		Conn:   conn,
		FS:     migrationFS,
		Dir:    opts.dir,
		URL:    opts.url,
		Result: result,
	})
}

// flywayLinearityOperands returns everything the linear guard needs from a
// converted Flyway directory: the surviving baseline, the versions whose
// position below the current high-water mark is an artifact of the layout
// projection rather than a claim about authoring order, and the source version
// token each converted version came from.
//
// They are resolved together because they answer one question between them —
// "was this file authored out of order?" — and it must be answered on the
// operands the source tool decides it with.
//
// A surviving baseline is converted into the band BELOW every survivor, because
// Atlas CE emits and executes it first whatever its own version — measured, and
// measured to hold across runs, not only within one conversion. On a database
// that already has migrations recorded it therefore sorts below all of them,
// which the linear guard would read as "authored earlier". It is not: the
// position encodes sum order, not chronology. Exempting exactly that one
// version keeps every other out-of-order migration refused, which is what Atlas
// CE does too.
//
// The tokens are the guard's other operand (#1098). A converted Flyway
// directory is ORDERED numerically — V2 executes before V10, which is what the
// int64 version encodes and what atlas.sum is written against — but the tool it
// came from decides whether a file "was added out of order" by comparing the
// version TOKEN as a string, where "10" sorts below "2". Carrying the token
// alongside the executed version lets the guard ask that question the way the
// source tool asks it, without renumbering anything already recorded. The
// comparison itself happens inside the migration lock, on the applied set the
// migrator reads there, so a concurrent writer cannot move the mark between the
// check and the run.
type flywayLinearity struct {
	baseline           *atlasmigrateimport.FlywayBaseline
	outOfOrderExempt   []int64
	sourceVersions     map[int64]string
	sourceChecksums    map[int64]string
	revisionTypes      map[int64]migrator.AtlasRevisionType
	repeatableVersions []int64
}

func flywayLinearityOperands(
	captured fs.FS,
	format atlasmigrateimport.Format,
) (flywayLinearity, error) {
	baseline, err := atlasmigrateimport.FlywaySurvivingBaseline(captured, format)
	if err != nil {
		return flywayLinearity{}, err
	}
	operands := flywayLinearity{baseline: baseline}
	if baseline != nil {
		operands.outOfOrderExempt = []int64{baseline.AtlasVersion}
		operands.revisionTypes = map[int64]migrator.AtlasRevisionType{
			baseline.AtlasVersion: migrator.AtlasRevisionTypeBaseline | migrator.AtlasRevisionTypeApplied,
		}
	}
	operands.sourceVersions, err = atlasmigrateimport.FlywaySourceVersions(captured, format)
	if err != nil {
		return flywayLinearity{}, err
	}
	operands.sourceChecksums, err = atlasmigrateimport.FlywaySourceChecksums(captured, format)
	if err != nil {
		return flywayLinearity{}, err
	}
	operands.repeatableVersions, err = atlasmigrateimport.FlywayCoveredRepeatableVersions(captured, format)
	if err != nil {
		return flywayLinearity{}, err
	}
	return operands, nil
}

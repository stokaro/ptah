package atlas

import (
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"strconv"
	"strings"

	"github.com/spf13/cobra"

	"go.5x5.cz/ptah/cmd/internal/cmdutil"
	"go.5x5.cz/ptah/cmd/internal/dbcli"
	"go.5x5.cz/ptah/cmd/migratevalidate"
	"go.5x5.cz/ptah/config/projectconfig"
	"go.5x5.cz/ptah/dbschema"
	"go.5x5.cz/ptah/internal/atlasargs"
	"go.5x5.cz/ptah/internal/atlasmigrate"
	"go.5x5.cz/ptah/internal/atlasmigrateimport"
	"go.5x5.cz/ptah/internal/atlasmigratereport"
	"go.5x5.cz/ptah/internal/atlasreport"
	"go.5x5.cz/ptah/internal/migratesum"
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
	// An atlas.hcl `migration { format = "" }` selects the native Atlas layout
	// rather than being refused. `migrate apply` registers no --dir-format flag,
	// so atlas.hcl is the only way to reach an empty configured format here, and
	// measured against the pinned community binary v1.3.0 a hashed directory
	// under that config applies cleanly and exits 0 (stokaro/ptah#990 item 1).
	// ResolveApplyDirFormat below maps the empty value to the Atlas format, the
	// same as the empty --dir-format the other verbs accept.

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
	skipChecks, err := resolveAtlasApplySkipChecks(cmd)
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
	captured, err := atlasmigrate.CaptureApplySource(source.FS(), resolvedDirFormat)
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
		return err
	}

	migrationFS, err := atlasmigrate.ConvertApplySource(captured, dir, resolvedDirFormat)
	if err != nil {
		return fmt.Errorf("atlas migrate apply --dir: %w", err)
	}

	conn, err := dbschema.ConnectToDatabase(cmd.Context(), opts.url)
	if err != nil {
		return fmt.Errorf("error connecting to database: %w", err)
	}
	defer dbschema.CloseAndWarn(conn)

	// A surviving Flyway baseline is converted into the band BELOW every
	// survivor, because Atlas CE emits and executes it first whatever its own
	// version — measured, and measured to hold across runs, not only within one
	// conversion. On a database that already has migrations recorded it
	// therefore sorts below all of them, which the linear guard would read as
	// "authored earlier". It is not: the position encodes sum order, not
	// chronology. Exempting exactly that one version keeps every other
	// out-of-order migration refused, which is what Atlas CE does too.
	flywayBaseline, err := atlasmigrateimport.FlywaySurvivingBaseline(captured, resolvedDirFormat)
	if err != nil {
		return fmt.Errorf("atlas migrate apply --dir: %w", err)
	}
	var outOfOrderExempt []int64
	if flywayBaseline != nil {
		outOfOrderExempt = []int64{flywayBaseline.AtlasVersion}
	}

	plan, err := atlasmigrate.PrepareApply(cmd.Context(), conn, atlasmigrate.ApplyOptions{
		Dir:                  dir,
		FS:                   migrationFS,
		DryRun:               opts.dryRun,
		ExecOrder:            execOrder,
		OutOfOrderExempt:     outOfOrderExempt,
		TxMode:               txMode,
		RevisionsSchema:      opts.revisionsSchema,
		MigrationLockTimeout: migrationLockTimeout,
		Amount:               amount,
		AllowDirty:           opts.allowDirty,
		BaselineVersion:      baselineVersion,
		SkipChecks:           skipChecks,
	})
	if err != nil {
		return err
	}

	// #982 changed the Atlas version a Flyway file converts to, which is the
	// key `atlas_schema_revisions` stores. A database migrated by an older Ptah
	// build therefore reads as entirely pending here. Refuse before executing
	// anything rather than re-running migrations that already ran.
	if err := checkLegacyFlywayRevisions(captured, resolvedDirFormat, plan, opts.revisionsSchema); err != nil {
		return err
	}

	// The exemption above only stops the linear guard from reading the
	// baseline's band position as "authored earlier". Whether a baseline may
	// run against a database that already has history is a separate question,
	// and one Atlas CE answers three incompatible ways (stokaro/ptah#1003).
	if err := checkFlywayBaselineHistory(flywayBaseline, execOrder, plan); err != nil {
		return err
	}

	out := cmd.OutOrStdout()
	emitApplyStart := func([]int64) {}
	if !formatOutput {
		emitApplyStart = func(selectedVersions []int64) {
			emitAtlasMigrateApplyStart(out, opts, baselineVersion, selectedVersions)
		}
	}
	result, err := plan.ExecuteWithPreflight(
		cmd.Context(),
		func(_ context.Context, lockedPlan migrator.MigrationPlan) error {
			emitApplyStart(lockedPlan.Versions)
			return nil
		},
	)
	if err != nil {
		if formatOutput && result.ApplyError != nil {
			writeErr := writeAtlasMigrateApplyFormat(cmd, opts, migrationFS, conn, result)
			if writeErr != nil {
				return fmt.Errorf("%w; additionally failed to write --format output: %v", err, writeErr)
			}
		}
		return err
	}
	if handled, err := finishAtlasMigrateApplyFreshNoop(cmd, opts, migrationFS, conn, result); handled || err != nil {
		return err
	}

	if opts.dryRun {
		emitAtlasMigrateApplyDeferredChecks(cmd, result.ChecksDeferred)
		if formatOutput {
			return writeAtlasMigrateApplyFormat(cmd, opts, migrationFS, conn, result)
		}
		fmt.Fprintf(out, "Would have applied %d migrations.\n", len(result.SelectedVersions))
		return nil
	}
	if formatOutput {
		return writeAtlasMigrateApplyFormat(cmd, opts, migrationFS, conn, result)
	}
	fmt.Fprintf(out, "Migration complete. Current version: %d\n", result.FinalStatus.CurrentVersion)
	return nil
}

// emitAtlasMigrateApplyDeferredChecks names the pre-migration checks a dry run
// validated but did not evaluate, so a preview never drops a guard silently.
//
// It writes to stderr on purpose. Stdout carries the machine-readable --format
// document, and Atlas emits no field for this, so putting the note there would
// invent one and corrupt a caller's parse.
func emitAtlasMigrateApplyDeferredChecks(cmd *cobra.Command, versions []int64) {
	if len(versions) == 0 {
		return
	}
	labels := make([]string, 0, len(versions))
	for _, version := range versions {
		labels = append(labels, strconv.FormatInt(version, 10))
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
	fmt.Fprintln(cmd.OutOrStdout(), "No migration files to execute.")
	return true, nil
}

func emitAtlasMigrateApplyStart(
	out io.Writer,
	opts atlasMigrateApplyOptions,
	baselineVersion int64,
	selectedVersions []int64,
) {
	if opts.dryRun {
		fmt.Fprintln(out, "Dry run mode: no changes will be made.")
	}
	if opts.dryRun && baselineVersion > 0 {
		fmt.Fprintf(out, "Would baseline migrations at version %d.\n", baselineVersion)
	}
	if len(selectedVersions) > 0 {
		fmt.Fprintf(out, "Migrating to version %d from %d pending migrations.\n",
			selectedVersions[len(selectedVersions)-1],
			len(selectedVersions),
		)
	}
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
// It is the same computation `ptah-compat migrate hash` writes and
// `migrate validate` checks (#984, #992), so a directory this gate refuses is
// one those two verbs also refuse, and one they call clean applies here.
//
// What this gate verifies is also what apply executes, for every layout. That
// holds structurally rather than by agreement: the importer selects the file
// set it converts with the same [atlasmigrateimport.SumFileNames] rule this
// gate hashes. It was not always true — until #982 the Flyway importer ran a
// wider selection than the checksum covered, so a superseded baseline and a
// lowercase-prefixed file executed on a directory both tools called clean.
//
// An empty covered set is exempt from the missing-sum refusal, and that
// predicate is measured rather than assumed. CE's refusal keys on the covered
// set being non-empty, NOT on the directory holding any *.sql: an unhashed
// golang-migrate directory holding only 1_init.down.sql, and an unhashed Flyway
// directory holding only U1__init.sql, both exit 0 with "No migration files to
// execute", while an unhashed Goose directory holding only foo.sql exits 1.
// SumFileNames returning an empty slice is exactly that predicate. The
// exemption is deliberately limited to the unhashed branch: a hashed directory
// whose covered files were all deleted is drift, and CE reports it as one.
func verifyConvertedAtlasApplyChecksum(
	cmd *cobra.Command,
	fsys fs.FS,
	format atlasmigrateimport.Format,
) error {
	names, err := atlasmigrateimport.SumFileNames(fsys, format)
	if err != nil {
		return err
	}
	result, hashed, err := migratesum.VerifyAtlasFilesHashed(fsys, names)
	switch {
	case errors.Is(err, migratesum.ErrSumFileMalformed):
		return migratevalidate.FailAtlasChecksumMismatch(cmd, nil)
	case errors.Is(err, migratesum.ErrCoveredEntryUnreadable):
		// A covered entry that is a directory (#991). It reaches here on the
		// converted path too, because SumFileNames selects by name and the
		// captured snapshot now records such a directory instead of dropping it.
		return migratevalidate.FailAtlasChecksumUnreadableEntry(cmd, err)
	case err != nil:
		return err
	case !hashed && len(names) == 0:
		return nil
	case !hashed:
		return migratevalidate.FailAtlasChecksumFileNotFound(cmd)
	case !result.OK():
		return migratevalidate.FailAtlasChecksumMismatch(cmd, result.FirstMismatch())
	}
	return nil
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
	value, ok := os.LookupEnv(applySkipChecksEnvVar)
	if !ok || value == "" {
		return false, nil
	}
	skip, err := strconv.ParseBool(value)
	if err != nil {
		return false, fmt.Errorf("invalid boolean value %q for %s", value, applySkipChecksEnvVar)
	}
	return skip, nil
}

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
	return atlasmigratereport.WriteApplyFormat(cmd.OutOrStdout(), opts.format, atlasmigratereport.ApplyFormatOptions{
		Conn:   conn,
		FS:     migrationFS,
		Dir:    opts.dir,
		URL:    opts.url,
		Result: result,
	})
}

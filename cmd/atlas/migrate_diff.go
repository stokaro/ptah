package atlas

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"strings"

	"github.com/spf13/cobra"

	"go.5x5.cz/ptah/cmd/internal/cmdutil"
	"go.5x5.cz/ptah/cmd/internal/dbcli"
	"go.5x5.cz/ptah/cmd/internal/editor"
	"go.5x5.cz/ptah/config/projectconfig"
	"go.5x5.cz/ptah/core/platform"
	"go.5x5.cz/ptah/dbschema"
	"go.5x5.cz/ptah/internal/atlasargs"
	"go.5x5.cz/ptah/internal/atlascompatpolicy"
	"go.5x5.cz/ptah/internal/atlasmigrate"
	"go.5x5.cz/ptah/internal/atlasmigrateimport"
	"go.5x5.cz/ptah/internal/atlasreport"
	"go.5x5.cz/ptah/internal/atlasschema"
	"go.5x5.cz/ptah/internal/atlassource"
	"go.5x5.cz/ptah/internal/atlasurl"
	"go.5x5.cz/ptah/internal/devdocker"
	"go.5x5.cz/ptah/internal/pathguard"
	"go.5x5.cz/ptah/internal/sqlitevirtual"
	"go.5x5.cz/ptah/migration/generator"
	"go.5x5.cz/ptah/migration/migrator"
)

type atlasMigrateDiffOptions struct {
	toURLs []string
	devURL string
	// devURLGiven records whether the operator supplied a dev database URL by
	// either spelling, which is a different question from whether devURL is
	// empty. The pinned community binary refuses an absent `--dev-url` with
	// cobra's required-flag wording and answers an explicitly empty one with the
	// client layer's missing-driver verdict, so the two rows need both facts.
	// See [requireAtlasDevURL].
	devURLGiven bool
	dirURL      string
	dirFormat   string
	format      string
	schemas     []string
	lockTimeout string
	dryRun      bool
	qualifier   string
	edit        bool
	policy      atlascompatpolicy.Policy
}

type atlasMigrateDiffRunner func(
	context.Context,
	*dbschema.DatabaseConnection,
	atlasmigrate.DiffOptions,
) (atlasmigrate.DiffResult, error)

func newAtlasMigrateDiffCommand(policy atlascompatpolicy.Policy) *cobra.Command {
	return newAtlasMigrateDiffCommandWithPolicyAndRunner(
		policy,
		atlasmigrate.GenerateDiff,
	)
}

func newAtlasMigrateDiffCommandWithRunner(run atlasMigrateDiffRunner) *cobra.Command {
	return newAtlasMigrateDiffCommandWithPolicyAndRunner(atlascompatpolicy.Full(), run)
}

func newAtlasMigrateDiffCommandWithPolicyAndRunner(
	policy atlascompatpolicy.Policy,
	run atlasMigrateDiffRunner,
) *cobra.Command {
	opts := atlasMigrateDiffOptions{policy: policy}
	cmd := &cobra.Command{
		Use:   "diff [flags] [name]",
		Short: "Compute migration diff against a desired schema",
		Long: `Atlas OSS ` + "`atlas migrate diff`" + ` command path.

Drops all tables in the --dev-url database, replays the local migration
directory on it, compares the resulting state to --to desired-state sources,
and writes new Atlas-style migration files plus atlas.sum when changes are
found. Use a disposable dev database. --to accepts local .hcl, .yaml, .yml,
and .sql schema files, one directly connectable database URL, one local Atlas
migration directory, or one env://<attribute> reference resolved through the
evaluated atlas.hcl env. A database used as --to must differ from --dev-url.
Use --schema to limit the comparison to selected schema names.
--qualifier prefixes every object in the generated statements with a custom
schema qualifier when working on a single schema. When the atlas.hcl env
enables diff.concurrent_index.create, new indexes are planned as CREATE INDEX
CONCURRENTLY; files carrying such statements are tagged with the Atlas
` + "`-- atlas:txmode none`" + ` directive, and plans mixing them with
transactional statements are split into a transactional file followed by a
concurrent-index file. atlas.sum is updated only after every migration file
was written. With --edit the generated migration files open in $VISUAL or
$EDITOR before the directory checksum is finalized. A docker:// --dev-url
starts a throwaway PostgreSQL, MySQL or MariaDB container for the run and
removes it afterwards; it needs a reachable container runtime, and it is
refused for an engine the Atlas community CLI does not start either.
When --env is set, the selected atlas.hcl env
can provide schema.src, dev, migration.dir, format.migrate.diff, and supported
diff policy values.`,
		RunE: func(cmd *cobra.Command, args []string) error {
			name := "migration"
			if len(args) == 1 {
				name = args[0]
			}
			return runAtlasMigrateDiff(cmd, opts, name, run)
		},
	}
	flags := cmd.Flags()
	flags.StringArrayVar(&opts.toURLs, "to", nil, "Desired schema target URL")
	flags.StringVar(&opts.devURL, "dev-url", "", "Dev database URL used to replay migrations and compute the diff")
	flags.StringVar(&opts.dirURL, "dir", "file://migrations", "Migration directory URL")
	flags.StringVar(&opts.dirFormat, "dir-format", "atlas", "Migration directory format")
	flags.StringVar(&opts.format, "format", "", "Atlas Go template output format")
	registerAtlasSchemaFlag(flags, &opts.schemas, "Schemas to diff")
	flags.StringVar(&opts.lockTimeout, "lock-timeout", "", "Timeout for acquiring Atlas migration directory locks")
	flags.StringVar(&opts.qualifier, "qualifier", "", "Qualify tables with a custom qualifier when working on a single schema")
	flags.BoolVar(&opts.edit, "edit", false, "Edit the generated migration files")
	flags.BoolVar(&opts.dryRun, "dry-run", false, "Print the generated migration file to stdout instead of writing it")
	if err := flags.MarkHidden("dry-run"); err != nil {
		panic(err)
	}
	// The arity validator is passed HERE, not declared in the literal above:
	// ConfigureCommandArgs assigns cmd.Args unconditionally, so a
	// `Args: cobra.MaximumNArgs(1)` field was overwritten with nil and this verb
	// silently accepted every extra positional. `migrate diff --to … one two`
	// exited 0 where the pinned community binary v1.3.0 exits 1 with
	// `accepts at most 1 arg(s), received 2` (stokaro/ptah#1231 case 8); the
	// message below is cobra's own, which is where that binary's comes from too.
	cmdutil.ConfigureCommandArgs(cmd, cobra.MaximumNArgs(1))
	return cmd
}

func runAtlasMigrateDiff(
	cmd *cobra.Command,
	opts atlasMigrateDiffOptions,
	name string,
	run atlasMigrateDiffRunner,
) (runErr error) {
	if err := sqlitevirtual.ValidateExplicitURLToggle(opts.devURL); err != nil {
		return cmdutil.Fail(cmd, err)
	}
	formatConfigured := cmd.Flags().Changed("format")
	// dirURLSpelled is --dir as the command line, its environment twin and the
	// flag default left it, captured before the atlas.hcl merge below can
	// replace it. The scheme requirement is read off that value rather than the
	// merged one: see [atlasDirSchemeIsAnswerable] for why a directory named by
	// atlas.hcl cannot answer the question at all.
	dirURLSpelled := opts.dirURL
	policy := atlasschema.DiffPolicy{}
	mode := ignoreMissingEnvSelection
	if needsAtlasMigrateDiffConfig(cmd) {
		mode = reportMissingEnvSelection
	}
	project, loaded, err := openAtlasProjectForCommand(cmd, mode)
	if err != nil {
		return cmdutil.Fail(cmd, err)
	}
	defer closeAtlasProject(&project, &runErr)
	projectCfg := project.Config
	if loaded {
		opts.devURL = dbcli.EffectiveString(
			cmd,
			"dev-url",
			opts.devURL,
			projectCfg.StringValue(projectconfig.StringDevURL),
		)
		opts.dirURL = dbcli.EffectiveString(
			cmd,
			"dir",
			opts.dirURL,
			projectCfg.StringValue(projectconfig.StringMigrationDir),
		)
		opts.schemas = effectiveAtlasSchemas(cmd, opts.schemas, projectCfg)
		formatValue := projectCfg.StringValue(projectconfig.StringFormatMigrateDiff)
		opts.format = dbcli.EffectiveString(cmd, "format", opts.format, formatValue)
		formatConfigured = formatConfigured || formatValue.Present
		policy, err = atlasDiffPolicy(projectCfg)
		if err != nil {
			return cmdutil.Fail(cmd, err)
		}
	}
	// Captured after the atlas.hcl merge, because an env supplying `dev` counts
	// as having given the flag: the pinned binary runs such an invocation rather
	// than refusing it.
	opts.devURLGiven = atlasDevURLGiven(cmd, opts.devURL)
	// Resolve the comparison-owned toggle once the effective dev dialect is
	// known, before resolving the migration directory, desired source, or dev
	// database. Invalid non-SQLite URLs retain their measured diagnostics below.
	if dialect, dialectErr := atlasurl.DialectFromURL(opts.devURL); dialectErr == nil {
		if err := sqlitevirtual.ValidateToggle(dialect); err != nil {
			return cmdutil.Fail(cmd, err)
		}
	}
	// This verb creates the directory it was pointed at, so it requires the
	// scheme the community binary requires: `migrate diff demo --dir mig2` exits
	// 1 there with `missing scheme for dir url. Did you mean "file://mig2"?` and
	// creates nothing, measured on the pinned v1.3.0 on 2026-08-06. The flag's
	// own default carries `file://`, so an omitted --dir passes.
	//
	// Hash, validate, status and lint apply the same requirement to their
	// command-line --dir. A directory named by atlas.hcl on either writing verb
	// remains separate work in stokaro/ptah#1186 and is deliberately not closed
	// here.
	if err := atlasargs.RequireDirScheme(dirURLSpelled); err != nil {
		return cmdutil.Fail(cmd, err)
	}
	// The migration directory is resolved and gated before anything else this
	// verb validates. The position is measured against the pinned community
	// binary v1.3.0, not chosen: on an unhashed directory it prints the checksum
	// refusal ahead of `--to` being absent, ahead of an unreachable --dev-url,
	// ahead of a malformed --format template and ahead of `--dir-format goose`,
	// while an unknown flag and a second positional still win over it. Atlas
	// runs the gate in a pre-run hook, so everything cobra validates before
	// RunE precedes it and everything the command body validates follows it.
	localDir, err := resolveAtlasMigrateDiffDir(cmd, &project, opts.dirURL)
	if err != nil {
		return cmdutil.Fail(cmd, fmt.Errorf("atlas migrate diff --dir: %w", err))
	}
	// Both spellings that can name the directory's layout are resolved here,
	// once, with the `--dir` query outranking `--dir-format`. Unknown query keys
	// are ignored exactly as they are on the verbs that already accept them, and
	// a value neither spelling can parse is refused ahead of the atlas.sum gate,
	// which is where the community binary refuses it. See
	// [resolveWritingVerbDirFormat] for the measured table.
	dirFormat, err := resolveWritingVerbDirFormat(opts.dirFormat, localDir.Query)
	if err != nil {
		return cmdutil.Fail(cmd, atlasDirFormatError(
			"diff",
			atlasDirFormatSpelling(localDir.Query),
			err,
		))
	}
	// Positioned after the format refusal above and before the atlas.sum gate
	// below, which is where every verb that reads a `--dir` query reports it;
	// see [reportIgnoredDirQuery] for the two rules that fix the position.
	if err := reportIgnoredDirQuery(cmd.ErrOrStderr(), "diff", localDir.Query); err != nil {
		return cmdutil.Fail(cmd, err)
	}
	// The gate runs over the SELECTED layout's covered set. On a foreign layout
	// that is not the Atlas set — golang-migrate's atlas.sum covers the
	// `.up.sql` halves alone — so verifying the Atlas one here would refuse a
	// directory the community binary, and this binary's own `migrate hash`,
	// both call clean.
	if err := verifyAtlasWriteDirChecksum(cmd, project, localDir, dirFormat); err != nil {
		return err
	}
	if err := validateAtlasMigrateDiffCurrentSource(project, localDir, dirFormat, opts.policy, opts.devURL); err != nil {
		return cmdutil.Fail(cmd, fmt.Errorf("validate current migration directory: %w", err))
	}
	if err := project.refuseWriteToReadOnlyMigrationDir(localDir, "atlas migrate diff"); err != nil {
		return err
	}
	writeDir := project.writeLocalDir(localDir)
	migrationsDir, err := resolveMigrateDiffDirectory(writeDir)
	if err != nil {
		return cmdutil.Fail(cmd, fmt.Errorf("resolve migration directory: %w", err))
	}

	projectEnv := atlassource.ProjectEnv{}
	if loaded {
		projectEnv, err = atlasSourceProjectEnv(cmd, projectCfg)
		if err != nil {
			return cmdutil.Fail(cmd, err)
		}
	}
	if loaded && !cmd.Flags().Changed("to") &&
		(len(projectCfg.SchemaSources) > 0 || atlasExternalSchemaConfigured(projectCfg)) {
		opts.toURLs = []string{"env://src"}
	}
	desired, err := prepareAtlasMigrateDiffSource(opts, dirFormat, projectEnv)
	if err != nil {
		return cmdutil.Fail(cmd, err)
	}
	migrationSourceValidator := opts.policy.MigrationSourceValidator(opts.devURL)
	if err := desired.ValidateLocalSchemaSources(opts.policy.ValidateLocalSchemaSource); err != nil {
		return cmdutil.Fail(cmd, fmt.Errorf("load --to schema: %w", err))
	}
	desired, err = desired.PrepareMigrationSource(migrationSourceValidator)
	if err != nil {
		return cmdutil.Fail(cmd, fmt.Errorf("load --to schema: %w", err))
	}
	qualifier, err := atlasmigrate.ParseQualifier(opts.qualifier)
	if err != nil {
		return cmdutil.Fail(cmd, err)
	}
	if formatConfigured && strings.TrimSpace(opts.format) == "" {
		return cmdutil.Fail(cmd, fmt.Errorf("--format must not be empty"))
	}
	format := atlasreport.NormalizeMigrateDiffFormat(opts.format)
	if err := atlasreport.ValidateSchemaDiffTemplate(format); err != nil {
		return cmdutil.Fail(cmd, err)
	}
	lockTimeout, err := migrator.ParseMigrationLockTimeout(opts.lockTimeout)
	if err != nil {
		return cmdutil.Fail(cmd, err)
	}

	// The dev database is provisioned here, after every refusal this verb can
	// answer from its flags and its directory. The release is deferred before
	// the connection is opened so it runs after the connection is closed.
	devURL, releaseDev, err := devdocker.Resolve(cmd.Context(), opts.devURL, devdocker.Options{})
	if err != nil {
		return cmdutil.Fail(cmd, err)
	}
	defer releaseDev()

	// The provisioning wait has its own budget, so the connect timeout below
	// starts once a server is already listening.
	connectCtx, cancel := dbcli.ConnectContext(cmd.Context(), dbcli.DefaultConnectTimeout)
	defer cancel()
	conn, err := dbschema.ConnectToDatabase(connectCtx, devURL)
	if err != nil {
		return cmdutil.Fail(cmd, fmt.Errorf("connect to --dev-url: %w", err))
	}
	defer dbschema.CloseAndWarn(conn)

	var preparePublication func([]string) error
	if opts.edit {
		preparePublication = func(stagedPaths []string) error {
			return editor.Open(cmd.Context(), "", stagedPaths...)
		}
	}
	schemaVars, err := atlasVarFlagValues(cmd)
	if err != nil {
		return cmdutil.Fail(cmd, err)
	}
	diffResult, err := run(cmd.Context(), conn, atlasmigrate.DiffOptions{
		Dir:          migrationsDir,
		ReplaySource: project.replaySource(localDir),
		// The same handle the preflight gate captured through. Handing it to the
		// writer is what keeps the publication bound to the project root the
		// directory was validated inside: without it the writer would carry only
		// migrationsDir and reopen it by pathname for locking, staging,
		// publication, the atlas.sum commit and recovery, so a directory or
		// ancestor replaced after resolution could take the write somewhere the
		// gate never looked (stokaro/ptah#1118).
		Root:                 migrateDiffWriterRoot(project, writeDir),
		Desired:              desired,
		SourceConnectTimeout: dbcli.DefaultConnectTimeout,
		Name:                 name,
		Format:               format,
		// The layout both spellings resolved to, carried through as one value.
		// The writer converts the existing directory through it, names and
		// composes the new files in it, and hashes its covered set.
		DirFormat: dirFormat,
		// The shared bidirectional planner is injected rather than imported by
		// internal/atlasmigrate: migration/generator already imports that package
		// for its directory and qualifier primitives. The adapter below preserves
		// that dependency direction while making every layout that publishes a
		// rollback share native reverse refinements and exact concurrent-index
		// identity. The native Atlas layout omits this hook because it publishes
		// no rollback half; its valid forward plan must not be refused for a
		// capability an unpublished reverse would require.
		PlanBidirectional:         compatBidirectionalPlannerForFormat(dirFormat),
		Schemas:                   opts.schemas,
		LockTimeout:               lockTimeout,
		Policy:                    policy,
		Qualifier:                 qualifier,
		DryRun:                    opts.dryRun,
		Diagnostics:               cmd.ErrOrStderr(),
		Vars:                      schemaVars,
		IgnoreUnknownHCLNames:     opts.policy.IgnoreUnknownHCLNames(),
		ValidateDesiredSchema:     opts.policy.ValidateDesiredSchema,
		ValidateInspectedSchema:   opts.policy.ValidateInspectedSchema,
		ValidateLiveObject:        atlasLiveSchemaObjectValidator(opts.policy),
		ValidateMigrationSource:   migrationSourceValidator,
		ValidateLocalSchemaSource: opts.policy.ValidateLocalSchemaSource,
		PreparePublication:        preparePublication,
		// The same predicate the preflight above already applied, re-applied to
		// the locked snapshot. Passing it in is what keeps this verb from
		// carrying a second definition of a verified directory: without it the
		// library's own recheck accepts a missing atlas.sum, so a directory that
		// lost its sum between the preflight and the lock would be written to
		// and re-hashed.
		VerifyDir: func(fsys fs.FS) error {
			return checkAtlasWriteDirChecksum(cmd, fsys, dirFormat)
		},
		// Checked here rather than on the way in, because that is where the
		// community binary decides it: a name it cannot turn into a file refuses
		// a diff that has changes and passes one that has none. See
		// [checkAtlasMigrationName] for the measured table.
		VerifyName: func(name string) error {
			return checkAtlasMigrationName("diff", name)
		},
	})
	if err != nil {
		return cmdutil.Fail(cmd, err)
	}
	if diffResult.Synced {
		fmt.Fprintln(cmd.OutOrStdout(), "The migration directory is synced with the desired state, no changes to be made")
		return nil
	}
	if opts.dryRun {
		fmt.Fprint(cmd.OutOrStdout(), diffResult.SQL)
		return nil
	}
	// Atlas's data.template_dir writer synchronizes the new file and atlas.sum
	// back to the source directory without printing their backing host paths.
	// Ordinary --dir output remains unchanged; exposing the project-local path
	// here would both diverge from the pinned binary and leak an implementation
	// detail hidden behind the mem:// migration URL.
	if project.isVirtualMigrationDir(localDir) {
		return nil
	}
	for _, migrationPath := range diffResult.MigrationPaths {
		fmt.Fprintf(cmd.OutOrStdout(), "Created migration file: %s\n", migrationPath)
	}
	fmt.Fprintf(cmd.OutOrStdout(), "Updated migration checksum: %s\n", diffResult.SumPath)
	return nil
}

func validateAtlasMigrateDiffCurrentSource(
	project atlasProject,
	dir atlasargs.LocalDir,
	format atlasmigrateimport.Format,
	policy atlascompatpolicy.Policy,
	devURL string,
) error {
	if !policy.IsStrictCE() {
		return nil
	}
	source, err := project.captureLocal(dir)
	if errors.Is(err, fs.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	fSys := source.FileSystem
	if !atlasmigrate.ReadsNativeAtlasDir(format) {
		loaded, err := atlasmigrateimport.LoadFS(fSys, dir.Path, format)
		if err != nil {
			return fmt.Errorf("read migration directory as %s: %w", format, err)
		}
		fSys = loaded.FS()
	}
	return policy.ValidateMigrationSourceForURL(fSys, devURL)
}

func compatBidirectionalPlannerForFormat(
	format atlasmigrateimport.Format,
) func(atlasmigrate.BidirectionalPlanInput) (atlasmigrate.BidirectionalPlan, error) {
	if atlasmigrate.ReadsNativeAtlasDir(format) {
		return nil
	}
	return planCompatBidirectionalSchemaDiff
}

func planCompatBidirectionalSchemaDiff(
	input atlasmigrate.BidirectionalPlanInput,
) (atlasmigrate.BidirectionalPlan, error) {
	createMode := generator.ConcurrentIndexDisabled
	if input.ConcurrentIndexCreate && platform.IsPostgresFamily(input.Dialect) {
		createMode = generator.ConcurrentIndexAll
	}
	dropMode := generator.ConcurrentIndexDisabled
	if input.ConcurrentIndexDrop && platform.IsPostgresFamily(input.Dialect) {
		dropMode = generator.ConcurrentIndexAll
	}
	plan, err := generator.PlanBidirectionalSchemaDiff(generator.BidirectionalSchemaPlanOptions{
		Diff:          input.Diff,
		DesiredSchema: input.DesiredSchema,
		CurrentSchema: input.CurrentSchema,
		Dialect:       input.Dialect,
		Capabilities:  input.Capabilities,
		Policy: generator.BidirectionalPlanPolicy{
			Create: createMode,
			Drop:   dropMode,
		},
	})
	if err != nil {
		return atlasmigrate.BidirectionalPlan{}, err
	}
	return atlasmigrate.BidirectionalPlan{
		ForwardNodes:                 plan.Forward.Nodes,
		ReverseNodes:                 plan.Reverse.Nodes,
		ReverseRequiresNoTransaction: plan.Reverse.RequiresNoTransaction,
	}, nil
}

// resolveAtlasMigrateDiffDir parses the --dir value into the directory this run
// writes into, confining it to the atlas.hcl project root when that is where
// the value came from.
//
// "Came from the project" is read off the project itself rather than passed in:
// an unloaded atlas.hcl leaves the zero value, whose root is nil and whose
// StringValue reports nothing present, so the two conditions below already
// answer it.
func resolveAtlasMigrateDiffDir(
	cmd *cobra.Command,
	project *atlasProject,
	dirURL string,
) (atlasargs.LocalDir, error) {
	if project.root != nil &&
		!cmd.Flags().Changed("dir") &&
		project.StringValue(projectconfig.StringMigrationDir).Present {
		dir, err := project.resolveProjectMigrationDir(dirURL)
		if err != nil {
			return atlasargs.LocalDir{}, err
		}
		return dir, nil
	}
	return atlasargs.ParseLocalDir(dirURL)
}

func resolveMigrateDiffDirectory(dir atlasargs.LocalDir) (string, error) {
	if dir.AllowedRoot != "" {
		return pathguard.ResolveWithinRoot(dir.Path, dir.AllowedRoot)
	}
	return pathguard.ResolveCLIPath(dir.Path)
}

// migrateDiffWriterRoot returns the opened root the writer must stay inside, or
// nil when the operator named the directory directly.
//
// It reads the same two conditions [atlasProject.localOptions] uses for the
// reading half, deliberately: "did this directory come from the project file"
// is one question, and answering it differently for the gate and for the writer
// is how the two end up bound to different filesystem objects.
func migrateDiffWriterRoot(
	project atlasProject,
	dir atlasargs.LocalDir,
) *pathguard.OpenedDirectory {
	return project.localOptions(dir).Root
}

func needsAtlasMigrateDiffConfig(cmd *cobra.Command) bool {
	return !cmd.Flags().Changed("to") ||
		!cmd.Flags().Changed("dev-url")
}

// prepareAtlasMigrateDiffSource validates the desired-state flags and the
// resolved directory layout.
//
// dirFormat arrives already resolved from both spellings rather than being
// re-derived from opts.dirFormat here, so "which layout is this run writing"
// has one answer.
//
// A foreign layout used to be refused at this position. It is not any more:
// stokaro/ptah#1013 taught this verb to write the five external layouts, so the
// value is now carried into the writer instead. What did NOT move is the
// position of the value's own validation — an unparsable `--dir-format ATLAS`
// is still refused ahead of the atlas.sum gate by
// [resolveWritingVerbDirFormat], and a parsable foreign one still reaches the
// gate first, which is where the community binary answers it: measured on the
// pinned v1.3.0, an unhashed directory with `--dir-format goose` prints the
// checksum error, not a format complaint.
func prepareAtlasMigrateDiffSource(
	opts atlasMigrateDiffOptions,
	dirFormat atlasmigrateimport.Format,
	projectEnv atlassource.ProjectEnv,
) (atlassource.Set, error) {
	if len(opts.toURLs) == 0 {
		return atlassource.Set{}, fmt.Errorf("--to is required")
	}
	// The community wording, and the absent/empty split behind it, are owned by
	// [requireAtlasDevURL]: an absent flag is refused as a required flag, an
	// explicitly empty one is opened and answered `sql/sqlclient: missing
	// driver`, and a scheme naming no driver is named. Measured on the pinned
	// binary v1.3.0 on 2026-08-13.
	devURL := atlasDevURLInput{value: opts.devURL, given: opts.devURLGiven}
	if err := devURL.refuse(); err != nil {
		return atlassource.Set{}, err
	}
	if opts.edit && !atlasmigrate.ReadsNativeAtlasDir(dirFormat) {
		// `migrate new` draws the same line, for the same reason: the editor
		// opens the staged files and the operator's edits are then hashed, and
		// on a layout whose rollback half is a second file or a directive
		// section that promise is one this surface has not measured.
		return atlassource.Set{}, fmt.Errorf(
			"atlas migrate diff --edit: --edit applies only to an atlas directory, but this directory is read as %s",
			dirFormat,
		)
	}
	if opts.edit && opts.dryRun {
		return atlassource.Set{}, fmt.Errorf("atlas migrate diff --edit cannot be combined with --dry-run: dry runs write no migration file to edit")
	}
	// A `docker://` value is no longer refused here. It names its own dialect,
	// so every check below -- and the isolation check in particular, which asks
	// whether the dev database and `--to` are the same database -- answers the
	// same way for it as for the URL it will be provisioned into. The container
	// itself is started later, next to the connection that uses it.
	toSet, err := atlassource.ClassifySet("--to", opts.toURLs, projectEnv)
	if err != nil {
		return atlassource.Set{}, err
	}
	if err := toSet.EnsureDevDatabase(opts.devURL); err != nil {
		return atlassource.Set{}, err
	}
	if err := toSet.EnsureDevIsolation(opts.devURL); err != nil {
		return atlassource.Set{}, err
	}
	if _, _, err := atlassource.PinDialect(opts.devURL, toSet); err != nil {
		return atlassource.Set{}, err
	}
	return toSet, nil
}

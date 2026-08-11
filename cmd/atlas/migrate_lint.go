package atlas

import (
	"errors"
	"fmt"
	"strings"

	"github.com/spf13/cobra"

	"go.5x5.cz/ptah/cmd/internal/cmdutil"
	"go.5x5.cz/ptah/cmd/internal/dbcli"
	"go.5x5.cz/ptah/cmd/internal/exitcode"
	"go.5x5.cz/ptah/config/projectconfig"
	"go.5x5.cz/ptah/internal/atlasargs"
	"go.5x5.cz/ptah/internal/atlascompatpolicy"
	"go.5x5.cz/ptah/internal/atlasreport"
	"go.5x5.cz/ptah/internal/atlasurl"
	"go.5x5.cz/ptah/internal/envbool"
	"go.5x5.cz/ptah/internal/migrationlintreport"
	migrationlint "go.5x5.cz/ptah/migration/lint"
)

const atlasMigrateLintFindingError = "lint findings exceed the failure threshold"

// atlasLintWithoutDevURLEnvVar re-enables linting a migration directory with no
// dev database.
//
// The pinned community binary v1.3.0 marks --dev-url required on this verb and
// exits 1 with `required flag(s) "dev-url" not set` before it looks at the
// directory, so the compatibility surface refuses the same invocation by
// default: a pipeline written against that binary must not get a clean report
// here where it gets a refusal there (stokaro/ptah#1231 case 2).
//
// The capability behind the refusal is real and is Ptah's own -- its analyzers
// read the migration files and do not need a database to reach a verdict -- so
// per AGENTS.md ("Compatibility never removes a capability") it stays reachable
// on this same surface rather than only through native `ptah migrations lint`.
// It is an environment variable rather than a flag because the conformance
// cli-surface tier asserts flag parity with that binary; precedent and spelling:
// [go.5x5.cz/ptah/internal/atlashclrender.KeepAtlasRefusedBlocksEnvVar].
const atlasLintWithoutDevURLEnvVar = "PTAH_ATLAS_LINT_WITHOUT_DEV_URL"

type atlasMigrateLintOptions struct {
	devURL    string
	dir       string
	dirFormat string
	format    string
	latest    uint
	gitBase   string
	gitDir    string
	atlasEnv  string
}

func newAtlasMigrateLintCommand(policy atlascompatpolicy.Policy) *cobra.Command {
	opts := atlasMigrateLintOptions{
		dir:       "file://migrations",
		dirFormat: atlasDirFormatDefault,
		gitDir:    ".",
	}
	cmd := &cobra.Command{
		Use:   "lint",
		Short: "Lint migration files",
		Long: `Run Atlas-compatible migration lint checks over a local migration directory.

A run needs a dev database and a scope, and the two refusals are separate. Give
--dev-url, or set ` + "`PTAH_ATLAS_LINT_WITHOUT_DEV_URL=1`" + ` to analyze the
directory with no dev database at all, which Ptah's analyzers can do and the
binary this surface stands in for cannot.

Give a scope too: --latest N, --git-base <ref>, or a lint block in atlas.hcl
that supplies one. Without a scope this surface refuses, because that binary
refuses, and answering an unscoped invocation would connect to --dev-url and
clean it on an argv that binary never connects on. Set
` + "`PTAH_ATLAS_LINT_ALL_VERSIONS=1`" + ` to lint the whole directory instead.
The two variables relax different requirements and neither implies the other.
Native ` + "`ptah migrations lint`" + ` needs neither and is unaffected by both.

Native Ptah equivalent: ptah migrations lint.`,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return runAtlasMigrateLint(cmd, policy, opts)
		},
	}
	flags := cmd.Flags()
	flags.StringVar(&opts.devURL, "dev-url", "", "Dev database URL")
	flags.StringVar(&opts.dir, "dir", opts.dir, "Migration directory URL")
	flags.StringVar(&opts.dirFormat, "dir-format", opts.dirFormat, "Migration directory format")
	flags.StringVar(&opts.format, "format", "", "Atlas Go template output format")
	flags.UintVar(&opts.latest, "latest", 0, "Number of latest migrations to lint")
	flags.StringVar(&opts.gitBase, "git-base", "", "Base Git branch for changeset linting")
	flags.StringVar(&opts.gitDir, "git-dir", opts.gitDir, "Repository working directory for --git-base")
	cmdutil.ConfigureCommandArgs(cmd, cmdutil.NoPositionalArgsHint("name the migration directory with --dir"))
	return cmd
}

func runAtlasMigrateLint(
	cmd *cobra.Command,
	policy atlascompatpolicy.Policy,
	opts atlasMigrateLintOptions,
) (runErr error) {
	// Both variables this verb owns are resolved first, before the project file
	// is opened and before any directory or dev database is touched. Resolving
	// them at their use sites left each one dormant on the runs that did not need
	// it -- a lint that named a scope never read PTAH_ATLAS_LINT_ALL_VERSIONS,
	// and a lint carrying --dev-url never read PTAH_ATLAS_LINT_WITHOUT_DEV_URL --
	// so a typo in either survived every healthy run of the pipeline that
	// exported it. See stokaro/ptah#1334.
	lintAllVersions, err := atlasMigrateLintAllVersions()
	if err != nil {
		return cmdutil.Fail(cmd, err)
	}
	withoutDevURL, err := lintWithoutDevURL()
	if err != nil {
		return cmdutil.Fail(cmd, err)
	}
	formatOutput := cmd.Flags().Changed("format")
	project, loaded, err := openAtlasProjectForCommand(cmd, ignoreMissingEnvSelection)
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
		opts.dir = dbcli.EffectiveString(
			cmd,
			"dir",
			opts.dir,
			projectCfg.StringValue(projectconfig.StringMigrationDir),
		)
		opts.dirFormat = dbcli.EffectiveString(
			cmd,
			"dir-format",
			opts.dirFormat,
			projectCfg.StringValue(projectconfig.StringMigrationFormat),
		)
		opts.atlasEnv = dbcli.EffectiveString(
			cmd,
			dbcli.EnvFlagName,
			opts.atlasEnv,
			projectCfg.StringValue(projectconfig.StringEnvName),
		)
		// Atlas CE treats an explicit --latest 0 as no latest selector. It
		// still suppresses lint.latest from the project, but it does not
		// suppress a project Git selector: `--latest 0 --git-base ...` is a
		// Git-selected run there, not a mutually-exclusive pair.
		latestChanged := cmd.Flags().Changed("latest")
		if !latestChanged || opts.latest == 0 {
			opts.gitBase = dbcli.EffectiveString(
				cmd,
				"git-base",
				opts.gitBase,
				projectCfg.StringValue(projectconfig.StringLintGitBase),
			)
			opts.gitDir = dbcli.EffectiveString(
				cmd,
				"git-dir",
				opts.gitDir,
				projectCfg.StringValue(projectconfig.StringLintGitDir),
			)
		}
		formatValue := projectCfg.StringValue(projectconfig.StringFormatMigrateLint)
		opts.format = dbcli.EffectiveString(cmd, "format", opts.format, formatValue)
		formatOutput = formatOutput || formatValue.Present
	}
	// The dev-url requirement is checked before anything the command body
	// validates, because on the pinned community binary v1.3.0 it is a cobra
	// required-flag check and therefore precedes the whole run: measured on that
	// binary, `migrate lint --latest 1` in a directory holding no `migrations`
	// answers `required flag(s) "dev-url" not set`, not `stat migrations`, and
	// the same invocation carrying a tampered atlas.sum answers it too. An
	// atlas.hcl env that supplies `dev` satisfies it there and here, which is why
	// this reads the merged value rather than the flag.
	if !withoutDevURL {
		if err := requireAtlasMigrateLintDevURL(opts.devURL); err != nil {
			return cmdutil.Fail(cmd, err)
		}
	}
	if err := validateAtlasMigrateLintOptions(opts); err != nil {
		return cmdutil.Fail(cmd, err)
	}
	// The flag value is validated here, before --format and before --dir is
	// parsed, because that is where it was validated when only the Atlas layout
	// was accepted: moving the whole resolution below --dir would change which
	// diagnostic an invocation carrying two bad values prints. The query
	// spelling cannot be resolved yet -- it lives in --dir -- so this pass sees
	// the configured value alone and the two are combined below.
	if _, err := resolveAtlasVerbDirFormat(cmd.ErrOrStderr(), "lint", opts.dirFormat, nil); err != nil {
		return cmdutil.Fail(cmd, err)
	}
	if !lintAllVersions {
		if err := requireAtlasMigrateLintScope(cmd, opts, projectCfg); err != nil {
			return cmdutil.Fail(cmd, err)
		}
	}
	if formatOutput {
		if err := validateAtlasMigrateLintFormat(opts.format); err != nil {
			return cmdutil.Fail(cmd, err)
		}
		if err := atlasreport.ValidateMigrateLintTemplate(opts.format); err != nil {
			return cmdutil.Fail(cmd, err)
		}
	}
	var localDir atlasargs.LocalDir
	if loaded &&
		!cmd.Flags().Changed("dir") &&
		projectCfg.StringValue(projectconfig.StringMigrationDir).Present {
		localDir, err = project.localDirWithQuery(opts.dir)
	} else {
		// A --dir spelled on the command line needs the scheme the community
		// binary requires. Measured on the pinned v1.3.0 on 2026-08-06,
		// `migrate lint --dir mig --dir-format goose --dev-url …` exits 1 with
		// `missing scheme for dir url. Did you mean "file://mig"?` where Ptah
		// exited 0 (stokaro/ptah#1186). The flag default is `file://migrations`,
		// so an omitted --dir passes.
		//
		// Only this branch is gated: it is the one carrying a command-line
		// value. A directory named by atlas.hcl takes the branch above, where
		// the community binary's own spelling rules are a separate question
		// #1186 leaves open.
		if err := atlasargs.RequireDirScheme(opts.dir); err != nil {
			return cmdutil.Fail(cmd, err)
		}
		localDir, err = atlasargs.ParseLocalDir(opts.dir)
	}
	if err != nil {
		return cmdutil.Fail(cmd, fmt.Errorf("atlas migrate lint --dir: %w", err))
	}
	format, err := resolveAtlasVerbDirFormat(cmd.ErrOrStderr(), "lint", opts.dirFormat, localDir.Query)
	if err != nil {
		return cmdutil.Fail(cmd, err)
	}
	source, err := project.captureLocal(localDir)
	if err != nil {
		return cmdutil.Fail(cmd, atlasMigrateLintDirCaptureError(localDir.Path, localDir.AllowedRoot, err))
	}
	dir := source.Display
	lintOptions := migrationlintreport.Options{
		DirFormat:     atlasDirFormatDefault,
		AtlasEnv:      opts.atlasEnv,
		DevURL:        opts.devURL,
		GitBase:       opts.gitBase,
		GitDir:        opts.gitDir,
		FailOn:        migrationlintreport.FailOnError,
		Latest:        opts.latest,
		Compatibility: migrationlint.CompatibilityProfileAtlas,
		// `.Schema.Current` and `.Schema.Desired` exist only in the templated
		// output, so only a run that renders a template pays for reading them.
		CaptureSchema: formatOutput,
		Changed: migrationlintreport.ChangedOptions{
			Dir:       true,
			DirFormat: true,
			AtlasEnv:  true,
			DevURL:    true,
			GitBase:   cmd.Flags().Changed("git-base"),
			GitDir:    cmd.Flags().Changed("git-dir"),
			Latest:    cmd.Flags().Changed("latest"),
		},
	}
	// The changeset selection is required, and requireAtlasMigrateLintScope
	// above is the one place that asks. This branch used to ask again here,
	// through migrationlintreport.SelectorConfigured, and became a duplicate
	// when #1307 landed the same requirement earlier in the run -- earlier
	// matters, because the unscoped argv reached --dev-url and CLEANED it before
	// a check at this point could answer. Two gates for one rule is two
	// sentences that have to agree, so this one is gone rather than kept.
	captured, err := captureAtlasDirSource(source.FileSystem, format)
	if err != nil {
		return cmdutil.Fail(cmd, fmt.Errorf("atlas migrate lint --dir: %w", err))
	}
	covered, err := captured.coveredNames()
	if err != nil {
		return cmdutil.Fail(cmd, fmt.Errorf("atlas migrate lint --dir: %w", err))
	}
	integrity, err := atlasreport.InspectMigrateLintIntegrity(captured.gateFS(), covered)
	if err != nil {
		return cmdutil.Fail(cmd, err)
	}
	if integrity.Failed() {
		if formatOutput {
			driver, err := atlasurl.DialectFromURL(opts.devURL)
			if err != nil {
				return cmdutil.Fail(cmd, err)
			}
			if err := atlasreport.WriteMigrateLintFormat(cmd.OutOrStdout(), opts.format, atlasreport.MigrateLintOptions{
				Driver:    driver,
				URL:       opts.devURL,
				Dir:       dir,
				Integrity: integrity,
			}); err != nil {
				return cmdutil.Fail(cmd, err)
			}
		}
		if !formatOutput {
			fmt.Fprintln(cmd.ErrOrStderr(), integrity.Error)
		}
		return exitcode.New(1, errors.New(integrity.Error))
	}
	if err := policy.MigrationSourceValidator(opts.devURL)(captured.gateFS()); err != nil {
		return cmdutil.Fail(cmd, err)
	}

	// A foreign layout is rebuilt here as up-only Atlas migrations, so what the
	// analyzers replay is the Atlas layout on either branch and the native
	// directory format below stays atlas for both.
	snapshot, err := captured.migrationFS(dir)
	if err != nil {
		return cmdutil.Fail(cmd, fmt.Errorf("atlas migrate lint --dir: %w", err))
	}
	lintOptions.Dir = dir
	lintOptions.FS = snapshot
	report, err := migrationlintreport.Build(cmd.Context(), lintOptions, projectCfg)
	if err != nil {
		if formatOutput {
			if err := writeAtlasMigrateLintReplayError(cmd, opts, dir, report, integrity, err); err != nil {
				return cmdutil.Fail(cmd, err)
			}
			return exitcode.New(1, err)
		}
		return cmdutil.Fail(cmd, err)
	}
	if formatOutput {
		if err := atlasreport.WriteMigrateLintFormat(cmd.OutOrStdout(), opts.format, atlasreport.MigrateLintOptions{
			Driver:    report.Dialect,
			URL:       opts.devURL,
			Dir:       dir,
			Analysis:  &report.Analysis,
			Integrity: integrity,
			Error:     report.Error,
			Schema: atlasreport.MigrateLintSchema{
				Current: report.SchemaCurrent,
				Desired: report.SchemaDesired,
			},
		}); err != nil {
			return cmdutil.Fail(cmd, err)
		}
	} else if err := atlasreport.WriteMigrateLintText(cmd.OutOrStdout(), atlasreport.MigrateLintOptions{
		Analysis: &report.Analysis,
	}); err != nil {
		return cmdutil.Fail(cmd, err)
	}
	if report.Failed {
		return exitcode.New(1, errors.New(atlasMigrateLintFindingError))
	}
	return nil
}

func writeAtlasMigrateLintReplayError(
	cmd *cobra.Command,
	opts atlasMigrateLintOptions,
	dir string,
	report migrationlintreport.Report,
	integrity atlasreport.MigrateLintIntegrity,
	replayErr error,
) error {
	driver, err := atlasurl.DialectFromURL(opts.devURL)
	if err != nil {
		return err
	}
	return atlasreport.WriteMigrateLintFormat(cmd.OutOrStdout(), opts.format, atlasreport.MigrateLintOptions{
		Driver:    driver,
		URL:       opts.devURL,
		Dir:       dir,
		Analysis:  &report.Analysis,
		Integrity: integrity,
		Error:     replayErr.Error(),
		Schema: atlasreport.MigrateLintSchema{
			Current: report.SchemaCurrent,
			Desired: report.SchemaDesired,
		},
	})
}

func validateAtlasMigrateLintOptions(opts atlasMigrateLintOptions) error {
	if opts.dir == "" {
		return fmt.Errorf("migrations directory is required")
	}
	return nil
}

// atlasMigrateLintAllVersionsEnvVar lints every version in the directory when
// the argv names no scope, which is what this surface did before it started
// refusing.
//
// It exists because compatibility must not delete a capability (AGENTS.md,
// "Compatibility never removes a capability"): Ptah's linter can review a whole
// directory, and a pipeline being ported that relied on the unscoped form has
// to be able to get it back on THIS surface rather than being sent to native
// `ptah`. It is an environment variable and not a flag because the conformance
// cli-surface tier asserts that ptah-compat registers exactly the flags the
// pinned binary registers. Precedent and spelling:
// [go.5x5.cz/ptah/internal/atlashclrender.KeepAtlasRefusedBlocksEnvVar].
const atlasMigrateLintAllVersionsEnvVar = "PTAH_ATLAS_LINT_ALL_VERSIONS"

// atlasLintAllVersions is the declaration of the variable, made once, on the
// verb that owns it. See [go.5x5.cz/ptah/internal/envbool].
var atlasLintAllVersions = envbool.New(atlasMigrateLintAllVersionsEnvVar, false)

// atlasMigrateLintAllVersions reports whether the opt-in lints the whole
// directory. Unset keeps the default and a valid false spelling keeps it too; an
// empty or unparsable value is a configuration error (stokaro/ptah#1334).
func atlasMigrateLintAllVersions() (bool, error) {
	return atlasLintAllVersions.Resolve()
}

// atlasMigrateLintScopeGiven reports whether anything named the set of versions
// to lint, mirroring how internal/migrationlintreport resolves the same two
// selectors: a positive --latest wins outright, while an explicit zero clears
// configured lint.latest and leaves Git eligible. An atlas.hcl `lint.latest`
// counts unless --git-base was spelled, and a --git-base counts once
// opts.gitBase has taken the project value.
//
// No "was a project loaded" argument is needed: a run that loaded none carries
// the zero projectconfig.Config, whose LintLatestValue reports Present false.
func atlasMigrateLintScopeGiven(
	cmd *cobra.Command,
	opts atlasMigrateLintOptions,
	projectCfg projectconfig.Config,
) bool {
	latestChanged := cmd.Flags().Changed("latest")
	if latestChanged && opts.latest > 0 {
		return true
	}
	if strings.TrimSpace(opts.gitBase) != "" {
		return true
	}
	// An explicit zero clears a configured lint.latest. With no usable Git
	// selector it therefore names no scope, matching Atlas CE v1.3.0.
	if latestChanged {
		return false
	}
	return !cmd.Flags().Changed("git-base") && projectCfg.LintLatestValue().Present
}

// requireAtlasMigrateLintScope refuses a lint run that names no scope, the way
// the pinned Atlas community binary v1.3.0 refuses one.
//
// Measured on 2026-08-08, `migrate lint --dir file://migrations --dev-url …`
// with no --latest and no --git-base, exit status read on a line of its own:
//
//	pinned binary   exit 1  Error: --latest or --git-base is required
//	ptah-compat     exit 0
//
// The same two answers whether ./migrations is empty or holds a hashed
// migration, and on SQLite, PostgreSQL 17 and MySQL 9.7. It is not only an exit
// code: Ptah reaches the dev database on that argv and CLEANS it. Against a
// MySQL dev schema holding one table, the pinned binary refuses before
// connecting and the table survives; ptah-compat dropped it. The refusal
// therefore has to come before the directory is captured and before
// internal/migrationlintreport touches --dev-url, which is where it is called.
//
// Where the pinned binary orders this refusal among the others was measured too,
// on the same fixture: `--dir migrations` (no scheme), a missing --dev-url and
// `--dir-format nosuch` all answer BEFORE it, while a broken --format template
// and a corrupt atlas.sum answer AFTER it. Hence the call site: after the
// dir-format resolution, before the --format template check and before the
// directory is captured. Two rows are not reproduced, and neither can make this
// surface exit 0 where that binary exits 1:
//
//   - a scheme-less --dir with no scope answers with this refusal here and with
//     `missing scheme for dir url` there; both exit 1, and Ptah already ordered
//     its --format check ahead of its scheme check before this;
//   - --dev-url is not required at all here, which is a separate looseness cell
//     that predates this change: `migrate lint --dir file://migrations
//     --latest 1` with no --dev-url is that binary 1 and ptah-compat 0 on the
//     base commit too. Left open rather than widened into here.
//
// Scoped to this surface. Native `ptah migrations lint` names a directory and
// reviews all of it; nothing here runs on that path.
func requireAtlasMigrateLintScope(
	cmd *cobra.Command,
	opts atlasMigrateLintOptions,
	projectCfg projectconfig.Config,
) error {
	if atlasMigrateLintScopeGiven(cmd, opts, projectCfg) {
		return nil
	}
	return errors.New("--latest or --git-base is required")
}

// requireAtlasMigrateLintDevURL reproduces the community CLI's required-flag
// refusal, wording included, unless the opt-in variable asks for Ptah's
// database-free analysis instead.
//
// It is called BEFORE [requireAtlasMigrateLintScope], and the order is measured
// rather than chosen. On the pinned Atlas community binary v1.3.0, an argv
// missing both answers the dev-url sentence:
//
//	$ atlas migrate lint --dir file://migrations
//	exit 1  Error: required flag(s) "dev-url" not set
//
// so a surface that answered the scope sentence there would diverge on a cell
// both refuse. The scope requirement is #1241's and arrived separately; this one
// is stokaro/ptah#1231 case 2, which #1307 explicitly left open rather than
// widening into itself.
func requireAtlasMigrateLintDevURL(devURL string) error {
	if strings.TrimSpace(devURL) != "" {
		return nil
	}
	return errors.New(`required flag(s) "dev-url" not set`)
}

// atlasLintWithoutDevURL is the declaration of the variable, made once, on the
// verb that owns it. See [go.5x5.cz/ptah/internal/envbool].
var atlasLintWithoutDevURL = envbool.New(atlasLintWithoutDevURLEnvVar, false)

// lintWithoutDevURL reports whether the opt-in asks for Ptah's database-free
// analysis. Unset keeps the requirement and a valid false spelling keeps it too;
// an empty or unparsable value is a configuration error (stokaro/ptah#1334).
//
// It relaxes ONLY the dev-database requirement. A run with no scope is still
// refused; [atlasMigrateLintAllVersions] is the separate opt-in for that, and
// neither variable implies the other.
func lintWithoutDevURL() (bool, error) {
	return atlasLintWithoutDevURL.Resolve()
}

func validateAtlasMigrateLintFormat(format string) error {
	if strings.TrimSpace(format) == "" {
		return fmt.Errorf("--format must not be empty")
	}
	return nil
}

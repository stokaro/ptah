package atlas

import (
	"fmt"
	"io"
	"os"
	"slices"
	"strconv"
	"strings"

	"github.com/spf13/cobra"
	"github.com/spf13/pflag"

	"go.5x5.cz/ptah/cmd/internal/dbcli"
	"go.5x5.cz/ptah/cmd/internal/migrationsource"
	"go.5x5.cz/ptah/config/projectconfig"
	"go.5x5.cz/ptah/dbschema"
	"go.5x5.cz/ptah/internal/atlasargs"
	"go.5x5.cz/ptah/internal/atlasmigrate"
	"go.5x5.cz/ptah/internal/atlasreport"
	"go.5x5.cz/ptah/internal/envbool"
	"go.5x5.cz/ptah/migration/generator"
	"go.5x5.cz/ptah/migration/migrator"
)

// newAtlasMigrateDownCommand wraps the table-driven `atlas migrate down`
// forward with a dedicated --format path. Without --format the original
// forward runs unchanged, so the default output and exit codes stay
// byte-identical to the plain forward; with --format (flag or PTAH_FORMAT) the
// verb executes the rollback through internal/atlasmigrate and renders the
// Atlas Go-template report instead of the native text output.
func newAtlasMigrateDownCommand() *cobra.Command {
	verb := atlasMigrateDownVerb()
	cmd := newAtlasAdapterCommand("migrate", verb)
	forward := cmd.RunE
	cmd.RunE = func(cmd *cobra.Command, args []string) error {
		if atlasArgsHaveHelp(args) || !atlasMigrateDownWantsFormat(verb, args) {
			return forward(cmd, args)
		}
		return runAtlasMigrateDownFormat(cmd, verb, args)
	}
	return cmd
}

// atlasArgsHaveHelp mirrors the forward adapter's help detection so a --help
// anywhere in the args keeps rendering the adapter help.
func atlasArgsHaveHelp(args []string) bool {
	for _, arg := range args {
		if arg == "--help" || arg == "-h" {
			return true
		}
	}
	return false
}

// atlasMigrateDownWantsFormat reports whether the invocation requests Atlas
// Go-template output: an explicit --format flag, or the PTAH_FORMAT
// environment value the arg mapper would otherwise inject.
func atlasMigrateDownWantsFormat(verb atlasVerb, args []string) bool {
	if atlasArgsHaveFlag(verb.flags, args, "format") {
		return true
	}
	return os.Getenv("PTAH_FORMAT") != ""
}

// atlasArgsHaveFlag reports whether args contain the named flag as a flag
// (not as the value of another value-taking flag), stopping at "--".
func atlasArgsHaveFlag(flags []atlasargs.Flag, args []string, name string) bool {
	valueFlags := atlasValueFlagNames(flags)
	for i := 0; i < len(args); i++ {
		arg := args[i]
		if arg == "--" {
			return false
		}
		flagName, inlineValue, ok := atlasFlagName(arg)
		if !ok {
			continue
		}
		if flagName == name {
			return true
		}
		if !inlineValue {
			if _, found := valueFlags[flagName]; found && i+1 < len(args) {
				i++
			}
		}
	}
	return false
}

type atlasMigrateDownFormatOptions struct {
	url             string
	dir             string
	devURL          string
	toVersion       string
	format          string
	revisionsSchema string
	lockTimeout     string
	dryRun          bool

	flagSet *pflag.FlagSet
	// rawDir preserves the pre-resolution --dir value for the report's Env.Dir.
	rawDir string
	// dirOptions preserve the live atlas.hcl root for contained project paths.
	dirOptions migrationsource.LocalOptions
}

func runAtlasMigrateDownFormat(
	cmd *cobra.Command,
	verb atlasVerb,
	args []string,
) (runErr error) {
	parentFlags, parentChanged, err := atlasProjectFlagsFromCommand(cmd)
	if err != nil {
		return err
	}
	project, remaining, err := extractAtlasProjectArgs(args)
	if err != nil {
		return err
	}
	project = mergeAtlasProjectArgs(atlasProjectArgValues{flags: parentFlags, changed: parentChanged}, project)

	opts, err := parseAtlasMigrateDownFormatArgs(verb, remaining)
	if err != nil {
		return err
	}
	loadedProject, err := applyAtlasMigrateDownFormatProjectConfig(cmd.ErrOrStderr(), opts, project)
	if err != nil {
		return err
	}
	defer closeAtlasProject(&loadedProject, &runErr)
	if strings.TrimSpace(opts.format) == "" {
		return fmt.Errorf("--format must not be empty")
	}
	if err := atlasreport.ValidateMigrateDownTemplate(opts.format); err != nil {
		return err
	}
	if opts.url == "" {
		return fmt.Errorf("database URL is required")
	}
	if opts.dir == "" {
		return fmt.Errorf("migrations directory is required")
	}
	targetVersion, err := parseAtlasMigrateDownTarget(opts.toVersion)
	if err != nil {
		return err
	}
	migrationLockTimeout, err := migrator.ParseMigrationLockTimeout(opts.lockTimeout)
	if err != nil {
		return err
	}
	source, err := migrationsource.CaptureLocal(opts.dir, opts.dirOptions)
	if err != nil {
		return fmt.Errorf("atlas migrate down --dir: %w", err)
	}
	dir := source.Display

	connectCtx, cancel := dbcli.ConnectContext(cmd.Context(), dbcli.DefaultConnectTimeout)
	defer cancel()
	conn, err := dbschema.ConnectToDatabase(connectCtx, opts.url)
	if err != nil {
		return fmt.Errorf("error connecting to database: %w", err)
	}
	defer dbschema.CloseAndWarn(conn)

	plan, err := atlasmigrate.PrepareDown(cmd.Context(), conn, atlasmigrate.DownOptions{
		Dir:                  dir,
		FS:                   source.FileSystem,
		TargetVersion:        targetVersion,
		DryRun:               opts.dryRun,
		RevisionsSchema:      opts.revisionsSchema,
		MigrationLockTimeout: migrationLockTimeout,
	})
	if err != nil {
		return err
	}

	// Replay the rollback plan on the dev database first, so a failing or
	// missing down migration aborts before the target is touched.
	if opts.devURL != "" && !plan.Noop() {
		err := generator.VerifyRollbackFromShadow(cmd.Context(), generator.RollbackFromShadowOptions{
			TargetConnection:  conn,
			ShadowDatabaseURL: opts.devURL,
			FS:                source.FileSystem,
			CurrentVersion:    plan.CurrentVersion,
			TargetVersion:     targetVersion,
			ProviderOptions: []migrator.FSProviderOption{
				migrator.WithMigrationDirFormat(migrator.MigrationDirFormatAtlas),
			},
			ConnectTimeout: dbcli.DefaultConnectTimeout,
		})
		if err != nil {
			return err
		}
	}

	result, execErr := plan.Execute(cmd.Context())
	writeErr := atlasreport.WriteMigrateDownFormat(cmd.OutOrStdout(), opts.format, atlasreport.MigrateDownResultOptions{
		Driver:           conn.Info().Dialect,
		URL:              opts.url,
		Dir:              opts.rawDir,
		FS:               source.FileSystem,
		Migrations:       result.Migrations,
		PlannedVersions:  result.PlannedVersions,
		RevertedVersions: result.RevertedVersions,
		CurrentVersion:   result.CurrentVersion,
		TargetVersion:    targetVersion,
		Reverted:         result.Reverted,
		StartedAt:        result.StartedAt,
		EndedAt:          result.EndedAt,
		ErrorText:        result.ErrorText,
		DownError:        result.DownError,
	})
	if execErr != nil {
		if writeErr != nil {
			return fmt.Errorf("%w; additionally failed to write --format output: %v", execErr, writeErr)
		}
		return execErr
	}
	return writeErr
}

// parseAtlasMigrateDownFormatArgs parses the Atlas down flag surface for the
// format path. The default output path forwards unrecognized flags to the
// native command; the format path executes directly, so an unrecognized flag
// fails loudly here instead of being silently dropped.
func parseAtlasMigrateDownFormatArgs(verb atlasVerb, args []string) (*atlasMigrateDownFormatOptions, error) {
	if err := rejectNativeOnlyAtlasFlags("migrate", verb, args); err != nil {
		return nil, err
	}
	opts := &atlasMigrateDownFormatOptions{}
	var toTag string
	var skipChecks, forcePlan bool
	flagSet := pflag.NewFlagSet("atlas migrate down", pflag.ContinueOnError)
	flagSet.SetOutput(io.Discard)
	flagSet.StringVarP(&opts.url, "url", "u", "", "")
	flagSet.StringVar(&opts.dir, "dir", "", "")
	flagSet.StringVar(&opts.devURL, "dev-url", "", "")
	flagSet.StringVar(&opts.toVersion, "to-version", "", "")
	flagSet.StringVar(&toTag, "to-tag", "", "")
	flagSet.BoolVar(&opts.dryRun, "dry-run", false, "")
	flagSet.StringVar(&opts.format, "format", "", "")
	flagSet.StringVar(&opts.revisionsSchema, "revisions-schema", "", "")
	flagSet.StringVar(&opts.lockTimeout, "lock-timeout", "", "")
	flagSet.BoolVar(&skipChecks, "skip-checks", false, "")
	flagSet.BoolVar(&forcePlan, "plan", false, "")
	if err := flagSet.Parse(args); err != nil {
		return nil, fmt.Errorf("atlas migrate down: %w", err)
	}
	if flagSet.NArg() > 0 {
		return nil, fmt.Errorf("atlas migrate down accepts no arguments, got %q", flagSet.Args())
	}
	if err := applyAtlasMigrateDownEnvFallback(flagSet); err != nil {
		return nil, err
	}
	for _, name := range atlasMigrateDownUnsupportedFlags {
		if !flagSet.Changed(name) {
			continue
		}
		flag, ok := atlasVerbFlag(verb, name)
		if !ok {
			return nil, fmt.Errorf("atlas migrate down does not accept --%s", name)
		}
		return nil, atlasargs.UnsupportedFlagError("migrate", "down", flag, "")
	}
	if opts.dir != "" {
		opts.rawDir = opts.dir
		dir, err := atlasargs.LocalDirValue(opts.dir)
		if err != nil {
			return nil, fmt.Errorf("atlas migrate down --dir: %w", err)
		}
		opts.dir = dir
	}
	opts.flagSet = flagSet
	return opts, nil
}

// atlasMigrateDownUnsupportedFlags are the Atlas down flags this command
// accepts for help parity but refuses at runtime.
var atlasMigrateDownUnsupportedFlags = []string{"to-tag", "skip-checks", "plan"}

// atlasMigrateDownExplicitOnlyFlags are the flags this path must not fill from
// a PTAH_<FLAG> environment value. It is deliberately NOT the unsupported list:
// PTAH_TO_TAG and PTAH_PLAN are requests for capabilities Ptah lacks, and the
// loud refusal is the right answer to them — silently discarding PTAH_TO_TAG
// would roll the whole history back to version 0 instead.
//
// Only --skip-checks is excluded, and only because `migrate apply` reads
// PTAH_SKIP_CHECKS as its pre-migration check bypass, so on this verb the
// variable is not an ask at all. It mirrors the EnvDisabled marker the arg
// mapper honors on the same flag (see atlasargs.ExplicitUnsupportedBoolReason);
// the two paths parse flags independently, so both need it.
//
// --format is deliberately absent: it is unsupported in the mapper but IS
// honored here, and atlasMigrateDownWantsFormat routes to this path on
// PTAH_FORMAT alone.
var atlasMigrateDownExplicitOnlyFlags = []string{"skip-checks"}

// applyAtlasMigrateDownEnvFallback fills unset flags from PTAH_<FLAG>
// environment values, mirroring the env convention the forward path's arg
// mapper applies.
func applyAtlasMigrateDownEnvFallback(flagSet *pflag.FlagSet) error {
	var envErr error
	flagSet.VisitAll(func(flag *pflag.Flag) {
		if envErr != nil || flag.Changed {
			return
		}
		if slices.Contains(atlasMigrateDownExplicitOnlyFlags, flag.Name) {
			return
		}
		value, ok := os.LookupEnv(atlasFlagEnvName(flag.Name))
		if !ok {
			return
		}
		if flag.Value.Type() == "bool" {
			// One grammar and one error for every boolean PTAH_* variable, and an
			// explicitly empty one is a configuration error rather than a silent
			// "unset". See [go.5x5.cz/ptah/internal/envbool] and
			// stokaro/ptah#1334.
			parsed, err := envbool.Parse(atlasFlagEnvName(flag.Name), value)
			if err != nil {
				envErr = fmt.Errorf("atlas migrate down: %w", err)
				return
			}
			if !parsed {
				return
			}
		} else if value == "" {
			return
		}
		if err := flag.Value.Set(value); err != nil {
			envErr = fmt.Errorf("atlas migrate down: invalid value %q for %s: %w", value, atlasFlagEnvName(flag.Name), err)
			return
		}
		flag.Changed = true
	})
	return envErr
}

func atlasFlagEnvName(flagName string) string {
	name := strings.NewReplacer("-", "_", ".", "_").Replace(flagName)
	return "PTAH_" + strings.ToUpper(name)
}

func atlasVerbFlag(verb atlasVerb, name string) (atlasargs.Flag, bool) {
	for _, flag := range verb.flags {
		if flag.Name == name {
			return flag, true
		}
	}
	return atlasargs.Flag{}, false
}

// applyAtlasMigrateDownFormatProjectConfig resolves atlas.hcl env values for
// the format path with the same precedence the dedicated apply and status
// commands use: an explicitly changed flag wins, an unset flag falls back to
// the selected env.
func applyAtlasMigrateDownFormatProjectConfig(
	diagnostics io.Writer,
	opts *atlasMigrateDownFormatOptions,
	projectArgs atlasProjectArgValues,
) (
	project atlasProject,
	returnErr error,
) {
	// Same rule as resolveAtlasVerbProject: -c and --env select a project file
	// and make it required, --var only supplies values to one and leaves it
	// optional (stokaro/ptah#1241 item 12).
	requirement := requiredAtlasProject
	if !projectArgs.changed {
		if len(projectArgs.flags.vars) == 0 {
			return atlasProject{}, nil
		}
		requirement = optionalAtlasProject
	}
	project, loaded, err := openAtlasProject(projectArgs.flags, requirement)
	if err != nil {
		return atlasProject{}, err
	}
	if !loaded {
		return atlasProject{}, nil
	}
	defer closeAtlasProjectOnError(&project, &returnErr)
	cfg := project.Config
	if err := dbcli.ReportIgnoredAtlasConstructs(diagnostics, cfg); err != nil {
		return atlasProject{}, err
	}
	opts.url = atlasDownEffective(
		opts.flagSet,
		"url",
		opts.url,
		cfg.StringValue(projectconfig.StringDatabaseURL),
	)
	opts.devURL = atlasDownEffective(
		opts.flagSet,
		"dev-url",
		opts.devURL,
		cfg.StringValue(projectconfig.StringDevURL),
	)
	opts.revisionsSchema = atlasDownEffective(
		opts.flagSet,
		"revisions-schema",
		opts.revisionsSchema,
		cfg.StringValue(projectconfig.StringMigrationRevisionsSchema),
	)
	opts.lockTimeout = atlasDownEffective(
		opts.flagSet,
		"lock-timeout",
		opts.lockTimeout,
		cfg.StringValue(projectconfig.StringMigrationLockTimeout),
	)
	dirFormat := cfg.StringValue(projectconfig.StringMigrationFormat)
	if dirFormat.Present {
		// The format path executes Atlas-format directories only, matching the
		// dedicated apply and status commands.
		if _, err := atlasMigrateDirFormatValue(dirFormat.Value); err != nil {
			return project, fmt.Errorf("atlas.hcl migration.format: %w", err)
		}
	}
	migrationDir := cfg.StringValue(projectconfig.StringMigrationDir)
	if !opts.flagSet.Changed("dir") && migrationDir.Present {
		dir, err := project.localDirWithQuery(migrationDir.Value)
		if err != nil {
			return project, fmt.Errorf("atlas.hcl migration.dir: %w", err)
		}
		if len(dir.Query) > 0 {
			return project,
				fmt.Errorf("atlas.hcl migration.dir: migration directory URL query parameters are not supported for this command")
		}
		opts.rawDir = migrationDir.Value
		opts.dir = dir.Path
		opts.dirOptions = project.localOptions(dir)
	}
	return project, nil
}

func atlasDownEffective(
	flagSet *pflag.FlagSet,
	name string,
	flagValue string,
	configValue projectconfig.Value[string],
) string {
	if flagSet.Changed(name) || !configValue.Present {
		return flagValue
	}
	return configValue.Value
}

func parseAtlasMigrateDownTarget(value string) (int64, error) {
	value = strings.TrimSpace(value)
	if value == "" {
		// The forward path maps a missing --to-version to the native --target
		// default of 0, rolling back everything; the format path keeps the same
		// semantics so both outputs describe the same operation.
		return 0, nil
	}
	target, err := strconv.ParseInt(value, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("invalid --to-version %q: %w", value, err)
	}
	if target < 0 {
		return 0, fmt.Errorf("--to-version must be greater than or equal to zero")
	}
	return target, nil
}

package atlas

import (
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"

	"github.com/spf13/cobra"
	"github.com/spf13/pflag"

	"github.com/stokaro/ptah/cmd/internal/dbcli"
	"github.com/stokaro/ptah/dbschema"
	"github.com/stokaro/ptah/internal/atlasargs"
	"github.com/stokaro/ptah/internal/atlasmigrate"
	"github.com/stokaro/ptah/internal/atlasreport"
	"github.com/stokaro/ptah/internal/pathguard"
	"github.com/stokaro/ptah/migration/generator"
	"github.com/stokaro/ptah/migration/migrator"
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
	confirm         bool

	flagSet *pflag.FlagSet
	// rawDir preserves the pre-resolution --dir value for the report's Env.Dir.
	rawDir string
}

func runAtlasMigrateDownFormat(cmd *cobra.Command, verb atlasVerb, args []string) error {
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
	if err := applyAtlasMigrateDownFormatProjectConfig(opts, project); err != nil {
		return err
	}
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
	dir, err := pathguard.ResolveCLIPath(opts.dir)
	if err != nil {
		return fmt.Errorf("invalid migration directory: %w", err)
	}

	connectCtx, cancel := dbcli.ConnectContext(cmd.Context(), dbcli.DefaultConnectTimeout)
	defer cancel()
	conn, err := dbschema.ConnectToDatabase(connectCtx, opts.url)
	if err != nil {
		return fmt.Errorf("error connecting to database: %w", err)
	}
	defer dbschema.CloseAndWarn(conn)

	plan, err := atlasmigrate.PrepareDown(cmd.Context(), conn, atlasmigrate.DownOptions{
		Dir:                  dir,
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
			ShadowDatabaseURL: opts.devURL,
			FS:                os.DirFS(dir),
			Dialect:           conn.Info().Dialect,
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

	if !opts.dryRun && !opts.confirm && !plan.Noop() {
		confirmed, err := confirmAtlasMigrateDownFormat(cmd, plan)
		if err != nil {
			return err
		}
		if !confirmed {
			return nil
		}
	}

	result, execErr := plan.Execute(cmd.Context())
	writeErr := atlasreport.WriteMigrateDownFormat(cmd.OutOrStdout(), opts.format, atlasreport.MigrateDownResultOptions{
		Driver:           conn.Info().Dialect,
		URL:              opts.url,
		Dir:              opts.rawDir,
		FS:               os.DirFS(dir),
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
// fails loudly here instead of being silently dropped. The native --confirm
// flag is accepted because it reaches the native command through the default
// path's pass-through and is the only non-interactive confirmation.
func parseAtlasMigrateDownFormatArgs(verb atlasVerb, args []string) (*atlasMigrateDownFormatOptions, error) {
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
	flagSet.BoolVar(&opts.confirm, "confirm", false, "")
	if err := flagSet.Parse(args); err != nil {
		return nil, fmt.Errorf("atlas migrate down: %w", err)
	}
	if flagSet.NArg() > 0 {
		return nil, fmt.Errorf("atlas migrate down accepts no arguments, got %q", flagSet.Args())
	}
	if err := applyAtlasMigrateDownEnvFallback(flagSet); err != nil {
		return nil, err
	}
	for _, name := range []string{"to-tag", "skip-checks", "plan"} {
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

// applyAtlasMigrateDownEnvFallback fills unset flags from PTAH_<FLAG>
// environment values, mirroring the env convention the forward path's arg
// mapper applies.
func applyAtlasMigrateDownEnvFallback(flagSet *pflag.FlagSet) error {
	var envErr error
	flagSet.VisitAll(func(flag *pflag.Flag) {
		if envErr != nil || flag.Changed {
			return
		}
		value, ok := os.LookupEnv(atlasFlagEnvName(flag.Name))
		if !ok || value == "" {
			return
		}
		if flag.Value.Type() == "bool" {
			parsed, err := strconv.ParseBool(value)
			if err != nil {
				envErr = fmt.Errorf("atlas migrate down: invalid boolean value %q for %s", value, atlasFlagEnvName(flag.Name))
				return
			}
			if !parsed {
				return
			}
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
func applyAtlasMigrateDownFormatProjectConfig(opts *atlasMigrateDownFormatOptions, project atlasProjectArgValues) error {
	if !project.changed {
		return nil
	}
	cfg, err := loadRequiredAtlasProjectConfig(project.flags)
	if err != nil {
		return err
	}
	opts.url = atlasDownEffective(opts.flagSet, "url", opts.url, cfg.DatabaseURL)
	opts.devURL = atlasDownEffective(opts.flagSet, "dev-url", opts.devURL, cfg.DevURL)
	opts.revisionsSchema = atlasDownEffective(opts.flagSet, "revisions-schema", opts.revisionsSchema, cfg.Migration.RevisionsSchema)
	opts.lockTimeout = atlasDownEffective(opts.flagSet, "lock-timeout", opts.lockTimeout, cfg.Migration.LockTimeout)
	if cfg.Migration.Format != "" {
		// The format path executes Atlas-format directories only, matching the
		// dedicated apply and status commands.
		if _, err := atlasMigrateDirFormatValue(cfg.Migration.Format); err != nil {
			return fmt.Errorf("atlas.hcl migration.format: %w", err)
		}
	}
	if !opts.flagSet.Changed("dir") && cfg.Migration.Dir != "" {
		dir, err := atlasProjectConfigLocalDirFromFlags(project.flags, cfg.Migration.Dir)
		if err != nil {
			return fmt.Errorf("atlas.hcl migration.dir: %w", err)
		}
		opts.rawDir = cfg.Migration.Dir
		opts.dir = dir
	}
	return nil
}

func atlasDownEffective(flagSet *pflag.FlagSet, name, flagValue, configValue string) string {
	if flagSet.Changed(name) || configValue == "" {
		return flagValue
	}
	return configValue
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

// confirmAtlasMigrateDownFormat asks for the same YES confirmation the native
// down command requires, on stderr so the rendered report stays alone on
// stdout. A declined confirmation cancels without writing a report.
func confirmAtlasMigrateDownFormat(cmd *cobra.Command, plan atlasmigrate.DownPlan) (bool, error) {
	prompt := cmd.ErrOrStderr()
	fmt.Fprintln(prompt, "⚠️  WARNING: Rolling back migrations can result in data loss!")
	fmt.Fprintf(prompt, "This will roll back the database from version %d to version %d.\n", plan.CurrentVersion, plan.TargetVersion)
	if len(plan.PlannedVersions) > 0 {
		fmt.Fprintf(prompt, "The following %d migration(s) will be rolled back: %v\n", len(plan.PlannedVersions), plan.PlannedVersions)
	}
	fmt.Fprint(prompt, "Are you sure you want to continue? Type 'YES' to confirm: ")

	var confirmation string
	if _, err := fmt.Fscan(cmd.InOrStdin(), &confirmation); err != nil {
		return false, fmt.Errorf("read rollback confirmation: %w", err)
	}
	if confirmation != "YES" {
		fmt.Fprintln(prompt, "Migration rollback canceled.")
		return false, nil
	}
	fmt.Fprintln(prompt)
	return true, nil
}

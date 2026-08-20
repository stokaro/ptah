// Package atlas exposes Atlas-compatible command paths.
package atlas

import (
	"context"
	"errors"
	"fmt"
	"os"
	"slices"
	"strings"

	"github.com/spf13/cobra"
	"github.com/spf13/pflag"

	"go.5x5.cz/ptah/cmd/internal/cmdadapter"
	"go.5x5.cz/ptah/cmd/internal/cmdflags"
	"go.5x5.cz/ptah/cmd/internal/cmdutil"
	"go.5x5.cz/ptah/cmd/internal/dbcli"
	"go.5x5.cz/ptah/cmd/internal/exitcode"
	"go.5x5.cz/ptah/cmd/internal/licensetext"
	"go.5x5.cz/ptah/cmd/internal/migrationsource"
	"go.5x5.cz/ptah/cmd/migratecheckpoint"
	"go.5x5.cz/ptah/cmd/migratedown"
	"go.5x5.cz/ptah/cmd/migrateedit"
	"go.5x5.cz/ptah/cmd/migratels"
	"go.5x5.cz/ptah/cmd/migraterebase"
	"go.5x5.cz/ptah/cmd/migraterm"
	"go.5x5.cz/ptah/cmd/migrateshow"
	"go.5x5.cz/ptah/cmd/migrationstest"
	"go.5x5.cz/ptah/cmd/schema"
	"go.5x5.cz/ptah/config/projectconfig"
	"go.5x5.cz/ptah/internal/atlasargs"
	"go.5x5.cz/ptah/internal/atlascompatpolicy"
	"go.5x5.cz/ptah/internal/atlassource"
	"go.5x5.cz/ptah/internal/buildinfo"
)

type atlasVerb struct {
	use        string
	displayUse string
	short      string
	native     string
	factory    func() *cobra.Command
	prefixArgs []string
	// quietNarration lowers the native log threshold so the target's own
	// lifecycle narration stays off this surface's stderr. It is applied by
	// quietingLogLevelArgs rather than through prefixArgs, because it must not
	// apply under a machine-readable log format.
	quietNarration      bool
	positionals         []atlasPositionalArg
	positionalOptional  bool
	nativeOnlyFlags     []string
	flags               []atlasargs.Flag
	nativeProjectConfig bool
	// projectConfig overrides how loaded atlas.hcl values map onto the verb's
	// Atlas flags. When nil, the generic applyAtlasProjectConfigToArgs is used.
	projectConfig atlasProjectArgsApplier
	// requireDirScheme marks a verb that refuses a --dir naming no scheme, the
	// way the mirrored surface does. It is opt-in per verb rather than the rule
	// for every forwarded verb because turning it on where it was off is a
	// behavior change for existing callers, and each verb's own measurement is
	// what says whether the mirrored one refuses.
	requireDirScheme bool
	// writesDir marks a verb that can CREATE the migration directory it was
	// pointed at. New uses this marker for its command-line scheme gate; diff
	// owns the same gate separately, as do the read-only hash, validate, status
	// and lint adapters. A directory named by atlas.hcl remains separate work in
	// stokaro/ptah#1186.
	writesDir bool
}

type atlasPositionalArg struct {
	name       string
	nativeName string
	// mapValue rewrites the positional value before it is appended as the
	// native flag pair.
	mapValue func(string) (string, error)
	// variadic marks a positional Atlas documents as repeatable. Ptah forwards
	// a single value and rejects multiple values loudly until multi-value
	// forwarding is implemented.
	variadic bool
	// repeatable marks a variadic positional whose native flag accumulates
	// every occurrence, so each value is forwarded as its own flag pair rather
	// than refused. It is a second field rather than an inference from variadic
	// because forwarding several values to a native flag that holds ONE would
	// silently keep the last and drop the rest, which is the failure the
	// refusal exists to prevent.
	repeatable bool
}

const (
	atlasDirFormatDefault = "atlas"
	// atlasDefaultMigrationDirURL is the migration directory every verb that
	// documents a `--dir` default falls back to. The pinned community binary
	// v1.3.0 prints `(default "file://migrations")` on `migrate apply`,
	// `migrate new`, `migrate diff`, `migrate status`, `migrate set`,
	// `migrate lint`, `migrate hash` and `migrate validate`; Ptah honored it
	// on some and not others (stokaro/ptah#1241 items 2 and 3), so the value
	// lives here once instead of being retyped per verb.
	atlasDefaultMigrationDirURL = "file://migrations"
	atlasErrorExitCode          = 1
	// atlasErrorPrefix is the diagnostic prefix of the whole compat surface.
	// It is declared once on the root command (see NewCompatCommand) and
	// resolved from the command tree at print time, so every diagnostic below
	// the root carries it whether it is printed here or by shared cmdutil
	// machinery.
	atlasErrorPrefix = "Error"
	// atlasStrictCompatAnnotation carries the immutable process policy through
	// shared compatibility helpers without re-reading the environment.
	atlasStrictCompatAnnotation = "ptah.atlas.strict-compat"
)

func failAtlasCommand(cmd *cobra.Command, err error) error {
	if _, writeErr := fmt.Fprintf(cmd.ErrOrStderr(), "%s: %s\n", cmdutil.ErrorPrefix(cmd), err); writeErr != nil {
		return exitcode.New(
			atlasErrorExitCode,
			fmt.Errorf("%w: write Atlas diagnostic: %w", err, writeErr),
		)
	}
	return exitcode.New(atlasErrorExitCode, err)
}

var unsupportedAtlasDirFormats = []string{
	"dbmate",
	"flyway",
	"golang-migrate",
	"goose",
	"liquibase",
}

// NewCompatCommand returns an Atlas-compatible root command.
func NewCompatCommand(use string) *cobra.Command {
	return NewCompatCommandWithPolicy(use, atlascompatpolicy.Full())
}

// NewCompatCommandWithPolicy returns an Atlas-compatible root command under
// policy. The ptah-compat process resolves the policy once before calling this
// constructor; tests and embedded callers choose it explicitly so command-tree
// reuse never depends on mutable process environment.
func NewCompatCommandWithPolicy(use string, policy atlascompatpolicy.Policy) *cobra.Command {
	use = strings.TrimSpace(use)
	if use == "" {
		use = "ptah-compat"
	}
	cmd := newAtlasCommand(use+" [command]", "Atlas-compatible Ptah command tree", `Atlas-compatible Ptah command tree.

This executable exposes Atlas-style commands at process root for scripts that
expect commands such as migrate apply or schema inspect. Commands that have an
existing Ptah equivalent forward to that native command.`, policy)
	if !policy.IsStrictCE() {
		cmdflags.InstallEnvBinding("PTAH", cmd)
	}
	cmdutil.SetErrorCodePolicy(cmd, atlasErrorExitCode)
	// The prefix is a property of the surface, exactly like the exit code
	// above: declaring it on the root is what makes every diagnostic below it
	// -- including the ones cobra routes through cmdutil's shared printers --
	// answer with the same prefix, instead of it depending on which file
	// happened to override a printer.
	cmdutil.SetErrorPrefixPolicy(cmd, atlasErrorPrefix)
	return cmd
}

// ValidateStrictCompatFlagEnvironment refuses environment twins that the full
// compatibility tree binds but the strict CE tree intentionally omits. It is
// called by the ptah-compat process before help, version, argument handling, or
// command work. Unknown PTAH-prefixed values are left alone so atlas.hcl may
// read ordinary user inputs through getenv.
func ValidateStrictCompatFlagEnvironment(policy atlascompatpolicy.Policy) error {
	if !policy.IsStrictCE() {
		return nil
	}
	root := NewCompatCommand("atlas")
	visited := make(map[*pflag.Flag]bool)
	var validationErr error
	var visit func(*cobra.Command)
	visit = func(cmd *cobra.Command) {
		for _, flags := range []*pflag.FlagSet{cmd.Flags(), cmd.PersistentFlags()} {
			flags.VisitAll(func(flag *pflag.Flag) {
				if validationErr != nil || visited[flag] {
					return
				}
				visited[flag] = true
				bindings := cmdflags.ForwardedEnvBindings(flag)
				if name, bound := cmdflags.EnvBindingName("PTAH", flag); bound {
					bindings = append(bindings, name)
				}
				for _, name := range slices.Compact(bindings) {
					value, present := os.LookupEnv(name)
					if !present {
						continue
					}
					validationErr = policy.ValidateFlagEnvironment(name, value, flag.Value.Type())
					if validationErr != nil {
						return
					}
				}
			})
		}
		for _, child := range cmd.Commands() {
			visit(child)
		}
	}
	visit(root)
	return validationErr
}

func newAtlasCommand(use, short, long string, policy atlascompatpolicy.Policy) *cobra.Command {
	cmd := &cobra.Command{
		Use:   use,
		Short: short,
		Long:  long,
		RunE:  runAtlasGroupHelp,
	}
	if policy.IsStrictCE() {
		cmd.Annotations = map[string]string{atlasStrictCompatAnnotation: "true"}
		cmd.PersistentPreRunE = func(cmd *cobra.Command, _ []string) error {
			if err := validateAtlasStrictCompatURLFlags(cmd, policy); err != nil {
				return failAtlasCommand(cmd, err)
			}
			return nil
		}
	}
	cmdutil.ConfigureCommandArgs(cmd, atlasRootArgs)
	schemaCommand := newAtlasSchemaCommand(policy)
	migrateCommand := newAtlasMigrateCommand(policy)
	cmd.AddCommand(newAtlasVersionCommand())
	cmd.AddCommand(newAtlasLicenseCommand())
	cmd.AddCommand(schemaCommand)
	cmd.AddCommand(migrateCommand)
	installAtlasCompletionCommand(cmd)
	installAtlasUsageTree(cmd)
	installAtlasProjectFlagResetTree(schemaCommand)
	installAtlasProjectFlagResetTree(migrateCommand)
	installAtlasProjectFlagResetRoot(cmd, schemaCommand, migrateCommand)
	return cmd
}

func atlasCompatibilityPolicy(cmd *cobra.Command) atlascompatpolicy.Policy {
	if cmd != nil && cmd.Root().Annotations[atlasStrictCompatAnnotation] == "true" {
		return atlascompatpolicy.StrictCE()
	}
	return atlascompatpolicy.Full()
}

func validateAtlasStrictCompatURLFlags(cmd *cobra.Command, policy atlascompatpolicy.Policy) error {
	for _, name := range []string{"url", "dev-url", "from", "to"} {
		flag := cmd.Flags().Lookup(name)
		if flag == nil || !flag.Changed {
			continue
		}
		values, err := atlasFlagStringValues(cmd, name, flag.Value.Type())
		if err != nil {
			return fmt.Errorf("read --%s for strict compatibility: %w", name, err)
		}
		for _, value := range values {
			if err := policy.ValidateURL(value); err != nil {
				return err
			}
		}
	}
	return nil
}

func atlasFlagStringValues(cmd *cobra.Command, name, valueType string) ([]string, error) {
	switch valueType {
	case "stringArray":
		return cmd.Flags().GetStringArray(name)
	case "stringSlice":
		return cmd.Flags().GetStringSlice(name)
	default:
		value, err := cmd.Flags().GetString(name)
		return []string{value}, err
	}
}

func newAtlasSchemaCommand(policy atlascompatpolicy.Policy) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "schema [command]",
		Short: "Atlas schema commands",
		RunE:  runAtlasGroupHelp,
	}
	// Atlas CE treats an extra token on a command group as a request for that
	// group's help, so the group intentionally accepts positional arguments.
	cmdutil.ConfigureCommandArgs(cmd, nil)
	registerAtlasProjectFlags(cmd.PersistentFlags(), &atlasProjectFlagValues{})
	cmd.AddCommand(newAtlasSchemaCleanCommand(policy))
	cmd.AddCommand(newAtlasSchemaInspectCommand(policy))
	cmd.AddCommand(newAtlasSchemaApplyCommand(policy))
	cmd.AddCommand(newAtlasSchemaDiffCommand(policy))
	cmd.AddCommand(newAtlasSchemaFmtCommand())
	if policy.IsStrictCE() {
		addAtlasCommunityGatedCommands(cmd, "schema", []string{"plan", "push", "test", "validate"})
	} else {
		cmd.AddCommand(newAtlasSchemaPlanCommand())
		cmd.AddCommand(newAtlasAdapterCommand("schema", atlasSchemaTestVerb()))
		cmd.AddCommand(newAtlasAdapterCommand("schema", atlasSchemaValidateVerb()))
		addAtlasUnsupportedCommands(cmd, []atlasUnsupportedVerb{
			{use: "push", short: "Push schema state to a remote registry"},
		})
	}
	return cmd
}

func newAtlasMigrateCommand(policy atlascompatpolicy.Policy) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "migrate [command]",
		Short: "Atlas migrate commands",
		RunE:  runAtlasGroupHelp,
	}
	// Atlas CE treats an extra token on a command group as a request for that
	// group's help, so the group intentionally accepts positional arguments.
	cmdutil.ConfigureCommandArgs(cmd, nil)
	registerAtlasProjectFlags(cmd.PersistentFlags(), &atlasProjectFlagValues{})
	cmd.AddCommand(newAtlasMigrateApplyCommand(policy))
	cmd.AddCommand(newAtlasMigrateLintCommand(policy))
	cmd.AddCommand(newAtlasMigrateStatusCommand())
	cmd.AddCommand(newAtlasMigrateSetCommand())
	cmd.AddCommand(newAtlasMigrateHashCommand(policy))
	cmd.AddCommand(newAtlasMigrateValidateCommand(policy))
	cmd.AddCommand(newAtlasMigrateNewCommand())
	cmd.AddCommand(newAtlasMigrateDiffCommand(policy))
	cmd.AddCommand(newAtlasMigrateImportCommand(policy))
	if policy.IsStrictCE() {
		addAtlasCommunityGatedCommands(cmd, "migrate", []string{
			"checkpoint", "down", "edit", "push", "rebase", "rm", "test",
		})
	} else {
		cmd.AddCommand(newAtlasMigrateDownCommand(policy))
		for _, verb := range atlasMigrateForwardVerbs() {
			cmd.AddCommand(newAtlasAdapterCommand("migrate", verb))
		}
		addAtlasUnsupportedCommands(cmd, []atlasUnsupportedVerb{
			{use: "push", short: "Push migration directory to a remote registry"},
		})
	}
	return cmd
}

// atlasMigrateForwardVerbs returns the `migrate` verbs that are plain
// table-driven forwards, in registration order. It is a named function rather
// than a literal inside newAtlasMigrateCommand so that a test can enumerate
// every forwarded verb — see TestAtlasVerbsCarryLogLevelExactlyWhereTargetTakesIt,
// which measures each one instead of only the verb an issue happened to name.
func atlasMigrateForwardVerbs() []atlasVerb {
	return []atlasVerb{
		{
			use:                "checkpoint",
			displayUse:         "checkpoint [flags] [name]",
			short:              "Squash migration history into a cumulative-schema checkpoint",
			native:             "migrations checkpoint",
			factory:            migratecheckpoint.NewMigrateCheckpointCommand,
			positionals:        []atlasPositionalArg{{name: "name", nativeName: "description"}},
			positionalOptional: true,
			flags: []atlasargs.Flag{
				atlasargs.NativeLocalDir("dir", "", "Migration directory", "migrations-dir"),
				atlasargs.NativeString("dev-url", "", "URL of the dev database the directory is replayed into", "shadow-db"),
				// Checkpoint writes both conventions: `atlas` emits the single
				// up-only file whose first line is `-- atlas:checkpoint` plus
				// atlas.sum, `ptah` emits the reversible `.checkpoint.` pair
				// plus ptah.sum. The READ side has honored the directive since
				// #954, so an Atlas checkpoint Ptah writes bootstraps and skips
				// identically under Atlas and under Ptah. Every other Atlas
				// directory format is still rejected loudly (see
				// docs/site/src/content/docs/reference/atlas-commands.md).
				atlasCheckpointDirFormatFlag(),
				// Atlas's published CLI reference registers these five on
				// `migrate checkpoint`; the pinned community binary registers
				// none of its own flags on this verb (every spelling answers
				// `unknown flag`), so the reference is the oracle here.
				//
				// --lock-name is deliberately absent: the named-lock family is
				// one feature across five verbs and lands as its own change.
				atlasargs.NativeStringArray("schema", "s", "Schema names the checkpoint covers", "schemas"),
				atlasargs.String("qualifier", "", "Qualify tables with a custom qualifier when working on a single schema"),
				atlasargs.NativeString(
					"lock-timeout",
					"",
					"How long to wait for the dev database's migration lock during the replay",
					"migration-lock-timeout",
				),
				atlasargs.Bool("edit", "", "Edit the generated checkpoint file(s)"),
			},
		},
		atlasMigrateEditVerb(),
		atlasMigrateLsVerb(),
		atlasMigrateRebaseVerb(),
		atlasMigrateRmVerb(),
		atlasMigrateShowVerb(),
		atlasMigrateTestVerb(),
	}
}

func atlasRootArgs(cmd *cobra.Command, args []string) error {
	if len(args) == 0 {
		return nil
	}

	unknownErr := fmt.Errorf("unknown command %q for %q", args[0], "atlas")
	var diagnostic strings.Builder
	fmt.Fprintf(&diagnostic, "%s: %s\n", cmdutil.ErrorPrefix(cmd), unknownErr)

	suggestions := cmd.SuggestionsFor(args[0])
	if len(suggestions) > 0 {
		diagnostic.WriteString("\nDid you mean this?\n")
		for _, suggestion := range suggestions {
			fmt.Fprintf(&diagnostic, "\t%s\n", suggestion)
		}
		diagnostic.WriteString("\n")
	}
	diagnostic.WriteString("Run 'atlas --help' for usage.\n")

	if _, err := fmt.Fprint(cmd.ErrOrStderr(), diagnostic.String()); err != nil {
		return exitcode.New(atlasErrorExitCode, fmt.Errorf("%w: write diagnostic: %w", unknownErr, err))
	}
	return exitcode.New(atlasErrorExitCode, unknownErr)
}

func installAtlasCompletionCommand(cmd *cobra.Command) {
	cmd.InitDefaultCompletionCmd()
	for _, child := range cmd.Commands() {
		if child.Name() != "completion" {
			continue
		}
		child.Use = "completion [command]"
		child.RunE = runAtlasGroupHelp
		cmdutil.ConfigureCommandArgs(child, nil)
		for _, shell := range child.Commands() {
			shell.Args = atlasCompletionShellArgs
		}
		return
	}
}

func runAtlasGroupHelp(cmd *cobra.Command, _ []string) error {
	if err := renderAtlasHelp(cmd); err != nil {
		return exitcode.New(atlasErrorExitCode, err)
	}
	return nil
}

func atlasCompletionShellArgs(cmd *cobra.Command, args []string) error {
	if len(args) == 0 {
		return nil
	}

	unknownErr := fmt.Errorf("unknown command %q for %q", args[0], "atlas completion "+cmd.Name())
	if _, err := fmt.Fprintf(cmd.ErrOrStderr(), "%s: %s\n", cmdutil.ErrorPrefix(cmd), unknownErr); err != nil {
		return exitcode.New(atlasErrorExitCode, fmt.Errorf("%w: write diagnostic: %w", unknownErr, err))
	}
	return exitcode.New(atlasErrorExitCode, unknownErr)
}

type atlasUnsupportedVerb struct {
	use   string
	short string
}

func addAtlasUnsupportedCommands(parent *cobra.Command, verbs []atlasUnsupportedVerb) {
	for _, verb := range verbs {
		parent.AddCommand(newAtlasUnsupportedCommand(verb))
	}
}

func addAtlasCommunityGatedCommands(parent *cobra.Command, group string, verbs []string) {
	for _, verb := range verbs {
		parent.AddCommand(newAtlasStrictCompatGatedCommand(group, verb))
	}
}

func newAtlasStrictCompatGatedCommand(group, verb string) *cobra.Command {
	path := "ptah-compat " + group + " " + verb
	body := atlasStrictCompatGateBody(path)
	cmd := &cobra.Command{
		Use:    verb,
		Hidden: true,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return failAtlasStrictCompatGate(cmd, path)
		},
	}
	cmd.SetHelpFunc(func(cmd *cobra.Command, _ []string) {
		_, _ = fmt.Fprint(cmd.OutOrStdout(), body)
	})
	cmd.Annotations = map[string]string{atlasPreserveHelpAnnotation: "true"}
	cmdutil.ConfigureCommandArgs(cmd, nil)
	return cmd
}

func atlasStrictCompatGateBody(path string) string {
	return fmt.Sprintf(`'%s' is unavailable while PTAH_ATLAS_STRICT_COMPAT is enabled.

Unset PTAH_ATLAS_STRICT_COMPAT to use Ptah's full compatibility surface.
`, path) + "\n"
}

func failAtlasStrictCompatGate(cmd *cobra.Command, path string) error {
	body := atlasStrictCompatGateBody(path)
	unsupportedErr := errors.New(strings.TrimSpace(strings.SplitN(body, "\n", 2)[0]))
	if _, err := fmt.Fprintf(cmd.ErrOrStderr(), "Abort: %s", body); err != nil {
		return exitcode.New(atlasErrorExitCode,
			fmt.Errorf("%w: write strict-compatibility diagnostic: %w", unsupportedErr, err))
	}
	return exitcode.New(atlasErrorExitCode, unsupportedErr)
}

func newAtlasUnsupportedCommand(verb atlasUnsupportedVerb) *cobra.Command {
	cmd := &cobra.Command{
		Use:   verb.use,
		Short: verb.short,
		Long:  "This compatibility command is not implemented by Ptah.",
		RunE: func(cmd *cobra.Command, _ []string) error {
			err := fmt.Errorf("%s is not implemented by Ptah", cmd.CommandPath())
			if _, writeErr := fmt.Fprintf(cmd.ErrOrStderr(), "%s: %s\n", cmdutil.ErrorPrefix(cmd), err); writeErr != nil {
				return exitcode.New(1, fmt.Errorf("%w: write diagnostic: %w", err, writeErr))
			}
			return exitcode.New(1, err)
		},
	}
	cmd.SetHelpFunc(func(cmd *cobra.Command, _ []string) {
		fmt.Fprintf(cmd.OutOrStdout(), "%s is not implemented by Ptah.\n", cmd.CommandPath())
	})
	cmd.Annotations = map[string]string{atlasPreserveHelpAnnotation: "true"}
	cmdutil.ConfigureCommandArgs(cmd, cmdutil.NoPositionalArgs)
	return cmd
}

func atlasMigrateDownVerb() atlasVerb {
	return atlasVerb{
		use:     "down",
		short:   "Roll back migrations",
		native:  "migrations down",
		factory: migratedown.NewMigrateDownCommand,
		// An Atlas-surface verb defaults to the Atlas revision-table layout, like
		// `migrate set` above: without this prefix the native --revision-format
		// default of "ptah" silently no-ops against the atlas_schema_revisions
		// rows `atlas migrate apply` writes. --confirm suppresses the native
		// safety prompt because Atlas migrate down executes non-interactively.
		//
		// --log-level warn is what keeps this verb as quiet as the rest of the
		// binary. The Atlas-compatible surface is quiet by construction
		// (cmd/ptah-compat/main.go installs cliobs.QuietDefaultLogger), but that
		// only lowers what the *default* logger accepts. This is the one
		// forwarded verb whose native target starts its own observability
		// runtime (cliobs.Start), and that runtime installs a fresh logger over
		// the quiet default at the native --log-level default of "info" — so the
		// migrator's lifecycle log and the dialect writers' "[DRY RUN] Would ..."
		// narration land on the compat command's stderr (stokaro/ptah#969).
		// warn, not error: it reproduces the #968 threshold exactly, so a
		// Warn-level diagnostic that exists on no other channel (a migration
		// lock that would not release, a skipped out-of-order migration, a
		// connection that would not close) still reaches the user.
		//
		// User args are appended after the prefix, so an explicit native
		// `--revision-format ptah` or `--log-level info` pass-through still
		// overrides the default (pflag keeps the last value).
		//
		// The level is NOT in this list, because it cannot be unconditional.
		// Under `--log-format json` the emitter turns the whole report into
		// Info-level records, so lowering the threshold deletes the command's
		// output rather than its narration -- measured, 2687 bytes became 0 on
		// a rollback that still ran. quietingLogLevelArgs applies it only where
		// the report reaches the writer directly.
		prefixArgs:          []string{"--revision-format", "atlas", "--confirm"},
		quietNarration:      true,
		nativeOnlyFlags:     []string{"confirm"},
		nativeProjectConfig: true,
		flags: []atlasargs.Flag{
			atlasargs.NativeString("url", "u", "Database URL", "db-url"),
			atlasargs.NativeLocalDir("dir", "", "Migration directory", "migrations-dir"),
			// Atlas uses --dev-url for dynamic down planning; Ptah replays and
			// verifies the pre-planned rollback on the same throwaway database
			// before touching the target (native --shadow-db).
			atlasargs.NativeString("dev-url", "", "Dev database URL the rollback plan is verified on before applying it", "shadow-db"),
			atlasargs.NativeString("to-version", "", "Target version to roll back to", "target"),
			// --to-tag targets a hosted registry tag, for which Ptah intentionally
			// has no counterpart (see docs/site/src/content/docs/reference/atlas-commands.md).
			atlasargs.UnsupportedStringReason("to-tag", "", "Target migration tag to roll back to",
				"migration tags require a hosted registry; use --to-version with a migration version instead"),
			atlasargs.NativeBool("dry-run", "", "Show rollback plan without applying it", "dry-run"),
			// --format is implemented by newAtlasMigrateDownCommand, which
			// intercepts it before the arg mapper runs. The Unsupported marker
			// here is defense-in-depth: if a --format value ever reaches the
			// mapper it still fails loudly instead of leaking to the native
			// command as an unknown flag.
			atlasargs.UnsupportedString("format", "", "Atlas Go template output format"),
			atlasargs.NativeString("revisions-schema", "", "Schema for the revision table", "migrations-schema"),
			atlasargs.NativeString("lock-timeout", "", "Timeout for acquiring migration locks", "migration-lock-timeout"),
			// --skip-checks skips the checks of a hosted pre-planned down
			// migration; Ptah reverts through locally reviewed down files and
			// has no generated checks to skip.
			//
			// Explicit-only, unlike the waivers around it, because `migrate
			// apply` reads PTAH_SKIP_CHECKS as its pre-migration check bypass.
			// An ambient value meant for an apply is not a request for hosted down
			// checks, and must not refuse a rollback.
			atlasargs.ExplicitUnsupportedBoolReason("skip-checks", "", "Skip down migration safety checks",
				"down checks require a hosted plan-approval workflow; Ptah reverts through locally reviewed down migrations and has no generated checks to skip"),
			// --plan forces Atlas's registry-bound dynamic down planning.
			// Ptah's local plan files (the `schema plan` workflow) are
			// declarative apply plans, not down plans, so forcing a down plan
			// has no local meaning and is rejected rather than faked.
			atlasargs.UnsupportedBoolReason("plan", "", "Force dynamic down planning",
				"dynamic down planning requires a hosted plan-approval workflow; use --dev-url to verify the pre-planned rollback on a dev database instead"),
		},
	}
}

// atlasMigrateTestVerb forwards `atlas migrate test` to the native
// `ptah migrations test` runner. Atlas's Pro-only migration testing verb maps
// onto Ptah's open testing engine: --dir is the migration directory
// (Atlas-format by default, like the other migrate verbs), --dev-url selects
// the throwaway database the cases run against (an ephemeral SQLite database
// when omitted), and the optional [paths] positional selects the directory of
// Ptah-native YAML test cases (native --dir, default ./tests).
func atlasMigrateTestVerb() atlasVerb {
	return atlasVerb{
		use:                "test",
		displayUse:         "test [flags] [paths]",
		short:              "Run declarative migration tests against a dev database",
		native:             "migrations test",
		factory:            migrationstest.NewMigrationsTestCommand,
		positionals:        []atlasPositionalArg{{name: "paths", nativeName: "dir"}},
		positionalOptional: true,
		flags: []atlasargs.Flag{
			atlasMigrationsDirFlag(),
			atlasMigrateDirFormatFlag("dir-format"),
			atlasTestDevURLFlag(),
			atlasargs.String("run", "", "Run only test cases matching a Go regular expression"),
			atlasTestReportFlag(),
			atlasTestSeedDirFlag(),
			// Atlas's published CLI reference registers --revisions-schema on
			// `migrate test` ("name of the schema the revisions table resides
			// in"), the same spelling it carries on apply, status and set. It
			// maps onto the native --migrations-schema, which places the
			// revision table a migrate_to step writes.
			atlasargs.NativeString(
				"revisions-schema",
				"",
				"Schema the revision table written by a migrate_to step resides in",
				"migrations-schema",
			),
		},
	}
}

// atlasTestDevURLFlag registers the --dev-url that `migrate test` and
// `schema test` run their cases against.
//
// A docker:// value is passed through to the native verb, which provisions it.
// Both test verbs used to refuse the scheme here, at flag-parse time, because
// nothing downstream could provision one; wiring the native runners to
// internal/devdocker is what made the refusal a deleted capability rather than
// an honest diagnostic (stokaro/ptah#844, AGENTS.md compatibility rule (c)).
//
// What is decidable from the URL text alone -- an image no engine table names,
// a form the pinned community binary rejects -- is still refused, by the
// provisioner, in that binary's own words. A directly connectable URL travels
// untouched, so a dialect Ptah does not support still reports itself at the
// connector.
func atlasTestDevURLFlag() atlasargs.Flag {
	return atlasargs.NativeString("dev-url", "", "Dev database URL the test cases run against", "db-url")
}

// atlasTestReportFlag registers the report format both test verbs forward to
// their native runner. The runners have written text, JSON and HTML reports
// since they were built; the Atlas-shaped verbs exposed no way to ask for one,
// so a machine-readable report was unreachable from this surface. The value is
// forwarded verbatim, which keeps the native runner the single place that
// decides which formats exist.
func atlasTestReportFlag() atlasargs.Flag {
	return atlasargs.String("report", "", "Report format: text, json, or html")
}

// atlasTestSeedDirFlag registers the run-level seed directory both test verbs
// forward to their native runner. Without it a case set whose seed step omits
// an inline dir could not run at all under the Atlas-shaped surface: the run
// was refused as invalid before any database was touched.
func atlasTestSeedDirFlag() atlasargs.Flag {
	flag := atlasargs.String("seed-dir", "", "Default directory for seed steps that omit dir")
	flag.MapValue = atlasTestSeedDirValue
	return flag
}

// atlasTestSeedDirValue maps one seed directory.
//
// This surface spells directories as file:// URLs, so one written that way
// resolves to its local path instead of reaching the native runner as a
// directory name that cannot exist. Any other scheme is refused in the words of
// the flag that has it: a seed directory is not the migration directory whose
// wording the shared local-directory mapper would otherwise borrow.
func atlasTestSeedDirValue(value string) (string, error) {
	if strings.Contains(value, "://") && !strings.HasPrefix(value, "file://") {
		return "", errors.New("a seed directory is a local path or a file:// URL")
	}
	dir, err := atlasargs.ParseLocalDir(value)
	if err != nil {
		return "", err
	}
	if len(dir.Query) > 0 {
		return "", errors.New("seed directory URL query parameters are not supported")
	}
	return dir.Path, nil
}

// atlasMigrationsDirFlag maps the Atlas --dir migration-directory URL (default
// file://migrations) onto the native --migrations-dir local path.
func atlasMigrationsDirFlag() atlasargs.Flag {
	flag := atlasargs.NativeStringDefault(
		"dir",
		"",
		"Migration directory",
		"migrations-dir",
		"file://migrations",
	)
	flag.MapValue = atlasargs.LocalDirValue
	return flag
}

// atlasMigrateEditVerb forwards `atlas migrate edit` to the native
// `ptah migrations edit` command. The {name | version} positional maps to the
// native --version value (a migration file name contributes its leading version
// digits), --dir maps to the native migration directory (Atlas-format by
// default via --dir-format), and the editor resolves from $VISUAL, then
// $EDITOR, like the native command. After the editor exits, the native command
// rewrites the directory checksum so validation keeps passing.
func atlasMigrateEditVerb() atlasVerb {
	return atlasVerb{
		use:        "edit",
		displayUse: "edit [flags] {name | version}",
		short:      "Edit a migration file and update the directory checksum",
		native:     "migrations edit",
		factory:    migrateedit.NewMigrateEditCommand,
		positionals: []atlasPositionalArg{{
			name:       "version",
			nativeName: "version",
			mapValue:   atlasMigrateVersionValue,
		}},
		nativeOnlyFlags: append(atlasMigrateMaintNativeOnlyFlags(), "down-file", "editor", "up-file"),
		flags:           atlasMigrateMaintFlags(),
	}
}

// atlasMigrateRebaseVerb forwards `atlas migrate rebase` to the native
// `ptah migrations rebase` command, which re-timestamps the selected migration
// to the end of history and rewrites the directory checksum. Atlas documents a
// repeatable {name | version} positional; Ptah forwards one migration per run
// and rejects multiple values and version ranges loudly.
func atlasMigrateRebaseVerb() atlasVerb {
	return atlasVerb{
		use:        "rebase",
		displayUse: "rebase [flags] {name | version}...",
		short:      "Move a migration to the end of history and update the directory checksum",
		native:     "migrations rebase",
		factory:    migraterebase.NewMigrateRebaseCommand,
		positionals: []atlasPositionalArg{{
			name:       "version",
			nativeName: "version",
			mapValue:   atlasMigrateVersionValue,
			variadic:   true,
		}},
		nativeOnlyFlags: atlasMigrateMaintNativeOnlyFlags(),
		flags:           atlasMigrateMaintFlags(),
	}
}

// atlasMigrateRmVerb forwards `atlas migrate rm` to the native
// `ptah migrations rm` command, which deletes the selected migration's files
// and rewrites the directory checksum.
func atlasMigrateRmVerb() atlasVerb {
	return atlasVerb{
		use:        "rm",
		displayUse: "rm [flags] {name | version}",
		short:      "Remove a migration file and update the directory checksum",
		native:     "migrations rm",
		factory:    migraterm.NewMigrateRmCommand,
		positionals: []atlasPositionalArg{{
			name:       "version",
			nativeName: "version",
			mapValue:   atlasMigrateVersionValue,
		}},
		nativeOnlyFlags: atlasMigrateMaintNativeOnlyFlags(),
		flags:           atlasMigrateMaintFlags(),
	}
}

// atlasMigrateLsVerb forwards `atlas migrate ls` to the native
// `ptah migrations ls` command, which lists a migration directory without
// contacting a database. --dir maps to the native migration directory, and -l
// and -s map onto the native --latest and --short.
func atlasMigrateLsVerb() atlasVerb {
	return atlasVerb{
		use:              "ls",
		displayUse:       "ls [flags]",
		short:            "List the migration files in the directory",
		native:           "migrations ls",
		factory:          migratels.NewMigrateLsCommand,
		prefixArgs:       atlasMigrateReadPrefixArgs(),
		nativeOnlyFlags:  atlasMigrateReadNativeOnlyFlags(),
		requireDirScheme: true,
		flags: []atlasargs.Flag{
			atlasMigrationsDirFlag(),
			atlasargs.Bool("latest", "l", "Print only the latest migration file"),
			atlasargs.Bool("short", "s", "Print only the migration version, omitting the description and the .sql suffix"),
		},
	}
}

// atlasMigrateShowVerb forwards `atlas migrate show` to the native
// `ptah migrations show` command, which prints a stored migration's SQL. The
// repeatable {name | version} positional maps to the native --version, one flag
// pair per value, so naming several migrations in one run prints all of them
// instead of refusing.
func atlasMigrateShowVerb() atlasVerb {
	return atlasVerb{
		use:        "show",
		displayUse: "show [flags] {name | version}...",
		short:      "Print the contents of one or more migration files",
		native:     "migrations show",
		factory:    migrateshow.NewMigrateShowCommand,
		positionals: []atlasPositionalArg{{
			name:       "version",
			nativeName: "version",
			mapValue:   atlasMigrateVersionValue,
			variadic:   true,
			repeatable: true,
		}},
		prefixArgs:       atlasMigrateReadPrefixArgs(),
		nativeOnlyFlags:  append(atlasMigrateReadNativeOnlyFlags(), "direction", "version"),
		requireDirScheme: true,
		flags:            []atlasargs.Flag{atlasMigrationsDirFlag()},
	}
}

// atlasMigrateReadPrefixArgs are the native decisions the read-only directory
// verbs make on their caller's behalf, because this surface offers no flag for
// either.
//
// --dir-format is pinned to the Atlas layout for the reason every other verb on
// this surface pins it: a directory reached through the Atlas spelling is an
// Atlas directory, and the verbs being mirrored register no --dir-format of
// their own, so accepting one here would be a flag they reject.
//
// --verify-sum is pinned on because the surface being mirrored refuses a
// migration directory whose integrity file is missing or stale before it lists
// or prints anything. The native default is the opposite -- these are read-only
// verbs, outside the always-on integrity class -- so without this the compat
// spelling would accept a directory the mirrored surface refuses, which is the
// one direction the compatibility policy does not allow.
func atlasMigrateReadPrefixArgs() []string {
	return []string{"--dir-format", atlasDirFormatDefault, "--verify-sum"}
}

// atlasMigrateReadNativeOnlyFlags lists the native read flags the Atlas-shaped
// ls and show verbs do not accept; use the native `ptah migrations ls|show`
// commands for them.
func atlasMigrateReadNativeOnlyFlags() []string {
	return []string{
		"dir-format",
		"migrations-dir",
		dbcli.PlainHTTPFlagName,
		"verify-sum",
	}
}

func atlasMigrateMaintFlags() []atlasargs.Flag {
	return []atlasargs.Flag{
		atlasMigrationsDirFlag(),
		atlasMigrateDirFormatFlag("dir-format"),
	}
}

// atlasMigrateMaintNativeOnlyFlags lists the native maintenance flags the
// Atlas-shaped edit/rebase/rm verbs do not accept; use the native
// `ptah migrations edit|rebase|rm` commands for them.
func atlasMigrateMaintNativeOnlyFlags() []string {
	return []string{
		"atlas-env",
		"connect-timeout",
		"db-url",
		"force",
		"migrations-dir",
		"migrations-schema",
		"migrations-table",
		"revision-table-format",
		"version",
	}
}

// atlasMigrateVersionValue maps an Atlas {name | version} positional onto the
// native --version value: a bare version passes through, and a migration file
// name (for example 20060102150405_name.sql) contributes its leading version
// digits.
func atlasMigrateVersionValue(value string) (string, error) {
	version := strings.TrimSpace(value)
	if strings.Contains(version, "...") {
		return "", fmt.Errorf("Atlas accepts version ranges, but Ptah does not implement range selection yet")
	}
	digits := version
	if i := strings.IndexFunc(version, func(r rune) bool { return r < '0' || r > '9' }); i >= 0 {
		if version[i] != '_' {
			return "", fmt.Errorf("cannot determine a migration version from %q", value)
		}
		digits = version[:i]
	}
	if digits == "" {
		return "", fmt.Errorf("cannot determine a migration version from %q", value)
	}
	return digits, nil
}

// atlasSchemaTestVerb forwards `atlas schema test` to the native
// `ptah schema test` runner. The desired schema URL (-u/--url) maps to the
// native desired-schema root, --dev-url selects the throwaway database
// (ephemeral SQLite when omitted), and the optional [paths] positional selects
// the directory of Ptah-native YAML test cases.
//
// -s/--schema restricts the desired schema before the cases run. Atlas registers
// it as a repeatable `strings` flag (atlasgo.io/cli-reference); the arg mapper
// rewrites every occurrence, and the native flag is a string array, so repeated
// values accumulate instead of the last one winning.
// atlasSchemaTestNativeCommand builds the native `schema test` this verb
// forwards to, with --var made explicit-only on the target.
//
// PTAH_VAR belongs to this surface. refreshAtlasProjectFlagEnvironment lifts it
// into the adapter's own --var before the verb runs, and what the native
// command receives afterwards is whatever the adapter decided from it: the
// run's values, the values of the data block that scoped the desired schema, or
// none at all. The forwarded target has the same env binding installed, so
// leaving it in place turns that last answer back into the first -- with no
// explicit --var to mark the flag as set, the target reads PTAH_VAR itself, and
// a scope declaring no vars silently inherits the run-wide value it exists to
// refuse. The test then passes against a schema nobody asked for.
//
// The opt-out loses nothing, because a run the adapter did not scope has
// already had the variable's value forwarded explicitly. It is scoped to this
// verb because this is the only one that decides a native flag's whole value
// and can decide it to be empty; a verb that grows the same shape needs the
// same opt-out, and for the same reason.
func atlasSchemaTestNativeCommand() *cobra.Command {
	cmd := schema.NewSchemaTestCommand()
	if err := cmdflags.DisableEnvBinding(cmd.Flags(), atlasVarFlagName); err != nil {
		panic(err)
	}
	return cmd
}

func atlasSchemaTestVerb() atlasVerb {
	return atlasVerb{
		use:                "test",
		displayUse:         "test [flags] [paths]",
		short:              "Run declarative schema tests against a dev database",
		native:             "schema test",
		factory:            atlasSchemaTestNativeCommand,
		positionals:        []atlasPositionalArg{{name: "paths", nativeName: "dir"}},
		positionalOptional: true,
		projectConfig:      applyAtlasSchemaTestProjectConfig,
		flags: []atlasargs.Flag{
			atlasSchemaTestSourceFlag(),
			atlasTestDevURLFlag(),
			atlasargs.String("run", "", "Run only test cases matching a Go regular expression"),
			atlasargs.String(atlasSchemaFlagName, atlasSchemaFlagShorthand, "Restrict the desired schema to these schema names"),
			atlasTestReportFlag(),
			atlasTestSeedDirFlag(),
		},
	}
}

// atlasSchemaTestSourceFlag registers `schema test`'s -u/--url desired-state
// source.
//
// The flag was typed as a migration directory, so every database URL was
// refused with "only local file:// migration directories are supported" -- a
// message naming a concept the flag does not have, on a flag that has accepted
// .sql and .hcl schema files since they were added. Database URLs now pass
// through to the resolver the sibling desired-state verbs already use, and the
// kinds that genuinely cannot be a desired state keep a loud refusal worded for
// what the flag actually is.
func atlasSchemaTestSourceFlag() atlasargs.Flag {
	flag := atlasargs.NativeString(
		"url",
		"u",
		"Desired schema source URL: a local file:// directory of Go schema annotations,"+
			" a .sql or .hcl schema file, or a database URL",
		"root-dir",
	)
	flag.MapValue = atlasSchemaTestSourceValue
	return flag
}

// atlasSchemaTestSourceValue maps one desired-state source URL.
//
// Plain paths and file:// URLs keep the pre-existing local handling verbatim,
// including its query-parameter and path-decoding errors, so a Go-annotation
// directory reaches the native scan exactly as before.
func atlasSchemaTestSourceValue(value string) (string, error) {
	trimmed := strings.TrimSpace(value)
	if !strings.Contains(trimmed, "://") || strings.HasPrefix(trimmed, "file://") {
		return atlasargs.LocalDirValue(value)
	}
	source, err := atlassource.Classify(trimmed)
	if err != nil {
		return "", err
	}
	if source.Kind == atlassource.KindDatabase {
		return trimmed, nil
	}
	return "", fmt.Errorf(
		"atlas schema test does not support %s desired-state sources;"+
			" pass a directory of Go schema annotations, a .sql or .hcl schema file, or a database URL",
		source.Kind,
	)
}

// atlasCheckpointDirFormatFlag registers checkpoint's --dir-format. Both
// writable conventions pass through: `atlas` writes the single-file
// `-- atlas:checkpoint` convention plus atlas.sum, `ptah` writes the reversible
// two-file pair plus ptah.sum. Every other Atlas directory format keeps the
// shared "not implemented" rejection.
//
// The default is `atlas`, matching the default Atlas registers on
// this verb and every other compat migrate verb — an unflagged Atlas pipeline
// runs against an Atlas-format directory and must get an Atlas checkpoint back.
// The native `ptah migrations checkpoint` default stays `ptah`.
func atlasCheckpointDirFormatFlag() atlasargs.Flag {
	flag := atlasargs.NativeStringDefault(
		"dir-format",
		"",
		"Migration directory format",
		"dir-format",
		atlasDirFormatDefault,
	)
	flag.MapValue = atlasCheckpointDirFormatValue
	return flag
}

func atlasCheckpointDirFormatValue(value string) (string, error) {
	normalized := strings.ToLower(strings.TrimSpace(value))
	switch normalized {
	case "ptah":
		return "ptah", nil
	case "", atlasDirFormatDefault:
		return atlasDirFormatDefault, nil
	default:
		if slices.Contains(unsupportedAtlasDirFormats, normalized) {
			return "", fmt.Errorf("Atlas accepts --dir-format=%s, but Ptah does not implement that directory format yet", normalized)
		}
		return "", fmt.Errorf("unknown Atlas migration directory format %q: expected atlas, golang-migrate, goose, flyway, liquibase, or dbmate", value)
	}
}

func atlasMigrateDirFormatFlag(nativeName string) atlasargs.Flag {
	flag := atlasargs.NativeStringDefault(
		"dir-format",
		"",
		"Migration directory format",
		nativeName,
		atlasDirFormatDefault,
	)
	flag.MapValue = atlasMigrateDirFormatValue
	return flag
}

// atlasMigrateDirFormatValue maps the Atlas --dir-format value onto the native
// directory format.
//
// An EMPTY value selects the native Atlas layout rather than being refused.
// Measured against the pinned community binary v1.3.0, `--dir-format ""` exits 0
// and reads the directory as Atlas on every verb that registers the flag —
// hash, validate, lint, new, status and set were each checked, not just the one
// stokaro/ptah#990 named — and `migrate checkpoint` already agreed with that
// through atlasCheckpointDirFormatValue. Refusing it here was the odd one out.
//
// This is the flag spelling. The query spelling of the same choice, `?format=`
// with an empty value, already resolves to the Atlas layout in
// atlasmigrate.ResolveApplyDirFormat, so the two spellings now agree.
func atlasMigrateDirFormatValue(value string) (string, error) {
	normalized := strings.ToLower(strings.TrimSpace(value))
	if normalized == "" || normalized == atlasDirFormatDefault {
		return atlasDirFormatDefault, nil
	}
	if slices.Contains(unsupportedAtlasDirFormats, normalized) {
		return "", fmt.Errorf("Atlas accepts --dir-format=%s, but Ptah does not implement that directory format yet", normalized)
	}
	return "", fmt.Errorf("unknown Atlas migration directory format %q: expected atlas, golang-migrate, goose, flyway, liquibase, or dbmate", value)
}

// newAtlasVersionCommand exposes `version` as a subcommand and deliberately no
// --version/-v flag. That asymmetry with the native ptah binary -- which
// answers to all three spellings (stokaro/ptah#1064) -- is the point: this
// surface mirrors a command set that carries neither flag, and the mirrored
// command set is what scripts pointed at this binary are written against.
//
// Measured on this tree, same argv, against the command set being mirrored:
//
//	--version -> "Error: unknown flag: --version\n", exit 1
//	-v        -> "Error: unknown shorthand flag: 'v' in -v\n", exit 1
//
// and neither surface's --help lists a --version row; both list only the
// `version` command. Adding the flag here would make this binary exit 0 where
// the surface it mirrors exits 1, which is a compatibility regression, not a
// fix. TestCompatCommand_RootRejectsVersionFlag pins that on the root command,
// which is where the regression would appear: setting cobra's Version field
// anywhere on this tree is all it takes to auto-register --version and -v.
func newAtlasVersionCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "version",
		Short: "Print Ptah version information",
		RunE: func(cmd *cobra.Command, _ []string) error {
			buildinfo.Write(cmd.OutOrStdout(), buildinfo.Resolve())
			return nil
		},
	}
	cmdutil.ConfigureCommand(cmd)
	return cmd
}

func newAtlasLicenseCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "license",
		Short: "Print license information",
		RunE: func(cmd *cobra.Command, _ []string) error {
			licensetext.Write(cmd.OutOrStdout())
			return nil
		},
	}
	cmdutil.ConfigureCommand(cmd)
	return cmd
}

func newAtlasAdapterCommand(group string, verb atlasVerb) *cobra.Command {
	mapper := atlasArgMapper(group, verb)
	if verb.factory != nil {
		cmd := cmdadapter.NewForwardCommandWithArgsMapper(
			atlasAdapterUse(verb),
			verb.short,
			verb.native,
			verb.factory,
			mapper,
			verb.prefixArgs...,
		)
		registerAtlasFlags(cmd, verb.flags)
		return cmd
	}
	cmd := &cobra.Command{
		Use:   atlasAdapterUse(verb),
		Short: verb.short,
		Long:  atlasCommandLong(group, verb),
		RunE: func(_ *cobra.Command, _ []string) error {
			if verb.native == "" {
				return fmt.Errorf("atlas %s %s is not implemented yet", group, verb.use)
			}
			return fmt.Errorf("atlas %s %s execution is not implemented yet; use `ptah %s`", group, verb.use, verb.native)
		},
	}
	registerAtlasFlags(cmd, verb.flags)
	cmdutil.ConfigureCommand(cmd)
	return cmd
}

func atlasAdapterUse(verb atlasVerb) string {
	if verb.displayUse != "" {
		return verb.displayUse
	}
	if len(verb.positionals) == 0 {
		return verb.use
	}
	parts := make([]string, 0, 1+len(verb.positionals))
	parts = append(parts, verb.use)
	for _, positional := range verb.positionals {
		parts = append(parts, "<"+positional.name+">")
	}
	return strings.Join(parts, " ")
}

func atlasCommandLong(group string, verb atlasVerb) string {
	if verb.native == "" {
		return fmt.Sprintf("Atlas OSS `atlas %s %s` command path. Runtime behavior is not implemented yet.", group, verb.use)
	}
	return fmt.Sprintf("Atlas OSS `atlas %s %s` command path. The current native Ptah implementation is `ptah %s`.", group, verb.use, verb.native)
}

func registerAtlasFlags(cmd *cobra.Command, flags []atlasargs.Flag) {
	for _, flag := range flags {
		switch flag.Kind {
		case atlasargs.StringFlag:
			cmd.Flags().StringP(flag.Name, flag.Shorthand, flag.Default, flag.Usage)
		case atlasargs.BoolFlag:
			cmd.Flags().BoolP(flag.Name, flag.Shorthand, false, flag.Usage)
		case atlasargs.UintFlag:
			cmd.Flags().UintP(flag.Name, flag.Shorthand, 0, flag.Usage)
		case atlasargs.StringArrayFlag:
			// StringSlice, not StringArray: Atlas prints these as `strings`
			// and splits a comma-separated value, which is the behavior a
			// pipeline passing `--schema a,b` expects.
			cmd.Flags().StringSliceP(flag.Name, flag.Shorthand, nil, flag.Usage)
		}
		if flag.NativeName != "" {
			if err := cmdflags.AddForwardedEnvBinding(
				cmd.Flags(),
				flag.Name,
				cmdflags.EnvName("PTAH", flag.NativeName),
			); err != nil {
				panic(err)
			}
		}
		if !flag.EnvDisabled {
			continue
		}
		// A flag the arg mapper will not read from the environment must not be
		// advertised with an "[env: PTAH_X]" suffix, or the help promises a
		// variable that does nothing. cmdflags owns that annotation, so the
		// opt-out has to be recorded there too — the same helper schema apply
		// uses for --auto-approve.
		if err := cmdflags.DisableEnvBinding(cmd.Flags(), flag.Name); err != nil {
			panic(err)
		}
	}
}

func atlasArgMapper(group string, verb atlasVerb) cmdadapter.ArgMapper {
	return func(
		cmd *cobra.Command,
		args []string,
		cleanup *cmdadapter.CleanupScope,
	) ([]string, context.Context, error) {
		project, err := resolveAtlasVerbProject(cmd, group, verb, args, cleanup)
		if err != nil {
			return nil, nil, err
		}
		if err := dbcli.ReportIgnoredAtlasConstructs(cmd.ErrOrStderr(), project.project.Config); err != nil {
			return nil, nil, err
		}
		args = project.args
		if err := rejectNativeOnlyAtlasFlags(group, verb, args); err != nil {
			return nil, nil, err
		}
		args, nativeTail, err := mapAtlasPositionalArgs(group, verb, args)
		if err != nil {
			return nil, nil, err
		}
		// After the positional split, because on the mirrored surface the
		// arity check runs before the command body that reads --dir, and
		// before the flag mapper, because a directory URL naming no scheme is
		// refused there before the value is parsed as one.
		if err := requireAtlasVerbDirScheme(cmd, group, verb, project); err != nil {
			return nil, nil, err
		}
		verbFlags, err := atlasEnvAwareSourceFlags(group, verb, project)
		if err != nil {
			return nil, nil, err
		}
		mapped, err := atlasargs.Map(group, verb.use, verbFlags, args)
		if err != nil {
			return nil, nil, err
		}
		mapped = append(quietingLogLevelArgs(verb, args), mapped...)
		mapped = append(mapped, project.nativeArgs...)
		return append(mapped, nativeTail...), project.context, nil
	}
}

// atlasVerbArgs is one adapter invocation's Atlas-form arguments after the
// selected atlas.hcl environment has been merged into them. The project itself
// stays owned by the caller's cleanup scope.
type atlasVerbArgs struct {
	// args are the Atlas-form arguments with the project selection flags
	// removed and the project's values appended: exactly what atlasargs.Map
	// receives.
	args []string
	// nativeArgs bypass the Atlas flag mapper and are appended directly to the
	// forwarded native command. They carry adapter-derived values without
	// registering a duplicate Atlas-facing flag.
	nativeArgs []string
	// context carries the project's rooted migration directory and, for verbs
	// that ask for it, the merged native project config.
	context context.Context
	// project is the loaded atlas.hcl, zero when none was selected. It stays
	// open for the caller's cleanup scope, so a caller that has to read the
	// migration directory itself -- the atlas.sum gate on the writing verbs --
	// can read it through the same rooted boundary the forwarded native command
	// gets, instead of reopening the path unbounded.
	project atlasProject
	// projectFlags are the project selection flags this invocation resolved,
	// kept so a flag mapper can locate the config file the values came from.
	projectFlags atlasProjectFlagValues
	// projectLoaded reports whether a project file was actually selected. The
	// adapter loads one only when -c or --env names it, so a bare run has no
	// environment for an env:// reference to be read out of.
	projectLoaded bool
}

// resolveAtlasVerbProject merges the Atlas project selection flags reachable
// from cmd with the inline ones, loads the selected atlas.hcl, and applies its
// values to the Atlas-form arguments.
//
// It is shared with the integrity verbs' converted-directory path
// (see newAtlasMigrateIntegrityCommand), which resolves the source directory
// format from this same state. Splitting it out is what keeps the format that
// selects the covered file set and the format the forwarded native command
// sees one decision rather than two computations that have to agree by
// inspection.
func resolveAtlasVerbProject(
	cmd *cobra.Command,
	group string,
	verb atlasVerb,
	args []string,
	cleanup *cmdadapter.CleanupScope,
) (atlasVerbArgs, error) {
	resolved := atlasVerbArgs{context: cmd.Context()}
	parentProjectFlags, parentChanged, err := atlasProjectFlagsFromCommand(cmd)
	if err != nil {
		return atlasVerbArgs{}, err
	}
	parentProject := atlasProjectArgValues{
		flags:   parentProjectFlags,
		changed: parentChanged,
	}
	project, remaining, err := extractAtlasProjectArgs(args)
	if err != nil {
		return atlasVerbArgs{}, err
	}
	project = mergeAtlasProjectArgs(parentProject, project)
	resolved.args = remaining
	resolved.projectFlags = project.flags
	// -c and --env SELECT a project file, so naming either one makes it
	// required. --var only supplies values to a file the caller may or may not
	// have, so it makes the file OPTIONAL: present, it is loaded and the
	// variables reach it; absent, the command runs with no project at all,
	// which is what the pinned community binary v1.3.0 does
	// (stokaro/ptah#1241 item 12).
	requirement := requiredAtlasProject
	if !project.changed {
		if len(project.flags.vars) == 0 {
			return resolved, nil
		}
		requirement = optionalAtlasProject
	}

	compatibilityPolicy := atlasCompatibilityPolicy(cmd)
	selected, err := loadAtlasAdapterProjectConfig(
		resolved.context,
		verb,
		project.flags,
		requirement,
		compatibilityPolicy,
	)
	if err != nil {
		return atlasVerbArgs{}, err
	}
	if !selected.loaded {
		if verb.projectConfig == nil {
			return resolved, nil
		}
		resolved.args, resolved.nativeArgs, err = verb.projectConfig(
			verb.flags,
			resolved.args,
			atlasProject{},
			project.flags,
		)
		if err != nil {
			return atlasVerbArgs{}, err
		}
		return resolved, nil
	}
	loadedProject, targetCfg := selected.project, selected.targetConfig
	cleanup.Add(loadedProject.Close)
	if err := compatibilityPolicy.ValidateProjectConfig(loadedProject.Config); err != nil {
		return atlasVerbArgs{}, err
	}
	if err := loadedProject.resolveMigrationDirForArgs(verb.flags, resolved.args); err != nil {
		return atlasVerbArgs{}, err
	}
	resolved.project = loadedProject
	resolved.projectLoaded = true
	applyProjectConfig := verb.projectConfig
	if applyProjectConfig == nil {
		applyProjectConfig = applyAtlasProjectConfigToArgs
	}
	resolved.args, resolved.nativeArgs, err = applyProjectConfig(
		verb.flags,
		resolved.args,
		loadedProject,
		project.flags,
	)
	if err != nil {
		return atlasVerbArgs{}, err
	}
	resolved.context = withAtlasProjectMigrationRoot(resolved.context, group, loadedProject)
	if verb.nativeProjectConfig {
		resolved.context = dbcli.WithProjectConfig(resolved.context, targetCfg)
	}
	return resolved, nil
}

func withAtlasProjectMigrationRoot(
	ctx context.Context,
	group string,
	project atlasProject,
) context.Context {
	if group != "migrate" || !project.migrationDirResolved {
		return ctx
	}
	if project.migrationFS != nil {
		return migrationsource.WithVirtual(
			ctx,
			project.migrationDir.Path,
			project.migrationFS,
			project.migrationDisplay,
		)
	}
	localOptions := project.localOptions(project.migrationDir)
	if localOptions.Root != nil {
		return migrationsource.WithRootedLocal(ctx, project.migrationDir.Path, localOptions.Root)
	}
	return migrationsource.WithLocalRoot(ctx, project.migrationDir.Path, localOptions.AllowedRoot)
}

// selectedAtlasProject is the outcome of resolving one adapter invocation's
// project file. loaded is false when no project was selected at all, which is
// a normal outcome for an optional requirement and never an error.
type selectedAtlasProject struct {
	project      atlasProject
	targetConfig projectconfig.Config
	loaded       bool
}

func loadAtlasAdapterProjectConfig(
	ctx context.Context,
	verb atlasVerb,
	flags atlasProjectFlagValues,
	requirement atlasProjectRequirement,
	policy atlascompatpolicy.Policy,
) (selectedAtlasProject, error) {
	if !verb.nativeProjectConfig {
		project, loaded, err := openAtlasProjectWithPolicy(ctx, flags, requirement, policy, "")
		return selectedAtlasProject{project: project, loaded: loaded}, err
	}
	if requirement == requiredAtlasProject {
		project, targetConfig, err := openRequiredMergedProjectConfigWithPolicy(ctx, flags, policy)
		return selectedAtlasProject{
			project:      project,
			targetConfig: targetConfig,
			loaded:       err == nil,
		}, err
	}
	project, loaded, err := openAtlasProjectWithPolicy(ctx, flags, optionalAtlasProject, policy, "")
	if err != nil || !loaded {
		return selectedAtlasProject{}, err
	}
	ptah, err := projectconfig.LoadPtahFile(projectconfig.PtahFileName, flags.envName)
	if err != nil {
		return selectedAtlasProject{}, errors.Join(err, project.Close())
	}
	return selectedAtlasProject{
		project:      project,
		targetConfig: projectconfig.Merge(ptah, project.Config),
		loaded:       true,
	}, nil
}

func rejectNativeOnlyAtlasFlags(group string, verb atlasVerb, args []string) error {
	for _, arg := range args {
		flagName, found := atlasLongFlagName(arg)
		if found && slices.Contains(verb.nativeOnlyFlags, flagName) {
			return fmt.Errorf("atlas %s %s does not accept native Ptah flag --%s", group, verb.use, flagName)
		}
	}
	return nil
}

// mapAtlasPositionalArgs splits declared positional values out of the
// Atlas-form args and returns them separately as native flag pairs. The native
// pairs are appended after atlasargs.Map has run, so a positional's native
// flag name is never re-mapped when it collides with an Atlas flag name of the
// same verb (for example the native test-case --dir versus the Atlas
// migration-directory --dir).
func mapAtlasPositionalArgs(group string, verb atlasVerb, args []string) (remaining, nativeTail []string, err error) {
	if len(verb.positionals) == 0 {
		return args, nil, nil
	}
	if len(verb.positionals) != 1 {
		return nil, nil, fmt.Errorf("atlas %s %s declares unsupported positional mapping", group, verb.use)
	}
	withoutPositionals, positionals := splitAtlasPositionals(verb.flags, args)
	positional := verb.positionals[0]
	switch len(positionals) {
	case 0:
		if verb.positionalOptional {
			return withoutPositionals, nil, nil
		}
		return nil, nil, fmt.Errorf("atlas %s %s requires %s argument", group, verb.use, positional.name)
	case 1:
		value, err := mapAtlasPositionalValue(group, verb, positional, positionals[0])
		if err != nil {
			return nil, nil, err
		}
		return withoutPositionals, []string{"--" + positional.nativeName, value}, nil
	default:
		if positional.repeatable {
			tail, err := mapRepeatedAtlasPositionals(group, verb, positional, positionals)
			if err != nil {
				return nil, nil, err
			}
			return withoutPositionals, tail, nil
		}
		if positional.variadic {
			return nil, nil, fmt.Errorf(
				"atlas %s %s accepts multiple %s arguments, but Ptah does not implement processing more than one per run yet",
				group, verb.use, positional.name)
		}
		return nil, nil, fmt.Errorf("atlas %s %s accepts one %s argument, got %q", group, verb.use, positional.name, positionals)
	}
}

// mapRepeatedAtlasPositionals turns every value of a repeatable positional into
// its own native flag pair, in the order they were written. Order is preserved
// rather than sorted because it is what the caller asked for and what decides
// the order the values are acted on.
func mapRepeatedAtlasPositionals(
	group string,
	verb atlasVerb,
	positional atlasPositionalArg,
	positionals []string,
) ([]string, error) {
	tail := make([]string, 0, 2*len(positionals))
	for _, raw := range positionals {
		value, err := mapAtlasPositionalValue(group, verb, positional, raw)
		if err != nil {
			return nil, err
		}
		tail = append(tail, "--"+positional.nativeName, value)
	}
	return tail, nil
}

func mapAtlasPositionalValue(group string, verb atlasVerb, positional atlasPositionalArg, value string) (string, error) {
	if positional.mapValue == nil {
		return value, nil
	}
	mapped, err := positional.mapValue(value)
	if err != nil {
		return "", fmt.Errorf("atlas %s %s %s argument: %w", group, verb.use, positional.name, err)
	}
	return mapped, nil
}

func splitAtlasPositionals(flags []atlasargs.Flag, args []string) (
	withoutPositionals,
	positionals []string,
) {
	valueFlags := atlasValueFlagNames(flags)
	withoutPositionals = make([]string, 0, len(args))
	positionals = make([]string, 0, 1)
	for i := 0; i < len(args); i++ {
		arg := args[i]
		if arg == "--" {
			positionals = append(positionals, args[i+1:]...)
			break
		}
		name, inlineValue, ok := atlasFlagName(arg)
		if ok {
			withoutPositionals = append(withoutPositionals, arg)
			if !inlineValue {
				if _, found := valueFlags[name]; found && i+1 < len(args) {
					i++
					withoutPositionals = append(withoutPositionals, args[i])
				}
			}
			continue
		}
		positionals = append(positionals, arg)
	}
	return withoutPositionals, positionals
}

func atlasValueFlagNames(flags []atlasargs.Flag) map[string]struct{} {
	names := make(map[string]struct{})
	for _, flag := range flags {
		if flag.Kind == atlasargs.BoolFlag {
			continue
		}
		names[flag.Name] = struct{}{}
		if flag.Shorthand != "" {
			names[flag.Shorthand] = struct{}{}
		}
	}
	return names
}

func atlasLongFlagName(arg string) (string, bool) {
	if !strings.HasPrefix(arg, "--") || len(arg) <= len("--") {
		return "", false
	}
	body := strings.TrimPrefix(arg, "--")
	before, _, _ := strings.Cut(body, "=")
	return before, true
}

func atlasFlagName(arg string) (name string, inlineValue, ok bool) {
	switch {
	case strings.HasPrefix(arg, "--") && len(arg) > len("--"):
		before, _ := atlasLongFlagName(arg)
		_, _, found := strings.Cut(strings.TrimPrefix(arg, "--"), "=")
		return before, found, true
	case strings.HasPrefix(arg, "-") && len(arg) == 2:
		return strings.TrimPrefix(arg, "-"), false, true
	default:
		return "", false, false
	}
}

// quietingLogLevelArgs returns the native --log-level argument that keeps a
// forwarded verb's own narration off this surface, or nothing when lowering the
// threshold would delete the command's output instead.
//
// The distinction is the log format, and it is not cosmetic. In text mode the
// report is written straight to the command's writer and no threshold reaches
// it, so lowering the level removes exactly what this surface does not want:
// the migrator's lifecycle records and the dialect writers' "[DRY RUN] Would
// ..." narration.
//
// Under a machine-readable format the emitter turns that same report into
// Info-level records, so the threshold and the output become one knob.
// Measured on a one-migration rollback: 2687 bytes across 15 records became 0,
// exit 0, with the rollback still performed. Silence on a destructive command
// is a worse defect than the narration this was fixing.
//
// The format is read from the raw arguments rather than from cobra, because
// this surface registers neither logging flag -- they are forwarded verbatim to
// the native command, so a flag lookup here finds nothing and reports the
// default for every invocation.
//
// The result is PREPENDED, so an explicit `--log-level` from the user still
// wins: pflag keeps the last value.
//
// warn rather than error: it reproduces the #968 threshold exactly, so a
// Warn-level diagnostic that exists on no other channel -- a migration lock
// that would not release, a skipped out-of-order migration -- still arrives.
func quietingLogLevelArgs(verb atlasVerb, args []string) []string {
	if !verb.quietNarration {
		return nil
	}
	if !logFormatWritesReportDirectly(args) {
		return nil
	}
	return []string{"--log-level", "warn"}
}

// logFormatWritesReportDirectly reports whether the selected log format sends a
// command's report to its writer rather than through the logger.
func logFormatWritesReportDirectly(args []string) bool {
	format := strings.TrimSpace(os.Getenv("PTAH_LOG_FORMAT"))
	for index, arg := range args {
		switch {
		case arg == "--log-format" && index+1 < len(args):
			format = args[index+1]
		case strings.HasPrefix(arg, "--log-format="):
			format = strings.TrimPrefix(arg, "--log-format=")
		}
	}
	format = strings.ToLower(strings.TrimSpace(format))
	return format == "" || format == "text"
}

// atlasEnvAwareSourceFlags binds the selected project environment into the
// test verbs' desired-state source flag.
//
// The flag list is built at registration, where no environment exists yet, so
// the mapper it carries could only ever refuse `env://`. Rewriting the entry
// here — the pattern atlasMigrateSourceFlags already uses for --dir — hands the
// mapper the environment the run actually selected, which is what --to and
// --from have had all along (stokaro/ptah#1761).
//
// Verbs other than `schema test` are returned untouched, and so is a run that
// selected no environment: with nothing selected there is no attribute to read,
// and the mapper's own refusal still names the scheme and the attribute.
func atlasEnvAwareSourceFlags(
	group string,
	verb atlasVerb,
	project atlasVerbArgs,
) ([]atlasargs.Flag, error) {
	if group != "schema" || verb.use != "test" || !project.projectLoaded {
		return verb.flags, nil
	}
	baseDir, err := atlasProjectConfigBaseDir(project.projectFlags)
	if err != nil {
		return nil, err
	}
	projectEnv := atlassource.ProjectEnv{
		Loaded:  true,
		Config:  project.project.Config,
		BaseDir: baseDir,
	}
	out := slices.Clone(verb.flags)
	for i := range out {
		if out[i].Name != "url" {
			continue
		}
		out[i].MapValue = atlasSchemaTestSourceValueWithEnv(projectEnv)
	}
	return out, nil
}

// atlasSchemaTestSourceValueWithEnv maps one desired-state source URL for
// `schema test`, expanding an `env://` reference through the selected
// environment first.
//
// An environment naming several sources is refused rather than silently
// reduced to the first. This is where the verb parts company with --to, which
// merges them: -u forwards to a single native --root-dir, so there is nowhere
// for the second source to go, and picking one would test a schema the
// environment does not describe.
func atlasSchemaTestSourceValueWithEnv(
	projectEnv atlassource.ProjectEnv,
) func(string) (string, error) {
	return func(value string) (string, error) {
		trimmed := strings.TrimSpace(value)
		if !strings.HasPrefix(strings.ToLower(trimmed), "env://") {
			return atlasSchemaTestSourceValue(value)
		}
		set, err := atlassource.ClassifySet("-u", []string{trimmed}, projectEnv)
		if err != nil {
			return "", err
		}
		if len(set.Sources) != 1 {
			return "", fmt.Errorf(
				"%q names %d desired-state sources, and this flag forwards one:"+
					" the verb reads a single --root-dir. Name the source itself,"+
					" or point the env at one source",
				value,
				len(set.Sources),
			)
		}
		// Raw, not Path: expandEnv already resolved the value against the
		// project's base directory, and the existing mapper is what decides
		// which kinds this verb reads.
		return atlasSchemaTestSourceValue(set.Sources[0].Raw)
	}
}

func atlasSchemaValidateNativeCommand() *cobra.Command {
	return schema.NewSchemaValidateCommand()
}

// atlasSchemaValidateVerb mirrors the native `schema validate` onto the
// Atlas-compatible surface.
//
// The pinned community binary v1.3.0 has no such verb, so it is registered
// beside the other verbs that binary does not carry and is community-gated in
// strict mode. There is no flag oracle for it on the community surface, so the
// flag set is the native one, named here rather than guessed at
// (stokaro/ptah#1711).
func atlasSchemaValidateVerb() atlasVerb {
	return atlasVerb{
		use:        "validate",
		displayUse: "validate [flags]",
		short:      "Report structural problems in a desired schema without a database",
		native:     "schema validate",
		factory:    atlasSchemaValidateNativeCommand,
		flags: []atlasargs.Flag{
			atlasargs.NativeStringArray("dialect", "", "Target dialect to validate against (repeatable)", "dialect"),
			atlasargs.NativeStringArray("root-dir", "", "Root directory to scan for Go entities (repeatable)", "root-dir"),
			atlasargs.NativeStringArray("schema-file", "", "YAML, HCL, or SQL desired-schema file (repeatable)", "schema-file"),
			atlasargs.NativeString("server-version", "", "Server version whose capability preset one target is validated against", "server-version"),
		},
	}
}

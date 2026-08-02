// Package atlas exposes Atlas-compatible command paths.
package atlas

import (
	"context"
	"fmt"
	"slices"
	"strings"

	"github.com/spf13/cobra"

	"go.5x5.cz/ptah/cmd/internal/buildinfo"
	"go.5x5.cz/ptah/cmd/internal/cmdadapter"
	"go.5x5.cz/ptah/cmd/internal/cmdflags"
	"go.5x5.cz/ptah/cmd/internal/cmdutil"
	"go.5x5.cz/ptah/cmd/internal/dbcli"
	"go.5x5.cz/ptah/cmd/internal/exitcode"
	"go.5x5.cz/ptah/cmd/internal/licensetext"
	"go.5x5.cz/ptah/cmd/internal/migrationsource"
	"go.5x5.cz/ptah/cmd/migrate"
	"go.5x5.cz/ptah/cmd/migratecheckpoint"
	"go.5x5.cz/ptah/cmd/migratedown"
	"go.5x5.cz/ptah/cmd/migrateedit"
	"go.5x5.cz/ptah/cmd/migraterebase"
	"go.5x5.cz/ptah/cmd/migraterm"
	"go.5x5.cz/ptah/cmd/migrationstest"
	"go.5x5.cz/ptah/cmd/schema"
	"go.5x5.cz/ptah/config/projectconfig"
	"go.5x5.cz/ptah/internal/atlasargs"
)

type atlasVerb struct {
	use                 string
	displayUse          string
	short               string
	native              string
	factory             func() *cobra.Command
	prefixArgs          []string
	positionals         []atlasPositionalArg
	positionalOptional  bool
	nativeOnlyFlags     []string
	flags               []atlasargs.Flag
	nativeProjectConfig bool
	// projectConfig overrides how loaded atlas.hcl values map onto the verb's
	// Atlas flags. When nil, the generic applyAtlasProjectConfigToArgs is used.
	projectConfig atlasProjectArgsApplier
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
}

const (
	atlasDirFormatDefault = "atlas"
	atlasErrorExitCode    = 1
)

func failAtlasCommand(cmd *cobra.Command, err error) error {
	if _, writeErr := fmt.Fprintf(cmd.ErrOrStderr(), "Error: %s\n", err); writeErr != nil {
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
	use = strings.TrimSpace(use)
	if use == "" {
		use = "ptah-compat"
	}
	cmd := newAtlasCommand(use+" [command]", "Atlas-compatible Ptah command tree", `Atlas-compatible Ptah command tree.

This executable exposes Atlas-style commands at process root for scripts that
expect commands such as migrate apply or schema inspect. Commands that have an
existing Ptah equivalent forward to that native command.`)
	cmdflags.InstallEnvBinding("PTAH", cmd)
	cmdutil.SetErrorCodePolicy(cmd, atlasErrorExitCode)
	return cmd
}

func newAtlasCommand(use, short, long string) *cobra.Command {
	cmd := &cobra.Command{
		Use:   use,
		Short: short,
		Long:  long,
		RunE:  runAtlasGroupHelp,
	}
	cmdutil.ConfigureCommandArgs(cmd, atlasRootArgs)
	schemaCommand := newAtlasSchemaCommand()
	migrateCommand := newAtlasMigrateCommand()
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

func newAtlasSchemaCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "schema [command]",
		Short: "Atlas schema commands",
		RunE:  runAtlasGroupHelp,
	}
	// Atlas CE treats an extra token on a command group as a request for that
	// group's help, so the group intentionally accepts positional arguments.
	cmdutil.ConfigureCommandArgs(cmd, nil)
	registerAtlasProjectFlags(cmd.PersistentFlags(), &atlasProjectFlagValues{})
	cmd.AddCommand(newAtlasSchemaCleanCommand())
	cmd.AddCommand(newAtlasSchemaInspectCommand())
	cmd.AddCommand(newAtlasSchemaApplyCommand())
	cmd.AddCommand(newAtlasSchemaDiffCommand())
	cmd.AddCommand(newAtlasSchemaFmtCommand())
	cmd.AddCommand(newAtlasSchemaPlanCommand())
	cmd.AddCommand(newAtlasAdapterCommand("schema", atlasSchemaTestVerb()))
	addAtlasUnsupportedCommands(cmd, []atlasUnsupportedVerb{
		{use: "push", short: "Push schema state to a remote registry"},
	})
	return cmd
}

func newAtlasMigrateCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "migrate [command]",
		Short: "Atlas migrate commands",
		RunE:  runAtlasGroupHelp,
	}
	// Atlas CE treats an extra token on a command group as a request for that
	// group's help, so the group intentionally accepts positional arguments.
	cmdutil.ConfigureCommandArgs(cmd, nil)
	registerAtlasProjectFlags(cmd.PersistentFlags(), &atlasProjectFlagValues{})
	cmd.AddCommand(newAtlasMigrateApplyCommand())
	cmd.AddCommand(newAtlasMigrateLintCommand())
	cmd.AddCommand(newAtlasMigrateStatusCommand())
	cmd.AddCommand(newAtlasMigrateDownCommand())
	cmd.AddCommand(newAtlasMigrateSetCommand())
	for _, verb := range []atlasVerb{
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
			},
		},
		atlasMigrateEditVerb(),
		{
			use:        "new",
			displayUse: "new [flags] [name]",
			short:      "Create a new migration file",
			native:     "migrations create",
			factory:    migrate.NewMigrateCreateCommand,
			flags: []atlasargs.Flag{
				atlasargs.NativeLocalDir("dir", "", "Migration directory", "migrations-dir"),
				atlasMigrateDirFormatFlag("dir-format"),
				atlasargs.NativeBool("edit", "", "Edit the created migration files", "edit"),
			},
		},
		atlasMigrateRebaseVerb(),
		atlasMigrateRmVerb(),
		atlasMigrateTestVerb(),
	} {
		cmd.AddCommand(newAtlasAdapterCommand("migrate", verb))
	}
	cmd.AddCommand(newAtlasMigrateHashCommand())
	cmd.AddCommand(newAtlasMigrateValidateCommand())
	cmd.AddCommand(newAtlasMigrateDiffCommand())
	cmd.AddCommand(newAtlasMigrateImportCommand())
	addAtlasUnsupportedCommands(cmd, []atlasUnsupportedVerb{
		{use: "push", short: "Push migration directory to a remote registry"},
	})
	return cmd
}

func atlasRootArgs(cmd *cobra.Command, args []string) error {
	if len(args) == 0 {
		return nil
	}

	unknownErr := fmt.Errorf("unknown command %q for %q", args[0], "atlas")
	var diagnostic strings.Builder
	fmt.Fprintf(&diagnostic, "Error: %s\n", unknownErr)

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
	if _, err := fmt.Fprintf(cmd.ErrOrStderr(), "Error: %s\n", unknownErr); err != nil {
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

func newAtlasUnsupportedCommand(verb atlasUnsupportedVerb) *cobra.Command {
	cmd := &cobra.Command{
		Use:   verb.use,
		Short: verb.short,
		Long:  "This compatibility command is not implemented by Ptah.",
		RunE: func(cmd *cobra.Command, _ []string) error {
			err := fmt.Errorf("%s is not implemented by Ptah", cmd.CommandPath())
			if _, writeErr := fmt.Fprintf(cmd.ErrOrStderr(), "Error: %s\n", err); writeErr != nil {
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
		// An Atlas-surface verb defaults to Atlas revision bookkeeping, like
		// `migrate set` above: without this prefix the native --revision-format
		// default of "ptah" silently no-ops against the atlas_schema_revisions
		// rows `atlas migrate apply` writes. User args are appended after the
		// prefix, so an explicit native `--revision-format ptah` pass-through
		// still overrides it (pflag keeps the last value).
		prefixArgs:          []string{"--revision-format", "atlas"},
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
			atlasargs.NativeString("dev-url", "", "Dev database URL the test cases run against", "db-url"),
			atlasargs.String("run", "", "Run only test cases matching a Go regular expression"),
		},
	}
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
// native Go-annotation root directory, --dev-url selects the throwaway
// database (ephemeral SQLite when omitted), and the optional [paths]
// positional selects the directory of Ptah-native YAML test cases.
func atlasSchemaTestVerb() atlasVerb {
	return atlasVerb{
		use:                "test",
		displayUse:         "test [flags] [paths]",
		short:              "Run declarative schema tests against a dev database",
		native:             "schema test",
		factory:            schema.NewSchemaTestCommand,
		positionals:        []atlasPositionalArg{{name: "paths", nativeName: "dir"}},
		positionalOptional: true,
		projectConfig:      applyAtlasSchemaTestProjectConfig,
		flags: []atlasargs.Flag{
			atlasargs.NativeLocalDir("url", "u", "Desired schema URL: local file:// directory with Go schema annotations", "root-dir"),
			atlasargs.NativeString("dev-url", "", "Dev database URL the test cases run against", "db-url"),
			atlasargs.String("run", "", "Run only test cases matching a Go regular expression"),
		},
	}
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

func atlasMigrateDirFormatValue(value string) (string, error) {
	normalized := strings.ToLower(strings.TrimSpace(value))
	if normalized == "" {
		return "", fmt.Errorf("migration directory format must not be empty")
	}
	if normalized == atlasDirFormatDefault {
		return atlasDirFormatDefault, nil
	}
	if slices.Contains(unsupportedAtlasDirFormats, normalized) {
		return "", fmt.Errorf("Atlas accepts --dir-format=%s, but Ptah does not implement that directory format yet", normalized)
	}
	return "", fmt.Errorf("unknown Atlas migration directory format %q: expected atlas, golang-migrate, goose, flyway, liquibase, or dbmate", value)
}

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
		args = project.args
		if err := rejectNativeOnlyAtlasFlags(group, verb, args); err != nil {
			return nil, nil, err
		}
		args, nativeTail, err := mapAtlasPositionalArgs(group, verb, args)
		if err != nil {
			return nil, nil, err
		}
		mapped, err := atlasargs.Map(group, verb.use, verb.flags, args)
		if err != nil {
			return nil, nil, err
		}
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
	// context carries the project's rooted migration directory and, for verbs
	// that ask for it, the merged native project config.
	context context.Context
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
	if !project.changed {
		return resolved, nil
	}

	loadedProject, targetCfg, err := loadAtlasAdapterProjectConfig(verb, project.flags)
	if err != nil {
		return atlasVerbArgs{}, err
	}
	cleanup.Add(loadedProject.Close)
	if err := loadedProject.resolveMigrationDirForArgs(verb.flags, resolved.args); err != nil {
		return atlasVerbArgs{}, err
	}
	applyProjectConfig := verb.projectConfig
	if applyProjectConfig == nil {
		applyProjectConfig = applyAtlasProjectConfigToArgs
	}
	resolved.args, err = applyProjectConfig(verb.flags, resolved.args, loadedProject, project.flags)
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
	localOptions := project.localOptions(project.migrationDir)
	if localOptions.Root != nil {
		return migrationsource.WithRootedLocal(ctx, project.migrationDir.Path, localOptions.Root)
	}
	return migrationsource.WithLocalRoot(ctx, project.migrationDir.Path, localOptions.AllowedRoot)
}

func loadAtlasAdapterProjectConfig(
	verb atlasVerb,
	flags atlasProjectFlagValues,
) (project atlasProject, targetConfig projectconfig.Config, err error) {
	if !verb.nativeProjectConfig {
		project, _, err := openAtlasProject(flags, requiredAtlasProject)
		return project, projectconfig.Config{}, err
	}
	return openRequiredMergedProjectConfig(flags)
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
		if positional.variadic {
			return nil, nil, fmt.Errorf(
				"atlas %s %s accepts multiple %s arguments, but Ptah does not implement processing more than one per run yet",
				group, verb.use, positional.name)
		}
		return nil, nil, fmt.Errorf("atlas %s %s accepts one %s argument, got %q", group, verb.use, positional.name, positionals)
	}
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
	withoutPositionals []string,
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

func atlasFlagName(arg string) (name string, inlineValue bool, ok bool) {
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

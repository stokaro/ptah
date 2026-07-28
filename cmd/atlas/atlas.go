// Package atlas exposes Atlas-compatible command paths.
package atlas

import (
	"errors"
	"fmt"
	"io"
	"slices"
	"strings"

	"github.com/spf13/cobra"

	"github.com/stokaro/ptah/cmd/internal/buildinfo"
	"github.com/stokaro/ptah/cmd/internal/cmdadapter"
	"github.com/stokaro/ptah/cmd/internal/cmdflags"
	"github.com/stokaro/ptah/cmd/internal/cmdutil"
	"github.com/stokaro/ptah/cmd/internal/exitcode"
	"github.com/stokaro/ptah/cmd/migrate"
	"github.com/stokaro/ptah/cmd/migratecheckpoint"
	"github.com/stokaro/ptah/cmd/migratedown"
	"github.com/stokaro/ptah/cmd/migratehash"
	"github.com/stokaro/ptah/cmd/migraterepair"
	"github.com/stokaro/ptah/cmd/migratevalidate"
	"github.com/stokaro/ptah/cmd/migrationstest"
	"github.com/stokaro/ptah/cmd/schema"
	"github.com/stokaro/ptah/internal/atlasargs"
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
}

const (
	atlasDirFormatDefault = "atlas"
	atlasErrorExitCode    = 1
)

var unsupportedAtlasDirFormats = []string{
	"dbmate",
	"flyway",
	"golang-migrate",
	"goose",
	"liquibase",
}

// NewAtlasCommand returns the Atlas command namespace.
func NewAtlasCommand() *cobra.Command {
	cmd := newAtlasCommand("atlas [command]", "Atlas OSS command namespace", `Atlas OSS command namespace.

These commands reserve the Atlas OSS CLI surface under Ptah. Commands that have
an existing Ptah equivalent forward to that native command while keeping the
native Ptah command tree separate for future redesign.`)
	cmdutil.SetErrorCodePolicy(cmd, atlasErrorExitCode)
	return cmd
}

// NewCompatCommand returns an Atlas-compatible root command.
func NewCompatCommand(use string) *cobra.Command {
	use = strings.TrimSpace(use)
	if use == "" {
		use = "ptah-compat"
	}
	cmd := newAtlasCommand(use+" [command]", "Atlas-compatible Ptah command tree", `Atlas-compatible Ptah command tree.

This executable exposes Atlas-style commands at process root for scripts that
expect commands such as migrate apply or schema inspect. Runtime behavior is the
same compatibility layer used by ptah atlas <command> ...`)
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
	cmd.AddCommand(newAtlasVersionCommand())
	cmd.AddCommand(newAtlasLicenseCommand())
	cmd.AddCommand(newAtlasSchemaCommand())
	cmd.AddCommand(newAtlasMigrateCommand())
	installAtlasCompletionCommand(cmd)
	installAtlasUsageTree(cmd)
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
	cmd.AddCommand(newAtlasAdapterCommand("schema", atlasSchemaTestVerb()))
	addAtlasUnsupportedCommunityCommands(cmd, "schema", []atlasUnsupportedCommunityVerb{
		{use: "plan", short: "Plan schema changes through Atlas Cloud"},
		{use: "push", short: "Push schema state to Atlas Cloud"},
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
	for _, verb := range []atlasVerb{
		atlasMigrateDownVerb(),
		{
			use:                "checkpoint",
			displayUse:         "checkpoint [flags] [name]",
			short:              "Squash migration history into a cumulative-schema checkpoint",
			native:             "migrations checkpoint",
			factory:            migratecheckpoint.NewMigrateCheckpointCommand,
			positionals:        []atlasPositionalArg{{name: "name", nativeName: "description"}},
			positionalOptional: true,
			// No --dir-format flag: checkpoint output is ptah-format only (see the
			// native command's format guard), so the directory is read and written
			// with the native ptah default rather than the atlas default the other
			// migrate verbs use.
			flags: []atlasargs.Flag{
				atlasargs.NativeLocalDir("dir", "", "Migration directory", "migrations-dir"),
				atlasargs.NativeString("dev-url", "", "URL of the dev database the directory is replayed into", "shadow-db"),
			},
		},
		{
			use:     "hash",
			short:   "Write or update the migration directory checksum",
			native:  "migrations hash",
			factory: migratehash.NewMigrateHashCommand,
			flags: []atlasargs.Flag{
				atlasargs.NativeLocalDir("dir", "", "Migration directory", "dir"),
				atlasMigrateDirFormatFlag("dir-format"),
			},
		},
		{
			use:        "new",
			displayUse: "new [flags] [name]",
			short:      "Create a new migration file",
			native:     "migrations create",
			factory:    migrate.NewMigrateCreateCommand,
			flags: []atlasargs.Flag{
				atlasargs.NativeLocalDir("dir", "", "Migration directory", "migrations-dir"),
				atlasMigrateDirFormatFlag("dir-format"),
				atlasargs.UnsupportedBool("edit", "", "Edit the created migration files"),
			},
		},
		{
			use:         "set",
			displayUse:  "set [flags] [version]",
			short:       "Set migration revision state",
			native:      "migrations repair",
			factory:     migraterepair.NewMigrateRepairCommand,
			prefixArgs:  []string{"--revision-format", "atlas", "--force"},
			positionals: []atlasPositionalArg{{name: "version", nativeName: "version"}},
			nativeOnlyFlags: []string{
				"atlas-env",
				"connect-timeout",
				"db-url",
				"force",
				"migrations-dir",
				"migrations-schema",
				"migrations-table",
				"resume-from",
				"revision-format",
				"version",
			},
			flags: []atlasargs.Flag{
				atlasargs.NativeString("url", "u", "Database URL", "db-url"),
				atlasargs.NativeLocalDir("dir", "", "Migration directory", "migrations-dir"),
				atlasMigrateDirFormatFlag("dir-format"),
				atlasargs.NativeString("revisions-schema", "", "Schema for the revision table", "migrations-schema"),
			},
		},
		atlasMigrateTestVerb(),
		{
			use:     "validate",
			short:   "Validate migration directory integrity",
			native:  "migrations validate",
			factory: migratevalidate.NewAtlasMigrateValidateCommand,
			flags: []atlasargs.Flag{
				atlasargs.NativeString("dev-url", "", "Dev database URL", "dev-url"),
				atlasargs.NativeLocalDir("dir", "", "Migration directory", "dir"),
				atlasMigrateDirFormatFlag("dir-format"),
			},
		},
	} {
		cmd.AddCommand(newAtlasAdapterCommand("migrate", verb))
	}
	cmd.AddCommand(newAtlasMigrateDiffCommand())
	cmd.AddCommand(newAtlasMigrateImportCommand())
	addAtlasUnsupportedCommunityCommands(cmd, "migrate", []atlasUnsupportedCommunityVerb{
		{use: "edit", short: "Edit migration files"},
		{use: "push", short: "Push migration directory to Atlas Cloud"},
		{use: "rebase", short: "Rebase migration files"},
		{use: "rm", short: "Remove migration files"},
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

type atlasUnsupportedCommunityVerb struct {
	use   string
	short string
}

func addAtlasUnsupportedCommunityCommands(parent *cobra.Command, group string, verbs []atlasUnsupportedCommunityVerb) {
	for _, verb := range verbs {
		parent.AddCommand(newAtlasUnsupportedCommunityCommand(group, verb))
	}
}

func newAtlasUnsupportedCommunityCommand(group string, verb atlasUnsupportedCommunityVerb) *cobra.Command {
	cmd := &cobra.Command{
		Use:   verb.use,
		Short: verb.short,
		Long:  fmt.Sprintf("Atlas CE `%s` command boundary.", atlasUnsupportedCommunityCommand(group, verb.use)),
		RunE: func(cmd *cobra.Command, _ []string) error {
			writeAtlasUnsupportedCommunityCommandAbort(cmd, group, verb.use)
			return exitcode.New(1, errors.New("atlas community-version unsupported command"))
		},
	}
	cmd.SetHelpFunc(func(cmd *cobra.Command, _ []string) {
		writeAtlasUnsupportedCommunityCommandHelp(cmd, group, verb.use)
	})
	cmd.Annotations = map[string]string{atlasPreserveHelpAnnotation: "true"}
	cmdutil.ConfigureCommandArgs(cmd, cmdutil.NoPositionalArgs)
	return cmd
}

func writeAtlasUnsupportedCommunityCommandHelp(cmd *cobra.Command, group, use string) {
	out := cmd.OutOrStdout()
	writeAtlasUnsupportedCommunityNotice(out, atlasUnsupportedCommunityCommand(group, use), "")
}

func writeAtlasUnsupportedCommunityCommandAbort(cmd *cobra.Command, group, use string) {
	out := cmd.ErrOrStderr()
	writeAtlasUnsupportedCommunityNotice(out, atlasUnsupportedCommunityCommand(group, use), "Abort: ")
	fmt.Fprintln(out)
	fmt.Fprintln(out, "You're running the community build of Atlas, which differs from the official version.")
	fmt.Fprintln(out, "If this error persists, try installing the official version as a troubleshooting step:")
	fmt.Fprintln(out)
	fmt.Fprintln(out, "  curl -sSf https://atlasgo.sh | sh")
	fmt.Fprintln(out)
	fmt.Fprintln(out, "More installation options: https://atlasgo.io/docs#installation")
}

func writeAtlasUnsupportedCommunityNotice(out io.Writer, command, prefix string) {
	fmt.Fprintf(out, "%s'%s' is not supported by the community version.\n\n", prefix, command)
	fmt.Fprintln(out, "To install the non-community version of Atlas, use the following command:")
	fmt.Fprintln(out)
	fmt.Fprintln(out, "\tcurl -sSf https://atlasgo.sh | sh")
	fmt.Fprintln(out)
	fmt.Fprintln(out, "Or, visit the website to see all installation options:")
	fmt.Fprintln(out)
	fmt.Fprintln(out, "\thttps://atlasgo.io/docs#installation")
}

func atlasUnsupportedCommunityCommand(group, use string) string {
	return "atlas " + group + " " + use
}

func atlasMigrateDownVerb() atlasVerb {
	return atlasVerb{
		use:                 "down",
		short:               "Roll back migrations",
		native:              "migrations down",
		factory:             migratedown.NewMigrateDownCommand,
		nativeProjectConfig: true,
		flags: []atlasargs.Flag{
			atlasargs.NativeString("url", "u", "Database URL", "db-url"),
			atlasargs.NativeLocalDir("dir", "", "Migration directory", "migrations-dir"),
			atlasargs.UnsupportedString("dev-url", "", "Dev database URL used by Atlas for dynamic down planning"),
			atlasargs.NativeString("to-version", "", "Target version to roll back to", "target"),
			atlasargs.UnsupportedString("to-tag", "", "Target migration tag to roll back to"),
			atlasargs.NativeBool("dry-run", "", "Show rollback plan without applying it", "dry-run"),
			atlasargs.UnsupportedString("format", "", "Atlas Go template output format"),
			atlasargs.NativeString("revisions-schema", "", "Schema for the revision table", "migrations-schema"),
			atlasargs.NativeString("lock-timeout", "", "Timeout for acquiring migration locks", "migration-lock-timeout"),
			atlasargs.UnsupportedBool("skip-checks", "", "Skip Atlas down migration safety checks"),
			atlasargs.UnsupportedBool("plan", "", "Force Atlas dynamic down planning"),
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
			atlasMigrateTestDirFlag(),
			atlasMigrateDirFormatFlag("dir-format"),
			atlasargs.NativeString("dev-url", "", "Dev database URL the test cases run against", "db-url"),
			atlasargs.String("run", "", "Run only test cases matching a Go regular expression"),
		},
	}
}

func atlasMigrateTestDirFlag() atlasargs.Flag {
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
	if normalized == "" || normalized == atlasDirFormatDefault {
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
			out := cmd.OutOrStdout()
			fmt.Fprintln(out, "Ptah")
			fmt.Fprintln(out, "License: MIT")
			fmt.Fprintln(out, "Copyright (c) 2025, 2026 Denis Voytyuk")
			fmt.Fprintln(out, "Source: https://github.com/stokaro/ptah")
			fmt.Fprintln(out, "Atlas compatibility: independent implementation; Ptah does not use Atlas source code.")
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
	}
}

func atlasArgMapper(group string, verb atlasVerb) cmdadapter.ArgMapper {
	return func(cmd *cobra.Command, args []string) ([]string, error) {
		parentProjectFlags, parentChanged, err := atlasProjectFlagsFromCommand(cmd)
		if err != nil {
			return nil, err
		}
		parentProject := atlasProjectArgValues{
			flags:   parentProjectFlags,
			changed: parentChanged,
		}
		project, remaining, err := extractAtlasProjectArgs(args)
		if err != nil {
			return nil, err
		}
		project = mergeAtlasProjectArgs(parentProject, project)
		args = remaining
		if project.changed {
			cfg, err := loadRequiredAtlasProjectConfig(project.flags)
			if err != nil {
				return nil, err
			}
			applyProjectConfig := verb.projectConfig
			if applyProjectConfig == nil {
				applyProjectConfig = applyAtlasProjectConfigToArgs
			}
			args, err = applyProjectConfig(verb.flags, args, cfg, project.flags)
			if err != nil {
				return nil, err
			}
			if verb.nativeProjectConfig {
				args, err = applyAtlasProjectConfigToNativeArgs(args, project.flags)
				if err != nil {
					return nil, err
				}
			}
		}
		if err := rejectNativeOnlyAtlasFlags(group, verb, args); err != nil {
			return nil, err
		}
		args, nativeTail, err := mapAtlasPositionalArgs(group, verb, args)
		if err != nil {
			return nil, err
		}
		mapped, err := atlasargs.Map(group, verb.use, verb.flags, args)
		if err != nil {
			return nil, err
		}
		return append(mapped, nativeTail...), nil
	}
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
		return withoutPositionals, []string{"--" + positional.nativeName, positionals[0]}, nil
	default:
		return nil, nil, fmt.Errorf("atlas %s %s accepts one %s argument, got %q", group, verb.use, positional.name, positionals)
	}
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

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
	"github.com/stokaro/ptah/cmd/migratedown"
	"github.com/stokaro/ptah/cmd/migratehash"
	"github.com/stokaro/ptah/cmd/migraterepair"
	"github.com/stokaro/ptah/cmd/migratevalidate"
	"github.com/stokaro/ptah/internal/atlasargs"
)

type atlasVerb struct {
	use                 string
	short               string
	native              string
	factory             func() *cobra.Command
	prefixArgs          []string
	positionals         []atlasPositionalArg
	nativeOnlyFlags     []string
	flags               []atlasargs.Flag
	nativeProjectConfig bool
}

type atlasPositionalArg struct {
	name       string
	nativeName string
}

const atlasDirFormatDefault = "atlas"

var unsupportedAtlasDirFormats = []string{
	"dbmate",
	"flyway",
	"golang-migrate",
	"goose",
	"liquibase",
}

// NewAtlasCommand returns the Atlas command namespace.
func NewAtlasCommand() *cobra.Command {
	return newAtlasCommand("atlas", "Atlas OSS command namespace", `Atlas OSS command namespace.

These commands reserve the Atlas OSS CLI surface under Ptah. Commands that have
an existing Ptah equivalent forward to that native command while keeping the
native Ptah command tree separate for future redesign.`)
}

// NewCompatCommand returns an Atlas-compatible root command.
func NewCompatCommand(use string) *cobra.Command {
	use = strings.TrimSpace(use)
	if use == "" {
		use = "ptah-compat"
	}
	cmd := newAtlasCommand(use, "Atlas-compatible Ptah command tree", `Atlas-compatible Ptah command tree.

This executable exposes Atlas-style commands at process root for scripts that
expect commands such as migrate apply or schema inspect. Runtime behavior is the
same compatibility layer used by ptah atlas <command> ...`)
	cmdflags.InstallEnvBinding("PTAH", cmd)
	return cmd
}

func newAtlasCommand(use, short, long string) *cobra.Command {
	cmd := &cobra.Command{
		Use:   use,
		Short: short,
		Long:  long,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return cmd.Help()
		},
	}
	cmdutil.ConfigureCommandArgs(cmd, nil)
	cmd.AddCommand(newAtlasVersionCommand())
	cmd.AddCommand(newAtlasLicenseCommand())
	cmd.AddCommand(newAtlasSchemaCommand())
	cmd.AddCommand(newAtlasMigrateCommand())
	return cmd
}

func newAtlasSchemaCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "schema",
		Short: "Atlas schema commands",
		RunE: func(cmd *cobra.Command, _ []string) error {
			return cmd.Help()
		},
	}
	cmdutil.ConfigureCommandArgs(cmd, cmdutil.NoPositionalArgs)
	registerAtlasProjectFlags(cmd.PersistentFlags(), &atlasProjectFlagValues{})
	cmd.AddCommand(newAtlasSchemaCleanCommand())
	cmd.AddCommand(newAtlasSchemaInspectCommand())
	cmd.AddCommand(newAtlasSchemaApplyCommand())
	cmd.AddCommand(newAtlasSchemaDiffCommand())
	cmd.AddCommand(newAtlasSchemaFmtCommand())
	addAtlasUnsupportedCommunityCommands(cmd, "schema", []atlasUnsupportedCommunityVerb{
		{use: "plan", short: "Plan schema changes through Atlas Cloud"},
		{use: "push", short: "Push schema state to Atlas Cloud"},
		{use: "test", short: "Test schemas through Atlas Cloud"},
	})
	return cmd
}

func newAtlasMigrateCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "migrate",
		Short: "Atlas migrate commands",
		RunE: func(cmd *cobra.Command, _ []string) error {
			return cmd.Help()
		},
	}
	cmdutil.ConfigureCommandArgs(cmd, cmdutil.NoPositionalArgs)
	registerAtlasProjectFlags(cmd.PersistentFlags(), &atlasProjectFlagValues{})
	cmd.AddCommand(newAtlasMigrateApplyCommand())
	cmd.AddCommand(newAtlasMigrateLintCommand())
	cmd.AddCommand(newAtlasMigrateStatusCommand())
	for _, verb := range []atlasVerb{
		atlasMigrateDownVerb(),
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
			use:     "new",
			short:   "Create a new migration file",
			native:  "migrations create",
			factory: migrate.NewMigrateCreateCommand,
			flags: []atlasargs.Flag{
				atlasargs.NativeLocalDir("dir", "", "Migration directory", "migrations-dir"),
				atlasMigrateDirFormatFlag("dir-format"),
			},
		},
		{
			use:         "set",
			short:       "Set migration revision state",
			native:      "migrations repair",
			factory:     migraterepair.NewMigrateRepairCommand,
			prefixArgs:  []string{"--revision-format", "atlas", "--force"},
			positionals: []atlasPositionalArg{{name: "revision", nativeName: "version"}},
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
		{
			use:     "validate",
			short:   "Validate migration directory integrity",
			native:  "migrations validate",
			factory: migratevalidate.NewMigrateValidateCommand,
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
		{use: "checkpoint", short: "Create migration checkpoint files"},
		{use: "edit", short: "Edit migration files"},
		{use: "push", short: "Push migration directory to Atlas Cloud"},
		{use: "rebase", short: "Rebase migration files"},
		{use: "rm", short: "Remove migration files"},
		{use: "test", short: "Test migration files through Atlas Cloud"},
	})
	return cmd
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
			args, err = applyAtlasProjectConfigToArgs(verb.flags, args, cfg, project.flags)
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
		args, err = mapAtlasPositionalArgs(group, verb, args)
		if err != nil {
			return nil, err
		}
		return atlasargs.Map(group, verb.use, verb.flags, args)
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

func mapAtlasPositionalArgs(group string, verb atlasVerb, args []string) ([]string, error) {
	if len(verb.positionals) == 0 {
		return args, nil
	}
	if len(verb.positionals) != 1 {
		return nil, fmt.Errorf("atlas %s %s declares unsupported positional mapping", group, verb.use)
	}
	withoutPositionals, positionals := splitAtlasPositionals(verb.flags, args)
	positional := verb.positionals[0]
	switch len(positionals) {
	case 0:
		return nil, fmt.Errorf("atlas %s %s requires %s argument", group, verb.use, positional.name)
	case 1:
		return append(withoutPositionals, "--"+positional.nativeName, positionals[0]), nil
	default:
		return nil, fmt.Errorf("atlas %s %s accepts one %s argument, got %q", group, verb.use, positional.name, positionals)
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

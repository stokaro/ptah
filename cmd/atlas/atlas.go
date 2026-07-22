// Package atlas exposes Atlas-compatible command paths.
package atlas

import (
	"fmt"
	"os"
	"slices"
	"strconv"
	"strings"

	"github.com/spf13/cobra"

	"github.com/stokaro/ptah/cmd/compare"
	"github.com/stokaro/ptah/cmd/dropall"
	"github.com/stokaro/ptah/cmd/internal/buildinfo"
	"github.com/stokaro/ptah/cmd/internal/cmdadapter"
	"github.com/stokaro/ptah/cmd/internal/cmdflags"
	"github.com/stokaro/ptah/cmd/internal/cmdutil"
	"github.com/stokaro/ptah/cmd/lint"
	"github.com/stokaro/ptah/cmd/migrate"
	"github.com/stokaro/ptah/cmd/migratedown"
	"github.com/stokaro/ptah/cmd/migratehash"
	"github.com/stokaro/ptah/cmd/migraterepair"
	"github.com/stokaro/ptah/cmd/migratestatus"
	"github.com/stokaro/ptah/cmd/migrateup"
	"github.com/stokaro/ptah/cmd/migratevalidate"
	"github.com/stokaro/ptah/cmd/readdb"
)

type atlasVerb struct {
	use        string
	short      string
	native     string
	factory    func() *cobra.Command
	prefixArgs []string
	flags      []atlasFlag
}

type atlasFlagKind int

const (
	atlasStringFlag atlasFlagKind = iota
	atlasStringArrayFlag
	atlasBoolFlag
	atlasUintFlag
)

type atlasFlag struct {
	name        string
	shorthand   string
	usage       string
	kind        atlasFlagKind
	nativeName  string
	unsupported bool
	mapValue    func(string) (string, error)
	envDisabled bool
}

type parsedAtlasFlag struct {
	name     string
	value    string
	hasValue bool
	ok       bool
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
	for _, verb := range []atlasVerb{
		{
			use:     "inspect",
			short:   "Inspect a database schema",
			native:  "db read",
			factory: readdb.NewReadDBCommand,
			flags: []atlasFlag{
				atlasNativeString("url", "u", "Database URL to inspect", "db-url"),
				atlasUnsupportedString("dev-url", "", "Dev database URL used by Atlas for file-backed inspection"),
				atlasNativeString("schema", "", "Schema to inspect; repeat by comma in native Ptah", "schemas"),
				atlasUnsupportedStringArray("exclude", "", "Schema objects to exclude from inspection"),
				atlasUnsupportedString("format", "", "Atlas Go template output format"),
			},
		},
		{
			use:    "apply",
			short:  "Apply a desired schema to a database",
			native: "",
			flags: []atlasFlag{
				atlasString("url", "u", "Database URL to apply to"),
				atlasStringArray("to", "", "Desired schema target"),
				atlasString("dev-url", "", "Dev database URL"),
				atlasBool("dry-run", "", "Show planned changes without applying them"),
				atlasBool("auto-approve", "", "Skip interactive approval"),
			},
		},
		{
			use:     "diff",
			short:   "Diff desired schema against a database",
			native:  "schema compare",
			factory: compare.NewCompareCommand,
			flags: []atlasFlag{
				atlasUnsupportedStringArray("from", "", "Source schema target"),
				atlasUnsupportedStringArray("to", "", "Desired schema target"),
				atlasUnsupportedString("dev-url", "", "Dev database URL"),
				atlasUnsupportedString("format", "", "Atlas Go template output format"),
			},
		},
		atlasSchemaCleanVerb(),
	} {
		cmd.AddCommand(newAtlasAdapterCommand("schema", verb))
	}
	cmd.AddCommand(newAtlasSchemaFmtCommand())
	return cmd
}

func atlasSchemaCleanVerb() atlasVerb {
	return atlasVerb{
		use:     "clean",
		short:   "Clean database schema objects",
		native:  "db drop-all",
		factory: dropall.NewDropAllCommand,
		flags: []atlasFlag{
			atlasNativeString("url", "u", "Database URL to clean", "db-url"),
			atlasNativeBool("dry-run", "", "Show planned cleanup without applying it", "dry-run"),
			atlasExplicitNativeBool("auto-approve", "", "Skip interactive approval", "auto-approve"),
		},
	}
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
	for _, verb := range []atlasVerb{
		atlasMigrateApplyVerb(),
		{
			use:    "diff",
			short:  "Generate migration SQL from differences",
			native: "",
			flags: []atlasFlag{
				atlasStringArray("to", "", "Desired schema target"),
				atlasString("dev-url", "", "Dev database URL"),
				atlasString("dir", "", "Migration directory"),
				atlasString("format", "", "Atlas Go template output format"),
			},
		},
		atlasMigrateDownVerb(),
		{
			use:     "hash",
			short:   "Write or update the migration directory checksum",
			native:  "migrations hash",
			factory: migratehash.NewMigrateHashCommand,
			flags:   []atlasFlag{atlasNativeLocalDir("dir", "", "Migration directory", "dir")},
		},
		atlasMigrateLintVerb(),
		{
			use:     "new",
			short:   "Create a new migration file",
			native:  "migrations create",
			factory: migrate.NewMigrateCreateCommand,
			flags:   []atlasFlag{atlasNativeLocalDir("dir", "", "Migration directory", "migrations-dir")},
		},
		{
			use:     "set",
			short:   "Set migration revision state",
			native:  "migrations repair",
			factory: migraterepair.NewMigrateRepairCommand,
			flags: []atlasFlag{
				atlasNativeString("url", "u", "Database URL", "db-url"),
				atlasNativeLocalDir("dir", "", "Migration directory", "migrations-dir"),
			},
		},
		{
			use:     "status",
			short:   "Show migration status",
			native:  "migrations status",
			factory: migratestatus.NewMigrateStatusCommand,
			flags: []atlasFlag{
				atlasNativeString("url", "u", "Database URL", "db-url"),
				atlasNativeLocalDir("dir", "", "Migration directory", "migrations-dir"),
			},
		},
		{
			use:     "validate",
			short:   "Validate migration directory integrity",
			native:  "migrations validate",
			factory: migratevalidate.NewMigrateValidateCommand,
			flags: []atlasFlag{
				atlasUnsupportedString("dev-url", "", "Dev database URL"),
				atlasNativeLocalDir("dir", "", "Migration directory", "dir"),
			},
		},
	} {
		cmd.AddCommand(newAtlasAdapterCommand("migrate", verb))
	}
	cmd.AddCommand(newAtlasMigrateImportCommand())
	return cmd
}

func atlasMigrateApplyVerb() atlasVerb {
	return atlasVerb{
		use:     "apply",
		short:   "Apply pending migrations",
		native:  "migrations up",
		factory: migrateup.NewMigrateUpCommand,
		flags: []atlasFlag{
			atlasNativeString("url", "u", "Database URL to apply migrations to", "db-url"),
			atlasNativeLocalDir("dir", "", "Migration directory", "migrations-dir"),
			atlasNativeBool("dry-run", "", "Show migrations without applying them", "dry-run"),
			atlasNativeString("tx-mode", "", "Transaction mode: file, all, or none", "tx-mode"),
		},
	}
}

func atlasMigrateDownVerb() atlasVerb {
	return atlasVerb{
		use:     "down",
		short:   "Roll back migrations",
		native:  "migrations down",
		factory: migratedown.NewMigrateDownCommand,
		flags: []atlasFlag{
			atlasNativeString("url", "u", "Database URL", "db-url"),
			atlasNativeLocalDir("dir", "", "Migration directory", "migrations-dir"),
			atlasUnsupportedString("dev-url", "", "Dev database URL used by Atlas for dynamic down planning"),
			atlasNativeString("to-version", "", "Target version to roll back to", "target"),
			atlasUnsupportedString("to-tag", "", "Target migration tag to roll back to"),
			atlasNativeBool("dry-run", "", "Show rollback plan without applying it", "dry-run"),
			atlasUnsupportedString("format", "", "Atlas Go template output format"),
			atlasNativeString("revisions-schema", "", "Schema for the revision table", "migrations-schema"),
			atlasNativeString("lock-timeout", "", "Timeout for acquiring migration locks", "migration-lock-timeout"),
			atlasUnsupportedBool("skip-checks", "", "Skip Atlas down migration safety checks"),
			atlasUnsupportedBool("plan", "", "Force Atlas dynamic down planning"),
		},
	}
}

func atlasMigrateLintVerb() atlasVerb {
	return atlasVerb{
		use:     "lint",
		short:   "Lint migration files",
		native:  "migrations lint",
		factory: lint.NewLintCommand,
		flags: []atlasFlag{
			atlasUnsupportedString("dev-url", "", "Dev database URL"),
			atlasNativeLocalDir("dir", "", "Migration directory", "dir"),
			atlasNativeUint("latest", "", "Number of latest migrations to lint", "latest"),
		},
	}
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
			verb.use,
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
		Use:   verb.use,
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

func atlasCommandLong(group string, verb atlasVerb) string {
	if verb.native == "" {
		return fmt.Sprintf("Atlas OSS `atlas %s %s` command path. Runtime behavior is not implemented yet.", group, verb.use)
	}
	return fmt.Sprintf("Atlas OSS `atlas %s %s` command path. The current native Ptah implementation is `ptah %s`.", group, verb.use, verb.native)
}

func atlasString(name, shorthand, usage string) atlasFlag {
	return atlasFlag{name: name, shorthand: shorthand, usage: usage, kind: atlasStringFlag}
}

func atlasStringArray(name, shorthand, usage string) atlasFlag {
	return atlasFlag{name: name, shorthand: shorthand, usage: usage, kind: atlasStringArrayFlag}
}

func atlasBool(name, shorthand, usage string) atlasFlag {
	return atlasFlag{name: name, shorthand: shorthand, usage: usage, kind: atlasBoolFlag}
}

func atlasUint(name, shorthand, usage string) atlasFlag {
	return atlasFlag{name: name, shorthand: shorthand, usage: usage, kind: atlasUintFlag}
}

func atlasNativeString(name, shorthand, usage, nativeName string) atlasFlag {
	f := atlasString(name, shorthand, usage)
	f.nativeName = nativeName
	return f
}

func atlasNativeUint(name, shorthand, usage, nativeName string) atlasFlag {
	f := atlasUint(name, shorthand, usage)
	f.nativeName = nativeName
	return f
}

func atlasNativeLocalDir(name, shorthand, usage, nativeName string) atlasFlag {
	f := atlasNativeString(name, shorthand, usage, nativeName)
	f.mapValue = atlasLocalDirValue
	return f
}

func atlasNativeBool(name, shorthand, usage, nativeName string) atlasFlag {
	f := atlasBool(name, shorthand, usage)
	f.nativeName = nativeName
	return f
}

func atlasExplicitNativeBool(name, shorthand, usage, nativeName string) atlasFlag {
	f := atlasNativeBool(name, shorthand, usage, nativeName)
	f.envDisabled = true
	return f
}

func atlasUnsupportedString(name, shorthand, usage string) atlasFlag {
	f := atlasString(name, shorthand, usage)
	f.unsupported = true
	return f
}

func atlasUnsupportedStringArray(name, shorthand, usage string) atlasFlag {
	f := atlasStringArray(name, shorthand, usage)
	f.unsupported = true
	return f
}

func atlasUnsupportedBool(name, shorthand, usage string) atlasFlag {
	f := atlasBool(name, shorthand, usage)
	f.unsupported = true
	return f
}

func atlasLocalDirValue(value string) (string, error) {
	if after, found := strings.CutPrefix(value, "file://"); found {
		return after, nil
	}
	if strings.Contains(value, "://") {
		return "", fmt.Errorf("only local file:// migration directories are supported")
	}
	return value, nil
}

func registerAtlasFlags(cmd *cobra.Command, flags []atlasFlag) {
	for _, flag := range flags {
		switch flag.kind {
		case atlasStringFlag:
			cmd.Flags().StringP(flag.name, flag.shorthand, "", flag.usage)
		case atlasStringArrayFlag:
			cmd.Flags().StringArrayP(flag.name, flag.shorthand, nil, flag.usage)
		case atlasBoolFlag:
			cmd.Flags().BoolP(flag.name, flag.shorthand, false, flag.usage)
		case atlasUintFlag:
			cmd.Flags().UintP(flag.name, flag.shorthand, 0, flag.usage)
		}
	}
}

func atlasArgMapper(group string, verb atlasVerb) cmdadapter.ArgMapper {
	return func(args []string) ([]string, error) {
		return mapAtlasArgs(group, verb, args)
	}
}

func mapAtlasArgs(group string, verb atlasVerb, args []string) ([]string, error) {
	args = appendAtlasEnvArgs(verb.flags, args)
	out := make([]string, 0, len(args))
	for i := 0; i < len(args); i++ {
		arg := args[i]
		if arg == "--" {
			out = append(out, args[i:]...)
			break
		}
		parsed := splitAtlasFlag(arg)
		if !parsed.ok {
			out = append(out, arg)
			continue
		}
		flag, found := findAtlasFlag(verb.flags, parsed.name)
		if !found {
			out = append(out, arg)
			continue
		}
		displayName := "--" + flag.name
		if len(parsed.name) == 1 {
			displayName = "-" + parsed.name
		}
		if flag.unsupported {
			return nil, fmt.Errorf("atlas %s %s accepts %s, but Ptah does not implement its behavior yet",
				group, verb.use, displayName)
		}
		nativeName := flag.name
		if flag.nativeName != "" {
			nativeName = flag.nativeName
		}
		nativeFlag := "--" + nativeName
		if flag.kind == atlasBoolFlag {
			if parsed.hasValue {
				out = append(out, nativeFlag+"="+parsed.value)
			} else {
				out = append(out, nativeFlag)
			}
			continue
		}
		if parsed.hasValue {
			value, err := mapAtlasFlagValue(flag, parsed.value)
			if err != nil {
				return nil, fmt.Errorf("atlas %s %s %s: %w", group, verb.use, displayName, err)
			}
			out = append(out, nativeFlag+"="+value)
			continue
		}
		out = append(out, nativeFlag)
		if i+1 < len(args) {
			i++
			value, err := mapAtlasFlagValue(flag, args[i])
			if err != nil {
				return nil, fmt.Errorf("atlas %s %s %s: %w", group, verb.use, displayName, err)
			}
			out = append(out, value)
		}
	}
	return out, nil
}

func appendAtlasEnvArgs(flags []atlasFlag, args []string) []string {
	out := args
	cloned := false
	for _, flag := range flags {
		if flag.envDisabled {
			continue
		}
		if atlasFlagPresent(args, flag) {
			continue
		}
		value, ok := os.LookupEnv(cmdflags.EnvName("PTAH", flag.name))
		if !ok || value == "" {
			continue
		}
		if flag.kind == atlasBoolFlag && atlasBoolEnvFalse(value) {
			continue
		}
		if !cloned {
			out = slices.Clone(args)
			cloned = true
		}
		out = append(out, "--"+flag.name+"="+value)
	}
	return out
}

func atlasBoolEnvFalse(value string) bool {
	parsed, err := strconv.ParseBool(value)
	return err == nil && !parsed
}

func atlasFlagPresent(args []string, flag atlasFlag) bool {
	long := "--" + flag.name
	short := ""
	if flag.shorthand != "" {
		short = "-" + flag.shorthand
	}
	for _, arg := range args {
		if arg == long || strings.HasPrefix(arg, long+"=") {
			return true
		}
		if short != "" && (arg == short || strings.HasPrefix(arg, short+"=")) {
			return true
		}
	}
	return false
}

func mapAtlasFlagValue(flag atlasFlag, value string) (string, error) {
	if flag.mapValue == nil {
		return value, nil
	}
	return flag.mapValue(value)
}

func splitAtlasFlag(arg string) parsedAtlasFlag {
	switch {
	case strings.HasPrefix(arg, "--") && len(arg) > len("--"):
		body := strings.TrimPrefix(arg, "--")
		if before, after, found := strings.Cut(body, "="); found {
			return parsedAtlasFlag{name: before, value: after, hasValue: true, ok: true}
		}
		return parsedAtlasFlag{name: body, ok: true}
	case strings.HasPrefix(arg, "-") && len(arg) == 2:
		return parsedAtlasFlag{name: strings.TrimPrefix(arg, "-"), ok: true}
	default:
		return parsedAtlasFlag{}
	}
}

func findAtlasFlag(flags []atlasFlag, name string) (atlasFlag, bool) {
	for _, flag := range flags {
		if flag.name == name || flag.shorthand == name {
			return flag, true
		}
	}
	return atlasFlag{}, false
}

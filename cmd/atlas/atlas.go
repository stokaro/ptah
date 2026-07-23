// Package atlas exposes Atlas-compatible command paths.
package atlas

import (
	"fmt"
	"strings"

	"github.com/spf13/cobra"

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
	"github.com/stokaro/ptah/cmd/migratevalidate"
	"github.com/stokaro/ptah/internal/atlasargs"
)

type atlasVerb struct {
	use        string
	short      string
	native     string
	factory    func() *cobra.Command
	prefixArgs []string
	flags      []atlasargs.Flag
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
	for _, verb := range []atlasVerb{atlasSchemaCleanVerb()} {
		cmd.AddCommand(newAtlasAdapterCommand("schema", verb))
	}
	cmd.AddCommand(newAtlasSchemaInspectCommand())
	cmd.AddCommand(newAtlasSchemaApplyCommand())
	cmd.AddCommand(newAtlasSchemaDiffCommand())
	cmd.AddCommand(newAtlasSchemaFmtCommand())
	return cmd
}

func atlasSchemaCleanVerb() atlasVerb {
	return atlasVerb{
		use:     "clean",
		short:   "Clean database schema objects",
		native:  "db drop-all",
		factory: dropall.NewDropAllCommand,
		flags: []atlasargs.Flag{
			atlasargs.NativeString("url", "u", "Database URL to clean", "db-url"),
			atlasargs.NativeBool("dry-run", "", "Show planned cleanup without applying it", "dry-run"),
			atlasargs.ExplicitNativeBool("auto-approve", "", "Skip interactive approval", "auto-approve"),
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
	cmd.AddCommand(newAtlasMigrateApplyCommand())
	for _, verb := range []atlasVerb{
		atlasMigrateDownVerb(),
		{
			use:     "hash",
			short:   "Write or update the migration directory checksum",
			native:  "migrations hash",
			factory: migratehash.NewMigrateHashCommand,
			flags:   []atlasargs.Flag{atlasargs.NativeLocalDir("dir", "", "Migration directory", "dir")},
		},
		atlasMigrateLintVerb(),
		{
			use:     "new",
			short:   "Create a new migration file",
			native:  "migrations create",
			factory: migrate.NewMigrateCreateCommand,
			flags:   []atlasargs.Flag{atlasargs.NativeLocalDir("dir", "", "Migration directory", "migrations-dir")},
		},
		{
			use:     "set",
			short:   "Set migration revision state",
			native:  "migrations repair",
			factory: migraterepair.NewMigrateRepairCommand,
			flags: []atlasargs.Flag{
				atlasargs.NativeString("url", "u", "Database URL", "db-url"),
				atlasargs.NativeLocalDir("dir", "", "Migration directory", "migrations-dir"),
			},
		},
		{
			use:     "status",
			short:   "Show migration status",
			native:  "migrations status",
			factory: migratestatus.NewMigrateStatusCommand,
			flags: []atlasargs.Flag{
				atlasargs.NativeString("url", "u", "Database URL", "db-url"),
				atlasargs.NativeLocalDir("dir", "", "Migration directory", "migrations-dir"),
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
				atlasargs.NativeString("dir-format", "", "Migration directory format", "dir-format"),
			},
		},
	} {
		cmd.AddCommand(newAtlasAdapterCommand("migrate", verb))
	}
	cmd.AddCommand(newAtlasMigrateDiffCommand())
	cmd.AddCommand(newAtlasMigrateImportCommand())
	return cmd
}

func atlasMigrateDownVerb() atlasVerb {
	return atlasVerb{
		use:     "down",
		short:   "Roll back migrations",
		native:  "migrations down",
		factory: migratedown.NewMigrateDownCommand,
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

func atlasMigrateLintVerb() atlasVerb {
	return atlasVerb{
		use:     "lint",
		short:   "Lint migration files",
		native:  "migrations lint",
		factory: lint.NewLintCommand,
		flags: []atlasargs.Flag{
			atlasargs.NativeString("dev-url", "", "Dev database URL", "dev-url"),
			atlasargs.NativeLocalDir("dir", "", "Migration directory", "dir"),
			atlasargs.NativeString("env", "", "Project env name to read from atlas.hcl", "env"),
			atlasargs.NativeUint("latest", "", "Number of latest migrations to lint", "latest"),
			atlasargs.NativeString("git-base", "", "Base Git branch for changeset linting", "git-base"),
			atlasargs.NativeString("git-dir", "", "Repository working directory for --git-base", "git-dir"),
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

func registerAtlasFlags(cmd *cobra.Command, flags []atlasargs.Flag) {
	for _, flag := range flags {
		switch flag.Kind {
		case atlasargs.StringFlag:
			cmd.Flags().StringP(flag.Name, flag.Shorthand, "", flag.Usage)
		case atlasargs.BoolFlag:
			cmd.Flags().BoolP(flag.Name, flag.Shorthand, false, flag.Usage)
		case atlasargs.UintFlag:
			cmd.Flags().UintP(flag.Name, flag.Shorthand, 0, flag.Usage)
		}
	}
}

func atlasArgMapper(group string, verb atlasVerb) cmdadapter.ArgMapper {
	return func(args []string) ([]string, error) {
		return atlasargs.Map(group, verb.use, verb.flags, args)
	}
}

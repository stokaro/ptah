package schema

import (
	"errors"
	"fmt"
	"strings"

	"github.com/spf13/cobra"

	"go.5x5.cz/ptah/cmd/internal/cmdutil"
	"go.5x5.cz/ptah/cmd/internal/dbcli"
	"go.5x5.cz/ptah/cmd/internal/exitcode"
	"go.5x5.cz/ptah/cmd/internal/serverversion"
	"go.5x5.cz/ptah/core/platform/capability"
	"go.5x5.cz/ptah/internal/schemaload"
	"go.5x5.cz/ptah/internal/schemavalidate"
	"go.5x5.cz/ptah/internal/servertarget"
)

const (
	validateRootDirFlag    = "root-dir"
	validateSchemaFileFlag = "schema-file"
	validateDialectFlag    = "dialect"
)

type schemaValidateOptions struct {
	rootDirs      []string
	schemaFiles   []string
	dialects      []string
	serverVersion string
	plainHTTP     bool
	configPath    string
	envName       string
}

// NewSchemaValidateCommand returns the native `schema validate` command, so
// the Atlas-compatible surface can forward its own verb to the same body.
func NewSchemaValidateCommand() *cobra.Command {
	return newSchemaValidateCommand()
}

func newSchemaValidateCommand() *cobra.Command {
	opts := schemaValidateOptions{}
	cmd := &cobra.Command{
		Use:   "validate",
		Short: "Report structural problems in a desired schema without a database",
		Long: `Load the desired state and report every structural problem found in it, without
connecting to any database.

The desired state comes from Go annotations (--root-dir) or schema files
(--schema-file, repeatable; sources merge into one composite schema), the same
way every other declarative verb takes it.

Validation is per target, because a declaration valid for one dialect can be
invalid for another. Pass --dialect once per target to check; repeated values
are each reported under their own name.

Exits 0 when nothing is wrong and prints nothing. Exits 1 and prints one line
per problem otherwise, so a pre-commit hook can use the status alone.`,
		SilenceErrors: true,
		// Wrapped so the expected-negative status survives: WrapRunE maps an
		// ordinary failure to exit 2 and leaves an explicit code alone.
		RunE: cmdutil.WrapRunE(func(cmd *cobra.Command, _ []string) error {
			return runSchemaValidate(cmd, opts)
		}),
	}
	flags := cmd.Flags()
	flags.StringArrayVar(&opts.rootDirs, validateRootDirFlag, nil, "Root directory to scan for Go entities (repeatable)")
	flags.StringArrayVar(&opts.schemaFiles, validateSchemaFileFlag, nil, "YAML, HCL, or SQL schema file describing the desired state (repeatable)")
	flags.StringArrayVar(&opts.dialects, validateDialectFlag, nil, "Target dialect to validate against (repeatable; required)")
	serverversion.Register(flags, &opts.serverVersion)
	dbcli.RegisterPlainHTTPFlag(flags, &opts.plainHTTP)
	dbcli.RegisterConfigFlag(flags, &opts.configPath)
	dbcli.RegisterEnvFlag(flags, &opts.envName)
	cmdutil.ConfigureCommandArgs(cmd, cmdutil.NoPositionalArgs)
	return cmd
}

func runSchemaValidate(cmd *cobra.Command, opts schemaValidateOptions) error {
	dialects := schemavalidate.Dialects(opts.dialects)
	if len(dialects) == 0 {
		return cmdutil.Fail(cmd, fmt.Errorf(
			"--%s is required: validation is per target, and a declaration valid for one dialect can be invalid for another",
			validateDialectFlag,
		))
	}
	// A version pinned for several targets cannot describe all of them, which
	// is the same refusal --server-version carries on every other offline verb.
	if opts.serverVersion != "" && len(dialects) > 1 {
		return cmdutil.Fail(cmd, fmt.Errorf(
			"--%s applies to one target: %d dialects were named, and one server version cannot describe all of them",
			serverversion.FlagName, len(dialects),
		))
	}
	if len(opts.rootDirs) == 0 && len(opts.schemaFiles) == 0 {
		return cmdutil.Fail(cmd, fmt.Errorf(
			"pass --%s or --%s: there is nothing to validate",
			validateRootDirFlag,
			validateSchemaFileFlag,
		))
	}
	projectCfg, err := dbcli.LoadProjectConfig(cmd, opts.configPath)
	if err != nil {
		return cmdutil.Fail(cmd, err)
	}
	schemaSourceEnv, err := dbcli.SchemaSourceProjectEnv(cmd, projectCfg)
	if err != nil {
		return cmdutil.Fail(cmd, err)
	}

	var problems []schemavalidate.Problem
	for _, dialect := range dialects {
		// Loaded once per dialect on purpose: loading is dialect-aware, so a
		// source that parses for one target can fail for another, and that
		// failure is itself a problem this verb owes an answer about.
		database, err := schemaload.LoadContext(cmd.Context(), schemaload.Options{
			RootDirs:        opts.rootDirs,
			SchemaFiles:     opts.schemaFiles,
			ProjectEnv:      schemaSourceEnv,
			EnvSelectorFlag: dbcli.SchemaSourceEnvSelectorFlag(cmd),
			Dialect:         dialect,
			PlainHTTP:       opts.plainHTTP,
		})
		if err != nil {
			problems = append(problems, schemavalidate.Problem{
				Dialect: dialect,
				Kind:    "source",
				Message: err.Error(),
			})
			continue
		}
		caps, err := validateCapabilities(dialect, opts.serverVersion)
		if err != nil {
			return cmdutil.Fail(cmd, err)
		}
		problems = append(problems, schemavalidate.CollectWithCapabilities(database, dialect, caps)...)
	}
	if len(problems) == 0 {
		return nil
	}
	lines := make([]string, 0, len(problems))
	for _, problem := range problems {
		lines = append(lines, problem.String())
	}
	fmt.Fprintln(cmd.OutOrStdout(), strings.Join(lines, "\n"))
	summary := problemCount(len(problems))
	fmt.Fprintln(cmd.ErrOrStderr(), summary)
	// Exit 1, not 2: finding problems is this verb's expected negative result,
	// the status the reference table gives a drift check or a lint finding. A
	// caller that cannot tell "the schema is wrong" from "you passed a bad
	// flag" cannot use the status alone, which is the point of the verb.
	return exitcode.New(1, errors.New(summary))
}

// problemCount names the count in the summary line, so a single problem does
// not read as "1 structural problems".
func problemCount(count int) string {
	if count == 1 {
		return "1 structural problem"
	}
	return fmt.Sprintf("%d structural problems", count)
}

// validateCapabilities picks the capability set one dialect is validated
// against.
//
// Unpinned, it is the dialect default, which is what the renderer uses on its
// own, so a run with no --server-version answers exactly what it answered
// before the flag existed. A pinned value goes through servertarget rather
// than capability.ForServerVersion so a string that names no server is
// refused: answering an unreadable version with the dialect default would
// report a clean schema against a server nobody asked about.
func validateCapabilities(dialect, serverVersion string) (capability.Capabilities, error) {
	if serverVersion == "" {
		return capability.ForDialect(dialect), nil
	}
	target, err := servertarget.Resolve(dialect, serverVersion)
	if err != nil {
		return nil, fmt.Errorf("invalid --%s: %w", serverversion.FlagName, err)
	}
	if target.Capabilities == nil {
		return capability.ForDialect(dialect), nil
	}
	return target.Capabilities, nil
}

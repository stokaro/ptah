package dbcli

import (
	"context"
	"fmt"
	"io"
	"path/filepath"
	"slices"
	"strings"

	"github.com/spf13/cobra"
	"github.com/spf13/pflag"

	"go.5x5.cz/ptah/cmd/internal/migrationsource"
	"go.5x5.cz/ptah/config/projectconfig"
	"go.5x5.cz/ptah/core/schemasource"
	"go.5x5.cz/ptah/internal/atlassource"
	"go.5x5.cz/ptah/internal/pathguard"
)

const (
	// EnvFlagName selects an env block from project config.
	EnvFlagName = "env"
	// ProjectVarFlagName supplies a value for an atlas.hcl variable block that
	// declares no default. It is the flag the evaluator's diagnostic names, so
	// it is registered wherever EnvFlagName is: an atlas.hcl reached through
	// --env can require a variable on any of those commands, and a command
	// that can print the advice but cannot honor it is the defect this flag
	// exists to close.
	ProjectVarFlagName = "var"
	// AllowExternalSchemaFlagName explicitly permits executing the
	// external_schema program loaded from ptah.yaml or from an atlas.hcl
	// data.external_schema source.
	AllowExternalSchemaFlagName = "allow-external-schema"
	// AtlasProjectConfigFlagName passes an Atlas project config path through
	// internal command adapters without exposing Atlas flags on native commands.
	AtlasProjectConfigFlagName = "atlas-project-config"
	// AtlasProjectVarFlagName passes Atlas project variable overrides through
	// internal command adapters without exposing Atlas flags on native commands.
	AtlasProjectVarFlagName = "atlas-project-var"
	schemaCommandFlagName   = "schema-cmd"
)

// ProjectConfigEnvAnnotation marks the --env flag that selects a ptah.yaml or
// atlas.hcl env block. The flag name alone does not identify it: `ptah seed`
// registers an unrelated --env naming the seed environment to apply, reads no
// project config, and must not grow a --var it would never honor. The
// annotation is what lets a test tell the two apart from the assembled command
// tree instead of from a hand-kept list of packages.
const ProjectConfigEnvAnnotation = "ptah_project_config_env"

const projectEnvFlagUsage = "Project env name to read from ptah.yaml or atlas.hcl"

type projectConfigContextKey struct{}

type projectConfigContextValue struct {
	config projectconfig.Config
}

// WithProjectConfig returns a context carrying an immutable project-config
// snapshot for a forwarded native command.
func WithProjectConfig(ctx context.Context, config projectconfig.Config) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	return context.WithValue(ctx, projectConfigContextKey{}, projectConfigContextValue{
		config: cloneProjectConfig(config),
	})
}

// RegisterEnvFlag registers the shared project env selection flag, bound to
// target, together with the variable-override flag its evaluator advises. The
// two are registered by one call because every command that can select an env
// can also hit an atlas.hcl variable with no default, and registering them
// separately is how eleven of the fourteen commands came to advise a flag they
// rejected.
func RegisterEnvFlag(flags *pflag.FlagSet, target *string) {
	flags.StringVar(target, EnvFlagName, "", projectEnvFlagUsage)
	markProjectEnvFlag(flags)
}

// RegisterProjectEnvFlag registers the same pair as [RegisterEnvFlag] without
// binding --env to a variable, for commands that read the value back off the
// flag set. It exists so no command has to spell EnvFlagName inline: five did,
// which is why a search for the helper found nine of the fourteen commands
// that can reach the variable diagnostic.
func RegisterProjectEnvFlag(flags *pflag.FlagSet) {
	flags.String(EnvFlagName, "", projectEnvFlagUsage)
	markProjectEnvFlag(flags)
}

func markProjectEnvFlag(flags *pflag.FlagSet) {
	RegisterProjectVarFlag(flags)
	if err := flags.SetAnnotation(EnvFlagName, ProjectConfigEnvAnnotation, []string{"true"}); err != nil {
		panic(err)
	}
}

// RegisterProjectVarFlag registers the atlas.hcl variable override flag. It is
// idempotent so a command that already carries the flag — from
// [RegisterEnvFlag] or from an Atlas-compatible surface that spells it the
// same way — keeps the binding it already has.
func RegisterProjectVarFlag(flags *pflag.FlagSet) {
	if flags.Lookup(ProjectVarFlagName) != nil {
		return
	}
	flags.StringArray(
		ProjectVarFlagName,
		nil,
		"Value for an atlas.hcl variable with no default, as name=value (repeatable)",
	)
}

// RegisterExternalSchemaOptInFlag registers the safety gate for executing an
// external_schema program loaded from ptah.yaml or atlas.hcl. An explicit
// --schema-cmd does not require this flag because supplying the command is
// already an opt-in.
func RegisterExternalSchemaOptInFlag(flags *pflag.FlagSet) {
	flags.Bool(
		AllowExternalSchemaFlagName,
		false,
		"Allow executing the external_schema program configured in ptah.yaml or atlas.hcl",
	)
}

// RegisterAtlasProjectInternalFlags registers hidden adapter-only flags used to
// forward Atlas project config selection into native command implementations.
func RegisterAtlasProjectInternalFlags(flags *pflag.FlagSet) {
	if flags.Lookup(AtlasProjectConfigFlagName) == nil {
		flags.String(AtlasProjectConfigFlagName, "", "Internal Atlas project config path")
		if err := flags.MarkHidden(AtlasProjectConfigFlagName); err != nil {
			panic(err)
		}
	}
	if flags.Lookup(AtlasProjectVarFlagName) == nil {
		flags.StringArray(AtlasProjectVarFlagName, nil, "Internal Atlas project variable override")
		if err := flags.MarkHidden(AtlasProjectVarFlagName); err != nil {
			panic(err)
		}
	}
}

// LoadProjectConfig loads project-level configuration for a command. The
// explicit Ptah config path controls ptah.yaml only; Atlas-compatible adapters
// can pass an internal atlas.hcl path and variable overrides through hidden
// flags. A project-config snapshot supplied by an adapter takes precedence and
// avoids reopening project files during the forwarded execution.
func LoadProjectConfig(cmd *cobra.Command, ptahConfigPath string) (projectconfig.Config, error) {
	if config, ok := projectConfigFromContext(cmd.Context()); ok {
		config = cloneProjectConfig(config)
		bindProjectMigrationDirectory(cmd, config)
		return config, nil
	}
	envName, err := stringFlag(cmd, EnvFlagName)
	if err != nil {
		return projectconfig.Config{}, err
	}
	atlasPath, err := stringFlag(cmd, AtlasProjectConfigFlagName)
	if err != nil {
		return projectconfig.Config{}, err
	}
	atlasVars, err := projectVars(cmd)
	if err != nil {
		return projectconfig.Config{}, err
	}
	config, err := projectconfig.Load(projectconfig.LoadOptions{
		Context:   cmd.Context(),
		PtahPath:  ptahConfigPath,
		AtlasPath: atlasPath,
		EnvName:   envName,
		AtlasVars: atlasVars,
		// So a refusal about a for_each env names the command the operator
		// ran rather than an internal API (stokaro/ptah#1696).
		Verb: cmd.CommandPath(),
	})
	if err != nil {
		return projectconfig.Config{}, err
	}
	if err := ReportIgnoredAtlasConstructs(cmd.ErrOrStderr(), config); err != nil {
		return projectconfig.Config{}, err
	}
	bindProjectMigrationDirectory(cmd, config)
	return config, nil
}

func bindProjectMigrationDirectory(cmd *cobra.Command, config projectconfig.Config) {
	dir := config.StringValue(projectconfig.StringMigrationDir)
	if !dir.Present {
		return
	}
	fsys, ok := config.MigrationDirectoryFS(dir.Value)
	if !ok {
		return
	}
	cmd.SetContext(migrationsource.WithVirtual(cmd.Context(), dir.Value, fsys, dir.Value))
}

// ReportIgnoredAtlasConstructs writes one warning for every atlas.hcl name
// that Atlas CE accepts without acting on. Ptah preserves this compatibility
// behavior, but makes the resulting no-op visible instead of silently hiding
// a likely typo or unsupported policy.
func ReportIgnoredAtlasConstructs(out io.Writer, config projectconfig.Config) error {
	for _, construct := range config.IgnoredConstructs {
		if _, err := fmt.Fprintf(
			out,
			"warning: atlas.hcl %s %q at %s:%d is ignored for Atlas compatibility and has no effect\n",
			construct.Kind,
			construct.Name,
			construct.Filename,
			construct.Line,
		); err != nil {
			return fmt.Errorf("write ignored atlas.hcl construct warning: %w", err)
		}
	}
	return nil
}

// projectVars resolves the atlas.hcl variable overrides for a command. The
// public --var wins when the user passed it; otherwise the hidden adapter-only
// flag supplies whatever a forwarding command already routed. The two are not
// concatenated: a repeated --var for one name is how a list(string) variable
// is built, so merging both sources would turn a scalar override into a
// two-element list.
func projectVars(cmd *cobra.Command) ([]string, error) {
	if flagChanged(cmd, ProjectVarFlagName) {
		return cmd.Flags().GetStringArray(ProjectVarFlagName)
	}
	adapterVars, err := stringArrayFlag(cmd, AtlasProjectVarFlagName)
	if err != nil {
		return nil, err
	}
	if len(adapterVars) > 0 {
		return adapterVars, nil
	}
	return stringArrayFlag(cmd, ProjectVarFlagName)
}

func projectConfigFromContext(ctx context.Context) (projectconfig.Config, bool) {
	if ctx == nil {
		return projectconfig.Config{}, false
	}
	value, ok := ctx.Value(projectConfigContextKey{}).(projectConfigContextValue)
	return value.config, ok
}

func cloneProjectConfig(config projectconfig.Config) projectconfig.Config {
	return projectconfig.Merge(projectconfig.Config{}, config)
}

// EffectiveString returns an explicit CLI value first, then a present project
// config value (including an empty value), then the flag's built-in default.
func EffectiveString(
	cmd *cobra.Command,
	flagName,
	flagValue string,
	configValue projectconfig.Value[string],
) string {
	if flagChanged(cmd, flagName) || !configValue.Present {
		return flagValue
	}
	return configValue.Value
}

// JoinSchemasValue converts a presence-aware schema list to its CLI form.
func JoinSchemasValue(value projectconfig.Value[[]string]) projectconfig.Value[string] {
	return projectconfig.Value[string]{
		Value:   JoinSchemas(value.Value),
		Present: value.Present,
	}
}

// ResolveExternalSchemaCommands resolves the external-command schema source for
// a command, preferring an explicit --schema-cmd. A ptah.yaml external_schema
// block or an atlas.hcl data.external_schema source requires
// --allow-external-schema because the conventional config files are
// auto-discovered and executing repository-controlled code must be explicit.
func ResolveExternalSchemaCommands(
	cmd *cobra.Command,
	schemaCmd,
	schemaFormat string,
	cfg projectconfig.Config,
) ([]schemasource.Command, error) {
	if commands := externalSchemaCommandsFromCLI(schemaCmd, schemaFormat); commands != nil {
		return commands, nil
	}
	if flagChanged(cmd, schemaCommandFlagName) {
		return nil, nil
	}
	if len(cfg.ExternalSchema.Program) == 0 {
		return nil, nil
	}
	allowed, err := cmd.Flags().GetBool(AllowExternalSchemaFlagName)
	if err != nil {
		return nil, err
	}
	if !allowed {
		return nil, fmt.Errorf(
			"%s is disabled by default; pass --%s to execute it",
			externalSchemaSourceLabel(cfg.ExternalSchema.Origin),
			AllowExternalSchemaFlagName,
		)
	}
	return externalSchemaCommandsFromConfig(
		cfg.ExternalSchema.Program,
		cfg.ExternalSchema.Format,
		cfg.ExternalSchema.WorkingDir,
		cfg.ExternalSchema.Env,
	)
}

// externalSchemaSourceLabel names the config construct that supplied the
// external schema program, so the safety-gate error points the user at the
// file they need to review before opting in.
func externalSchemaSourceLabel(origin string) string {
	if origin == projectconfig.AtlasFileName {
		return "atlas.hcl data.external_schema"
	}
	return "ptah.yaml external_schema"
}

func externalSchemaCommandsFromCLI(commandLine, format string) []schemasource.Command {
	if strings.TrimSpace(commandLine) == "" {
		return nil
	}
	return []schemasource.Command{{
		Args:   strings.Fields(commandLine),
		Format: format,
	}}
}

func externalSchemaCommandsFromConfig(
	program []string,
	format,
	dir string,
	env []string,
) ([]schemasource.Command, error) {
	if len(program) == 0 {
		return nil, nil
	}
	if strings.TrimSpace(dir) != "" {
		// No boundary, and the reason is worth stating because this call site
		// looks like the one that should keep one: it names the working
		// directory of a program Ptah EXECUTES.
		//
		// Two things say otherwise. The control on this surface is the opt-in
		// flag above -- once an operator has authorized running an arbitrary
		// program, restricting which directory it starts in prevents nothing
		// the program cannot undo with one chdir. And by the time the value
		// arrives here the config layer has already joined a relative
		// working_dir onto the atlas.hcl directory, so a root anchored at the
		// process working directory would refuse an ordinary configuration
		// whose config file lives somewhere else (stokaro/ptah#1622).
		resolvedDir, err := pathguard.ResolveCLIPath(dir)
		if err != nil {
			return nil, fmt.Errorf("resolve external_schema working_dir: %w", err)
		}
		dir = resolvedDir
	}
	return []schemasource.Command{{
		Args:   slices.Clone(program),
		Format: format,
		Dir:    dir,
		Env:    slices.Clone(env),
	}}, nil
}

func stringFlag(cmd *cobra.Command, name string) (string, error) {
	flag := cmd.Flags().Lookup(name)
	if flag == nil {
		return "", nil
	}
	return flag.Value.String(), nil
}

func stringArrayFlag(cmd *cobra.Command, name string) ([]string, error) {
	if cmd.Flags().Lookup(name) == nil {
		return nil, nil
	}
	return cmd.Flags().GetStringArray(name)
}

func flagChanged(cmd *cobra.Command, name string) bool {
	flag := cmd.Flags().Lookup(name)
	return flag != nil && flag.Changed
}

// SchemaSourceProjectEnv describes the project environment an `env://` schema
// source is expanded through, or the zero value when the run selected none.
//
// A native command reaches the same atlas.hcl the adapter does: the config is
// discovered under its default name when no path was forwarded, so `--env`
// selects a real environment either way. What the two sides did not share was a
// route from that environment to a desired-state source, which is why
// --schema-file refused every `env://` reference while --to accepted it
// (stokaro/ptah#1760).
//
// The zero value is returned when no environment was selected. That is not a
// degraded mode: with no environment there is no `src` attribute to read, and
// the caller's refusal names both the reference and the alternative.
func SchemaSourceProjectEnv(
	cmd *cobra.Command,
	config projectconfig.Config,
) (atlassource.ProjectEnv, error) {
	envName, err := stringFlag(cmd, EnvFlagName)
	if err != nil {
		return atlassource.ProjectEnv{}, err
	}
	if strings.TrimSpace(envName) == "" {
		return atlassource.ProjectEnv{}, nil
	}
	atlasPath, err := stringFlag(cmd, AtlasProjectConfigFlagName)
	if err != nil {
		return atlassource.ProjectEnv{}, err
	}
	if strings.TrimSpace(atlasPath) == "" {
		atlasPath = projectconfig.AtlasFileName
	}
	return atlassource.ProjectEnv{
		Loaded:  true,
		Config:  config,
		BaseDir: filepath.Dir(atlasPath),
	}, nil
}

// SchemaSourceEnvSelectorFlag names the flag a command offers for selecting a
// project environment, or is empty when it offers none.
//
// It is read off the flag set rather than assumed, so a command that stops
// registering the flag stops promising it in the refusal too.
func SchemaSourceEnvSelectorFlag(cmd *cobra.Command) string {
	if cmd.Flags().Lookup(EnvFlagName) == nil {
		return ""
	}
	return EnvFlagName
}

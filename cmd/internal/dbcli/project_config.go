package dbcli

import (
	"context"
	"fmt"
	"slices"
	"strings"

	"github.com/spf13/cobra"
	"github.com/spf13/pflag"

	"github.com/stokaro/ptah/config/projectconfig"
	"github.com/stokaro/ptah/core/schemasource"
	"github.com/stokaro/ptah/internal/pathguard"
)

const (
	// EnvFlagName selects an env block from project config.
	EnvFlagName = "env"
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

// RegisterEnvFlag registers the shared project env selection flag.
func RegisterEnvFlag(flags *pflag.FlagSet, target *string) {
	flags.StringVar(target, EnvFlagName, "", "Project env name to read from ptah.yaml or atlas.hcl")
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
		return cloneProjectConfig(config), nil
	}
	envName, err := stringFlag(cmd, EnvFlagName)
	if err != nil {
		return projectconfig.Config{}, err
	}
	atlasPath, err := stringFlag(cmd, AtlasProjectConfigFlagName)
	if err != nil {
		return projectconfig.Config{}, err
	}
	atlasVars, err := stringArrayFlag(cmd, AtlasProjectVarFlagName)
	if err != nil {
		return projectconfig.Config{}, err
	}
	return projectconfig.Load(projectconfig.LoadOptions{
		PtahPath:  ptahConfigPath,
		AtlasPath: atlasPath,
		EnvName:   envName,
		AtlasVars: atlasVars,
	})
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
	flagName string,
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
	schemaCmd string,
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
	format string,
	dir string,
	env []string,
) ([]schemasource.Command, error) {
	if len(program) == 0 {
		return nil, nil
	}
	if strings.TrimSpace(dir) != "" {
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

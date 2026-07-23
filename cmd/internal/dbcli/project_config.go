package dbcli

import (
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"

	"github.com/stokaro/ptah/config/projectconfig"
)

const (
	// EnvFlagName selects an env block from project config.
	EnvFlagName = "env"
	// AtlasProjectConfigFlagName passes an Atlas project config path through
	// internal command adapters without exposing Atlas flags on native commands.
	AtlasProjectConfigFlagName = "atlas-project-config"
	// AtlasProjectVarFlagName passes Atlas project variable overrides through
	// internal command adapters without exposing Atlas flags on native commands.
	AtlasProjectVarFlagName = "atlas-project-var"
)

// RegisterEnvFlag registers the shared project env selection flag.
func RegisterEnvFlag(flags *pflag.FlagSet, target *string) {
	flags.StringVar(target, EnvFlagName, "", "Project env name to read from ptah.yaml or atlas.hcl")
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
// flags.
func LoadProjectConfig(cmd *cobra.Command, ptahConfigPath string) (projectconfig.Config, error) {
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

// EffectiveString returns flagValue when flagName was explicitly set, otherwise
// configValue when it is non-empty, otherwise flagValue.
func EffectiveString(cmd *cobra.Command, flagName, flagValue, configValue string) string {
	if flagChanged(cmd, flagName) || configValue == "" {
		return flagValue
	}
	return configValue
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

package dbcli

import (
	"github.com/go-extras/cobraflags"
	"github.com/spf13/cobra"

	"github.com/stokaro/ptah/config/projectconfig"
)

const (
	// EnvFlagName selects an env block from project config.
	EnvFlagName = "env"
)

// NewEnvFlag returns the shared project env selection flag.
func NewEnvFlag() cobraflags.Flag {
	return &cobraflags.StringFlag{
		Name:  EnvFlagName,
		Value: "",
		Usage: "Project env name to read from ptah.yaml or atlas.hcl",
	}
}

// LoadProjectConfig loads project-level configuration for a command. The
// explicit Ptah config path controls ptah.yaml only; atlas.hcl is discovered by
// its conventional name in the current working directory.
func LoadProjectConfig(cmd *cobra.Command, ptahConfigPath string) (projectconfig.Config, error) {
	envName, err := stringFlag(cmd, EnvFlagName)
	if err != nil {
		return projectconfig.Config{}, err
	}
	return projectconfig.Load(projectconfig.LoadOptions{
		PtahPath: ptahConfigPath,
		EnvName:  envName,
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

func flagChanged(cmd *cobra.Command, name string) bool {
	flag := cmd.Flags().Lookup(name)
	return flag != nil && flag.Changed
}

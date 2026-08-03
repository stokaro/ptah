package dbcli_test

import (
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/spf13/cobra"

	"go.5x5.cz/ptah/cmd/internal/dbcli"
)

// atlasHCLRequiringVar is a project config whose env cannot be evaluated
// without a value for the variable, which is what makes the evaluator print
// its "requires a default or --var name=value" advice.
const atlasHCLRequiringVar = `variable "dburl" {}

env "local" {
  url = var.dburl
}
`

func chdirTemp(c *qt.C, files map[string]string) string {
	dir := c.TempDir()
	for name, content := range files {
		c.Assert(os.WriteFile(filepath.Join(dir, name), []byte(content), 0o600), qt.IsNil)
	}
	original, err := os.Getwd()
	c.Assert(err, qt.IsNil)
	c.Assert(os.Chdir(dir), qt.IsNil)
	c.Cleanup(func() {
		c.Assert(os.Chdir(original), qt.IsNil)
	})
	return dir
}

func envVarCommand(c *qt.C) *cobra.Command {
	cmd := &cobra.Command{Use: "test"}
	var envName string
	dbcli.RegisterEnvFlag(cmd.Flags(), &envName)
	return cmd
}

// TestLoadProjectConfigHonorsPublicVarFlag is the positive half of the item-2
// fix: the flag the diagnostic names actually supplies the value.
func TestLoadProjectConfigHonorsPublicVarFlag(t *testing.T) {
	c := qt.New(t)
	chdirTemp(c, map[string]string{"atlas.hcl": atlasHCLRequiringVar})

	cmd := envVarCommand(c)
	c.Assert(cmd.Flags().Set(dbcli.EnvFlagName, "local"), qt.IsNil)
	c.Assert(cmd.Flags().Set(dbcli.ProjectVarFlagName, "dburl=sqlite://file.db"), qt.IsNil)

	cfg, err := dbcli.LoadProjectConfig(cmd, "")

	c.Assert(err, qt.IsNil)
	c.Assert(cfg.DatabaseURL, qt.Equals, "sqlite://file.db")
}

// TestLoadProjectConfigWithoutVarAdvisesTheFlagItAccepts is the negative half:
// the advice still fires when nothing was passed, and the flag it names is one
// the command registers.
func TestLoadProjectConfigWithoutVarAdvisesTheFlagItAccepts(t *testing.T) {
	c := qt.New(t)
	chdirTemp(c, map[string]string{"atlas.hcl": atlasHCLRequiringVar})

	cmd := envVarCommand(c)
	c.Assert(cmd.Flags().Set(dbcli.EnvFlagName, "local"), qt.IsNil)

	_, err := dbcli.LoadProjectConfig(cmd, "")

	c.Assert(err, qt.ErrorMatches, `atlas\.hcl variable "dburl" requires a default or --var dburl=value`)
	c.Assert(cmd.Flags().Lookup(dbcli.ProjectVarFlagName), qt.IsNotNil)
}

// TestLoadProjectConfigPublicVarWinsOverAdapterVar pins the precedence rule.
// The two sources are not concatenated: a repeated --var for one name builds a
// list(string), so merging them would turn a scalar override into a list.
func TestLoadProjectConfigPublicVarWinsOverAdapterVar(t *testing.T) {
	c := qt.New(t)
	chdirTemp(c, map[string]string{"atlas.hcl": atlasHCLRequiringVar})

	cmd := envVarCommand(c)
	dbcli.RegisterAtlasProjectInternalFlags(cmd.Flags())
	c.Assert(cmd.Flags().Set(dbcli.EnvFlagName, "local"), qt.IsNil)
	c.Assert(cmd.Flags().Set(dbcli.AtlasProjectVarFlagName, "dburl=sqlite://adapter.db"), qt.IsNil)
	c.Assert(cmd.Flags().Set(dbcli.ProjectVarFlagName, "dburl=sqlite://public.db"), qt.IsNil)

	cfg, err := dbcli.LoadProjectConfig(cmd, "")

	c.Assert(err, qt.IsNil)
	c.Assert(cfg.DatabaseURL, qt.Equals, "sqlite://public.db")
}

// TestLoadProjectConfigStillHonorsAdapterVar is the non-interference control
// for the precedence rule above: the hidden forwarding flag keeps working when
// the public one is absent.
func TestLoadProjectConfigStillHonorsAdapterVar(t *testing.T) {
	c := qt.New(t)
	chdirTemp(c, map[string]string{"atlas.hcl": atlasHCLRequiringVar})

	cmd := envVarCommand(c)
	dbcli.RegisterAtlasProjectInternalFlags(cmd.Flags())
	c.Assert(cmd.Flags().Set(dbcli.EnvFlagName, "local"), qt.IsNil)
	c.Assert(cmd.Flags().Set(dbcli.AtlasProjectVarFlagName, "dburl=sqlite://adapter.db"), qt.IsNil)

	cfg, err := dbcli.LoadProjectConfig(cmd, "")

	c.Assert(err, qt.IsNil)
	c.Assert(cfg.DatabaseURL, qt.Equals, "sqlite://adapter.db")
}

// TestRegisterEnvFlagAnnotatesTheProjectEnvFlag pins the marker that lets the
// command-tree gate tell a project env from `ptah seed --env`.
func TestRegisterEnvFlagAnnotatesTheProjectEnvFlag(t *testing.T) {
	c := qt.New(t)

	bound := envVarCommand(c)
	c.Assert(
		bound.Flags().Lookup(dbcli.EnvFlagName).Annotations[dbcli.ProjectConfigEnvAnnotation],
		qt.DeepEquals,
		[]string{"true"},
	)

	unbound := &cobra.Command{Use: "test"}
	dbcli.RegisterProjectEnvFlag(unbound.Flags())
	c.Assert(
		unbound.Flags().Lookup(dbcli.EnvFlagName).Annotations[dbcli.ProjectConfigEnvAnnotation],
		qt.DeepEquals,
		[]string{"true"},
	)
	c.Assert(unbound.Flags().Lookup(dbcli.ProjectVarFlagName), qt.IsNotNil)
}

// TestExplicitConfigOverHCLNamesTheConfigFlag holds the ptah.yaml diagnostic's
// spelling of the flag to the flag this package actually registers. The
// projectconfig package cannot import this one, so it spells "config" itself;
// this is what stops the two from drifting apart.
func TestExplicitConfigOverHCLNamesTheConfigFlag(t *testing.T) {
	c := qt.New(t)
	chdirTemp(c, map[string]string{"atlas.hcl": atlasHCLRequiringVar})

	cmd := envVarCommand(c)
	_, err := dbcli.LoadProjectConfig(cmd, "atlas.hcl")

	c.Assert(err, qt.IsNotNil)
	c.Assert(err.Error(), qt.Contains, "--"+dbcli.ConfigFlagName)
	c.Assert(err.Error(), qt.Not(qt.Contains), "projectconfig")
}

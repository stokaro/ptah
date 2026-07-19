package dbcli_test

import (
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/spf13/cobra"

	"github.com/stokaro/ptah/cmd/internal/dbcli"
	"github.com/stokaro/ptah/config/projectconfig"
)

func TestEffectiveStringPrefersExplicitCLIFlag(t *testing.T) {
	c := qt.New(t)
	cmd := &cobra.Command{Use: "test"}
	cmd.Flags().String("db-url", "builtin", "")
	c.Assert(cmd.Flags().Set("db-url", "cli"), qt.IsNil)

	got := dbcli.EffectiveString(cmd, "db-url", "cli", "config")

	c.Assert(got, qt.Equals, "cli")
}

func TestEffectiveStringUsesConfigWhenFlagIsDefault(t *testing.T) {
	c := qt.New(t)
	cmd := &cobra.Command{Use: "test"}
	cmd.Flags().String("migrations-dir", "./migrations", "")

	got := dbcli.EffectiveString(cmd, "migrations-dir", "./migrations", "atlas-migrations")

	c.Assert(got, qt.Equals, "atlas-migrations")
}

func TestLoadProjectConfigReadsAtlasEnvAndPtahFallback(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	c.Assert(os.WriteFile(filepath.Join(dir, projectconfig.PtahFileName), []byte(`url: postgres://ptah/db
migration:
  dir: ./ptah-migrations
`), 0o600), qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(dir, projectconfig.AtlasFileName), []byte(`env "local" {
  url = "postgres://atlas/db"
  migration {
    dir = "file://atlas-migrations"
  }
}
`), 0o600), qt.IsNil)
	originalWD, err := os.Getwd()
	c.Assert(err, qt.IsNil)
	c.Assert(os.Chdir(dir), qt.IsNil)
	defer func() {
		c.Assert(os.Chdir(originalWD), qt.IsNil)
	}()

	cmd := &cobra.Command{Use: "test"}
	cmd.Flags().String(dbcli.EnvFlagName, "", "")
	cfg, err := dbcli.LoadProjectConfig(cmd, "")

	c.Assert(err, qt.IsNil)
	c.Assert(cfg.DatabaseURL, qt.Equals, "postgres://atlas/db")
	c.Assert(cfg.Migration.Dir, qt.Equals, "atlas-migrations")
	c.Assert(cfg.Migration.Format, qt.Equals, "atlas")
	c.Assert(cfg.Migration.RevisionFormat, qt.Equals, "atlas")
}

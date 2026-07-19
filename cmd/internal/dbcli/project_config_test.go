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

func TestLoadProjectConfigReadsNamedPtahEnvRuntimeDefaults(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	c.Assert(os.WriteFile(filepath.Join(dir, projectconfig.PtahFileName), []byte(`env:
  prod:
    url: postgres://prod/db
    dev: postgres://prod/shadow
    schemas: [public, tenant]
    migration:
      dir: ./migrations
      format: atlas
      revisions_schema: atlas
      revisions_table: atlas_schema_revisions
      revision_format: atlas
      lock_timeout: 3s
      statement_timeout: 30s
      connect_timeout: 10s
      migration_lock_timeout: 15s
      exec_order: non-linear
    lint:
      dialect: postgres
      disabled-rules: [MF103]
`), 0o600), qt.IsNil)
	originalWD, err := os.Getwd()
	c.Assert(err, qt.IsNil)
	c.Assert(os.Chdir(dir), qt.IsNil)
	defer func() {
		c.Assert(os.Chdir(originalWD), qt.IsNil)
	}()

	cmd := &cobra.Command{Use: "test"}
	cmd.Flags().String(dbcli.EnvFlagName, "", "")
	c.Assert(cmd.Flags().Set(dbcli.EnvFlagName, "prod"), qt.IsNil)

	cfg, err := dbcli.LoadProjectConfig(cmd, "")

	c.Assert(err, qt.IsNil)
	c.Assert(cfg.EnvName, qt.Equals, "prod")
	c.Assert(cfg.DatabaseURL, qt.Equals, "postgres://prod/db")
	c.Assert(cfg.DevURL, qt.Equals, "postgres://prod/shadow")
	c.Assert(cfg.Schemas, qt.DeepEquals, []string{"public", "tenant"})
	c.Assert(cfg.Migration.Dir, qt.Equals, "./migrations")
	c.Assert(cfg.Migration.Format, qt.Equals, "atlas")
	c.Assert(cfg.Migration.RevisionsSchema, qt.Equals, "atlas")
	c.Assert(cfg.Migration.RevisionsTable, qt.Equals, "atlas_schema_revisions")
	c.Assert(cfg.Migration.RevisionFormat, qt.Equals, "atlas")
	c.Assert(cfg.Migration.LockTimeout, qt.Equals, "3s")
	c.Assert(cfg.Migration.StatementTimeout, qt.Equals, "30s")
	c.Assert(cfg.Migration.ConnectTimeout, qt.Equals, "10s")
	c.Assert(cfg.Migration.MigrationLockTimeout, qt.Equals, "15s")
	c.Assert(cfg.Migration.ExecOrder, qt.Equals, "non-linear")
	c.Assert(cfg.Lint.Dialect, qt.Equals, "postgres")
	c.Assert(cfg.Lint.DisabledRules, qt.DeepEquals, []string{"MF103"})
}

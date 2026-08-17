package dbcli_test

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/spf13/cobra"

	"go.5x5.cz/ptah/cmd/internal/dbcli"
	"go.5x5.cz/ptah/config/projectconfig"
)

func TestEffectiveStringPrefersExplicitCLIFlag(t *testing.T) {
	c := qt.New(t)
	cmd := &cobra.Command{Use: "test"}
	cmd.Flags().String("db-url", "builtin", "")
	c.Assert(cmd.Flags().Set("db-url", "cli"), qt.IsNil)

	got := dbcli.EffectiveString(cmd, "db-url", "cli", projectconfig.Value[string]{
		Value:   "config",
		Present: true,
	})

	c.Assert(got, qt.Equals, "cli")
}

func TestEffectiveStringUsesConfigWhenFlagIsDefault(t *testing.T) {
	c := qt.New(t)
	cmd := &cobra.Command{Use: "test"}
	cmd.Flags().String("migrations-dir", "./migrations", "")

	got := dbcli.EffectiveString(cmd, "migrations-dir", "./migrations", projectconfig.Value[string]{
		Value:   "atlas-migrations",
		Present: true,
	})

	c.Assert(got, qt.Equals, "atlas-migrations")
}

func TestEffectiveStringUsesExplicitEmptyConfigValue(t *testing.T) {
	c := qt.New(t)
	cmd := &cobra.Command{Use: "test"}
	cmd.Flags().String("migrations-dir", "./migrations", "")

	got := dbcli.EffectiveString(cmd, "migrations-dir", "./migrations", projectconfig.Value[string]{
		Present: true,
	})

	c.Assert(got, qt.Equals, "")
}

func TestEffectiveStringUsesDefaultWhenConfigValueIsAbsent(t *testing.T) {
	c := qt.New(t)
	cmd := &cobra.Command{Use: "test"}
	cmd.Flags().String("migrations-dir", "./migrations", "")

	got := dbcli.EffectiveString(
		cmd,
		"migrations-dir",
		"./migrations",
		projectconfig.Value[string]{},
	)

	c.Assert(got, qt.Equals, "./migrations")
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

func TestLoadProjectConfigReportsIgnoredAtlasConstructs(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	t.Chdir(dir)
	c.Assert(os.WriteFile(projectconfig.AtlasFileName, []byte(`env "local" {
  project = "ignored"
}
`), 0o600), qt.IsNil)

	cmd := &cobra.Command{Use: "test"}
	cmd.Flags().String(dbcli.EnvFlagName, "local", "")
	var stderr bytes.Buffer
	cmd.SetErr(&stderr)
	cfg, err := dbcli.LoadProjectConfig(cmd, "")

	c.Assert(err, qt.IsNil)
	c.Assert(cfg.IgnoredConstructs, qt.HasLen, 1)
	c.Assert(
		stderr.String(),
		qt.Equals,
		"warning: atlas.hcl attribute \"project\" at atlas.hcl:2 is ignored for Atlas compatibility and has no effect\n",
	)
}

func TestReportIgnoredAtlasConstructsReturnsWriterError(t *testing.T) {
	c := qt.New(t)
	writeErr := errors.New("diagnostics unavailable")

	err := dbcli.ReportIgnoredAtlasConstructs(
		failingDiagnosticsWriter{err: writeErr},
		projectconfig.Config{IgnoredConstructs: []projectconfig.IgnoredAtlasConstruct{
			{Name: "project", Kind: "attribute", Filename: "atlas.hcl", Line: 2},
		}},
	)

	c.Assert(err, qt.ErrorIs, writeErr)
}

type failingDiagnosticsWriter struct {
	err error
}

func (w failingDiagnosticsWriter) Write([]byte) (int, error) {
	return 0, w.err
}

func TestLoadProjectConfigReadsInternalAtlasProjectFlags(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	c.Assert(os.WriteFile(filepath.Join(dir, "custom.hcl"), []byte(`variable "dir" {}

env "local" {
  migration {
    dir = "file://${var.dir}"
  }
  schema {
    src = var.schema
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
	dbcli.RegisterAtlasProjectInternalFlags(cmd.Flags())
	c.Assert(cmd.Flags().Set(dbcli.EnvFlagName, "local"), qt.IsNil)
	c.Assert(cmd.Flags().Set(dbcli.AtlasProjectConfigFlagName, "custom.hcl"), qt.IsNil)
	c.Assert(cmd.Flags().Set(dbcli.AtlasProjectVarFlagName, "dir=migrations"), qt.IsNil)
	c.Assert(cmd.Flags().Set(dbcli.AtlasProjectVarFlagName, "schema=file://a.hcl"), qt.IsNil)
	c.Assert(cmd.Flags().Set(dbcli.AtlasProjectVarFlagName, "schema=file://b.hcl"), qt.IsNil)

	cfg, err := dbcli.LoadProjectConfig(cmd, "")

	c.Assert(err, qt.IsNil)
	c.Assert(cfg.Migration.Dir, qt.Equals, "migrations")
	c.Assert(cfg.SchemaSources, qt.DeepEquals, []string{"file://a.hcl", "file://b.hcl"})
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
    online_ddl:
      tool: ghost
      threshold_rows: 500000
      args: [--allow-on-master]
      fallback: error
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
	c.Assert(cfg.OnlineDDL.Tool, qt.Equals, projectconfig.OnlineDDLToolGhost)
	c.Assert(cfg.OnlineDDL.ThresholdRows, qt.Equals, int64(500000))
	c.Assert(cfg.OnlineDDL.Args, qt.DeepEquals, []string{"--allow-on-master"})
	c.Assert(cfg.OnlineDDL.Fallback, qt.Equals, projectconfig.OnlineDDLFallbackError)
	c.Assert(cfg.Lint.Dialect, qt.Equals, "postgres")
	c.Assert(cfg.Lint.DisabledRules, qt.DeepEquals, []string{"MF103"})
}

func TestLoadProjectConfigUsesImmutableContextSnapshot(t *testing.T) {
	c := qt.New(t)
	snapshot, err := projectconfig.ParsePtah([]byte(`url: postgres://snapshot/db
schemas: [public]
`), "snapshot.yaml", "")
	c.Assert(err, qt.IsNil)
	latest := 3
	snapshot.Lint.Latest = &latest
	snapshot.Lint.RuleConfigs = map[string]projectconfig.LintRuleConfig{
		"DS": {
			Severity: "warning",
			Exclude:  []string{"migrations/legacy/*"},
		},
	}
	snapshot.IgnoredConstructs = []projectconfig.IgnoredAtlasConstruct{
		{Name: "project", Kind: "attribute", Filename: "atlas.hcl", Line: 2},
	}
	cmd := &cobra.Command{Use: "test"}
	cmd.SetContext(dbcli.WithProjectConfig(cmd.Context(), snapshot))
	snapshot.DatabaseURL = "postgres://mutated/db"
	snapshot.Schemas[0] = "mutated"
	latest = 9
	snapshot.Lint.RuleConfigs["DS"] = projectconfig.LintRuleConfig{
		Severity: "error",
		Exclude:  []string{"mutated/*"},
	}
	snapshot.IgnoredConstructs[0].Name = "mutated"

	first, err := dbcli.LoadProjectConfig(cmd, filepath.Join(t.TempDir(), "missing.yaml"))

	c.Assert(err, qt.IsNil)
	c.Assert(first.DatabaseURL, qt.Equals, "postgres://snapshot/db")
	c.Assert(first.Schemas, qt.DeepEquals, []string{"public"})
	c.Assert(*first.Lint.Latest, qt.Equals, 3)
	c.Assert(first.Lint.RuleConfigs["DS"], qt.DeepEquals, projectconfig.LintRuleConfig{
		Severity: "warning",
		Exclude:  []string{"migrations/legacy/*"},
	})
	c.Assert(first.IgnoredConstructs, qt.DeepEquals, []projectconfig.IgnoredAtlasConstruct{
		{Name: "project", Kind: "attribute", Filename: "atlas.hcl", Line: 2},
	})
	first.DatabaseURL = "postgres://consumer-mutation/db"
	first.Schemas[0] = "consumer-mutation"
	*first.Lint.Latest = 12
	first.Lint.RuleConfigs["DS"] = projectconfig.LintRuleConfig{Severity: "off"}
	first.IgnoredConstructs[0].Name = "consumer-mutation"

	second, err := dbcli.LoadProjectConfig(cmd, filepath.Join(t.TempDir(), "missing.yaml"))

	c.Assert(err, qt.IsNil)
	c.Assert(second.DatabaseURL, qt.Equals, "postgres://snapshot/db")
	c.Assert(second.Schemas, qt.DeepEquals, []string{"public"})
	c.Assert(*second.Lint.Latest, qt.Equals, 3)
	c.Assert(second.Lint.RuleConfigs["DS"], qt.DeepEquals, projectconfig.LintRuleConfig{
		Severity: "warning",
		Exclude:  []string{"migrations/legacy/*"},
	})
	c.Assert(second.IgnoredConstructs, qt.DeepEquals, []projectconfig.IgnoredAtlasConstruct{
		{Name: "project", Kind: "attribute", Filename: "atlas.hcl", Line: 2},
	})
}

func TestExternalSchemaCommandsPrefersFlag(t *testing.T) {
	c := qt.New(t)
	cmd := &cobra.Command{Use: "test"}
	cmd.Flags().String("schema-cmd", "", "")
	dbcli.RegisterExternalSchemaOptInFlag(cmd.Flags())
	c.Assert(cmd.Flags().Set("schema-cmd", "go run ./flag-loader"), qt.IsNil)

	cfg := projectconfig.Config{
		ExternalSchema: projectconfig.ExternalSchemaConfig{
			Program: []string{"config-loader"},
			Format:  "hcl",
		},
	}
	commands, err := dbcli.ResolveExternalSchemaCommands(cmd, "go run ./flag-loader", "yaml", cfg)

	c.Assert(err, qt.IsNil)
	c.Assert(commands, qt.HasLen, 1)
	c.Assert(commands[0].Args, qt.DeepEquals, []string{"go", "run", "./flag-loader"})
	c.Assert(commands[0].Format, qt.Equals, "yaml")
}

func TestExternalSchemaCommandsExplicitEmptyFlagDisablesConfig(t *testing.T) {
	c := qt.New(t)
	cmd := &cobra.Command{Use: "test"}
	cmd.Flags().String("schema-cmd", "", "")
	dbcli.RegisterExternalSchemaOptInFlag(cmd.Flags())
	c.Assert(cmd.Flags().Set("schema-cmd", ""), qt.IsNil)

	cfg := projectconfig.Config{
		ExternalSchema: projectconfig.ExternalSchemaConfig{
			Program: []string{"config-loader"},
		},
	}
	commands, err := dbcli.ResolveExternalSchemaCommands(cmd, "", "sql", cfg)

	c.Assert(err, qt.IsNil)
	c.Assert(commands, qt.IsNil)
}

func TestExternalSchemaCommandsFallsBackToConfig(t *testing.T) {
	c := qt.New(t)
	cmd := &cobra.Command{Use: "test"}
	dbcli.RegisterExternalSchemaOptInFlag(cmd.Flags())
	c.Assert(cmd.Flags().Set(dbcli.AllowExternalSchemaFlagName, "true"), qt.IsNil)
	workingDir, err := filepath.Abs(".")
	c.Assert(err, qt.IsNil)

	cfg := projectconfig.Config{
		ExternalSchema: projectconfig.ExternalSchemaConfig{
			Program:    []string{"go", "run", "./config-loader"},
			Format:     "hcl",
			WorkingDir: ".",
			Env:        []string{"K=V"},
		},
	}
	commands, err := dbcli.ResolveExternalSchemaCommands(cmd, "", "sql", cfg)

	c.Assert(err, qt.IsNil)
	c.Assert(commands, qt.HasLen, 1)
	c.Assert(commands[0].Args, qt.DeepEquals, []string{"go", "run", "./config-loader"})
	c.Assert(commands[0].Format, qt.Equals, "hcl")
	c.Assert(commands[0].Dir, qt.Equals, workingDir)
	c.Assert(commands[0].Env, qt.DeepEquals, []string{"K=V"})
}

// TestExternalSchemaCommandsResolvesAWorkingDirectoryThatLeavesTheProject
// records what stokaro/ptah#1622 decided about this call site rather than what
// it used to refuse.
//
// The refusal was a spelling filter: the same directory named absolutely was
// always accepted, and the config layer has already joined a relative
// working_dir onto the atlas.hcl directory before the value arrives here. The
// control on this surface is the opt-in flag, which the next test pins -- once
// an operator authorizes running an arbitrary program, the directory it starts
// in is not a boundary.
func TestExternalSchemaCommandsResolvesAWorkingDirectoryThatLeavesTheProject(t *testing.T) {
	c := qt.New(t)
	cmd := &cobra.Command{Use: "test"}
	dbcli.RegisterExternalSchemaOptInFlag(cmd.Flags())
	c.Assert(cmd.Flags().Set(dbcli.AllowExternalSchemaFlagName, "true"), qt.IsNil)
	cfg := projectconfig.Config{
		ExternalSchema: projectconfig.ExternalSchemaConfig{
			Program:    []string{"config-loader"},
			WorkingDir: "..",
		},
	}

	commands, err := dbcli.ResolveExternalSchemaCommands(cmd, "", "sql", cfg)

	c.Assert(err, qt.IsNil)
	c.Assert(commands, qt.HasLen, 1)
	c.Assert(commands[0].Dir, qt.Not(qt.Equals), "")
	c.Assert(filepath.IsAbs(commands[0].Dir), qt.IsTrue)
}

func TestExternalSchemaCommandsRejectsImplicitConfigExecution(t *testing.T) {
	c := qt.New(t)
	cmd := &cobra.Command{Use: "test"}
	dbcli.RegisterExternalSchemaOptInFlag(cmd.Flags())
	cfg := projectconfig.Config{
		ExternalSchema: projectconfig.ExternalSchemaConfig{
			Program:    []string{"config-loader"},
			WorkingDir: "..",
		},
	}

	commands, err := dbcli.ResolveExternalSchemaCommands(cmd, "", "sql", cfg)

	c.Assert(err, qt.ErrorMatches, "ptah.yaml external_schema is disabled by default; pass --allow-external-schema to execute it")
	c.Assert(commands, qt.IsNil)
}

func TestExternalSchemaCommandsRejectsImplicitAtlasConfigExecution(t *testing.T) {
	c := qt.New(t)
	cmd := &cobra.Command{Use: "test"}
	dbcli.RegisterExternalSchemaOptInFlag(cmd.Flags())
	cfg := projectconfig.Config{
		ExternalSchema: projectconfig.ExternalSchemaConfig{
			Program: []string{"config-loader"},
			Origin:  projectconfig.AtlasFileName,
		},
	}

	commands, err := dbcli.ResolveExternalSchemaCommands(cmd, "", "sql", cfg)

	c.Assert(err, qt.ErrorMatches, "atlas.hcl data.external_schema is disabled by default; pass --allow-external-schema to execute it")
	c.Assert(commands, qt.IsNil)
}

func TestExternalSchemaCommandsAllowsAtlasConfigWithOptIn(t *testing.T) {
	c := qt.New(t)
	cmd := &cobra.Command{Use: "test"}
	dbcli.RegisterExternalSchemaOptInFlag(cmd.Flags())
	c.Assert(cmd.Flags().Set(dbcli.AllowExternalSchemaFlagName, "true"), qt.IsNil)
	cfg := projectconfig.Config{
		ExternalSchema: projectconfig.ExternalSchemaConfig{
			Program: []string{"config-loader", "--dialect", "sqlite"},
			Format:  "sql",
			Origin:  projectconfig.AtlasFileName,
		},
	}

	commands, err := dbcli.ResolveExternalSchemaCommands(cmd, "", "sql", cfg)

	c.Assert(err, qt.IsNil)
	c.Assert(commands, qt.HasLen, 1)
	c.Assert(commands[0].Args, qt.DeepEquals, []string{"config-loader", "--dialect", "sqlite"})
	c.Assert(commands[0].Format, qt.Equals, "sql")
}

func TestExternalSchemaCommandsNilWhenNeitherSet(t *testing.T) {
	c := qt.New(t)
	cmd := &cobra.Command{Use: "test"}
	dbcli.RegisterExternalSchemaOptInFlag(cmd.Flags())

	commands, err := dbcli.ResolveExternalSchemaCommands(cmd, "", "sql", projectconfig.Config{})

	c.Assert(err, qt.IsNil)
	c.Assert(commands, qt.IsNil)
}

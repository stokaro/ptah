package projectconfig_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"github.com/stokaro/ptah/config/projectconfig"
)

func TestParsePtahPresence_ExplicitZeroValuesOverrideRoot(t *testing.T) {
	c := qt.New(t)

	cfg, err := projectconfig.ParsePtah([]byte(`
url: postgres://root/db
dev: postgres://root/dev
schemas: [public]
exclude: [audit]
migration:
  dir: migrations
  format: ptah
  revisions_schema: revisions
  revisions_table: schema_migrations
  revision_format: ptah
  lock_timeout: 1s
  statement_timeout: 2s
  connect_timeout: 3s
  migration_lock_timeout: 4s
  exec_order: linear
  tx_mode: all
  pre_up_hook: backup
  pre_down_hook: verify
  pg_dump_to: pg.sql
  mysqldump_to: mysql.sql
  webhook: https://example.test/hook
online_ddl:
  tool: ghost
  threshold_rows: 100
  args: [--assume-rbr]
  fallback: plain
lint:
  dialect: postgres
  disabled-rules: [DS101]
  latest: 5
external_schema:
  program: [schema-loader]
  format: sql
  working_dir: ./schema
  env: [MODE=root]
diff:
  skip: [drop_table]
  concurrent_index: true
env:
  prod:
    url: ""
    dev: ""
    schemas: []
    exclude: []
    migrate:
      generate:
        shadow_db: postgres://ignored/dev
    migration:
      dir: ""
      format: ""
      revisions_schema: ""
      revisions_table: ""
      revision_format: ""
      lock_timeout: ""
      statement_timeout: ""
      connect_timeout: ""
      migration_lock_timeout: ""
      exec_order: ""
      tx_mode: ""
      pre_up_hook: ""
      pre_down_hook: ""
      pg_dump_to: ""
      mysqldump_to: ""
      webhook: ""
    online_ddl:
      tool: ""
      threshold_rows: 0
      args: []
      fallback: ""
    lint:
      dialect: ""
      disabled-rules: []
      latest: 0
    external_schema:
      program: []
      format: ""
      working_dir: ""
      env: []
    diff:
      skip: []
      concurrent_index: false
`), "ptah.yaml", "prod")

	c.Assert(err, qt.IsNil)
	c.Assert(cfg.EnvName, qt.Equals, "prod")
	c.Assert(cfg.DatabaseURL, qt.Equals, "")
	c.Assert(cfg.DevURL, qt.Equals, "")
	c.Assert(cfg.Schemas, qt.HasLen, 0)
	c.Assert(cfg.Exclude, qt.HasLen, 0)
	c.Assert(cfg.Migration, qt.DeepEquals, projectconfig.MigrationConfig{})
	c.Assert(cfg.OnlineDDL.Tool, qt.Equals, "")
	c.Assert(cfg.OnlineDDL.ThresholdRows, qt.Equals, int64(0))
	c.Assert(cfg.OnlineDDL.Args, qt.HasLen, 0)
	c.Assert(cfg.OnlineDDL.Fallback, qt.Equals, "")
	c.Assert(cfg.Lint.Dialect, qt.Equals, "")
	c.Assert(cfg.Lint.DisabledRules, qt.HasLen, 0)
	c.Assert(cfg.Lint.Latest, qt.IsNotNil)
	c.Assert(*cfg.Lint.Latest, qt.Equals, 0)
	c.Assert(cfg.ExternalSchema.Program, qt.HasLen, 0)
	c.Assert(cfg.ExternalSchema.Format, qt.Equals, "")
	c.Assert(cfg.ExternalSchema.WorkingDir, qt.Equals, "")
	c.Assert(cfg.ExternalSchema.Env, qt.HasLen, 0)
	c.Assert(cfg.Diff.Skip.DropTable, qt.DeepEquals, projectconfig.ConfigBool{Set: true})
	c.Assert(cfg.Diff.Skip.DropColumn, qt.DeepEquals, projectconfig.ConfigBool{Set: true})
	c.Assert(cfg.Diff.Skip.DropIndex, qt.DeepEquals, projectconfig.ConfigBool{Set: true})
	c.Assert(cfg.Diff.Skip.DropEnum, qt.DeepEquals, projectconfig.ConfigBool{Set: true})
	c.Assert(cfg.Diff.ConcurrentIndex.Create, qt.DeepEquals, projectconfig.ConfigBool{Set: true})
}

func TestMergePresence_AtlasEmptyMigrationOverridesPtah(t *testing.T) {
	c := qt.New(t)

	base, err := projectconfig.ParsePtah([]byte(`
url: postgres://root/db
dev: postgres://root/dev
exclude: [audit]
migration:
  dir: migrations
  format: ptah
  revisions_schema: revisions
  revisions_table: schema_migrations
  revision_format: ptah
  lock_timeout: 1s
  exec_order: linear
  tx_mode: all
`), "ptah.yaml", "")
	c.Assert(err, qt.IsNil)

	override, err := projectconfig.ParseAtlas([]byte(`
env "prod" {
  url     = ""
  dev     = ""
  src     = []
  exclude = []

  migration {
    dir              = ""
    revisions_schema = ""
    lock_timeout     = ""
    exec_order       = ""
    tx_mode          = ""
  }
}
`), "atlas.hcl", "prod")
	c.Assert(err, qt.IsNil)

	cfg := projectconfig.Merge(base, override)

	c.Assert(cfg.DatabaseURL, qt.Equals, "")
	c.Assert(cfg.DevURL, qt.Equals, "")
	c.Assert(cfg.SchemaSources, qt.HasLen, 0)
	c.Assert(cfg.Exclude, qt.HasLen, 0)
	c.Assert(cfg.Migration.Dir, qt.Equals, "")
	c.Assert(cfg.Migration.Format, qt.Equals, "atlas")
	c.Assert(cfg.Migration.RevisionsSchema, qt.Equals, "")
	c.Assert(cfg.Migration.RevisionsTable, qt.Equals, "schema_migrations")
	c.Assert(cfg.Migration.RevisionFormat, qt.Equals, "atlas")
	c.Assert(cfg.Migration.LockTimeout, qt.Equals, "")
	c.Assert(cfg.Migration.ExecOrder, qt.Equals, "")
	c.Assert(cfg.Migration.TxMode, qt.Equals, "")
}

func TestMergePresence_PersistsForFormatAndLintRuleFields(t *testing.T) {
	c := qt.New(t)

	parsed, err := projectconfig.ParseAtlas([]byte(`
lint {
  destructive {
    error = true
  }
}

env "prod" {
  format {
    migrate {
      apply  = "apply"
      diff   = "diff"
      lint   = "lint"
      status = "status"
    }
    schema {
      apply   = "apply"
      clean   = "clean"
      diff    = "diff"
      inspect = "inspect"
    }
  }
}
`), "atlas.hcl", "prod")
	c.Assert(err, qt.IsNil)

	fields := projectconfig.Merge(projectconfig.Config{}, parsed)
	rule := fields.Lint.RuleConfigs["DS"]
	rule.Severity = ""
	fields.Lint.RuleConfigs["DS"] = rule
	fields.Format = projectconfig.FormatConfig{}

	base := projectconfig.Config{
		Lint: projectconfig.LintConfig{
			RuleConfigs: map[string]projectconfig.LintRuleConfig{
				"DS": {
					Severity: "warning",
					Exclude:  []string{"legacy/**"},
				},
			},
		},
		Format: projectconfig.FormatConfig{
			Migrate: projectconfig.MigrateFormatConfig{
				Apply:  "base-apply",
				Diff:   "base-diff",
				Lint:   "base-lint",
				Status: "base-status",
			},
			Schema: projectconfig.SchemaFormatConfig{
				Apply:   "base-apply",
				Clean:   "base-clean",
				Diff:    "base-diff",
				Inspect: "base-inspect",
			},
		},
	}
	cfg := projectconfig.Merge(base, fields)

	c.Assert(cfg.Format, qt.DeepEquals, projectconfig.FormatConfig{})
	c.Assert(cfg.Lint.RuleConfigs["DS"].Severity, qt.Equals, "")
	c.Assert(cfg.Lint.RuleConfigs["DS"].Exclude, qt.DeepEquals, []string{"legacy/**"})

	rules := projectconfig.Merge(projectconfig.Config{}, parsed)
	rules.Lint.RuleConfigs = map[string]projectconfig.LintRuleConfig{}
	cfg = projectconfig.Merge(base, rules)

	c.Assert(cfg.Lint.RuleConfigs, qt.HasLen, 0)
}

func TestMergeProgrammaticNonZeroValuesOverrideEverySection(t *testing.T) {
	c := qt.New(t)
	baseLatest := 1
	overrideLatest := 2
	base := projectconfig.Config{
		EnvName:       "base",
		DatabaseURL:   "postgres://base/db",
		DevURL:        "postgres://base/dev",
		SchemaSources: []string{"file://base.hcl"},
		Schemas:       []string{"base"},
		Exclude:       []string{"base"},
		Schema: projectconfig.SchemaConfig{
			Mode: projectconfig.SchemaModeConfig{
				Tables: projectconfig.ConfigBool{Value: true, Set: true},
			},
		},
		Migration: projectconfig.MigrationConfig{Dir: "base"},
		OnlineDDL: projectconfig.OnlineDDLConfig{Tool: projectconfig.OnlineDDLToolGhost, ThresholdRows: 1},
		Lint: projectconfig.LintConfig{
			Dialect: "postgres",
			RuleConfigs: map[string]projectconfig.LintRuleConfig{
				"DS": {Severity: "warning"},
			},
			Latest: &baseLatest,
		},
		Format: projectconfig.FormatConfig{
			Migrate: projectconfig.MigrateFormatConfig{Apply: "base"},
		},
		Diff: projectconfig.DiffConfig{
			ConcurrentIndex: projectconfig.DiffConcurrentIndexConfig{
				Create: projectconfig.ConfigBool{Value: true, Set: true},
			},
		},
		ExternalSchema: projectconfig.ExternalSchemaConfig{Program: []string{"base"}},
	}
	override := projectconfig.Config{
		EnvName:       "override",
		DatabaseURL:   "postgres://override/db",
		DevURL:        "postgres://override/dev",
		SchemaSources: []string{"file://override.hcl"},
		Schemas:       []string{"override"},
		Exclude:       []string{"override"},
		Schema: projectconfig.SchemaConfig{
			Mode: projectconfig.SchemaModeConfig{
				Tables: projectconfig.ConfigBool{Set: true},
			},
		},
		Migration: projectconfig.MigrationConfig{
			Dir:                  "override",
			Format:               "atlas",
			RevisionsSchema:      "override",
			RevisionsTable:       "override",
			RevisionFormat:       "atlas",
			LockTimeout:          "1s",
			StatementTimeout:     "2s",
			ConnectTimeout:       "3s",
			MigrationLockTimeout: "4s",
			ExecOrder:            "non-linear",
			TxMode:               "none",
			PreUpHook:            "up",
			PreDownHook:          "down",
			PostgresDumpTo:       "pg.sql",
			MySQLDumpTo:          "mysql.sql",
			Webhook:              "https://example.test/hook",
		},
		OnlineDDL: projectconfig.OnlineDDLConfig{
			Tool:          projectconfig.OnlineDDLToolPTOSC,
			ThresholdRows: 2,
			Args:          []string{"--alter-foreign-keys-method=auto"},
			Fallback:      projectconfig.OnlineDDLFallbackPlain,
		},
		Lint: projectconfig.LintConfig{
			Dialect:       "mysql",
			DisabledRules: []string{"DS101"},
			RuleConfigs: map[string]projectconfig.LintRuleConfig{
				"DS": {
					Severity: "error",
					Exclude:  []string{"generated/**"},
				},
			},
			Latest: &overrideLatest,
		},
		Format: projectconfig.FormatConfig{
			Migrate: projectconfig.MigrateFormatConfig{
				Apply:  "apply",
				Diff:   "diff",
				Lint:   "lint",
				Status: "status",
			},
			Schema: projectconfig.SchemaFormatConfig{
				Apply:   "apply",
				Clean:   "clean",
				Diff:    "diff",
				Inspect: "inspect",
			},
		},
		Diff: projectconfig.DiffConfig{
			ConcurrentIndex: projectconfig.DiffConcurrentIndexConfig{
				Create: projectconfig.ConfigBool{Set: true},
			},
		},
		ExternalSchema: projectconfig.ExternalSchemaConfig{
			Program:    []string{"override"},
			Format:     "hcl",
			WorkingDir: "./override",
			Env:        []string{"MODE=override"},
		},
	}

	cfg := projectconfig.Merge(base, override)

	c.Assert(cfg.EnvName, qt.Equals, override.EnvName)
	c.Assert(cfg.DatabaseURL, qt.Equals, override.DatabaseURL)
	c.Assert(cfg.DevURL, qt.Equals, override.DevURL)
	c.Assert(cfg.SchemaSources, qt.DeepEquals, override.SchemaSources)
	c.Assert(cfg.Schemas, qt.DeepEquals, override.Schemas)
	c.Assert(cfg.Exclude, qt.DeepEquals, override.Exclude)
	c.Assert(cfg.Schema, qt.DeepEquals, override.Schema)
	c.Assert(cfg.Migration, qt.DeepEquals, override.Migration)
	c.Assert(cfg.OnlineDDL, qt.DeepEquals, override.OnlineDDL)
	c.Assert(cfg.Lint, qt.DeepEquals, override.Lint)
	c.Assert(cfg.Format, qt.DeepEquals, override.Format)
	c.Assert(cfg.Diff, qt.DeepEquals, override.Diff)
	c.Assert(cfg.ExternalSchema, qt.DeepEquals, override.ExternalSchema)
}

func TestMergeClonesBaseLintLatest(t *testing.T) {
	c := qt.New(t)
	latest := 3
	base := projectconfig.Config{
		Lint: projectconfig.LintConfig{
			Latest: &latest,
		},
	}

	merged := projectconfig.Merge(base, projectconfig.Config{})
	*base.Lint.Latest = 9

	c.Assert(merged.Lint.Latest, qt.IsNotNil)
	c.Assert(*merged.Lint.Latest, qt.Equals, 3)
}

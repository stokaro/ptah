package projectconfig_test

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"github.com/stokaro/ptah/config/projectconfig"
)

type atlasProjectConfigGolden struct {
	EnvName       string                     `json:"env_name"`
	DatabaseURL   string                     `json:"database_url"`
	DevURL        string                     `json:"dev_url"`
	SchemaSources []string                   `json:"schema_sources"`
	Exclude       []string                   `json:"exclude"`
	Migration     atlasMigrationConfigGolden `json:"migration"`
	Lint          atlasLintConfigGolden      `json:"lint"`
}

type atlasMigrationConfigGolden struct {
	Dir             string `json:"dir"`
	Format          string `json:"format"`
	RevisionFormat  string `json:"revision_format"`
	RevisionsSchema string `json:"revisions_schema"`
	LockTimeout     string `json:"lock_timeout"`
	ExecOrder       string `json:"exec_order"`
	TxMode          string `json:"tx_mode"`
}

type atlasLintConfigGolden struct {
	Latest *int `json:"latest"`
}

func TestParseAtlasProjectConfig(t *testing.T) {
	c := qt.New(t)
	raw := []byte(`env "local" {
  url = "postgres://app@localhost:5432/app?sslmode=disable"
  dev = "docker://postgres/16/dev"
  src = ["file://schema.hcl", "schema.sql"]
  exclude = ["tmp_*"]
  migration {
    dir              = "file://migrations"
    format           = "atlas"
    revisions_schema = "atlas"
    lock_timeout     = "3s"
    exec_order       = "linear"
    tx_mode          = "none"
  }
  lint {
    latest = 5
  }
}
`)

	cfg, err := projectconfig.ParseAtlas(raw, "atlas.hcl", "")

	c.Assert(err, qt.IsNil)
	c.Assert(cfg.EnvName, qt.Equals, "local")
	c.Assert(cfg.DatabaseURL, qt.Equals, "postgres://app@localhost:5432/app?sslmode=disable")
	c.Assert(cfg.DevURL, qt.Equals, "docker://postgres/16/dev")
	c.Assert(cfg.SchemaSources, qt.DeepEquals, []string{"file://schema.hcl", "schema.sql"})
	c.Assert(cfg.Exclude, qt.DeepEquals, []string{"tmp_*"})
	c.Assert(cfg.Migration.Dir, qt.Equals, "migrations")
	c.Assert(cfg.Migration.Format, qt.Equals, "atlas")
	c.Assert(cfg.Migration.RevisionsSchema, qt.Equals, "atlas")
	c.Assert(cfg.Migration.RevisionFormat, qt.Equals, "atlas")
	c.Assert(cfg.Migration.LockTimeout, qt.Equals, "3s")
	c.Assert(cfg.Migration.ExecOrder, qt.Equals, "linear")
	c.Assert(cfg.Migration.TxMode, qt.Equals, "none")
	c.Assert(cfg.Lint.Latest, qt.IsNotNil)
	c.Assert(*cfg.Lint.Latest, qt.Equals, 5)
}

func TestParseAtlasProjectConfigGolden_HappyPath(t *testing.T) {
	c := qt.New(t)
	tests := []struct {
		name    string
		input   string
		golden  string
		envName string
	}{
		{
			name:    "complete local environment",
			input:   "complete.hcl",
			golden:  "complete.golden.json",
			envName: "",
		},
		{
			name:    "selected production environment",
			input:   "multiple-envs.hcl",
			golden:  "production.golden.json",
			envName: "production",
		},
	}

	for _, tt := range tests {
		c.Run(tt.name, func(c *qt.C) {
			raw := readAtlasProjectConfigFixture(c, tt.input)

			cfg, err := projectconfig.ParseAtlas(raw, "atlas.hcl", tt.envName)
			c.Assert(err, qt.IsNil)

			got, err := json.MarshalIndent(newAtlasProjectConfigGolden(cfg), "", "  ")
			c.Assert(err, qt.IsNil)
			got = append(got, '\n')

			want := readAtlasProjectConfigFixture(c, tt.golden)
			c.Assert(string(got), qt.Equals, string(want))
		})
	}
}

func TestParseAtlasProjectConfigGolden_FailurePath(t *testing.T) {
	c := qt.New(t)
	tests := []struct {
		name    string
		input   string
		wantErr string
	}{
		{
			name:    "Atlas Cloud block",
			input:   "unsupported-cloud.hcl",
			wantErr: `unsupported atlas\.hcl construct "atlas" at atlas\.hcl:1`,
		},
		{
			name:    "unknown environment attribute",
			input:   "unsupported-attribute.hcl",
			wantErr: `unsupported atlas\.hcl construct "project" at atlas\.hcl:2`,
		},
	}

	for _, tt := range tests {
		c.Run(tt.name, func(c *qt.C) {
			raw := readAtlasProjectConfigFixture(c, tt.input)

			_, err := projectconfig.ParseAtlas(raw, "atlas.hcl", "")
			c.Assert(err, qt.ErrorMatches, tt.wantErr)
		})
	}
}

func readAtlasProjectConfigFixture(c *qt.C, name string) []byte {
	c.Helper()

	data, err := os.ReadFile(filepath.Join("testdata", "atlas", name))
	c.Assert(err, qt.IsNil)
	return data
}

func newAtlasProjectConfigGolden(cfg projectconfig.Config) atlasProjectConfigGolden {
	return atlasProjectConfigGolden{
		EnvName:       cfg.EnvName,
		DatabaseURL:   cfg.DatabaseURL,
		DevURL:        cfg.DevURL,
		SchemaSources: cfg.SchemaSources,
		Exclude:       cfg.Exclude,
		Migration: atlasMigrationConfigGolden{
			Dir:             cfg.Migration.Dir,
			Format:          cfg.Migration.Format,
			RevisionFormat:  cfg.Migration.RevisionFormat,
			RevisionsSchema: cfg.Migration.RevisionsSchema,
			LockTimeout:     cfg.Migration.LockTimeout,
			ExecOrder:       cfg.Migration.ExecOrder,
			TxMode:          cfg.Migration.TxMode,
		},
		Lint: atlasLintConfigGolden{
			Latest: cfg.Lint.Latest,
		},
	}
}

func TestParseAtlasProjectConfigPreservesMigrationDirURLSemantics(t *testing.T) {
	c := qt.New(t)
	tests := []struct {
		name string
		dir  string
		want string
	}{
		{
			name: "file URL query",
			dir:  "file://migrations?format=atlas",
			want: "file://migrations?format=atlas",
		},
		{
			name: "encoded file URL path",
			dir:  "file://migration%20files",
			want: "file://migration%20files",
		},
		{
			name: "plain query-like path",
			dir:  "migrations?format=atlas",
			want: "migrations?format=atlas",
		},
	}

	for _, tt := range tests {
		c.Run(tt.name, func(c *qt.C) {
			raw := fmt.Appendf(nil, `env "local" {
  migration {
    dir = %q
  }
}
`, tt.dir)

			cfg, err := projectconfig.ParseAtlas(raw, "atlas.hcl", "local")

			c.Assert(err, qt.IsNil)
			c.Assert(cfg.Migration.Dir, qt.Equals, tt.want)
		})
	}
}

func TestParseAtlasProjectConfigMigrationEnumIdentifiers(t *testing.T) {
	c := qt.New(t)
	tests := []struct {
		name          string
		format        string
		execOrder     string
		wantFormat    string
		wantExecOrder string
	}{
		{
			name:          "atlas linear",
			format:        "atlas",
			execOrder:     "LINEAR",
			wantFormat:    "atlas",
			wantExecOrder: "linear",
		},
		{
			name:          "golang migrate linear skip",
			format:        "golang-migrate",
			execOrder:     "LINEAR_SKIP",
			wantFormat:    "golang-migrate",
			wantExecOrder: "linear-skip",
		},
		{
			name:          "goose non linear",
			format:        "goose",
			execOrder:     "NON_LINEAR",
			wantFormat:    "goose",
			wantExecOrder: "non-linear",
		},
		{
			name:          "flyway",
			format:        "flyway",
			execOrder:     "LINEAR",
			wantFormat:    "flyway",
			wantExecOrder: "linear",
		},
		{
			name:          "liquibase",
			format:        "liquibase",
			execOrder:     "LINEAR",
			wantFormat:    "liquibase",
			wantExecOrder: "linear",
		},
		{
			name:          "dbmate",
			format:        "dbmate",
			execOrder:     "LINEAR",
			wantFormat:    "dbmate",
			wantExecOrder: "linear",
		},
	}

	for _, tt := range tests {
		c.Run(tt.name, func(c *qt.C) {
			raw := fmt.Appendf(nil, `env "local" {
  migration {
    dir        = "file://migrations"
    format     = %s
    exec_order = %s
    tx_mode    = "file"
  }
}
`, tt.format, tt.execOrder)

			cfg, err := projectconfig.ParseAtlas(raw, "atlas.hcl", "local")

			c.Assert(err, qt.IsNil)
			c.Assert(cfg.Migration.Dir, qt.Equals, "migrations")
			c.Assert(cfg.Migration.Format, qt.Equals, tt.wantFormat)
			c.Assert(cfg.Migration.ExecOrder, qt.Equals, tt.wantExecOrder)
			c.Assert(cfg.Migration.TxMode, qt.Equals, "file")
		})
	}
}

func TestParseAtlasProjectConfigMigrationEnumIdentifiersFailurePath(t *testing.T) {
	c := qt.New(t)
	tests := []struct {
		name    string
		attr    string
		wantErr string
	}{
		{
			name:    "unknown format",
			attr:    "format = atals",
			wantErr: `unsupported atlas\.hcl construct "format" at atlas\.hcl:3`,
		},
		{
			name:    "unknown execution order",
			attr:    "exec_order = LINER",
			wantErr: `unsupported atlas\.hcl construct "exec_order" at atlas\.hcl:3`,
		},
		{
			name:    "bare transaction mode",
			attr:    "tx_mode = file",
			wantErr: `unsupported atlas\.hcl construct "tx_mode" at atlas\.hcl:3`,
		},
		{
			name:    "enum traversal",
			attr:    "format = local.format",
			wantErr: `unsupported atlas\.hcl construct "format" at atlas\.hcl:3`,
		},
		{
			name:    "uppercase format identifier",
			attr:    "format = ATLAS",
			wantErr: `unsupported atlas\.hcl construct "format" at atlas\.hcl:3`,
		},
		{
			name:    "lowercase linear identifier",
			attr:    "exec_order = linear",
			wantErr: `unsupported atlas\.hcl construct "exec_order" at atlas\.hcl:3`,
		},
		{
			name:    "lowercase linear skip identifier",
			attr:    "exec_order = linear_skip",
			wantErr: `unsupported atlas\.hcl construct "exec_order" at atlas\.hcl:3`,
		},
	}

	for _, tt := range tests {
		c.Run(tt.name, func(c *qt.C) {
			raw := fmt.Appendf(nil, `env "local" {
  migration {
    %s
  }
}
`, tt.attr)

			_, err := projectconfig.ParseAtlas(raw, "atlas.hcl", "local")

			c.Assert(err, qt.ErrorMatches, tt.wantErr)
		})
	}
}

func TestParseAtlasProjectConfigEnvLintGit(t *testing.T) {
	c := qt.New(t)
	raw := []byte(`env "ci" {
  lint {
    git {
      base = "master"
      dir  = "."
    }
  }
}
`)

	cfg, err := projectconfig.ParseAtlas(raw, "atlas.hcl", "ci")

	c.Assert(err, qt.IsNil)
	c.Assert(cfg.Lint.GitBase, qt.Equals, "master")
	c.Assert(cfg.Lint.GitDir, qt.Equals, ".")
}

func TestParseAtlasProjectConfigGlobalLint(t *testing.T) {
	c := qt.New(t)
	raw := []byte(`lint {
  git {
    base = "origin/master"
    dir  = "repo"
  }
}
`)

	cfg, err := projectconfig.ParseAtlas(raw, "atlas.hcl", "")

	c.Assert(err, qt.IsNil)
	c.Assert(cfg.Lint.GitBase, qt.Equals, "origin/master")
	c.Assert(cfg.Lint.GitDir, qt.Equals, "repo")
}

func TestParseAtlasProjectConfigLintPolicyBlocks(t *testing.T) {
	c := qt.New(t)
	raw := []byte(`lint {
  destructive {
    error = false
  }
  concurrent_index {
    error = true
  }
  data_depend {
    error = false
  }
  incompatible {
    error = true
  }
  nestedtx {
    error = true
  }
}
env "ci" {
  lint {
    destructive {
      error = true
    }
  }
}
`)

	cfg, err := projectconfig.ParseAtlas(raw, "atlas.hcl", "ci")

	c.Assert(err, qt.IsNil)
	c.Assert(cfg.Lint.RuleConfigs["DS"].Severity, qt.Equals, "error")
	c.Assert(cfg.Lint.RuleConfigs["PG101"].Severity, qt.Equals, "error")
	c.Assert(cfg.Lint.RuleConfigs["PG103"].Severity, qt.Equals, "error")
	c.Assert(cfg.Lint.RuleConfigs["DD"].Severity, qt.Equals, "warning")
	c.Assert(cfg.Lint.RuleConfigs["BC"].Severity, qt.Equals, "error")
	c.Assert(cfg.Lint.RuleConfigs["TX201"].Severity, qt.Equals, "error")
}

func TestParseAtlasProjectConfigEnvOnlyLintPolicyBlocks(t *testing.T) {
	c := qt.New(t)
	raw := []byte(`env "ci" {
  lint {
    destructive {
      error = false
    }
    concurrent_index {
      error = true
    }
    data_depend {
      error = false
    }
    incompatible {
      error = true
    }
    nestedtx {
      error = true
    }
  }
}
`)

	cfg, err := projectconfig.ParseAtlas(raw, "atlas.hcl", "ci")

	c.Assert(err, qt.IsNil)
	c.Assert(cfg.Lint.RuleConfigs["DS"].Severity, qt.Equals, "warning")
	c.Assert(cfg.Lint.RuleConfigs["PG101"].Severity, qt.Equals, "error")
	c.Assert(cfg.Lint.RuleConfigs["PG103"].Severity, qt.Equals, "error")
	c.Assert(cfg.Lint.RuleConfigs["DD"].Severity, qt.Equals, "warning")
	c.Assert(cfg.Lint.RuleConfigs["BC"].Severity, qt.Equals, "error")
	c.Assert(cfg.Lint.RuleConfigs["TX201"].Severity, qt.Equals, "error")
}

func TestParseAtlasProjectConfigEnvSchemaFormatAndDiffBlocks(t *testing.T) {
	c := qt.New(t)
	raw := []byte(`env "local" {
  schema {
    src = ["file://schema.hcl"]
    mode {
      funcs       = false
      objects     = false
      tables      = false
      triggers    = false
      types       = false
      views       = false
      roles       = true
      permissions = true
      sensitive   = DENY
    }
  }
  format {
    schema {
      apply   = "{{ sql . }}"
      clean   = "{{ json . }}"
      diff    = "{{ len .Changes }}"
      inspect = "json"
    }
    migrate {
      apply  = "{{ json . }}"
      diff   = format("{{ json . | json_merge %q }}", jsonencode({ EnvName = "local" }))
      lint   = "{{ json .Files }}"
      status = "{{ json .Pending }}"
    }
  }
  diff {
    skip {
      drop_table = true
    }
    concurrent_index {
      create = true
      drop   = false
    }
  }
}
`)

	cfg, err := projectconfig.ParseAtlas(raw, "atlas.hcl", "local")

	c.Assert(err, qt.IsNil)
	c.Assert(cfg.SchemaSources, qt.DeepEquals, []string{"file://schema.hcl"})
	c.Assert(cfg.Schema.Mode.Tables, qt.DeepEquals, projectconfig.ConfigBool{Value: false, Set: true})
	c.Assert(cfg.Schema.Mode.Funcs, qt.DeepEquals, projectconfig.ConfigBool{Value: false, Set: true})
	c.Assert(cfg.Schema.Mode.Objects, qt.DeepEquals, projectconfig.ConfigBool{Value: false, Set: true})
	c.Assert(cfg.Schema.Mode.Triggers, qt.DeepEquals, projectconfig.ConfigBool{Value: false, Set: true})
	c.Assert(cfg.Schema.Mode.Types, qt.DeepEquals, projectconfig.ConfigBool{Value: false, Set: true})
	c.Assert(cfg.Schema.Mode.Views, qt.DeepEquals, projectconfig.ConfigBool{Value: false, Set: true})
	c.Assert(cfg.Schema.Mode.Roles, qt.DeepEquals, projectconfig.ConfigBool{Value: true, Set: true})
	c.Assert(cfg.Schema.Mode.Permissions, qt.DeepEquals, projectconfig.ConfigBool{Value: true, Set: true})
	c.Assert(cfg.Schema.Mode.ExcludePatterns(), qt.DeepEquals, []string{
		"*[type=table]",
		"*[type=view|materialized_view]",
		"*[type=trigger]",
		"*[type=function]",
		"*[type=enum]",
		"*[type=extension]",
	})
	c.Assert(cfg.Format.Schema.Apply, qt.Equals, "{{ sql . }}")
	c.Assert(cfg.Format.Schema.Clean, qt.Equals, "{{ json . }}")
	c.Assert(cfg.Format.Schema.Diff, qt.Equals, "{{ len .Changes }}")
	c.Assert(cfg.Format.Schema.Inspect, qt.Equals, "json")
	c.Assert(cfg.Format.Migrate.Apply, qt.Equals, "{{ json . }}")
	c.Assert(cfg.Format.Migrate.Diff, qt.Equals, `{{ json . | json_merge "{\"EnvName\":\"local\"}" }}`)
	c.Assert(cfg.Format.Migrate.Lint, qt.Equals, "{{ json .Files }}")
	c.Assert(cfg.Format.Migrate.Status, qt.Equals, "{{ json .Pending }}")
	c.Assert(cfg.Diff.Skip.DropTable, qt.DeepEquals, projectconfig.ConfigBool{Value: true, Set: true})
	c.Assert(cfg.Diff.ConcurrentIndex.Create, qt.DeepEquals, projectconfig.ConfigBool{Value: true, Set: true})
	c.Assert(cfg.Diff.ConcurrentIndex.Drop, qt.DeepEquals, projectconfig.ConfigBool{Value: false, Set: true})
}

func TestParseAtlasProjectConfigEnvDiffOverridesGlobalDiff(t *testing.T) {
	c := qt.New(t)
	raw := []byte(`diff {
  skip {
    drop_table = true
  }
  concurrent_index {
    create = true
  }
}
env "local" {
  diff {
    skip {
      drop_table = false
    }
  }
}
`)

	cfg, err := projectconfig.ParseAtlas(raw, "atlas.hcl", "local")

	c.Assert(err, qt.IsNil)
	c.Assert(cfg.Diff.Skip.DropTable, qt.DeepEquals, projectconfig.ConfigBool{Value: false, Set: true})
	c.Assert(cfg.Diff.ConcurrentIndex.Create, qt.DeepEquals, projectconfig.ConfigBool{Value: true, Set: true})
}

func TestParseAtlasProjectConfigEnvInheritsGlobalLint(t *testing.T) {
	c := qt.New(t)
	raw := []byte(`lint {
  latest = 2
}
env "ci" {
  url = "sqlite://app.db"
}
`)

	cfg, err := projectconfig.ParseAtlas(raw, "atlas.hcl", "ci")

	c.Assert(err, qt.IsNil)
	c.Assert(cfg.EnvName, qt.Equals, "ci")
	c.Assert(cfg.Lint.Latest, qt.IsNotNil)
	c.Assert(*cfg.Lint.Latest, qt.Equals, 2)
	c.Assert(cfg.DatabaseURL, qt.Equals, "sqlite://app.db")
}

func TestParseAtlasProjectConfigEnvLintGitOverridesGlobalLatest(t *testing.T) {
	c := qt.New(t)
	raw := []byte(`lint {
  latest = 2
}
env "ci" {
  lint {
    git {
      base = "main"
    }
  }
}
`)

	cfg, err := projectconfig.ParseAtlas(raw, "atlas.hcl", "ci")

	c.Assert(err, qt.IsNil)
	c.Assert(cfg.Lint.Latest, qt.IsNil)
	c.Assert(cfg.Lint.GitBase, qt.Equals, "main")
}

func TestParseAtlasProjectConfigEnvLintLatestOverridesGlobalGit(t *testing.T) {
	c := qt.New(t)
	raw := []byte(`lint {
  git {
    base = "main"
    dir  = "."
  }
}
env "ci" {
  lint {
    latest = 2
  }
}
`)

	cfg, err := projectconfig.ParseAtlas(raw, "atlas.hcl", "ci")

	c.Assert(err, qt.IsNil)
	c.Assert(cfg.Lint.Latest, qt.IsNotNil)
	c.Assert(*cfg.Lint.Latest, qt.Equals, 2)
	c.Assert(cfg.Lint.GitBase, qt.Equals, "")
	c.Assert(cfg.Lint.GitDir, qt.Equals, "")
}

func TestParseAtlasProjectConfigAcceptsSingleSource(t *testing.T) {
	c := qt.New(t)
	raw := []byte(`env "local" {
  src = "file://schema.hcl"
}
`)

	cfg, err := projectconfig.ParseAtlas(raw, "atlas.hcl", "")

	c.Assert(err, qt.IsNil)
	c.Assert(cfg.SchemaSources, qt.DeepEquals, []string{"file://schema.hcl"})
}

func TestParseAtlasProjectConfigEvaluatesVariablesLocalsAndFunctions(t *testing.T) {
	c := qt.New(t)
	t.Setenv("PTAH_TEST_DATABASE_URL", "sqlite://env.db")
	raw := []byte(`variable "schema_name" {
  description = "Schema file stem."
  default     = "app"
}

locals {
  schema_source = "file://${var.schema_name}.hcl"
  dev_url       = local.z_dev_url
  z_dev_url     = "${getenv("PTAH_TEST_DATABASE_URL")}?mode=dev"
}

env "local" {
  url = getenv("PTAH_TEST_DATABASE_URL")
  dev = local.dev_url
  src = local.schema_source
  lint {
    latest = 3
  }
}
`)

	cfg, err := projectconfig.ParseAtlas(raw, "atlas.hcl", "local")

	c.Assert(err, qt.IsNil)
	c.Assert(cfg.DatabaseURL, qt.Equals, "sqlite://env.db")
	c.Assert(cfg.DevURL, qt.Equals, "sqlite://env.db?mode=dev")
	c.Assert(cfg.SchemaSources, qt.DeepEquals, []string{"file://app.hcl"})
	c.Assert(cfg.Lint.Latest, qt.IsNotNil)
	c.Assert(*cfg.Lint.Latest, qt.Equals, 3)
}

func TestParseAtlasProjectConfigVariableOverrideWinsOverDefault(t *testing.T) {
	c := qt.New(t)
	raw := []byte(`variable "schema_name" {
  default = "app"
}

env "local" {
  src = "file://${var.schema_name}.hcl"
}
`)

	cfg, err := projectconfig.ParseAtlasWithOptions(raw, "atlas.hcl", projectconfig.AtlasLoadOptions{
		EnvName: "local",
		Vars:    []string{"schema_name=tenant"},
	})

	c.Assert(err, qt.IsNil)
	c.Assert(cfg.SchemaSources, qt.DeepEquals, []string{"file://tenant.hcl"})
}

func TestParseAtlasProjectConfigRepeatedVariableOverrideBecomesList(t *testing.T) {
	c := qt.New(t)
	raw := []byte(`variable "schema" {}

env "local" {
  src = var.schema
}
`)

	cfg, err := projectconfig.ParseAtlasWithOptions(raw, "atlas.hcl", projectconfig.AtlasLoadOptions{
		EnvName: "local",
		Vars: []string{
			"schema=file://a.hcl",
			"schema=file://b.hcl",
		},
	})

	c.Assert(err, qt.IsNil)
	c.Assert(cfg.SchemaSources, qt.DeepEquals, []string{"file://a.hcl", "file://b.hcl"})
}

func TestParseAtlasProjectConfigRejectsMalformedVariableOverride(t *testing.T) {
	c := qt.New(t)

	_, err := projectconfig.ParseAtlasWithOptions([]byte(`variable "schema" {}
`), "atlas.hcl", projectconfig.AtlasLoadOptions{Vars: []string{"schema"}})

	c.Assert(err, qt.ErrorMatches, `atlas variable overrides must use name=value, got "schema"`)
}

func TestLoadAtlasProjectConfigEvaluatesFileFunction(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	atlasPath := filepath.Join(dir, "atlas.hcl")
	c.Assert(os.WriteFile(filepath.Join(dir, "database-url.txt"), []byte(`sqlite://file-function.db`), 0o600), qt.IsNil)
	c.Assert(os.WriteFile(atlasPath, []byte(`locals {
  database_url = file("database-url.txt")
}

env "local" {
  url = local.database_url
}
`), 0o600), qt.IsNil)

	cfg, err := projectconfig.LoadAtlasFile(atlasPath, "local")

	c.Assert(err, qt.IsNil)
	c.Assert(cfg.DatabaseURL, qt.Equals, "sqlite://file-function.db")
}

func TestLoadAtlasProjectConfigEvaluatesHCLSchemaDataSourcePath(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	atlasPath := filepath.Join(dir, "atlas.hcl")
	c.Assert(os.WriteFile(filepath.Join(dir, "schema.hcl"), []byte(`schema "main" {}`), 0o600), qt.IsNil)
	c.Assert(os.WriteFile(atlasPath, []byte(`data "hcl_schema" "app" {
  path = "schema.hcl"
}

env "local" {
  src = data.hcl_schema.app.url
}
`), 0o600), qt.IsNil)

	cfg, err := projectconfig.LoadAtlasFile(atlasPath, "local")

	c.Assert(err, qt.IsNil)
	c.Assert(cfg.SchemaSources, qt.DeepEquals, []string{"file://schema.hcl"})
}

func TestLoadAtlasProjectConfigEvaluatesFilesetHCLSchemaDataSource(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	schemaDir := filepath.Join(dir, "schema")
	c.Assert(os.Mkdir(schemaDir, 0o700), qt.IsNil)
	c.Assert(os.Mkdir(filepath.Join(schemaDir, "nested"), 0o700), qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(schemaDir, "b.hcl"), []byte(`schema "main" {}`), 0o600), qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(schemaDir, "a.hcl"), []byte(`schema "main" {}`), 0o600), qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(schemaDir, "nested", "c.hcl"), []byte(`schema "main" {}`), 0o600), qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(dir, "ignored.sql"), []byte(`CREATE TABLE ignored (id int);`), 0o600), qt.IsNil)
	atlasPath := filepath.Join(dir, "atlas.hcl")
	c.Assert(os.WriteFile(atlasPath, []byte(`data "hcl_schema" "app" {
  paths = fileset("schema/**/*.hcl")
}

env "local" {
  src = data.hcl_schema.app.url
}
`), 0o600), qt.IsNil)

	cfg, err := projectconfig.LoadAtlasFile(atlasPath, "local")

	c.Assert(err, qt.IsNil)
	c.Assert(cfg.SchemaSources, qt.DeepEquals, []string{
		"file://schema/a.hcl",
		"file://schema/b.hcl",
		"file://schema/nested/c.hcl",
	})
}

func TestParseAtlasProjectConfigSelectsEnv(t *testing.T) {
	c := qt.New(t)
	raw := []byte(`env "dev" {
  url = "postgres://dev/db"
}
env "prod" {
  url = "postgres://prod/db"
}
`)

	cfg, err := projectconfig.ParseAtlas(raw, "atlas.hcl", "prod")

	c.Assert(err, qt.IsNil)
	c.Assert(cfg.EnvName, qt.Equals, "prod")
	c.Assert(cfg.DatabaseURL, qt.Equals, "postgres://prod/db")

	_, err = projectconfig.ParseAtlas(raw, "atlas.hcl", "")
	c.Assert(err, qt.ErrorMatches, `atlas\.hcl contains multiple env blocks; pass --env`)
}

func TestParseAtlasProjectConfigSkipsUnselectedEnvEvaluation(t *testing.T) {
	c := qt.New(t)
	raw := []byte(`env "dev" {
  url = "sqlite://dev.db"
}
env "prod" {
  url = missing.value
}
`)

	cfg, err := projectconfig.ParseAtlas(raw, "atlas.hcl", "dev")

	c.Assert(err, qt.IsNil)
	c.Assert(cfg.DatabaseURL, qt.Equals, "sqlite://dev.db")

	_, err = projectconfig.ParseAtlas(raw, "atlas.hcl", "prod")
	c.Assert(err, qt.ErrorMatches, `unsupported atlas\.hcl construct "url" at atlas\.hcl:5`)
}

func TestLoadAtlasProjectConfigEmptyEnvURLOverridesPtahFallback(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	atlasPath := filepath.Join(dir, "atlas.hcl")
	ptahPath := filepath.Join(dir, "ptah.yaml")
	c.Assert(os.WriteFile(ptahPath, []byte(`url: sqlite://ptah-fallback.db
`), 0o600), qt.IsNil)
	c.Assert(os.WriteFile(atlasPath, []byte(`env "local" {
  url = getenv("PTAH_TEST_UNSET_DATABASE_URL")
}
`), 0o600), qt.IsNil)

	cfg, err := projectconfig.Load(projectconfig.LoadOptions{
		PtahPath:  ptahPath,
		AtlasPath: atlasPath,
		EnvName:   "local",
	})

	c.Assert(err, qt.IsNil)
	c.Assert(cfg.DatabaseURL, qt.Equals, "")
}

func TestParseAtlasProjectConfigUsesSingleUnlabeledEnv(t *testing.T) {
	c := qt.New(t)
	raw := []byte(`env {
  url = "postgres://default/db"
  migration {
    dir = "file://migrations"
  }
}
`)

	cfg, err := projectconfig.ParseAtlas(raw, "atlas.hcl", "")

	c.Assert(err, qt.IsNil)
	c.Assert(cfg.EnvName, qt.Equals, "")
	c.Assert(cfg.DatabaseURL, qt.Equals, "postgres://default/db")
	c.Assert(cfg.Migration.Dir, qt.Equals, "migrations")
	c.Assert(cfg.Migration.Format, qt.Equals, "atlas")
	c.Assert(cfg.Migration.RevisionFormat, qt.Equals, "atlas")
}

func TestParseAtlasProjectConfigRequiresEnvWhenMultipleBlocksExist(t *testing.T) {
	c := qt.New(t)
	raw := []byte(`env {
  url = "postgres://default/db"
}
env "prod" {
  url = "postgres://prod/db"
}
`)

	_, err := projectconfig.ParseAtlas(raw, "atlas.hcl", "")

	c.Assert(err, qt.ErrorMatches, `atlas\.hcl contains multiple env blocks; pass --env`)
}

func TestParseAtlasProjectConfigRejectsUnsupportedConstructs(t *testing.T) {
	tests := []struct {
		name string
		raw  string
		err  string
	}{
		{
			name: "schema repo block",
			raw: `env "local" {
  schema {
    repo {
      name = "app"
    }
  }
}
`,
			err: `unsupported atlas\.hcl construct "repo" at atlas\.hcl:3`,
		},
		{
			name: "schema mode sensitive allow",
			raw: `env "local" {
  schema {
    mode {
      sensitive = ALLOW
    }
  }
}
`,
			err: `unsupported atlas\.hcl construct "sensitive" at atlas\.hcl:4`,
		},
		{
			name: "diff skip drop schema",
			raw: `diff {
  skip {
    drop_schema = true
  }
}
`,
			err: `unsupported atlas\.hcl construct "drop_schema" at atlas\.hcl:3`,
		},
		{
			name: "lint format attr",
			raw: `lint {
  format = "{{ json . }}"
}
`,
			err: `unsupported atlas\.hcl construct "format" at atlas\.hcl:2`,
		},
		{
			name: "lint destructive force",
			raw: `lint {
  destructive {
    force = true
  }
}
`,
			err: `unsupported atlas\.hcl construct "force" at atlas\.hcl:3`,
		},
		{
			name: "lint destructive allow table",
			raw: `lint {
  destructive {
    allow_table {
      match = "deprecated_.+"
    }
  }
}
`,
			err: `unsupported atlas\.hcl construct "allow_table" at atlas\.hcl:3`,
		},
		{
			name: "lint duplicate destructive block",
			raw: `lint {
  destructive {
    error = true
  }
  destructive {
    error = false
  }
}
`,
			err: `unsupported atlas\.hcl construct "destructive" at atlas\.hcl:5`,
		},
		{
			name: "lint check block",
			raw: `lint {
  check "DS102" {
    error = true
  }
}
`,
			err: `unsupported atlas\.hcl construct "check" at atlas\.hcl:2`,
		},
		{
			name: "lint custom rule",
			raw: `lint {
  rule "hcl" "custom" {
    src = ["schema.rule.hcl"]
  }
}
`,
			err: `unsupported atlas\.hcl construct "rule" at atlas\.hcl:2`,
		},
		{
			name: "lint non linear block",
			raw: `lint {
  non_linear {
    error = true
  }
}
`,
			err: `unsupported atlas\.hcl construct "non_linear" at atlas\.hcl:2`,
		},
		{
			name: "lint naming block",
			raw: `lint {
  naming {
    error = true
  }
}
`,
			err: `unsupported atlas\.hcl construct "naming" at atlas\.hcl:2`,
		},
		{
			name: "lint ownership block",
			raw: `lint {
  ownership "github" {
    repo = "stokaro/ptah"
  }
}
`,
			err: `unsupported atlas\.hcl construct "ownership" at atlas\.hcl:2`,
		},
		{
			name: "lint statement block",
			raw: `lint {
  statement {
    error = true
  }
}
`,
			err: `unsupported atlas\.hcl construct "statement" at atlas\.hcl:2`,
		},
		{
			name: "lint constraint drop block",
			raw: `lint {
  condrop {
    error = true
  }
}
`,
			err: `unsupported atlas\.hcl construct "condrop" at atlas\.hcl:2`,
		},
		{
			name: "cloud block",
			raw: `atlas {
  cloud {}
}
`,
			err: `unsupported atlas\.hcl construct "atlas" at atlas\.hcl:1`,
		},
		{
			name: "unsupported data source",
			raw: `data "external" "app" {
  program = ["echo", "{}"]
}
`,
			err: `unsupported atlas\.hcl construct "data.external" at atlas\.hcl:1`,
		},
		{
			name: "unsupported hcl schema data attribute",
			raw: `data "hcl_schema" "app" {
  path  = "schema.hcl"
  query = "table.users"
}
`,
			err: `unsupported atlas\.hcl construct "query" at atlas\.hcl:3`,
		},
		{
			name: "variable without default",
			raw: `variable "url" {}
env "local" {
  url = var.url
}
`,
			err: `atlas\.hcl variable "url" requires a default or --var url=value`,
		},
		{
			name: "variable type is unsupported",
			raw: `variable "url" {
  type    = string
  default = "sqlite://typed.db"
}
`,
			err: `unsupported atlas\.hcl construct "type" at atlas\.hcl:2`,
		},
		{
			name: "variable sensitive is unsupported",
			raw: `variable "url" {
  sensitive = true
  default   = "sqlite://typed.db"
}
`,
			err: `unsupported atlas\.hcl construct "sensitive" at atlas\.hcl:2`,
		},
		{
			name: "duplicate local",
			raw: `locals {
  url = "sqlite://first.db"
}
locals {
  url = "sqlite://second.db"
}
`,
			err: `duplicate atlas\.hcl local "url" at atlas\.hcl:5`,
		},
		{
			name: "file function rejects parent traversal",
			raw: `env "local" {
  url = file("../secret.txt")
}
`,
			err: `unsupported atlas\.hcl construct "url" at atlas\.hcl:2`,
		},
		{
			name: "file function rejects absolute paths",
			raw: `env "local" {
  url = file("/tmp/secret.txt")
}
`,
			err: `unsupported atlas\.hcl construct "url" at atlas\.hcl:2`,
		},
		{
			name: "hcl schema data source rejects remote path",
			raw: `data "hcl_schema" "app" {
  path = "https://example.com/schema.hcl"
}
`,
			err: `unsupported atlas\.hcl construct "path" at atlas\.hcl:2`,
		},
		{
			name: "fileset rejects parent traversal",
			raw: `data "hcl_schema" "app" {
  paths = fileset("../*.hcl")
}
`,
			err: `unsupported atlas\.hcl construct "paths" at atlas\.hcl:2`,
		},
		{
			name: "unknown migration attribute",
			raw: `env "local" {
  migration {
    remote_dir = "atlas://example"
  }
}
`,
			err: `unsupported atlas\.hcl construct "remote_dir" at atlas\.hcl:3`,
		},
		{
			name: "duplicate migration block",
			raw: `env "local" {
  migration {}
  migration {}
}
`,
			err: `unsupported atlas\.hcl construct "migration" at atlas\.hcl:3`,
		},
		{
			name: "exclude object",
			raw: `env "local" {
  exclude = { tmp = "tmp_*" }
}
`,
			err: `unsupported atlas\.hcl construct "exclude" at atlas\.hcl:2`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)

			_, err := projectconfig.ParseAtlas([]byte(tt.raw), "atlas.hcl", "")

			c.Assert(err, qt.ErrorMatches, tt.err)
		})
	}
}

func TestLoadMergesAtlasOverPtah(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	ptahPath := filepath.Join(dir, "ptah.yaml")
	atlasPath := filepath.Join(dir, "atlas.hcl")
	c.Assert(os.WriteFile(ptahPath, []byte(`url: postgres://ptah/db
exclude: [tmp_*]
migration:
  dir: ./ptah-migrations
  revisions_schema: revisions
  lock_timeout: 1s
  exec_order: non-linear
`), 0o600), qt.IsNil)
	c.Assert(os.WriteFile(atlasPath, []byte(`env "local" {
  url = "postgres://atlas/db"
  src = []
  exclude = []
  migration {
    dir              = "file://atlas-migrations"
    revisions_schema = ""
    lock_timeout     = ""
  }
}
`), 0o600), qt.IsNil)

	cfg, err := projectconfig.Load(projectconfig.LoadOptions{
		PtahPath:  ptahPath,
		AtlasPath: atlasPath,
	})

	c.Assert(err, qt.IsNil)
	c.Assert(cfg.DatabaseURL, qt.Equals, "postgres://atlas/db")
	c.Assert(cfg.SchemaSources, qt.DeepEquals, []string{})
	c.Assert(cfg.Exclude, qt.DeepEquals, []string{})
	c.Assert(cfg.Migration.Dir, qt.Equals, "atlas-migrations")
	c.Assert(cfg.Migration.RevisionsSchema, qt.Equals, "")
	c.Assert(cfg.Migration.LockTimeout, qt.Equals, "")
	c.Assert(cfg.Migration.ExecOrder, qt.Equals, "non-linear")
	c.Assert(cfg.Migration.Format, qt.Equals, "atlas")
	c.Assert(cfg.Migration.RevisionFormat, qt.Equals, "atlas")
}

func TestParseAtlasProjectConfigGlobalLintLogFeedsFormat(t *testing.T) {
	c := qt.New(t)
	raw := []byte(`lint {
  latest = 1
  destructive {
    error = false
  }
  log = "{{ range .Files }}{{ println .Name }}{{ end }}"
}
`)

	cfg, err := projectconfig.ParseAtlas(raw, "atlas.hcl", "")

	c.Assert(err, qt.IsNil)
	// lint.log feeds the same format IR as format.migrate.lint.
	c.Assert(cfg.Format.Migrate.Lint, qt.Equals, "{{ range .Files }}{{ println .Name }}{{ end }}")
	// The analysis parts of the lint block remain intact alongside log.
	c.Assert(cfg.Lint.Latest, qt.IsNotNil)
	c.Assert(*cfg.Lint.Latest, qt.Equals, 1)
	c.Assert(cfg.Lint.RuleConfigs["DS"].Severity, qt.Equals, "warning")
}

func TestParseAtlasProjectConfigEnvLintLogOverridesGlobal(t *testing.T) {
	c := qt.New(t)
	raw := []byte(`lint {
  latest = 1
  log = "GLOBAL"
}
env "ci" {
  lint {
    latest = 2
    log = "{{ len .Files | println }}"
  }
}
`)

	cfg, err := projectconfig.ParseAtlas(raw, "atlas.hcl", "ci")

	c.Assert(err, qt.IsNil)
	c.Assert(cfg.Format.Migrate.Lint, qt.Equals, "{{ len .Files | println }}")
	c.Assert(cfg.Lint.Latest, qt.IsNotNil)
	c.Assert(*cfg.Lint.Latest, qt.Equals, 2)
}

func TestParseAtlasProjectConfigEnvInheritsGlobalLintLog(t *testing.T) {
	c := qt.New(t)
	raw := []byte(`lint {
  log = "GLOBAL"
}
env "ci" {
  lint {
    latest = 3
  }
}
`)

	cfg, err := projectconfig.ParseAtlas(raw, "atlas.hcl", "ci")

	c.Assert(err, qt.IsNil)
	c.Assert(cfg.Format.Migrate.Lint, qt.Equals, "GLOBAL")
	c.Assert(cfg.Lint.Latest, qt.IsNotNil)
	c.Assert(*cfg.Lint.Latest, qt.Equals, 3)
}

func TestParseAtlasProjectConfigLintLogRejectsEmpty(t *testing.T) {
	c := qt.New(t)
	raw := []byte(`lint {
  log = ""
}
`)

	_, err := projectconfig.ParseAtlas(raw, "atlas.hcl", "")

	c.Assert(err, qt.IsNotNil)
}

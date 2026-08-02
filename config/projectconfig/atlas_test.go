package projectconfig_test

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"testing/fstest"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/config/projectconfig"
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

// The unsupported-attribute fixture is no longer a failure path: `project` is a
// name the community binary parses and ignores, so refusing it was stricter
// than Atlas. The fixture is kept and asserted from the tolerant side.
func TestParseAtlasProjectConfigGoldenUnknownAttributeIsIgnored(t *testing.T) {
	c := qt.New(t)
	raw := readAtlasProjectConfigFixture(c, "unsupported-attribute.hcl")

	cfg, err := projectconfig.ParseAtlas(raw, "atlas.hcl", "")

	c.Assert(err, qt.IsNil)
	names := make([]string, 0, len(cfg.IgnoredConstructs))
	for _, ignored := range cfg.IgnoredConstructs {
		names = append(names, ignored.Name)
	}
	c.Assert(names, qt.Contains, "project")
}

func TestParseAtlasProjectConfigGolden_FailurePath(t *testing.T) {
	c := qt.New(t)
	tests := []struct {
		name    string
		input   string
		wantErr string
	}{
		{
			name:    "hosted service block",
			input:   "unsupported-cloud.hcl",
			wantErr: `atlas block is not supported by the community version of Atlas`,
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
			wantErr: `atlas\.hcl "format" at atlas\.hcl:3 must be one of .*`,
		},
		{
			name:    "unknown execution order",
			attr:    "exec_order = LINER",
			wantErr: `atlas\.hcl "exec_order" at atlas\.hcl:3 must be one of .*`,
		},
		{
			name:    "bare transaction mode",
			attr:    "tx_mode = file",
			wantErr: `cannot evaluate atlas\.hcl "tx_mode" at atlas\.hcl:3: .*Unknown variable.*`,
		},
		{
			name:    "enum traversal",
			attr:    "format = local.format",
			wantErr: `atlas\.hcl "format" at atlas\.hcl:3 must be one of .*`,
		},
		{
			name:    "uppercase format identifier",
			attr:    "format = ATLAS",
			wantErr: `atlas\.hcl "format" at atlas\.hcl:3 must be one of .*`,
		},
		{
			name:    "lowercase linear identifier",
			attr:    "exec_order = linear",
			wantErr: `atlas\.hcl "exec_order" at atlas\.hcl:3 must be one of .*`,
		},
		{
			name:    "lowercase linear skip identifier",
			attr:    "exec_order = linear_skip",
			wantErr: `atlas\.hcl "exec_order" at atlas\.hcl:3 must be one of .*`,
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

// TestParseAtlasProjectConfigOverrideSkipsDefaultEvaluation pins the lazy
// default: a default expression that cannot evaluate in this environment (for
// example file() on a machine-specific path) must not fail an invocation that
// overrides the variable.
func TestParseAtlasProjectConfigOverrideSkipsDefaultEvaluation(t *testing.T) {
	c := qt.New(t)
	raw := []byte(`variable "schema" {
  default = file("missing.txt")
}

env "local" {
  src = var.schema
}
`)

	cfg, err := projectconfig.ParseAtlasWithOptions(raw, "atlas.hcl", projectconfig.AtlasLoadOptions{
		EnvName: "local",
		Vars:    []string{"schema=file://override.hcl"},
	})

	c.Assert(err, qt.IsNil)
	c.Assert(cfg.SchemaSources, qt.DeepEquals, []string{"file://override.hcl"})
}

// TestParseAtlasProjectConfigErroringDefaultWithoutOverrideFails is the other
// direction of the lazy default: without an override the erroring default
// still fails loudly.
func TestParseAtlasProjectConfigErroringDefaultWithoutOverrideFails(t *testing.T) {
	c := qt.New(t)
	raw := []byte(`variable "schema" {
  default = file("missing.txt")
}

env "local" {
  src = var.schema
}
`)

	_, err := projectconfig.ParseAtlas(raw, "atlas.hcl", "local")

	c.Assert(err, qt.ErrorMatches, `unsupported atlas\.hcl construct "default" at atlas\.hcl:2`)
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

// typedVariableConsumerHCL routes each typed variable into a config field that
// demands the declared cty type: lint.latest requires a number,
// diff.concurrent_index.create requires a bool, and src accepts a string list.
// A string that merely looked converted would fail those attribute parsers.
const typedVariableConsumerHCL = `env "local" {
  src     = var.sources
  exclude = ["${var.stem}_*"]
  lint {
    latest = var.latest
  }
  diff {
    concurrent_index {
      create = var.concurrent
    }
  }
}
`

func assertTypedVariableConfig(c *qt.C, cfg projectconfig.Config) {
	c.Helper()
	c.Assert(cfg.SchemaSources, qt.DeepEquals, []string{"file://a.hcl", "file://b.hcl"})
	c.Assert(cfg.Exclude, qt.DeepEquals, []string{"app_*"})
	c.Assert(cfg.Lint.Latest, qt.IsNotNil)
	c.Assert(*cfg.Lint.Latest, qt.Equals, 3)
	c.Assert(cfg.Diff.ConcurrentIndex.Create.Set, qt.IsTrue)
	c.Assert(cfg.Diff.ConcurrentIndex.Create.Value, qt.IsTrue)
}

func TestParseAtlasProjectConfigTypedVariableDefaults(t *testing.T) {
	c := qt.New(t)
	raw := []byte(`variable "stem" {
  type    = string
  default = "app"
}

variable "latest" {
  type    = number
  default = 3
}

variable "concurrent" {
  type    = bool
  default = true
}

variable "sources" {
  type    = list(string)
  default = ["file://a.hcl", "file://b.hcl"]
}

` + typedVariableConsumerHCL)

	cfg, err := projectconfig.ParseAtlas(raw, "atlas.hcl", "local")

	c.Assert(err, qt.IsNil)
	assertTypedVariableConfig(c, cfg)
}

func TestParseAtlasProjectConfigTypedVariableConvertsCompatibleDefault(t *testing.T) {
	c := qt.New(t)
	raw := []byte(`variable "latest" {
  type    = number
  default = "3"
}

env "local" {
  lint {
    latest = var.latest
  }
}
`)

	cfg, err := projectconfig.ParseAtlas(raw, "atlas.hcl", "local")

	c.Assert(err, qt.IsNil)
	c.Assert(cfg.Lint.Latest, qt.IsNotNil)
	c.Assert(*cfg.Lint.Latest, qt.Equals, 3)
}

func TestParseAtlasProjectConfigTypedVariableOverridesConvert(t *testing.T) {
	c := qt.New(t)
	raw := []byte(`variable "stem" {
  type = string
}

variable "latest" {
  type = number
}

variable "concurrent" {
  type = bool
}

variable "sources" {
  type = list(string)
}

` + typedVariableConsumerHCL)

	cfg, err := projectconfig.ParseAtlasWithOptions(raw, "atlas.hcl", projectconfig.AtlasLoadOptions{
		EnvName: "local",
		Vars: []string{
			"stem=app",
			"latest=3",
			"concurrent=true",
			"sources=file://a.hcl",
			"sources=file://b.hcl",
		},
	})

	c.Assert(err, qt.IsNil)
	assertTypedVariableConfig(c, cfg)
}

func TestParseAtlasProjectConfigTypedListVariableSingleOverride(t *testing.T) {
	c := qt.New(t)
	// url = jsonencode(var.sources) is shape-revealing: src accepts a bare
	// string as well as a list, so only the JSON rendering proves the single
	// override became a one-element list rather than staying a scalar.
	raw := []byte(`variable "sources" {
  type = list(string)
}

env "local" {
  src = var.sources
  url = jsonencode(var.sources)
}
`)

	cfg, err := projectconfig.ParseAtlasWithOptions(raw, "atlas.hcl", projectconfig.AtlasLoadOptions{
		EnvName: "local",
		Vars:    []string{"sources=file://only.hcl"},
	})

	c.Assert(err, qt.IsNil)
	c.Assert(cfg.SchemaSources, qt.DeepEquals, []string{"file://only.hcl"})
	c.Assert(cfg.DatabaseURL, qt.Equals, `["file://only.hcl"]`)
}

// TestParseAtlasProjectConfigTypedVariableOverrideWrongShape covers the
// override shape boundary, including the rejecting side of cty's
// bool-conversion asymmetry: "True" and "yes" are wrong-shape errors while
// "1" converts (pinned by
// TestParseAtlasProjectConfigBoolOverrideNumericString below).
func TestParseAtlasProjectConfigTypedVariableOverrideWrongShape(t *testing.T) {
	tests := []struct {
		name string
		raw  string
		vars []string
		err  string
	}{
		{
			name: "number rejects non-numeric override",
			raw: `variable "latest" {
  type = number
}
`,
			vars: []string{"latest=abc"},
			err:  `atlas\.hcl variable "latest" expects number, got --var value "abc"`,
		},
		{
			name: "bool rejects non-bool override",
			raw: `variable "concurrent" {
  type = bool
}
`,
			vars: []string{"concurrent=maybe"},
			err:  `atlas\.hcl variable "concurrent" expects bool, got --var value "maybe"`,
		},
		{
			name: "bool rejects capitalized True override",
			raw: `variable "concurrent" {
  type = bool
}
`,
			vars: []string{"concurrent=True"},
			err:  `atlas\.hcl variable "concurrent" expects bool, got --var value "True"`,
		},
		{
			name: "string rejects repeated overrides",
			raw: `variable "url" {
  type = string
}
`,
			vars: []string{"url=sqlite://a.db", "url=sqlite://b.db"},
			err:  `atlas\.hcl variable "url" expects string, got 2 --var values`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)

			_, err := projectconfig.ParseAtlasWithOptions([]byte(tt.raw), "atlas.hcl", projectconfig.AtlasLoadOptions{
				Vars: tt.vars,
			})

			c.Assert(err, qt.ErrorMatches, tt.err)
		})
	}
}

// TestParseAtlasProjectConfigBoolOverrideNumericString pins the accepted side
// of cty's bool-conversion asymmetry: "1" converts to true. Routing the value
// into diff.concurrent_index.create, which demands a real cty bool, proves
// the conversion happened rather than a string passing through.
func TestParseAtlasProjectConfigBoolOverrideNumericString(t *testing.T) {
	c := qt.New(t)
	raw := []byte(`variable "concurrent" {
  type = bool
}

env "local" {
  diff {
    concurrent_index {
      create = var.concurrent
    }
  }
}
`)

	cfg, err := projectconfig.ParseAtlasWithOptions(raw, "atlas.hcl", projectconfig.AtlasLoadOptions{
		EnvName: "local",
		Vars:    []string{"concurrent=1"},
	})

	c.Assert(err, qt.IsNil)
	c.Assert(cfg.Diff.ConcurrentIndex.Create.Set, qt.IsTrue)
	c.Assert(cfg.Diff.ConcurrentIndex.Create.Value, qt.IsTrue)
}

func TestParseAtlasProjectConfigSensitiveVariableEvaluates(t *testing.T) {
	c := qt.New(t)
	raw := []byte(`variable "url" {
  type      = string
  sensitive = true
  default   = "sqlite://secret.db"
}

env "local" {
  url = var.url
}
`)

	cfg, err := projectconfig.ParseAtlas(raw, "atlas.hcl", "local")

	c.Assert(err, qt.IsNil)
	c.Assert(cfg.DatabaseURL, qt.Equals, "sqlite://secret.db")
}

func TestParseAtlasProjectConfigSensitiveVariableRedactsOverrideError(t *testing.T) {
	c := qt.New(t)
	raw := []byte(`variable "token" {
  type      = number
  sensitive = true
}

env "local" {
  lint {
    latest = var.token
  }
}
`)

	_, err := projectconfig.ParseAtlasWithOptions(raw, "atlas.hcl", projectconfig.AtlasLoadOptions{
		EnvName: "local",
		Vars:    []string{"token=hunter2-secret"},
	})

	c.Assert(err, qt.ErrorMatches, `atlas\.hcl variable "token" expects number, got --var value \(sensitive value\)`)
	c.Assert(err.Error(), qt.Not(qt.Contains), "hunter2-secret")
}

// TestParseAtlasProjectConfigTypedVariableWithoutDefaultRequiresVar pins the
// native-binary path: the native ptah CLI has no --var flag, so a typed
// variable without a default must fail with the existing named error.
func TestParseAtlasProjectConfigTypedVariableWithoutDefaultRequiresVar(t *testing.T) {
	c := qt.New(t)
	raw := []byte(`variable "schema_file" {
  type        = string
  description = "Path to the SQL schema file used as the desired state"
}

env "dev" {
  src = "file://${var.schema_file}"
  dev = "sqlite://dev?mode=memory"
}
`)

	_, err := projectconfig.ParseAtlas(raw, "atlas.hcl", "dev")

	c.Assert(err, qt.ErrorMatches, `atlas\.hcl variable "schema_file" requires a default or --var schema_file=value`)
}

// TestLoadAtlasProjectConfigTypedVariableDefault pins the other half of the
// native-binary path: loading atlas.hcl without variable overrides must
// evaluate a typed variable through its default.
func TestLoadAtlasProjectConfigTypedVariableDefault(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	atlasPath := filepath.Join(dir, "atlas.hcl")
	c.Assert(os.WriteFile(atlasPath, []byte(`variable "schema_file" {
  type    = string
  default = "schema.sql"
}

env "dev" {
  src = "file://${var.schema_file}"
  dev = "sqlite://dev?mode=memory"
}
`), 0o600), qt.IsNil)

	cfg, err := projectconfig.LoadAtlasFile(atlasPath, "dev")

	c.Assert(err, qt.IsNil)
	c.Assert(cfg.SchemaSources, qt.DeepEquals, []string{"file://schema.sql"})
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

func TestParseAtlasFSWithOptionsEvaluatesFunctionsThroughProvidedFS(t *testing.T) {
	c := qt.New(t)
	projectFS := fstest.MapFS{
		"database-url.txt":  {Data: []byte("sqlite://rooted.db")},
		"schema/a.hcl":      {Data: []byte(`schema "main" {}`)},
		"schema/nested.hcl": {Data: []byte(`schema "main" {}`)},
	}
	raw := []byte(`locals {
  database_url = file("database-url.txt")
}

data "hcl_schema" "app" {
  paths = fileset("schema/**/*.hcl")
}

env "local" {
  url = local.database_url
  src = data.hcl_schema.app.url
}
`)

	cfg, err := projectconfig.ParseAtlasFSWithOptions(
		raw,
		"/path/that/must/not/be-read/atlas.hcl",
		projectFS,
		projectconfig.AtlasLoadOptions{EnvName: "local"},
	)

	c.Assert(err, qt.IsNil)
	c.Assert(cfg.DatabaseURL, qt.Equals, "sqlite://rooted.db")
	c.Assert(cfg.SchemaSources, qt.DeepEquals, []string{
		"file://schema/a.hcl",
		"file://schema/nested.hcl",
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
	c.Assert(err, qt.ErrorMatches, `cannot evaluate atlas\.hcl "url" at atlas\.hcl:5: .*Unknown variable.*`)
}

// Label arity and duplicate blocks are the two positions still refused in an
// unselected env; the community binary accepts both. They are tracked as known
// remaining divergences rather than folded into the unknown-name tolerance.
func TestParseAtlasProjectConfigUnsupportedConstructsInUnselectedEnv(t *testing.T) {
	c := qt.New(t)
	tests := []struct {
		name    string
		raw     string
		wantErr string
	}{
		{
			name: "environment attribute",
			raw: `env "dev" {
  url = "sqlite://dev.db"
}
env "prod" {
  url     = missing.value
  project = "production"
}
`,
			// The community binary never decodes an env it was not asked for, so
			// an unknown name there is accepted. Measured: `--env dev` with an
			// unresolvable reference in `prod` exits 0.
			wantErr: "",
		},
		{
			name: "environment block",
			raw: `env "dev" {
  url = "sqlite://dev.db"
}
env "prod" {
  url = missing.value
  cloud {}
}
`,
			wantErr: "",
		},
		{
			name: "nested migration attribute",
			raw: `env "dev" {
  url = "sqlite://dev.db"
}
env "prod" {
  url = missing.value
  migration {
    dir        = "file://migrations"
    remote_dir = "atlas://team/project"
  }
}
`,
			wantErr: "",
		},
		{
			name: "labeled nested block",
			raw: `env "dev" {
  url = "sqlite://dev.db"
}
env "prod" {
  url = missing.value
  migration "remote" {
    dir = "file://migrations"
  }
}
`,
			wantErr: `unsupported atlas\.hcl construct "migration" at atlas\.hcl:6`,
		},
		{
			name: "duplicate nested block",
			raw: `env "dev" {
  url = "sqlite://dev.db"
}
env "prod" {
  url = missing.value
  migration {
    dir = "file://migrations"
  }
  migration {
    dir = "file://other"
  }
}
`,
			wantErr: `unsupported atlas\.hcl construct "migration" at atlas\.hcl:9`,
		},
	}

	for _, test := range tests {
		c.Run(test.name, func(c *qt.C) {
			_, err := projectconfig.ParseAtlas([]byte(test.raw), "atlas.hcl", "dev")

			if test.wantErr == "" {
				c.Assert(err, qt.IsNil)
				return
			}
			c.Assert(err, qt.ErrorMatches, test.wantErr)
		})
	}
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

// TestParseAtlasProjectConfigToleratesConstructsCEIgnores covers atlas.hcl names
// that are real Atlas constructs but that the community binary parses and never
// acts on. Refusing them would be stricter than Atlas; each was measured by
// planting a value the field cannot hold and confirming the community binary
// reports no decode failure, with a name it does decode used as the control in
// the same command.
//
// Names the community binary DOES decode stay in the rejection table above --
// tolerating those would accept a policy and silently not enforce it.
func TestParseAtlasProjectConfigToleratesConstructsCEIgnores(t *testing.T) {
	tests := []struct {
		name    string
		raw     string
		ignored string
	}{
		{
			name: "schema mode sensitive allow",
			raw: `env "local" {
  schema {
    mode {
      sensitive = "ALLOW"
    }
  }
}
`,
			ignored: "sensitive",
		},
		{
			name: "lint format attr",
			raw: `lint {
  format = "{{ json . }}"
}
`,
			ignored: "format",
		},
		{
			name: "lint destructive force",
			raw: `lint {
  destructive {
    force = true
  }
}
`,
			ignored: "force",
		},
		{
			name: "lint check block",
			raw: `lint {
  check "DS102" {
    error = true
  }
}
`,
			ignored: "check",
		},
		{
			name: "lint custom rule",
			raw: `lint {
  rule "hcl" "custom" {
    src = ["schema.rule.hcl"]
  }
}
`,
			ignored: "rule",
		},
		{
			name: "lint non linear block",
			raw: `lint {
  non_linear {
    error = true
  }
}
`,
			ignored: "non_linear",
		},
		{
			name: "lint naming block",
			raw: `lint {
  naming {
    error = true
  }
}
`,
			ignored: "naming",
		},
		{
			name: "lint ownership block",
			raw: `lint {
  ownership "github" {
    repo = "stokaro/ptah"
  }
}
`,
			ignored: "ownership",
		},
		{
			name: "lint statement block",
			raw: `lint {
  statement {
    error = true
  }
}
`,
			ignored: "statement",
		},
		{
			name: "unknown migration attribute",
			raw: `env "local" {
  migration {
    remote_dir = "atlas://example"
  }
}
`,
			ignored: "remote_dir",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)

			cfg, err := projectconfig.ParseAtlas([]byte(tt.raw), "atlas.hcl", "local")

			c.Assert(err, qt.IsNil)
			names := make([]string, 0, len(cfg.IgnoredConstructs))
			for _, ignored := range cfg.IgnoredConstructs {
				names = append(names, ignored.Name)
			}
			c.Assert(names, qt.Contains, tt.ignored)
		})
	}
}

func TestParseAtlasProjectConfigRejectsUnsupportedConstructs(t *testing.T) {
	tests := []struct {
		name string
		raw  string
		err  string
	}{
		{
			// Unquoted ALLOW is an HCL variable reference, and the tolerance is
			// name-level: the body of an ignored name is still evaluated. The
			// community binary reports the same diagnostic, word for word.
			name: "schema mode sensitive bare identifier",
			raw: `env "local" {
  schema {
    mode {
      sensitive = ALLOW
    }
  }
}
`,
			err: `cannot evaluate atlas\.hcl "sensitive" at atlas\.hcl:4: .*There is no variable named "ALLOW"\.`,
		},
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
			err: `atlas block is not supported by the community version of Atlas`,
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
			name: "variable object type is unsupported",
			raw: `variable "conn" {
  type = object({ url = string })
}
`,
			err: `atlas\.hcl variable "conn" type at atlas\.hcl:2 is not supported: supported types are string, number, bool, and list\(string\)`,
		},
		{
			name: "variable map type is unsupported",
			raw: `variable "labels" {
  type = map(string)
}
`,
			err: `atlas\.hcl variable "labels" type at atlas\.hcl:2 is not supported: supported types are string, number, bool, and list\(string\)`,
		},
		{
			name: "variable tuple type is unsupported",
			raw: `variable "pair" {
  type = tuple([string, number])
}
`,
			err: `atlas\.hcl variable "pair" type at atlas\.hcl:2 is not supported: supported types are string, number, bool, and list\(string\)`,
		},
		{
			name: "variable list of number type is unsupported",
			raw: `variable "ports" {
  type = list(number)
}
`,
			err: `atlas\.hcl variable "ports" type at atlas\.hcl:2 is not supported: supported types are string, number, bool, and list\(string\)`,
		},
		{
			name: "variable default does not match type",
			raw: `variable "latest" {
  type    = number
  default = ["nope"]
}
`,
			err: `atlas\.hcl variable "latest" default does not match type number at atlas\.hcl:3`,
		},
		{
			name: "variable sensitive requires a bool",
			raw: `variable "url" {
  sensitive = "yes"
  default   = "sqlite://typed.db"
}
`,
			err: `atlas\.hcl "sensitive" at atlas\.hcl:2 must be a bool`,
		},
		{
			name: "variable validation block is unsupported",
			raw: `variable "url" {
  type    = string
  default = "sqlite://typed.db"
  validation {
    condition     = true
    error_message = "unreachable"
  }
}
`,
			err: `unsupported atlas\.hcl construct "validation" at atlas\.hcl:4`,
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
			err: `cannot evaluate atlas\.hcl "url" at atlas\.hcl:2: .*path escapes atlas\.hcl directory.*`,
		},
		{
			name: "file function rejects absolute paths",
			raw: `env "local" {
  url = file("/tmp/secret.txt")
}
`,
			err: `cannot evaluate atlas\.hcl "url" at atlas\.hcl:2: .*absolute paths are not supported.*`,
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
			err: `cannot evaluate atlas\.hcl "paths" at atlas\.hcl:2: .*`,
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
			err: `atlas\.hcl "exclude" at atlas\.hcl:2 must be a list of strings`,
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

// TestAtlasParserDistinguishesFailureClasses pins that an unknown NAME, a bad
// VALUE on a known key, and an expression that cannot be evaluated produce
// three different errors.
//
// They were one message until stokaro/ptah#1014, and collapsing them again is
// the specific regression this test exists to catch. The distinction is
// load-bearing: Atlas CE tolerates an unknown name while still failing on the
// other two, so an accept-and-ignore change has to relax exactly one branch.
// While the three share a message, no test can tell whether a refusal came from
// the branch meant to relax or from one meant to stay, and a relaxation would
// silently turn real agreements with CE into coincidental ones.
func TestAtlasParserDistinguishesFailureClasses(t *testing.T) {
	c := qt.New(t)

	const envBody = `
  dev = "sqlite://dev.db?mode=memory&_fk=1"
}
`
	tests := []struct {
		name    string
		urlExpr string
		wantErr string
	}{
		{
			name:    "evaluation failure on a known key",
			urlExpr: "var.nope",
			wantErr: `cannot evaluate atlas\.hcl "url" .*`,
		},
		{
			name:    "wrong value type on a known key",
			urlExpr: "42",
			wantErr: `atlas\.hcl "url" .* must be a string`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			body := "env \"local\" {\n  url = " + tt.urlExpr + envBody
			_, err := projectconfig.ParseAtlas([]byte(body), "atlas.hcl", "local")
			c.Assert(err, qt.IsNotNil)
			c.Assert(err, qt.ErrorMatches, tt.wantErr)
		})
	}

	// The third class, for contrast: an unknown NAME still has its own message
	// and must not be reported as either of the two above.
	//
	// The position matters. Unknown names are now tolerated at every level, so
	// this uses `schema.repo` -- one of the few names the community binary
	// genuinely decodes, which is why Ptah still refuses it rather than
	// accepting a policy it does not enforce.
	unknown := "env \"local\" {\n  schema {\n    repo {\n      name = \"app\"\n    }\n  }\n  url = \"sqlite://m.db\"" + envBody
	_, err := projectconfig.ParseAtlas([]byte(unknown), "atlas.hcl", "local")
	c.Assert(err, qt.IsNotNil)
	c.Assert(err, qt.ErrorMatches, `unsupported atlas\.hcl construct "repo" .*`)
}

// TestAtlasParserScrubsSensitiveValuesFromDiagnostics is a regression test for
// a leak this package's own error-class split introduced.
//
// Carrying the HCL diagnostic through is what makes an evaluation failure
// readable — it names the offending sub-expression, and for file()/fileset() it
// reports the sandbox violation. But HCL renders *evaluated arguments* into its
// text: `file(var.secret)` fails with `openat <the secret>: no such file`. So
// passing the diagnostic through verbatim publishes exactly the value that
// `sensitive = true` exists to hide, on stderr, where it lands in CI logs.
//
// The rest of this parser already refuses to put variable values in messages
// (see redactedAtlasVariableValue and the default-mismatch error). This keeps
// that invariant when the text comes from HCL rather than from us.
func TestAtlasParserScrubsSensitiveValuesFromDiagnostics(t *testing.T) {
	c := qt.New(t)

	const secret = "SUPERSECRET_TOKEN_12345"
	raw := []byte(`
variable "secret" {
  type      = string
  default   = "` + secret + `"
  sensitive = true
}
env "local" {
  url = file(var.secret)
  dev = "sqlite://dev.db?mode=memory&_fk=1"
}
`)

	_, err := projectconfig.ParseAtlas(raw, "atlas.hcl", "local")
	c.Assert(err, qt.IsNotNil)
	c.Assert(err.Error(), qt.Not(qt.Contains), secret)
	c.Assert(err.Error(), qt.Contains, "(sensitive value)")

	// The control: a NON-sensitive variable keeps its value in the diagnostic,
	// because that is what makes the message actionable. Without this half, a
	// scrubber that redacted everything would pass the test above while
	// destroying every diagnostic in the package.
	rawOpen := []byte(`
variable "path" {
  type    = string
  default = "VISIBLE_PATH_VALUE"
}
env "local" {
  url = file(var.path)
  dev = "sqlite://dev.db?mode=memory&_fk=1"
}
`)
	_, err = projectconfig.ParseAtlas(rawOpen, "atlas.hcl", "local")
	c.Assert(err, qt.IsNotNil)
	c.Assert(err.Error(), qt.Contains, "VISIBLE_PATH_VALUE")
}

// TestAtlasToleratesUnknownTopLevelNames pins Atlas CE's unknown-name policy at
// the top level of atlas.hcl, measured on the pinned CE v1.3.0 binary.
//
// Every position here was measured rather than assumed, and three of them
// contradict the obvious reading:
//
//   - unknown ATTRIBUTES are tolerated too, not only blocks;
//   - the body of a tolerated construct is still EVALUATED, so a bad reference
//     inside one is fatal -- tolerance is name-level, not subtree-level;
//   - `atlas` is the one top-level name CE knows and refuses, so it must not
//     be folded into the tolerated path.
func TestAtlasToleratesUnknownTopLevelNames(t *testing.T) {
	const env = `
env "local" {
  url = "sqlite://m.db"
  dev = "sqlite://d.db"
}
`
	tolerated := []struct{ name, construct string }{
		{"unknown block with one label", "check \"migrate_apply\" {\n  drift {\n    on_error = \"FAIL\"\n  }\n}"},
		{"nonsense control, same shape", "frobnicate_nonsense \"zzz\" {\n  totally_made_up = \"yes\"\n}"},
		{"unknown block with no label", "frobnicate {\n  x = 1\n}"},
		{"unknown block with two labels", "frobnicate \"a\" \"b\" {\n  x = 1\n}"},
		{"unknown attribute", "frobnicate = \"yes\""},
	}
	for _, tt := range tolerated {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			cfg, err := projectconfig.ParseAtlas([]byte(tt.construct+env), "atlas.hcl", "local")
			c.Assert(err, qt.IsNil)
			// Tolerated means recorded, not invisible: the caller must be able
			// to say something, because CE's silence is the footgun.
			c.Assert(len(cfg.IgnoredConstructs) > 0, qt.IsTrue)
		})
	}

	refused := []struct{ name, construct, wantErr string }{
		{
			// The discriminator for the whole rule. Same block as the
			// tolerated rows, only the value differs.
			name:      "bad reference inside a tolerated block",
			construct: "frobnicate \"z\" {\n  v = var.undefined_ref\n}",
			wantErr:   `cannot evaluate atlas\.hcl "v" .*`,
		},
		{
			name:      "atlas block is known and gated, not unknown",
			construct: "atlas {\n  cloud {\n    token = \"t\"\n  }\n}",
			wantErr:   `atlas block is not supported by the community version of Atlas`,
		},
		{
			name:      "atlas block with a label",
			construct: "atlas \"x\" {\n  k = \"v\"\n}",
			wantErr:   `init block "atlas" cannot have labels`,
		},
	}
	for _, tt := range refused {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			_, err := projectconfig.ParseAtlas([]byte(tt.construct+env), "atlas.hcl", "local")
			c.Assert(err, qt.IsNotNil)
			c.Assert(err, qt.ErrorMatches, tt.wantErr)
		})
	}
}

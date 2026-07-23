package projectconfig_test

import (
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"github.com/stokaro/ptah/config/projectconfig"
)

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
  exec_order: non-linear
`), 0o600), qt.IsNil)
	c.Assert(os.WriteFile(atlasPath, []byte(`env "local" {
  url = "postgres://atlas/db"
  src = []
  exclude = []
  migration {
    dir = "file://atlas-migrations"
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
	c.Assert(cfg.Migration.ExecOrder, qt.Equals, "non-linear")
	c.Assert(cfg.Migration.Format, qt.Equals, "atlas")
	c.Assert(cfg.Migration.RevisionFormat, qt.Equals, "atlas")
}

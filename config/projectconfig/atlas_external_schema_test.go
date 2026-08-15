package projectconfig_test

import (
	"os"
	"path/filepath"
	"strconv"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/config/projectconfig"
)

func TestParseAtlasExternalSchemaDataSource_HappyPath(t *testing.T) {
	t.Run("program only defaults format to sql", func(t *testing.T) {
		c := qt.New(t)
		raw := []byte(`data "external_schema" "app" {
  program = ["./gen.sh"]
}

env "dev" {
  src = data.external_schema.app.url
}
`)

		cfg, err := projectconfig.ParseAtlas(raw, "atlas.hcl", "dev")

		c.Assert(err, qt.IsNil)
		c.Assert(cfg.ExternalSchema.Program, qt.DeepEquals, []string{"./gen.sh"})
		c.Assert(cfg.ExternalSchema.Format, qt.Equals, "sql")
		c.Assert(cfg.ExternalSchema.WorkingDir, qt.Equals, "")
		c.Assert(cfg.ExternalSchema.Env, qt.IsNil)
		c.Assert(cfg.ExternalSchema.Origin, qt.Equals, projectconfig.AtlasFileName)
		c.Assert(cfg.SchemaSources, qt.IsNil)
		c.Assert(cfg.SchemaSourcesValue().Present, qt.IsFalse)
	})

	t.Run("all attributes", func(t *testing.T) {
		c := qt.New(t)
		raw := []byte(`data "external_schema" "app" {
  program     = ["python3", "export.py", "--dialect", "postgres"]
  format      = "yaml"
  working_dir = "loaders"
  env         = ["APP_ENV=ci", "EMPTY="]
}

env "dev" {
  src = data.external_schema.app.url
}
`)

		cfg, err := projectconfig.ParseAtlas(raw, "atlas.hcl", "dev")

		c.Assert(err, qt.IsNil)
		c.Assert(cfg.ExternalSchema.Program, qt.DeepEquals, []string{"python3", "export.py", "--dialect", "postgres"})
		c.Assert(cfg.ExternalSchema.Format, qt.Equals, "yaml")
		c.Assert(cfg.ExternalSchema.WorkingDir, qt.Equals, "loaders")
		c.Assert(cfg.ExternalSchema.Env, qt.DeepEquals, []string{"APP_ENV=ci", "EMPTY="})
		c.Assert(cfg.ExternalSchema.Origin, qt.Equals, projectconfig.AtlasFileName)
	})

	t.Run("yml format normalizes to yaml", func(t *testing.T) {
		c := qt.New(t)
		raw := []byte(`data "external_schema" "app" {
  program = ["./gen.sh"]
  format  = "yml"
}

env "dev" {
  src = data.external_schema.app.url
}
`)

		cfg, err := projectconfig.ParseAtlas(raw, "atlas.hcl", "dev")

		c.Assert(err, qt.IsNil)
		c.Assert(cfg.ExternalSchema.Format, qt.Equals, "yaml")
	})

	t.Run("schema block src spelling", func(t *testing.T) {
		c := qt.New(t)
		raw := []byte(`data "external_schema" "app" {
  program = ["./gen.sh"]
}

env "dev" {
  schema {
    src = data.external_schema.app.url
  }
}
`)

		cfg, err := projectconfig.ParseAtlas(raw, "atlas.hcl", "dev")

		c.Assert(err, qt.IsNil)
		c.Assert(cfg.ExternalSchema.Program, qt.DeepEquals, []string{"./gen.sh"})
		c.Assert(cfg.SchemaSources, qt.IsNil)
	})
}

func TestParseAtlasExternalSchemaDataSource_FailurePath(t *testing.T) {
	tests := []struct {
		name    string
		raw     string
		wantErr string
	}{
		{
			name: "missing program",
			raw: `data "external_schema" "app" {
}

env "dev" {
  src = data.external_schema.app.url
}
`,
			wantErr: `atlas\.hcl data\.external_schema "app" requires a non-empty program list at atlas\.hcl:1`,
		},
		{
			name: "empty program list",
			raw: `data "external_schema" "app" {
  program = []
}

env "dev" {
  src = data.external_schema.app.url
}
`,
			wantErr: `atlas\.hcl data\.external_schema "app" requires a non-empty program list at atlas\.hcl:2`,
		},
		{
			name: "blank program executable",
			raw: `data "external_schema" "app" {
  program = [" "]
}

env "dev" {
  src = data.external_schema.app.url
}
`,
			wantErr: `atlas\.hcl data\.external_schema "app" requires a non-empty program list at atlas\.hcl:2`,
		},
		{
			name: "invalid format",
			raw: `data "external_schema" "app" {
  program = ["./gen.sh"]
  format  = "toml"
}

env "dev" {
  src = data.external_schema.app.url
}
`,
			wantErr: `atlas\.hcl data\.external_schema "app" format must be sql, hcl, or yaml, got "toml" at atlas\.hcl:3`,
		},
		{
			name: "malformed env entry",
			raw: `data "external_schema" "app" {
  program = ["./gen.sh"]
  env     = ["MISSING_EQUALS"]
}

env "dev" {
  src = data.external_schema.app.url
}
`,
			wantErr: `atlas\.hcl data\.external_schema "app" env entries must be KEY=VALUE, got "MISSING_EQUALS" at atlas\.hcl:3`,
		},
		{
			name: "env entry with empty key",
			raw: `data "external_schema" "app" {
  program = ["./gen.sh"]
  env     = ["=value"]
}

env "dev" {
  src = data.external_schema.app.url
}
`,
			wantErr: `atlas\.hcl data\.external_schema "app" env entries must be KEY=VALUE, got "=value" at atlas\.hcl:3`,
		},
		{
			name: "unknown attribute",
			raw: `data "external_schema" "app" {
  program = ["./gen.sh"]
  command = "./gen.sh"
}

env "dev" {
  src = data.external_schema.app.url
}
`,
			wantErr: `unsupported atlas\.hcl construct "command" at atlas\.hcl:3`,
		},
		{
			name: "child block",
			raw: `data "external_schema" "app" {
  program = ["./gen.sh"]
  retry {
  }
}

env "dev" {
  src = data.external_schema.app.url
}
`,
			wantErr: `unsupported atlas\.hcl construct "retry" at atlas\.hcl:3`,
		},
		{
			name: "duplicate data source name",
			raw: `data "external_schema" "app" {
  program = ["./gen.sh"]
}

data "external_schema" "app" {
  program = ["./other.sh"]
}

env "dev" {
  src = data.external_schema.app.url
}
`,
			wantErr: `duplicate atlas\.hcl data\.external_schema "app" at atlas\.hcl:5`,
		},
		{
			name: "marker as env url",
			raw: `data "external_schema" "app" {
  program = ["./gen.sh"]
}

env "dev" {
  url = data.external_schema.app.url
  src = "schema.sql"
}
`,
			wantErr: `atlas\.hcl data\.external_schema\.app\.url can only be the env desired-state source \(env src or schema\.src\), not env url`,
		},
		{
			name: "marker as env dev",
			raw: `data "external_schema" "app" {
  program = ["./gen.sh"]
}

env "dev" {
  dev = data.external_schema.app.url
  src = "schema.sql"
}
`,
			wantErr: `atlas\.hcl data\.external_schema\.app\.url can only be the env desired-state source \(env src or schema\.src\), not env dev`,
		},
		{
			name: "marker as migration dir",
			raw: `data "external_schema" "app" {
  program = ["./gen.sh"]
}

env "dev" {
  src = "schema.sql"
  migration {
    dir = data.external_schema.app.url
  }
}
`,
			wantErr: `atlas\.hcl data\.external_schema\.app\.url can only be the env desired-state source \(env src or schema\.src\), not env migration\.dir`,
		},
		{
			name: "marker in exclude list",
			raw: `data "external_schema" "app" {
  program = ["./gen.sh"]
}

env "dev" {
  src     = "schema.sql"
  exclude = [data.external_schema.app.url]
}
`,
			wantErr: `atlas\.hcl data\.external_schema\.app\.url can only be the env desired-state source \(env src or schema\.src\), not env exclude`,
		},
		{
			name: "marker mixed with other schema sources",
			raw: `data "external_schema" "app" {
  program = ["./gen.sh"]
}

env "dev" {
  src = [data.external_schema.app.url, "schema.sql"]
}
`,
			wantErr: `atlas\.hcl data\.external_schema\.app\.url must be the only env src value`,
		},
		{
			name: "undeclared marker",
			raw: `env "dev" {
  src = "ptah-external-schema://ghost"
}
`,
			wantErr: `atlas\.hcl env src references undeclared data\.external_schema "ghost"`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			_, err := projectconfig.ParseAtlas([]byte(test.raw), "atlas.hcl", "dev")
			c.Assert(err, qt.ErrorMatches, test.wantErr)
		})
	}
}

func TestParseAtlasExternalSchemaCoexistsWithHCLSchema(t *testing.T) {
	raw := []byte(`data "hcl_schema" "declared" {
  path = "schema.hcl"
}

data "external_schema" "generated" {
  program = ["./gen.sh"]
}

env "files" {
  src = data.hcl_schema.declared.url
}

env "program" {
  src = data.external_schema.generated.url
}
`)

	t.Run("env selecting the hcl_schema source", func(t *testing.T) {
		c := qt.New(t)
		cfg, err := projectconfig.ParseAtlas(raw, "atlas.hcl", "files")

		c.Assert(err, qt.IsNil)
		c.Assert(cfg.SchemaSources, qt.DeepEquals, []string{"file://schema.hcl"})
		c.Assert(cfg.ExternalSchema.Program, qt.IsNil)
	})

	t.Run("env selecting the external_schema source", func(t *testing.T) {
		c := qt.New(t)
		cfg, err := projectconfig.ParseAtlas(raw, "atlas.hcl", "program")

		c.Assert(err, qt.IsNil)
		c.Assert(cfg.SchemaSources, qt.IsNil)
		c.Assert(cfg.ExternalSchema.Program, qt.DeepEquals, []string{"./gen.sh"})
	})
}

func TestLoadAtlasExternalSchemaWorkingDirResolvesAgainstConfigDir(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	atlasPath := filepath.Join(dir, "atlas.hcl")
	c.Assert(os.WriteFile(atlasPath, []byte(`data "external_schema" "app" {
  program     = ["./gen.sh"]
  working_dir = "loaders"
}

env "dev" {
  src = data.external_schema.app.url
}
`), 0o600), qt.IsNil)

	cfg, err := projectconfig.LoadAtlasFile(atlasPath, "dev")

	c.Assert(err, qt.IsNil)
	c.Assert(cfg.ExternalSchema.WorkingDir, qt.Equals, filepath.Join(dir, "loaders"))
}

func TestLoadAtlasExternalSchemaAbsoluteWorkingDirIsKept(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	absolute := filepath.Join(dir, "elsewhere")
	atlasPath := filepath.Join(dir, "atlas.hcl")
	c.Assert(os.WriteFile(atlasPath, []byte(`data "external_schema" "app" {
  program     = ["./gen.sh"]
  working_dir = `+strconvQuoteForHCL(absolute)+`
}

env "dev" {
  src = data.external_schema.app.url
}
`), 0o600), qt.IsNil)

	cfg, err := projectconfig.LoadAtlasFile(atlasPath, "dev")

	c.Assert(err, qt.IsNil)
	c.Assert(cfg.ExternalSchema.WorkingDir, qt.Equals, absolute)
}

func TestLoadAtlasExternalSchemaUnreferencedSourceIsNotExecuted(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	sentinel := filepath.Join(dir, "executed.sentinel")
	script := filepath.Join(dir, "gen.sh")
	// The script would create the sentinel file if anything ran it.
	c.Assert(os.WriteFile(script, []byte("#!/bin/sh\ntouch "+sentinel+"\n"), 0o700), qt.IsNil) //nolint:gosec // executable test fixture in a private temp dir
	atlasPath := filepath.Join(dir, "atlas.hcl")
	c.Assert(os.WriteFile(atlasPath, []byte(`data "external_schema" "unused" {
  program = [`+strconvQuoteForHCL(script)+`]
}

env "dev" {
  src = "schema.sql"
}
`), 0o600), qt.IsNil)

	cfg, err := projectconfig.LoadAtlasFile(atlasPath, "dev")

	c.Assert(err, qt.IsNil)
	c.Assert(cfg.ExternalSchema.Program, qt.IsNil)
	c.Assert(cfg.SchemaSources, qt.DeepEquals, []string{"schema.sql"})
	_, statErr := os.Stat(sentinel)
	c.Assert(os.IsNotExist(statErr), qt.IsTrue)
}

func TestLoadExternalSchemaPrecedence_HappyPath(t *testing.T) {
	t.Run("atlas.hcl data source replaces ptah.yaml external_schema wholesale", func(t2 *testing.T) {
		c := qt.New(t2)
		dir := t.TempDir()
		ptahPath := filepath.Join(dir, "ptah.yaml")
		c.Assert(os.WriteFile(ptahPath, []byte(`external_schema:
  program: ["./from-ptah.sh"]
  format: yaml
  working_dir: "ptah-dir"
  env: ["FROM=ptah"]
`), 0o600), qt.IsNil)
		atlasPath := filepath.Join(dir, "atlas.hcl")
		c.Assert(os.WriteFile(atlasPath, []byte(`data "external_schema" "app" {
  program = ["./from-atlas.sh"]
}

env "dev" {
  src = data.external_schema.app.url
}
`), 0o600), qt.IsNil)

		cfg, err := projectconfig.Load(projectconfig.LoadOptions{
			PtahPath:  ptahPath,
			AtlasPath: atlasPath,
			EnvName:   "dev",
		})

		c.Assert(err, qt.IsNil)
		c.Assert(cfg.ExternalSchema.Program, qt.DeepEquals, []string{"./from-atlas.sh"})
		// The whole block is replaced: the atlas.hcl defaults win over the
		// ptah.yaml values so the two files never mix into a hybrid program.
		c.Assert(cfg.ExternalSchema.Format, qt.Equals, "sql")
		c.Assert(cfg.ExternalSchema.WorkingDir, qt.Equals, "")
		c.Assert(cfg.ExternalSchema.Env, qt.IsNil)
		c.Assert(cfg.ExternalSchema.Origin, qt.Equals, projectconfig.AtlasFileName)
	})

	t.Run("ptah.yaml external_schema survives when atlas.hcl declares none", func(t2 *testing.T) {
		c := qt.New(t2)
		dir := t.TempDir()
		ptahPath := filepath.Join(dir, "ptah.yaml")
		c.Assert(os.WriteFile(ptahPath, []byte(`external_schema:
  program: ["./from-ptah.sh"]
  format: yaml
`), 0o600), qt.IsNil)
		atlasPath := filepath.Join(dir, "atlas.hcl")
		c.Assert(os.WriteFile(atlasPath, []byte(`env "dev" {
  url = "sqlite://app.db"
}
`), 0o600), qt.IsNil)

		cfg, err := projectconfig.Load(projectconfig.LoadOptions{
			PtahPath:  ptahPath,
			AtlasPath: atlasPath,
			EnvName:   "dev",
		})

		c.Assert(err, qt.IsNil)
		c.Assert(cfg.ExternalSchema.Program, qt.DeepEquals, []string{"./from-ptah.sh"})
		c.Assert(cfg.ExternalSchema.Format, qt.Equals, "yaml")
		c.Assert(cfg.ExternalSchema.Origin, qt.Equals, projectconfig.PtahFileName)
	})
}

// strconvQuoteForHCL quotes a filesystem path for embedding in an HCL string
// literal; Go string quoting and HCL string escapes agree for the characters
// that appear in test paths (notably Windows backslashes).
func strconvQuoteForHCL(value string) string {
	return strconv.Quote(value)
}

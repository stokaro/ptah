package projectconfig_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/config/projectconfig"
)

func TestParsePtahExternalSchema(t *testing.T) {
	c := qt.New(t)

	cfg, err := projectconfig.ParsePtah([]byte(`external_schema:
  program: ["go", "run", "./loader"]
  format: sql
  working_dir: ./project
  env: ["FOO=bar", "BAZ=qux"]
`), "ptah.yaml", "")

	c.Assert(err, qt.IsNil)
	c.Assert(cfg.ExternalSchema.Program, qt.DeepEquals, []string{"go", "run", "./loader"})
	c.Assert(cfg.ExternalSchema.Format, qt.Equals, "sql")
	c.Assert(cfg.ExternalSchema.WorkingDir, qt.Equals, "./project")
	c.Assert(cfg.ExternalSchema.Env, qt.DeepEquals, []string{"FOO=bar", "BAZ=qux"})
}

func TestParsePtahExternalSchemaEnvOverridesBase(t *testing.T) {
	c := qt.New(t)

	cfg, err := projectconfig.ParsePtah([]byte(`external_schema:
  program: ["base-loader"]
  format: sql
env:
  prod:
    external_schema:
      program: ["prod-loader", "--flag"]
      working_dir: ./prod
`), "ptah.yaml", "prod")

	c.Assert(err, qt.IsNil)
	c.Assert(cfg.EnvName, qt.Equals, "prod")
	// The env block's program replaces the base program; base format survives
	// because the env block did not set it.
	c.Assert(cfg.ExternalSchema.Program, qt.DeepEquals, []string{"prod-loader", "--flag"})
	c.Assert(cfg.ExternalSchema.Format, qt.Equals, "sql")
	c.Assert(cfg.ExternalSchema.WorkingDir, qt.Equals, "./prod")
}

func TestParsePtahExternalSchemaEnvEmptyProgramClearsBase(t *testing.T) {
	c := qt.New(t)

	// An explicit empty program list in the env block disables the inherited
	// base loader (presence semantics, like schemas/exclude).
	cfg, err := projectconfig.ParsePtah([]byte(`external_schema:
  program: ["base-loader"]
env:
  prod:
    external_schema:
      program: []
`), "ptah.yaml", "prod")

	c.Assert(err, qt.IsNil)
	c.Assert(cfg.ExternalSchema.Program, qt.HasLen, 0)
}

func TestParsePtahExternalSchemaAbsentIsZero(t *testing.T) {
	c := qt.New(t)

	cfg, err := projectconfig.ParsePtah([]byte(`url: postgres://app/db`), "ptah.yaml", "")

	c.Assert(err, qt.IsNil)
	c.Assert(cfg.ExternalSchema.Program, qt.IsNil)
	c.Assert(cfg.ExternalSchema.Format, qt.Equals, "")
}

func TestMergeExternalSchema(t *testing.T) {
	c := qt.New(t)

	base := projectconfig.Config{
		ExternalSchema: projectconfig.ExternalSchemaConfig{
			Program: []string{"base"},
			Format:  "sql",
			Env:     []string{"BASE=1"},
		},
	}
	override := projectconfig.Config{
		ExternalSchema: projectconfig.ExternalSchemaConfig{
			WorkingDir: "./override",
		},
	}

	merged := projectconfig.Merge(base, override)

	// Scalar override left blank keeps the base; working_dir comes from override.
	c.Assert(merged.ExternalSchema.Program, qt.DeepEquals, []string{"base"})
	c.Assert(merged.ExternalSchema.Format, qt.Equals, "sql")
	c.Assert(merged.ExternalSchema.Env, qt.DeepEquals, []string{"BASE=1"})
	c.Assert(merged.ExternalSchema.WorkingDir, qt.Equals, "./override")
}

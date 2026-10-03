package projectconfig_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/config/projectconfig"
)

// compositeMarker is the desired-state source a `data "composite_schema"
// "project"` block mints.
const compositeMarker = projectconfig.CompositeSchemaMarkerScheme + "://project"

// TestParseAtlasCompositeSchema_HappyPath reads `data "composite_schema"` the
// way Atlas's documentation describes it: an ordered list of `schema` blocks,
// each naming a desired state by URL, with a label naming the schema its
// objects belong to. The env's source is the marker, and the parts are read
// back through it.
func TestParseAtlasCompositeSchema_HappyPath(t *testing.T) {
	tests := []struct {
		name string
		raw  string
		want projectconfig.CompositeSchema
	}{
		{
			name: "labeled parts keep their order and URLs",
			raw: `data "composite_schema" "project" {
  schema "auth" {
    url = "file://auth.hcl"
  }
  schema "public" {
    url = "file://schema.sql"
  }
}

env "dev" {
  src = data.composite_schema.project.url
}
`,
			want: projectconfig.CompositeSchema{Name: "project", Parts: []projectconfig.CompositeSchemaPart{
				{Schema: "auth", URL: "file://auth.hcl", Filename: "atlas.hcl", Line: 2},
				{Schema: "public", URL: "file://schema.sql", Filename: "atlas.hcl", Line: 5},
			}},
		},
		{
			name: "an unlabeled part has no schema",
			raw: `data "composite_schema" "project" {
  schema {
    url = "file://realm.sql"
  }
}

env "dev" {
  schema {
    src = data.composite_schema.project.url
  }
}
`,
			want: projectconfig.CompositeSchema{Name: "project", Parts: []projectconfig.CompositeSchemaPart{
				{URL: "file://realm.sql", Filename: "atlas.hcl", Line: 2},
			}},
		},
		{
			name: "a part minted by hcl_schema carries that block's vars",
			raw: `data "hcl_schema" "auth" {
  path = "auth.hcl"
  vars = {
    tenant = "acme"
  }
}

data "composite_schema" "project" {
  schema "auth" {
    url = data.hcl_schema.auth.url
  }
}

env "dev" {
  src = data.composite_schema.project.url
}
`,
			want: projectconfig.CompositeSchema{Name: "project", Parts: []projectconfig.CompositeSchemaPart{
				{
					Schema: "auth", URL: "file://auth.hcl", Filename: "atlas.hcl", Line: 9,
					VarValues: map[string]string{"tenant": "acme"}, VarsScoped: true,
				},
			}},
		},
		{
			name: "a part minted by external_schema carries its program",
			raw: `data "external_schema" "orm" {
  program = ["./export.sh", "--dialect", "postgres"]
  env     = ["APP_ENV=ci"]
}

data "composite_schema" "project" {
  schema "inventory" {
    url = data.external_schema.orm.url
  }
}

env "dev" {
  src = data.composite_schema.project.url
}
`,
			want: projectconfig.CompositeSchema{Name: "project", Parts: []projectconfig.CompositeSchemaPart{
				{
					Schema: "inventory", URL: "ptah-external-schema://orm", Filename: "atlas.hcl", Line: 7,
					ExternalSchema: &projectconfig.ExternalSchemaConfig{
						Program: []string{"./export.sh", "--dialect", "postgres"},
						Format:  "sql",
						Env:     []string{"APP_ENV=ci"},
						Origin:  projectconfig.AtlasFileName,
					},
				},
			}},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			cfg, err := projectconfig.ParseAtlas([]byte(test.raw), "atlas.hcl", "dev")

			c.Assert(err, qt.IsNil)
			c.Assert(cfg.SchemaSources, qt.DeepEquals, []string{compositeMarker})
			c.Assert(cfg.HasCompositeSchemaSource(), qt.IsTrue)
			composite, ok := cfg.CompositeSchema(compositeMarker)
			c.Assert(ok, qt.IsTrue)
			c.Assert(composite, qt.DeepEquals, test.want)
			// The external part does not become the env's own external schema:
			// the composition is the desired state, not the program.
			c.Assert(cfg.ExternalSchema.Program, qt.IsNil)
		})
	}
}

// TestParseAtlasCompositeSchema_StaysLazy declares a composition nothing
// references, whose part names a data source that does not exist. The block
// is shape-checked and never evaluated, as the other data sources are.
func TestParseAtlasCompositeSchema_StaysLazy(t *testing.T) {
	c := qt.New(t)
	raw := []byte(`data "composite_schema" "unused" {
  schema "public" {
    url = data.external_schema.missing.url
  }
}

env "dev" {
  src = "file://schema.sql"
}
`)

	cfg, err := projectconfig.ParseAtlas(raw, "atlas.hcl", "dev")

	c.Assert(err, qt.IsNil)
	c.Assert(cfg.SchemaSources, qt.DeepEquals, []string{"file://schema.sql"})
	c.Assert(cfg.HasCompositeSchemaSource(), qt.IsFalse)
	_, ok := cfg.CompositeSchema(projectconfig.CompositeSchemaMarkerScheme + "://unused")
	c.Assert(ok, qt.IsFalse)
}

// TestConfigCompositeSchema_ReturnsACopy changes what one read returned and
// reads again. The config is shared between the commands of one run, so a
// caller must not be able to edit it through a returned value.
func TestConfigCompositeSchema_ReturnsACopy(t *testing.T) {
	c := qt.New(t)
	raw := []byte(`data "external_schema" "orm" {
  program = ["./export.sh"]
}

data "composite_schema" "project" {
  schema "public" {
    url = data.external_schema.orm.url
  }
}

env "dev" {
  src = data.composite_schema.project.url
}
`)
	cfg, err := projectconfig.ParseAtlas(raw, "atlas.hcl", "dev")
	c.Assert(err, qt.IsNil)
	first, ok := cfg.CompositeSchema(compositeMarker)
	c.Assert(ok, qt.IsTrue)

	first.Parts[0].Schema = "changed"
	first.Parts[0].ExternalSchema.Program[0] = "changed"
	second, ok := cfg.CompositeSchema(compositeMarker)

	c.Assert(ok, qt.IsTrue)
	c.Assert(second.Parts[0].Schema, qt.Equals, "public")
	c.Assert(second.Parts[0].ExternalSchema.Program, qt.DeepEquals, []string{"./export.sh"})
}

// TestParseAtlasCompositeSchema_FailurePath refuses a block whose shape Atlas's
// documentation does not describe, and a marker outside the desired-state
// source, before anything is evaluated.
func TestParseAtlasCompositeSchema_FailurePath(t *testing.T) {
	tests := []struct {
		name    string
		raw     string
		wantErr string
	}{
		{
			name: "a block with no schema block",
			raw: `data "composite_schema" "project" {}

env "dev" {
  src = data.composite_schema.project.url
}
`,
			wantErr: `atlas.hcl data.composite_schema "project" requires at least one schema block at atlas.hcl:1`,
		},
		{
			name: "an attribute on the block itself",
			raw: `data "composite_schema" "project" {
  url = "file://schema.sql"
  schema "public" {
    url = "file://schema.sql"
  }
}

env "dev" {
  src = data.composite_schema.project.url
}
`,
			wantErr: `unsupported atlas.hcl construct "url" at atlas.hcl:2`,
		},
		{
			name: "a schema block with two labels",
			raw: `data "composite_schema" "project" {
  schema "public" "extra" {
    url = "file://schema.sql"
  }
}

env "dev" {
  src = data.composite_schema.project.url
}
`,
			wantErr: `unsupported atlas.hcl construct "schema" at atlas.hcl:2`,
		},
		{
			name: "a block other than schema",
			raw: `data "composite_schema" "project" {
  table "users" {
    url = "file://schema.sql"
  }
}

env "dev" {
  src = data.composite_schema.project.url
}
`,
			wantErr: `unsupported atlas.hcl construct "table" at atlas.hcl:2`,
		},
		{
			name: "a schema block without url",
			raw: `data "composite_schema" "project" {
  schema "public" {}
}

env "dev" {
  src = data.composite_schema.project.url
}
`,
			wantErr: `atlas.hcl data.composite_schema "project" schema block requires url at atlas.hcl:2`,
		},
		{
			name: "an attribute beside url",
			raw: `data "composite_schema" "project" {
  schema "public" {
    url  = "file://schema.sql"
    vars = {}
  }
}

env "dev" {
  src = data.composite_schema.project.url
}
`,
			wantErr: `unsupported atlas.hcl construct "vars" at atlas.hcl:4`,
		},
		{
			name: "an empty url",
			raw: `data "composite_schema" "project" {
  schema "public" {
    url = ""
  }
}

env "dev" {
  src = data.composite_schema.project.url
}
`,
			wantErr: `atlas.hcl data.composite_schema "project" schema "public" url is empty at atlas.hcl:3`,
		},
		{
			name: "a part that is another composition",
			raw: `data "composite_schema" "inner" {
  schema "public" {
    url = "file://schema.sql"
  }
}

data "composite_schema" "project" {
  schema "public" {
    url = data.composite_schema.inner.url
  }
}

env "dev" {
  src = data.composite_schema.project.url
}
`,
			wantErr: `atlas.hcl data.composite_schema "project" schema "public" names another data.composite_schema ` +
				`at atlas.hcl:9; list its parts in this block instead`,
		},
		{
			name: "the marker as the env url",
			raw: `data "composite_schema" "project" {
  schema "public" {
    url = "file://schema.sql"
  }
}

env "dev" {
  url = data.composite_schema.project.url
}
`,
			wantErr: `atlas.hcl data.composite_schema.project.url can only be the env desired-state source ` +
				`\(env src or schema.src\), not env url`,
		},
		{
			name: "the marker as the env dev url",
			raw: `data "composite_schema" "project" {
  schema "public" {
    url = "file://schema.sql"
  }
}

env "dev" {
  dev = data.composite_schema.project.url
}
`,
			wantErr: `atlas.hcl data.composite_schema.project.url can only be the env desired-state source ` +
				`\(env src or schema.src\), not env dev`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			cfg, err := projectconfig.ParseAtlas([]byte(test.raw), "atlas.hcl", "dev")

			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(cfg.SchemaSources, qt.IsNil)
		})
	}
}

// TestParseAtlasCompositeSchema_StrictRefusal is the community binary's answer,
// which a strict compatibility run selects. Measured on the pinned v1.3.0:
// `migrate diff`, `schema inspect` and `migrate hash` refuse a project with
// this block at exit 1, whether or not the env references it.
func TestParseAtlasCompositeSchema_StrictRefusal(t *testing.T) {
	tests := []struct {
		name string
		raw  string
	}{
		{
			name: "referenced",
			raw: `data "composite_schema" "project" {
  schema "public" {
    url = "file://schema.sql"
  }
}

env "dev" {
  src = data.composite_schema.project.url
}
`,
		},
		{
			name: "declared and not referenced",
			raw: `data "composite_schema" "unused" {
  schema "public" {
    url = "file://schema.sql"
  }
}

env "dev" {
  src = "file://schema.sql"
}
`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			cfg, err := projectconfig.ParseAtlasWithOptions([]byte(test.raw), "atlas.hcl", projectconfig.AtlasLoadOptions{
				EnvName:               "dev",
				RejectCompositeSchema: true,
			})

			c.Assert(err, qt.ErrorMatches, `missing data source handler for "composite_schema"`)
			c.Assert(cfg.SchemaSources, qt.IsNil)
		})
	}
}

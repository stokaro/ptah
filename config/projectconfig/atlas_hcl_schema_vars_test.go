package projectconfig_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/config/projectconfig"
)

// TestParseAtlasHCLSchemaVars pins the decode half of stokaro/ptah#934 item 4:
// `data "hcl_schema" { vars }` is accepted, and its values are scoped to the
// files that data source selects.
//
// Measured on the pinned Atlas community binary v1.3.0 with
// `schema apply --env local --dry-run` against an `s.hcl` declaring
// `variable "tenant" { type = string }` with no default, exit codes read
// directly from unpiped invocations:
//
//	vars = { tenant = "acme" }   -> 0  DEFAULT 'acme'
//	vars = { tenant = 42 }       -> 0  DEFAULT '42'
//	vars = { tenant = true }     -> 0  DEFAULT 'true'
//	vars = {}                    -> 1  }  missing value for required
//	vars = null                  -> 1  }  variable "tenant"
//	vars = "acme"                -> 1  }  Unsuitable value: map of any
//	vars = [1, 2]                -> 1  }  single type required
//	vars = { tenant = [1, 2] }   -> 1     variable "tenant": string required
//
// The empty and null rows are the control for the rest: they prove the earlier
// rows carry a value rather than the file having a default all along.
func TestParseAtlasHCLSchemaVars(t *testing.T) {
	tests := []struct {
		name       string
		raw        string
		wantURL    string
		wantValues map[string]string
		wantScoped bool
	}{
		{
			name: "a string value reaches the selected file",
			raw: `data "hcl_schema" "app" {
  paths = ["s.hcl"]
  vars = {
    tenant = "acme"
  }
}

env "local" {
  url = "sqlite://file.db"
  src = data.hcl_schema.app.url
}
`,
			wantURL:    "file://s.hcl",
			wantValues: map[string]string{"tenant": "acme"},
			wantScoped: true,
		},
		{
			// The binary converts a number to the string form of the literal.
			name: "a number value is carried as its literal text",
			raw: `data "hcl_schema" "app" {
  paths = ["s.hcl"]
  vars = {
    tenant = 42
  }
}

env "local" {
  url = "sqlite://file.db"
  src = data.hcl_schema.app.url
}
`,
			wantURL:    "file://s.hcl",
			wantValues: map[string]string{"tenant": "42"},
			wantScoped: true,
		},
		{
			name: "a bool value is carried as its literal text",
			raw: `data "hcl_schema" "app" {
  paths = ["s.hcl"]
  vars = {
    tenant = true
  }
}

env "local" {
  url = "sqlite://file.db"
  src = data.hcl_schema.app.url
}
`,
			wantURL:    "file://s.hcl",
			wantValues: map[string]string{"tenant": "true"},
			wantScoped: true,
		},
		{
			// The whole point of the boolean half of the scope. A data source
			// with no `vars` still closes the boundary, so `--var` must not
			// reach the file behind it.
			name: "a data source with no vars still closes the boundary",
			raw: `data "hcl_schema" "app" {
  paths = ["s.hcl"]
}

env "local" {
  url = "sqlite://file.db"
  src = data.hcl_schema.app.url
}
`,
			wantURL:    "file://s.hcl",
			wantValues: nil,
			wantScoped: true,
		},
		{
			name: "a null vars is read as no values given",
			raw: `data "hcl_schema" "app" {
  paths = ["s.hcl"]
  vars  = null
}

env "local" {
  url = "sqlite://file.db"
  src = data.hcl_schema.app.url
}
`,
			wantURL:    "file://s.hcl",
			wantValues: nil,
			wantScoped: true,
		},
		{
			// The literal names the same file the data source would have
			// selected, but nothing references the data source, so the file is
			// outside every scope and keeps the run's global --var.
			name: "a declared but unreferenced data source scopes nothing",
			raw: `data "hcl_schema" "app" {
  paths = ["s.hcl"]
  vars = {
    tenant = "acme"
  }
}

env "local" {
  url = "sqlite://file.db"
  src = "file://s.hcl"
}
`,
			wantURL:    "file://s.hcl",
			wantValues: nil,
			wantScoped: false,
		},
		{
			// Two data sources, one referenced. The other block's values must
			// not reach this file.
			name: "another data source's vars do not cross",
			raw: `data "hcl_schema" "app" {
  paths = ["s.hcl"]
  vars = {
    tenant = "acme"
  }
}

data "hcl_schema" "other" {
  paths = ["t.hcl"]
}

env "local" {
  url = "sqlite://file.db"
  src = data.hcl_schema.other.url
}
`,
			wantURL:    "file://t.hcl",
			wantValues: nil,
			wantScoped: true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			cfg, err := projectconfig.ParseAtlas([]byte(test.raw), "atlas.hcl", "local")

			c.Assert(err, qt.IsNil)
			c.Assert(cfg.SchemaSources, qt.DeepEquals, []string{test.wantURL})
			values, scoped := cfg.SchemaSourceVars(test.wantURL)
			c.Check(scoped, qt.Equals, test.wantScoped)
			c.Check(values, qt.DeepEquals, test.wantValues)
		})
	}
}

// TestParseAtlasHCLSchemaVarsFailurePath pins the value shapes the pinned
// community binary refuses, so a malformed `vars` cannot become an exit 0 on
// this side. Every row below is exit 1 on that binary; see
// TestParseAtlasHCLSchemaVars for the measurement table.
func TestParseAtlasHCLSchemaVarsFailurePath(t *testing.T) {
	tests := []struct {
		name    string
		raw     string
		wantErr string
	}{
		{
			name: "a string is not a map",
			raw: `data "hcl_schema" "app" {
  paths = ["s.hcl"]
  vars  = "acme"
}
`,
			wantErr: `atlas.hcl "vars" at atlas.hcl:3 must be a map of values`,
		},
		{
			name: "a list is not a map",
			raw: `data "hcl_schema" "app" {
  paths = ["s.hcl"]
  vars  = [1, 2]
}
`,
			wantErr: `atlas.hcl "vars" at atlas.hcl:3 must be a map of values`,
		},
		{
			name: "a member that is not a scalar is refused by name",
			raw: `data "hcl_schema" "app" {
  paths = ["s.hcl"]
  vars = {
    tenant = [1, 2]
  }
}
`,
			wantErr: `atlas.hcl "vars.tenant" at atlas.hcl:3 must be a string, a number, or a bool`,
		},
		{
			name: "an unevaluable expression is reported as one",
			raw: `data "hcl_schema" "app" {
  paths = ["s.hcl"]
  vars = {
    tenant = nosuch.value
  }
}
`,
			wantErr: `cannot evaluate atlas.hcl "vars" at atlas.hcl:3.*`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			_, err := projectconfig.ParseAtlas([]byte(test.raw), "atlas.hcl", "local")

			c.Assert(err, qt.ErrorMatches, test.wantErr)
		})
	}
}

// TestParseAtlasHCLSchemaVarsRefusesAmbiguousOwnership keeps the one shape that
// has no honest answer: two referenced data sources selecting the same file
// with different values. Picking one would make the desired state depend on map
// iteration order, so the parse refuses and names both blocks.
func TestParseAtlasHCLSchemaVarsRefusesAmbiguousOwnership(t *testing.T) {
	c := qt.New(t)
	raw := `data "hcl_schema" "app" {
  path = "s.hcl"
  vars = {
    tenant = "acme"
  }
}

data "hcl_schema" "other" {
  path = "s.hcl"
  vars = {
    tenant = "other"
  }
}

env "local" {
  url = "sqlite://file.db"
  src = [data.hcl_schema.app.url, data.hcl_schema.other.url]
}
`

	_, err := projectconfig.ParseAtlas([]byte(raw), "atlas.hcl", "local")

	c.Assert(err, qt.ErrorMatches,
		`atlas.hcl data.hcl_schema "app" and "other" both select "file://s.hcl" with different vars at atlas.hcl:\d+`)
}

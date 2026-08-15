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

// TestParseAtlasHCLSchemaVarsFollowsTheConditionalBranch pins which data source
// owns a file when the `src` expression names two and takes one.
//
// A conditional evaluates to exactly one branch, so the desired state is
// determined even though both blocks appear in the expression. Measured on the
// pinned Atlas community binary v1.3.0 with both blocks selecting the same
// s.hcl and `schema apply --env local --dry-run`, exit codes read directly from
// unpiped invocations:
//
//	default = true   -> 0  DEFAULT 'acme'
//	default = false  -> 0  DEFAULT 'zzz'
//
// Reading the branch not taken as a reference made ptah-compat refuse both rows
// at exit 1, `both select "file://s.hcl" with different vars`.
//
// The two rows are the discriminating pair: one fixture, one file, one env, and
// only the predicate differs. An implementation that picked a branch by name
// order would answer 'acme' twice, and one that ignored the vars would fail
// both for want of a value.
func TestParseAtlasHCLSchemaVarsFollowsTheConditionalBranch(t *testing.T) {
	tests := []struct {
		name       string
		raw        string
		wantValues map[string]string
	}{
		{
			name: "a true predicate takes the first branch's data source",
			raw: `variable "use_app" {
  type    = bool
  default = true
}

data "hcl_schema" "app" {
  path = "s.hcl"
  vars = {
    tenant = "acme"
  }
}

data "hcl_schema" "other" {
  path = "s.hcl"
  vars = {
    tenant = "zzz"
  }
}

env "local" {
  url = "sqlite://file.db"
  src = var.use_app ? data.hcl_schema.app.url : data.hcl_schema.other.url
}
`,
			wantValues: map[string]string{"tenant": "acme"},
		},
		{
			name: "a false predicate takes the second branch's data source",
			raw: `variable "use_app" {
  type    = bool
  default = false
}

data "hcl_schema" "app" {
  path = "s.hcl"
  vars = {
    tenant = "acme"
  }
}

data "hcl_schema" "other" {
  path = "s.hcl"
  vars = {
    tenant = "zzz"
  }
}

env "local" {
  url = "sqlite://file.db"
  src = var.use_app ? data.hcl_schema.app.url : data.hcl_schema.other.url
}
`,
			wantValues: map[string]string{"tenant": "zzz"},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			cfg, err := projectconfig.ParseAtlas([]byte(test.raw), "atlas.hcl", "local")

			c.Assert(err, qt.IsNil)
			c.Assert(cfg.SchemaSources, qt.DeepEquals, []string{"file://s.hcl"})
			values, scoped := cfg.SchemaSourceVars("file://s.hcl")
			c.Check(scoped, qt.Equals, true)
			c.Check(values, qt.DeepEquals, test.wantValues)
		})
	}
}

// TestParseAtlasHCLSchemaVarsFollowsAConditionalUnderAnIndex extends the
// branch rule past a top-level conditional.
//
// `(cond ? a.url : b.url)[0]` is an index over a conditional, and an index is
// one of the shapes that carries a selected URL through unchanged. Reading the
// flat variable list there reports both branches again, so two blocks selecting
// the same file with different vars were refused as ambiguous even though the
// predicate settles which one the desired state comes from.
//
// This is Ptah's own spelling, like the sibling in
// [TestParseAtlasHCLSchemaVarsRefusesOnlyEvaluatedAmbiguity]: a data source's
// `url` is a list of file:// URLs here and can be indexed, while the pinned
// Atlas community binary v1.3.0 mints one opaque URL per data source. The rule
// it pins is the one that IS oracle-backed at the top level, in
// [TestParseAtlasHCLSchemaVarsFollowsTheConditionalBranch].
func TestParseAtlasHCLSchemaVarsFollowsAConditionalUnderAnIndex(t *testing.T) {
	tests := []struct {
		name       string
		raw        string
		wantValues map[string]string
	}{
		{
			name: "a true predicate under an index takes the first branch",
			raw: `variable "use_app" {
  type    = bool
  default = true
}

data "hcl_schema" "app" {
  paths = ["s.hcl"]
  vars = {
    tenant = "acme"
  }
}

data "hcl_schema" "other" {
  paths = ["s.hcl"]
  vars = {
    tenant = "zzz"
  }
}

env "local" {
  url = "sqlite://file.db"
  src = (var.use_app ? data.hcl_schema.app.url : data.hcl_schema.other.url)[0]
}
`,
			wantValues: map[string]string{"tenant": "acme"},
		},
		{
			// A literal index parses as a traversal off the parenthesized
			// conditional; a computed one parses as an index expression. Both
			// wrap the same conditional, and each reaches this walk through a
			// different arm, so both are carried as rows.
			name: "a computed index over a true predicate takes the first branch",
			raw: `variable "use_app" {
  type    = bool
  default = true
}

variable "which" {
  type    = number
  default = 0
}

data "hcl_schema" "app" {
  paths = ["s.hcl"]
  vars = {
    tenant = "acme"
  }
}

data "hcl_schema" "other" {
  paths = ["s.hcl"]
  vars = {
    tenant = "zzz"
  }
}

env "local" {
  url = "sqlite://file.db"
  src = (var.use_app ? data.hcl_schema.app.url : data.hcl_schema.other.url)[var.which]
}
`,
			wantValues: map[string]string{"tenant": "acme"},
		},
		{
			name: "a false predicate under an index takes the second branch",
			raw: `variable "use_app" {
  type    = bool
  default = false
}

data "hcl_schema" "app" {
  paths = ["s.hcl"]
  vars = {
    tenant = "acme"
  }
}

data "hcl_schema" "other" {
  paths = ["s.hcl"]
  vars = {
    tenant = "zzz"
  }
}

env "local" {
  url = "sqlite://file.db"
  src = (var.use_app ? data.hcl_schema.app.url : data.hcl_schema.other.url)[0]
}
`,
			wantValues: map[string]string{"tenant": "zzz"},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			cfg, err := projectconfig.ParseAtlas([]byte(test.raw), "atlas.hcl", "local")

			c.Assert(err, qt.IsNil)
			c.Assert(cfg.SchemaSources, qt.DeepEquals, []string{"file://s.hcl"})
			values, scoped := cfg.SchemaSourceVars("file://s.hcl")
			c.Check(scoped, qt.Equals, true)
			c.Check(values, qt.DeepEquals, test.wantValues)
		})
	}
}

// TestParseAtlasHCLSchemaVarsRefusesAmbiguousOwnershipAcrossSpellings closes
// the same ambiguity under two spellings of one path.
//
// `path = "s.hcl"` and `path = "./s.hcl"` are different strings and the same
// file. The scope is filed under both the project file's spelling and the
// base-directory resolved one, because commands ask with either; the resolved
// key is where the two spellings meet, so it is checked for conflicts too.
// Filing it unchecked let the second block's values replace the first's under
// the key every command reads.
func TestParseAtlasHCLSchemaVarsRefusesAmbiguousOwnershipAcrossSpellings(t *testing.T) {
	c := qt.New(t)
	raw := `data "hcl_schema" "app" {
  path = "s.hcl"
  vars = {
    tenant = "acme"
  }
}

data "hcl_schema" "other" {
  path = "./s.hcl"
  vars = {
    tenant = "zzz"
  }
}

env "local" {
  url = "sqlite://file.db"
  src = [data.hcl_schema.app.url, data.hcl_schema.other.url]
}
`

	_, err := projectconfig.ParseAtlas([]byte(raw), "atlas.hcl", "local")

	c.Assert(err, qt.ErrorMatches,
		`atlas.hcl data.hcl_schema "app" and "other" both select "file://\./s\.hcl" with different vars at atlas.hcl:\d+`)
}

// TestParseAtlasHCLSchemaVarsRefusesOnlyEvaluatedAmbiguity keeps the refusal
// scoped to the files a run actually reads.
//
// Both blocks below select shared.hcl, but the env selects one path out of each
// block and shared.hcl is not among them: no source carries a scope for it, so
// the two blocks have nothing to disagree about and the project is determined.
// The refusal used to be decided over every URL a referenced block could mint,
// which rejected this file.
//
// This arm is Ptah's own spelling rather than a parity row: a data source's
// `url` is a list of file:// URLs here, so it can be indexed, while the pinned
// binary mints one opaque URL per data source and has no `url[0]`. The rule it
// pins is internal: a URL that scopes nothing cannot make a project ambiguous.
// The genuine collision stays refused in
// [TestParseAtlasHCLSchemaVarsRefusesAmbiguousOwnership].
func TestParseAtlasHCLSchemaVarsRefusesOnlyEvaluatedAmbiguity(t *testing.T) {
	c := qt.New(t)
	raw := `data "hcl_schema" "app" {
  paths = ["a.hcl", "shared.hcl"]
  vars = {
    tenant = "acme"
  }
}

data "hcl_schema" "other" {
  paths = ["b.hcl", "shared.hcl"]
  vars = {
    tenant = "zzz"
  }
}

env "local" {
  url = "sqlite://file.db"
  src = [data.hcl_schema.app.url[0], data.hcl_schema.other.url[0]]
}
`

	cfg, err := projectconfig.ParseAtlas([]byte(raw), "atlas.hcl", "local")

	c.Assert(err, qt.IsNil)
	c.Assert(cfg.SchemaSources, qt.DeepEquals, []string{"file://a.hcl", "file://b.hcl"})
	appValues, appScoped := cfg.SchemaSourceVars("file://a.hcl")
	c.Check(appScoped, qt.Equals, true)
	c.Check(appValues, qt.DeepEquals, map[string]string{"tenant": "acme"})
	otherValues, otherScoped := cfg.SchemaSourceVars("file://b.hcl")
	c.Check(otherScoped, qt.Equals, true)
	c.Check(otherValues, qt.DeepEquals, map[string]string{"tenant": "zzz"})
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

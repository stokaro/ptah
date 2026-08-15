package atlassource_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/config/projectconfig"
	"go.5x5.cz/ptah/internal/atlassource"
)

// TestClassifySetCarriesTheProjectVariableScope pins the join stokaro/ptah#934
// item 4 needs: the scope an atlas.hcl `data "hcl_schema"` block puts around its
// files has to survive classification, because that is the only layer between
// the project file and the loader that sees every desired-state URL.
//
// It matters that this runs for URLs that arrive ALREADY classified rather than
// through env:// expansion: `schema apply`, `schema diff` and `migrate diff`
// resolve the project's schema sources against the atlas.hcl directory and hand
// the resulting file:// URLs to --to, so a scope attached only on the env://
// path would never reach them.
func TestClassifySetCarriesTheProjectVariableScope(t *testing.T) {
	tests := []struct {
		name       string
		raw        string
		wantValues map[string]string
		wantScoped bool
	}{
		{
			name: "a data source's vars reach the source it selects",
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
			wantValues: map[string]string{"tenant": "acme"},
			wantScoped: true,
		},
		{
			// The rule (a) half. A data source with no vars closes the boundary,
			// so the run's --var must not reach the file behind it. Measured on
			// the pinned Atlas community binary v1.3.0: that shape with
			// `--var tenant=acme` is exit 1, `missing value for required
			// variable "tenant"`.
			name: "a data source with no vars still closes the boundary",
			raw: `data "hcl_schema" "app" {
  paths = ["s.hcl"]
}

env "local" {
  url = "sqlite://file.db"
  src = data.hcl_schema.app.url
}
`,
			wantValues: nil,
			wantScoped: true,
		},
		{
			// The control. The same file reached without a data source keeps the
			// run's --var: that shape is exit 0 on the pinned binary with the
			// same flag.
			name: "a literal source is left unscoped",
			raw: `env "local" {
  url = "sqlite://file.db"
  src = "file://s.hcl"
}
`,
			wantValues: nil,
			wantScoped: false,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			cfg, err := projectconfig.ParseAtlas([]byte(test.raw), "atlas.hcl", "local")
			c.Assert(err, qt.IsNil)
			env := atlassource.ProjectEnv{Loaded: true, Config: cfg, BaseDir: "."}

			set, err := atlassource.ClassifySet("--to", cfg.SchemaSources, env)

			c.Assert(err, qt.IsNil)
			c.Assert(set.Sources, qt.HasLen, 1)
			c.Check(set.Sources[0].VarsScoped, qt.Equals, test.wantScoped)
			c.Check(set.Sources[0].VarValues, qt.DeepEquals, test.wantValues)
		})
	}
}

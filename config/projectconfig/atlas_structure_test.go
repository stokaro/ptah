package projectconfig_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/config/projectconfig"
)

func TestParseAtlasProjectConfigRejectsUnsupportedEnvStructureRegardlessOfSelection(t *testing.T) {
	c := qt.New(t)

	tests := []struct {
		name    string
		body    string
		wantErr string
	}{
		{
			name: "migration nested block",
			body: `migration {
    remote {}
  }`,
			wantErr: `unsupported atlas\.hcl construct "remote" at atlas\.hcl:[0-9]+`,
		},
		{
			name: "schema mode nested block",
			body: `schema {
    mode {
      custom {}
    }
  }`,
			wantErr: `unsupported atlas\.hcl construct "custom" at atlas\.hcl:[0-9]+`,
		},
		{
			name: "schema repo nested block",
			body: `schema {
    repo {
      cloud {}
    }
  }`,
			wantErr: `unsupported atlas\.hcl construct "cloud" at atlas\.hcl:[0-9]+`,
		},
		{
			name: "migrate format unknown attribute",
			body: `format {
    migrate {
      custom = "template"
    }
  }`,
			wantErr: `unsupported atlas\.hcl construct "custom" at atlas\.hcl:[0-9]+`,
		},
		{
			name: "migrate format nested block",
			body: `format {
    migrate {
      custom {}
    }
  }`,
			wantErr: `unsupported atlas\.hcl construct "custom" at atlas\.hcl:[0-9]+`,
		},
		{
			name: "schema format unknown attribute",
			body: `format {
    schema {
      custom = "template"
    }
  }`,
			wantErr: `unsupported atlas\.hcl construct "custom" at atlas\.hcl:[0-9]+`,
		},
		{
			name: "schema format nested block",
			body: `format {
    schema {
      custom {}
    }
  }`,
			wantErr: `unsupported atlas\.hcl construct "custom" at atlas\.hcl:[0-9]+`,
		},
		{
			name: "lint concurrent index nested block",
			body: `lint {
    concurrent_index {
      custom {}
    }
  }`,
			wantErr: `unsupported atlas\.hcl construct "custom" at atlas\.hcl:[0-9]+`,
		},
		{
			name: "lint constraint drop nested block",
			body: `lint {
    condrop {
      custom {}
    }
  }`,
			wantErr: `unsupported atlas\.hcl construct "custom" at atlas\.hcl:[0-9]+`,
		},
		{
			name: "lint data dependency nested block",
			body: `lint {
    data_depend {
      custom {}
    }
  }`,
			wantErr: `unsupported atlas\.hcl construct "custom" at atlas\.hcl:[0-9]+`,
		},
		{
			name: "lint destructive nested block",
			body: `lint {
    destructive {
      custom {}
    }
  }`,
			wantErr: `unsupported atlas\.hcl construct "custom" at atlas\.hcl:[0-9]+`,
		},
		{
			name: "lint incompatible nested block",
			body: `lint {
    incompatible {
      custom {}
    }
  }`,
			wantErr: `unsupported atlas\.hcl construct "custom" at atlas\.hcl:[0-9]+`,
		},
		{
			name: "lint nested transaction nested block",
			body: `lint {
    nestedtx {
      custom {}
    }
  }`,
			wantErr: `unsupported atlas\.hcl construct "custom" at atlas\.hcl:[0-9]+`,
		},
		{
			name: "lint git nested block",
			body: `lint {
    git {
      remote {}
    }
  }`,
			wantErr: `unsupported atlas\.hcl construct "remote" at atlas\.hcl:[0-9]+`,
		},
		{
			name: "diff skip nested block",
			body: `diff {
    skip {
      custom {}
    }
  }`,
			wantErr: `unsupported atlas\.hcl construct "custom" at atlas\.hcl:[0-9]+`,
		},
		{
			name: "diff concurrent index nested block",
			body: `diff {
    concurrent_index {
      custom {}
    }
  }`,
			wantErr: `unsupported atlas\.hcl construct "custom" at atlas\.hcl:[0-9]+`,
		},
	}

	for _, test := range tests {
		c.Run(test.name, func(c *qt.C) {
			raw := []byte(`env "selected" {
  url = "sqlite://selected.db"
}
env "other" {
  ` + test.body + `
}
`)

			_, err := projectconfig.ParseAtlas(raw, "atlas.hcl", "selected")
			c.Assert(err, qt.ErrorMatches, test.wantErr)

			_, err = projectconfig.ParseAtlas(raw, "atlas.hcl", "other")
			c.Assert(err, qt.ErrorMatches, test.wantErr)
		})
	}
}

func TestParseAtlasProjectConfigSkipsIgnoredExpressionEvaluationInUnselectedEnv(t *testing.T) {
	c := qt.New(t)

	tests := []struct {
		name string
		body string
	}{
		{
			name: "attribute",
			body: `project = missing.value`,
		},
		{
			name: "block body",
			body: `cloud {
    value = missing.value
  }`,
		},
	}

	for _, test := range tests {
		c.Run(test.name, func(c *qt.C) {
			raw := []byte(`env "selected" {
	  url = "sqlite://selected.db"
	}
	env "other" {
	  ` + test.body + `
}
`)

			cfg, err := projectconfig.ParseAtlas(raw, "atlas.hcl", "selected")

			c.Assert(err, qt.IsNil)
			c.Assert(cfg.DatabaseURL, qt.Equals, "sqlite://selected.db")
			c.Assert(cfg.IgnoredConstructs, qt.HasLen, 1)
		})
	}
}

func TestParseAtlasProjectConfigToleratesOpenEnvBodiesRegardlessOfSelection(t *testing.T) {
	c := qt.New(t)

	tests := []struct {
		name string
		body string
	}{
		{name: "env attribute", body: `custom = "literal"`},
		{name: "env block", body: `custom {}`},
		{name: "diff attribute", body: `diff { custom = "literal" }`},
		{name: "diff block", body: `diff {
    custom {}
  }`},
		{name: "format attribute", body: `format { custom = "literal" }`},
		{name: "format block", body: `format {
    custom {}
  }`},
		{name: "lint attribute", body: `lint { custom = "literal" }`},
		{name: "lint block", body: `lint {
    custom {}
  }`},
		{name: "schema attribute", body: `schema { custom = "literal" }`},
		{name: "schema block", body: `schema {
    custom {}
  }`},
	}

	for _, test := range tests {
		c.Run(test.name, func(c *qt.C) {
			raw := []byte(`env "selected" {
  url = "sqlite://selected.db"
}
env "other" {
  ` + test.body + `
}
`)

			selected, err := projectconfig.ParseAtlas(raw, "atlas.hcl", "other")
			c.Assert(err, qt.IsNil)
			c.Assert(selected.IgnoredConstructs, qt.HasLen, 1)

			unselected, err := projectconfig.ParseAtlas(raw, "atlas.hcl", "selected")
			c.Assert(err, qt.IsNil)
			c.Assert(unselected.IgnoredConstructs, qt.HasLen, 1)
		})
	}
}

func TestParseAtlasProjectConfigToleratesUnknownLeafAttributesRegardlessOfSelection(t *testing.T) {
	c := qt.New(t)

	tests := []struct {
		name string
		body string
	}{
		{
			name: "schema mode",
			body: `schema {
    mode {
      custom = "literal"
    }
  }`,
		},
		{
			name: "diff skip",
			body: `diff {
    skip {
      custom = "literal"
    }
  }`,
		},
		{
			name: "diff concurrent index",
			body: `diff {
    concurrent_index {
      custom = "literal"
    }
  }`,
		},
		{
			name: "lint analyzer",
			body: `lint {
    destructive {
      custom = "literal"
    }
  }`,
		},
		{
			name: "lint git",
			body: `lint {
    git {
      custom = "literal"
    }
  }`,
		},
		{
			name: "migration",
			body: `migration {
    custom = "literal"
  }`,
		},
		{
			name: "schema repository",
			body: `schema {
    repo {
      custom = "literal"
    }
  }`,
		},
	}

	for _, test := range tests {
		c.Run(test.name, func(c *qt.C) {
			raw := []byte(`env "selected" {
  url = "sqlite://selected.db"
}
env "other" {
  ` + test.body + `
}
`)

			selected, err := projectconfig.ParseAtlas(raw, "atlas.hcl", "other")
			c.Assert(err, qt.IsNil)
			c.Assert(selected.IgnoredConstructs, qt.HasLen, 1)

			unselected, err := projectconfig.ParseAtlas(raw, "atlas.hcl", "selected")
			c.Assert(err, qt.IsNil)
			c.Assert(unselected.IgnoredConstructs, qt.HasLen, 1)
		})
	}
}

func TestParseAtlasProjectConfigClassifiesSensitiveModeRegardlessOfSelection(t *testing.T) {
	c := qt.New(t)
	raw := []byte(`env "selected" {
  schema {
    mode {
      sensitive = "ALLOW"
    }
  }
}
env "other" {
  schema {
    mode {
      sensitive = DENY
    }
  }
}
`)
	want := []projectconfig.IgnoredAtlasConstruct{
		{Name: "sensitive", Kind: "attribute", Filename: "atlas.hcl", Line: 4},
		{Name: "sensitive", Kind: "attribute", Filename: "atlas.hcl", Line: 11},
	}

	selected, err := projectconfig.ParseAtlas(raw, "atlas.hcl", "selected")
	c.Assert(err, qt.IsNil)
	c.Assert(selected.IgnoredConstructs, qt.DeepEquals, want)

	other, err := projectconfig.ParseAtlas(raw, "atlas.hcl", "other")
	c.Assert(err, qt.IsNil)
	c.Assert(other.IgnoredConstructs, qt.DeepEquals, want)
}

func TestParseAtlasProjectConfigEvaluatesIgnoredExpressionsInSelectedEnv(t *testing.T) {
	c := qt.New(t)

	tests := []struct {
		name    string
		body    string
		wantErr string
	}{
		{
			name:    "attribute",
			body:    `project = missing.value`,
			wantErr: `cannot evaluate atlas\.hcl "project" at atlas\.hcl:[0-9]+: .*Unknown variable.*`,
		},
		{
			name: "block body",
			body: `cloud {
    value = missing.value
  }`,
			wantErr: `cannot evaluate atlas\.hcl "value" at atlas\.hcl:[0-9]+: .*Unknown variable.*`,
		},
		{
			name: "schema mode attribute",
			body: `schema {
    mode {
      custom = missing.value
    }
  }`,
			wantErr: `cannot evaluate atlas\.hcl "custom" at atlas\.hcl:[0-9]+: .*Unknown variable.*`,
		},
		{
			name: "diff skip attribute",
			body: `diff {
    skip {
      custom = missing.value
    }
  }`,
			wantErr: `cannot evaluate atlas\.hcl "custom" at atlas\.hcl:[0-9]+: .*Unknown variable.*`,
		},
		{
			name: "diff concurrent index attribute",
			body: `diff {
    concurrent_index {
      custom = missing.value
    }
  }`,
			wantErr: `cannot evaluate atlas\.hcl "custom" at atlas\.hcl:[0-9]+: .*Unknown variable.*`,
		},
	}

	for _, test := range tests {
		c.Run(test.name, func(c *qt.C) {
			raw := []byte(`env "selected" {
  url = "sqlite://selected.db"
}
env "other" {
  ` + test.body + `
}
`)

			_, err := projectconfig.ParseAtlas(raw, "atlas.hcl", "other")

			c.Assert(err, qt.ErrorMatches, test.wantErr)
		})
	}
}

func TestParseAtlasProjectConfigRecordsIgnoredConstructsOnceAcrossAllEnvs(t *testing.T) {
	c := qt.New(t)
	raw := []byte(`env "selected" {
  url     = "sqlite://selected.db"
  project = "local"
}
env "other" {
  cloud {}
}
`)

	cfg, err := projectconfig.ParseAtlas(raw, "atlas.hcl", "selected")

	c.Assert(err, qt.IsNil)
	c.Assert(cfg.IgnoredConstructs, qt.DeepEquals, []projectconfig.IgnoredAtlasConstruct{
		{Name: "project", Kind: "attribute", Filename: "atlas.hcl", Line: 3},
		{Name: "cloud", Kind: "block", Filename: "atlas.hcl", Line: 6},
	})
}

func TestParseAtlasProjectConfigOrdersIgnoredGlobalLintAttributesDeterministically(t *testing.T) {
	c := qt.New(t)
	raw := []byte(`lint {
  zebra = "last"
  alpha = "first"
}
`)

	cfg, err := projectconfig.ParseAtlas(raw, "atlas.hcl", "")

	c.Assert(err, qt.IsNil)
	c.Assert(cfg.IgnoredConstructs, qt.DeepEquals, []projectconfig.IgnoredAtlasConstruct{
		{Name: "alpha", Kind: "attribute", Filename: "atlas.hcl", Line: 3},
		{Name: "zebra", Kind: "attribute", Filename: "atlas.hcl", Line: 2},
	})
}

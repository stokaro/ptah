package projectconfig_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/config/projectconfig"
)

// parseLintNaming parses a project file and returns its naming block.
func parseLintNaming(c *qt.C, raw string) *projectconfig.LintNamingConfig {
	c.Helper()
	cfg, err := projectconfig.ParseAtlas([]byte(raw), "atlas.hcl", "local")
	c.Assert(err, qt.IsNil)
	return cfg.Lint.Naming
}

// TestParseAtlas_LintNamingBlockCarriesTheConvention reads the Atlas naming
// analyzer's block: a default pattern, one block per kind, and `error`.
func TestParseAtlas_LintNamingBlockCarriesTheConvention(t *testing.T) {
	c := qt.New(t)

	naming := parseLintNaming(c, `env "local" {
  url = "sqlite://app.db"

  lint {
    naming {
      error   = true
      match   = "^[a-z]+$"
      message = "must be lowercase"
      index {
        match   = "^[a-z]+_idx$"
        message = "must end with _idx"
      }
      foreign_key {
        match = "^fk_"
      }
    }
  }
}
`)

	c.Assert(naming, qt.DeepEquals, &projectconfig.LintNamingConfig{
		Match:      "^[a-z]+$",
		Message:    "must be lowercase",
		Error:      true,
		Index:      &projectconfig.LintNamingPattern{Match: "^[a-z]+_idx$", Message: "must end with _idx"},
		ForeignKey: &projectconfig.LintNamingPattern{Match: "^fk_"},
	})
}

func TestParseAtlas_LintNamingBlockIsAbsentWhenNotWritten(t *testing.T) {
	c := qt.New(t)

	naming := parseLintNaming(c, `env "local" {
  url = "sqlite://app.db"
  lint {
    destructive {
      error = false
    }
  }
}
`)

	c.Assert(naming, qt.IsNil)
}

// TestParseAtlas_LintNamingBlockRefusesWhatItCannotHold: a second naming
// block, a labeled one, and a repeated kind are structure errors, the same
// answer every other lint block gives.
func TestParseAtlas_LintNamingBlockRefusesWhatItCannotHold(t *testing.T) {
	tests := []struct {
		name string
		raw  string
	}{
		{
			name: "two naming blocks",
			raw:  "env \"local\" {\n  url = \"sqlite://app.db\"\n  lint {\n    naming { match = \"^a\" }\n    naming { match = \"^b\" }\n  }\n}\n",
		},
		{
			name: "a labeled naming block",
			raw:  "env \"local\" {\n  url = \"sqlite://app.db\"\n  lint {\n    naming \"x\" { match = \"^a\" }\n  }\n}\n",
		},
		{
			name: "a kind block written twice",
			raw:  "env \"local\" {\n  url = \"sqlite://app.db\"\n  lint {\n    naming {\n      table { match = \"^a\" }\n      table { match = \"^b\" }\n    }\n  }\n}\n",
		},
		{
			name: "a match that is not a string",
			raw:  "env \"local\" {\n  url = \"sqlite://app.db\"\n  lint {\n    naming { match = 1 }\n  }\n}\n",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			_, err := projectconfig.ParseAtlas([]byte(test.raw), "atlas.hcl", "local")
			c.Assert(err, qt.IsNotNil)
		})
	}
}

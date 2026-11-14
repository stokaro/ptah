package projectconfig_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/config/projectconfig"
)

// excludeOriginPtah is a ptah.yaml whose top-level exclude list an atlas.hcl
// env may replace.
const excludeOriginPtah = "exclude: [audit_log]\n"

// TestExcludeOrigin_HappyPath names the file whose exclude list the merged
// config carries. The list and its origin come from the same file: an
// atlas.hcl env that sets `exclude`, an empty list included, takes both, and
// one that does not leaves the ptah.yaml list and its origin in place.
func TestExcludeOrigin_HappyPath(t *testing.T) {
	tests := []struct {
		name        string
		atlas       string
		wantExclude []string
		wantOrigin  string
	}{
		{
			name:        "an env that names no exclude list",
			atlas:       "env \"prod\" {\n  url = \"sqlite://app.db\"\n}\n",
			wantExclude: []string{"audit_log"},
			wantOrigin:  projectconfig.PtahFileName,
		},
		{
			name:        "an env exclude list replaces the ptah.yaml list",
			atlas:       "env \"prod\" {\n  exclude = [\"reporting_cache\"]\n}\n",
			wantExclude: []string{"reporting_cache"},
			wantOrigin:  projectconfig.AtlasFileName,
		},
		{
			name:        "an empty env exclude list replaces it too",
			atlas:       "env \"prod\" {\n  exclude = []\n}\n",
			wantExclude: make([]string, 0),
			wantOrigin:  projectconfig.AtlasFileName,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			base, err := projectconfig.ParsePtah([]byte(excludeOriginPtah), projectconfig.PtahFileName, "prod")
			c.Assert(err, qt.IsNil)
			override, err := projectconfig.ParseAtlas([]byte(test.atlas), projectconfig.AtlasFileName, "prod")
			c.Assert(err, qt.IsNil)

			cfg := projectconfig.Merge(base, override)

			c.Assert(cfg.Exclude, qt.DeepEquals, test.wantExclude)
			c.Assert(cfg.ExcludeOrigin, qt.Equals, test.wantOrigin)
		})
	}
}

// TestExcludeOrigin_NamesTheFileAsSpelled records the path the caller gave
// the parser, so a list read through `--config conf/staging.yaml` is reported
// against that file rather than the conventional name.
func TestExcludeOrigin_NamesTheFileAsSpelled(t *testing.T) {
	t.Run("ptah.yaml under another name", func(t *testing.T) {
		c := qt.New(t)

		cfg, err := projectconfig.ParsePtah([]byte(excludeOriginPtah), "conf/staging.yaml", "")

		c.Assert(err, qt.IsNil)
		c.Assert(cfg.ExcludeOrigin, qt.Equals, "conf/staging.yaml")
	})
	t.Run("atlas.hcl in another directory", func(t *testing.T) {
		c := qt.New(t)

		cfg, err := projectconfig.ParseAtlas(
			[]byte("env \"prod\" {\n  exclude = [\"reporting_cache\"]\n}\n"), "infra/atlas.hcl", "prod",
		)

		c.Assert(err, qt.IsNil)
		c.Assert(cfg.ExcludeOrigin, qt.Equals, "infra/atlas.hcl")
	})
}

// TestExcludeOrigin_NoList leaves the origin empty when no file sets a list.
func TestExcludeOrigin_NoList(t *testing.T) {
	c := qt.New(t)

	cfg, err := projectconfig.ParsePtah([]byte("url: sqlite://app.db\n"), projectconfig.PtahFileName, "")

	c.Assert(err, qt.IsNil)
	c.Assert(cfg.Exclude, qt.IsNil)
	c.Assert(cfg.ExcludeOrigin, qt.Equals, "")
}

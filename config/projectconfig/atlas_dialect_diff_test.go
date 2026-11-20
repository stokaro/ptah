package projectconfig_test

import (
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/config/projectconfig"
)

// dialectDiffProject parses an atlas.hcl whose top level is blocks and whose
// one env is the plain `dev` the load selects.
func dialectDiffProject(c *qt.C, blocks, env string, opts projectconfig.AtlasLoadOptions) (projectconfig.Config, error) {
	c.Helper()
	opts.EnvName = "dev"
	return projectconfig.ParseAtlasWithOptions([]byte(blocks+"\nenv \"dev\" {\n  url = \"clickhouse://localhost/app\"\n"+env+"}\n"),
		filepath.Join(c.TempDir(), "atlas.hcl"), opts)
}

func TestParseAtlas_DialectDiffBlock_HappyPath(t *testing.T) {
	tests := []struct {
		name   string
		blocks string
		want   bool
		set    bool
	}{
		{name: "allowed", blocks: "diff \"clickhouse\" {\n  allow_recreate = true\n}\n", want: true, set: true},
		{name: "an explicit false", blocks: "diff \"clickhouse\" {\n  allow_recreate = false\n}\n", want: false, set: true},
		{name: "the dialect's alias", blocks: "diff \"ch\" {\n  allow_recreate = true\n}\n", want: true, set: true},
		{name: "a block that says nothing", blocks: "diff \"clickhouse\" {\n}\n", want: false, set: false},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			cfg, err := dialectDiffProject(c, test.blocks, "", projectconfig.AtlasLoadOptions{})

			c.Assert(err, qt.IsNil)
			c.Assert(cfg.Diff.TableRebuildAllowed("clickhouse"), qt.Equals, test.want)
			c.Assert(cfg.Diff.AllowRecreate["clickhouse"].Set, qt.Equals, test.set)
			c.Assert(cfg.Diff.TableRebuildAllowed("postgres"), qt.IsFalse)
		})
	}

	t.Run("an unlabeled diff block and a labeled one are both read", func(t *testing.T) {
		c := qt.New(t)

		cfg, err := dialectDiffProject(c,
			"diff {\n  skip {\n    drop_table = true\n  }\n}\ndiff \"clickhouse\" {\n  allow_recreate = true\n}\n", "",
			projectconfig.AtlasLoadOptions{})

		c.Assert(err, qt.IsNil)
		c.Assert(cfg.Diff.Skip.DropTable.Value, qt.IsTrue)
		c.Assert(cfg.Diff.TableRebuildDialects(), qt.DeepEquals, []string{"clickhouse"})
	})

	t.Run("an attribute and a block it does not read are reported as ignored", func(t *testing.T) {
		c := qt.New(t)

		cfg, err := dialectDiffProject(c,
			"diff \"clickhouse\" {\n  allow_recreate = true\n  allow_recreation = true\n  extra {\n  }\n}\n", "",
			projectconfig.AtlasLoadOptions{})

		c.Assert(err, qt.IsNil)
		c.Assert(cfg.Diff.TableRebuildAllowed("clickhouse"), qt.IsTrue)
		c.Assert(cfg.IgnoredConstructs, qt.HasLen, 2)
	})

	t.Run("a later env value merges over the inherited one", func(t *testing.T) {
		c := qt.New(t)
		base := projectconfig.Config{Diff: projectconfig.DiffConfig{AllowRecreate: map[string]projectconfig.ConfigBool{
			"clickhouse": {Value: true, Set: true},
		}}}
		override := projectconfig.Config{Diff: projectconfig.DiffConfig{AllowRecreate: map[string]projectconfig.ConfigBool{
			"clickhouse": {Value: false, Set: true},
		}}}

		merged := projectconfig.Merge(base, override)

		c.Assert(merged.Diff.TableRebuildAllowed("clickhouse"), qt.IsFalse)
		c.Assert(base.Diff.TableRebuildAllowed("clickhouse"), qt.IsTrue)
	})
}

func TestParseAtlas_DialectDiffBlock_FailurePath(t *testing.T) {
	tests := []struct {
		name    string
		blocks  string
		wantErr string
	}{
		{
			name:    "a dialect with no table rebuild",
			blocks:  "diff \"mysql\" {\n  allow_recreate = true\n}\n",
			wantErr: `unsupported atlas.hcl construct "diff" at .*atlas.hcl:1`,
		},
		{
			name:    "an unknown dialect",
			blocks:  "diff \"nosuch\" {\n  allow_recreate = true\n}\n",
			wantErr: `unsupported atlas.hcl construct "diff" at .*atlas.hcl:1`,
		},
		{
			name:    "two labels",
			blocks:  "diff \"clickhouse\" \"extra\" {\n  allow_recreate = true\n}\n",
			wantErr: `unsupported atlas.hcl construct "diff" at .*atlas.hcl:1`,
		},
		{
			name:    "a value that is not a bool",
			blocks:  "diff \"clickhouse\" {\n  allow_recreate = \"yes\"\n}\n",
			wantErr: `(?s).*allow_recreate.*`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			cfg, err := dialectDiffProject(c, test.blocks, "", projectconfig.AtlasLoadOptions{})

			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(cfg.Diff.AllowRecreate, qt.IsNil)
		})
	}

	t.Run("a labeled diff block inside an env", func(t *testing.T) {
		c := qt.New(t)

		_, err := dialectDiffProject(c, "", "  diff \"clickhouse\" {\n    allow_recreate = true\n  }\n", projectconfig.AtlasLoadOptions{})

		c.Assert(err, qt.ErrorMatches, `unsupported atlas.hcl construct "diff" at .*atlas.hcl:4`)
	})
}

// The community binary accepts the block under any label and applies nothing
// from it, so a strict policy reads every one of them as ignored, the label no
// dialect rebuilds for included.
func TestParseAtlas_IgnoreDialectDiffBlocksAnswersAsCommunityEdition(t *testing.T) {
	tests := []struct {
		name   string
		blocks string
	}{
		{name: "a rebuild dialect", blocks: "diff \"clickhouse\" {\n  allow_recreate = true\n}\n"},
		{name: "a dialect with no table rebuild", blocks: "diff \"mysql\" {\n  allow_recreate = true\n}\n"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			cfg, err := dialectDiffProject(c, test.blocks, "", projectconfig.AtlasLoadOptions{IgnoreDialectDiffBlocks: true})

			c.Assert(err, qt.IsNil)
			c.Assert(cfg.Diff.TableRebuildDialects(), qt.HasLen, 0)
			c.Assert(cfg.IgnoredConstructs, qt.HasLen, 1)
			c.Assert(cfg.IgnoredConstructs[0].Kind, qt.Equals, "block")
			c.Assert(cfg.IgnoredConstructs[0].Name, qt.Equals, "diff")
		})
	}
}

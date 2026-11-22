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

	t.Run("an env block reports what it does not read", func(t *testing.T) {
		c := qt.New(t)

		cfg, err := dialectDiffProject(c, "",
			"  diff \"clickhouse\" {\n    allow_recreate = true\n    allow_recreation = true\n    extra {\n    }\n  }\n",
			projectconfig.AtlasLoadOptions{})

		c.Assert(err, qt.IsNil)
		c.Assert(cfg.Diff.TableRebuildAllowed("clickhouse"), qt.IsTrue)
		c.Assert(cfg.IgnoredConstructs, qt.HasLen, 2)
	})

	t.Run("an unlabeled and a labeled diff block in one env are both read", func(t *testing.T) {
		c := qt.New(t)

		cfg, err := dialectDiffProject(c, "",
			"  diff {\n    skip {\n      drop_table = true\n    }\n  }\n  diff \"clickhouse\" {\n    allow_recreate = true\n  }\n",
			projectconfig.AtlasLoadOptions{})

		c.Assert(err, qt.IsNil)
		c.Assert(cfg.Diff.Skip.DropTable.Value, qt.IsTrue)
		c.Assert(cfg.Diff.TableRebuildDialects(), qt.DeepEquals, []string{"clickhouse"})
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

// An env's diff "<dialect>" block is read as the top-level one is, and merges
// over it for its dialect: a value the env sets wins, and an env that sets
// none keeps the top-level one.
func TestParseAtlas_DialectDiffBlockInAnEnv_HappyPath(t *testing.T) {
	const allowed = "diff \"clickhouse\" {\n  allow_recreate = true\n}\n"
	const refused = "diff \"clickhouse\" {\n  allow_recreate = false\n}\n"
	tests := []struct {
		name   string
		blocks string
		env    string
		want   bool
		set    bool
	}{
		{name: "allowed in the env", env: "  " + allowed, want: true, set: true},
		{name: "the dialect's alias", env: "  diff \"ch\" {\n    allow_recreate = true\n  }\n", want: true, set: true},
		{name: "an env refusal over a top-level request", blocks: allowed, env: "  " + refused, want: false, set: true},
		{name: "an env request over a top-level refusal", blocks: refused, env: "  " + allowed, want: true, set: true},
		{name: "an env with no block keeps the top-level request", blocks: allowed, env: "", want: true, set: true},
		{name: "an env block that says nothing keeps the top-level request", blocks: allowed, env: "  diff \"clickhouse\" {\n  }\n", want: true, set: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			cfg, err := dialectDiffProject(c, test.blocks, test.env, projectconfig.AtlasLoadOptions{})

			c.Assert(err, qt.IsNil)
			c.Assert(cfg.Diff.TableRebuildAllowed("clickhouse"), qt.Equals, test.want)
			c.Assert(cfg.Diff.AllowRecreate["clickhouse"].Set, qt.Equals, test.set)
			c.Assert(cfg.Diff.TableRebuildAllowed("postgres"), qt.IsFalse)
			c.Assert(cfg.IgnoredConstructs, qt.HasLen, 0)
		})
	}
}

func TestParseAtlas_DialectDiffBlock_FailurePath(t *testing.T) {
	tests := []struct {
		name    string
		blocks  string
		env     string
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
		{
			name:    "two blocks for one dialect",
			blocks:  "diff \"clickhouse\" {\n  allow_recreate = true\n}\ndiff \"ch\" {\n  allow_recreate = false\n}\n",
			wantErr: `duplicate diff "ch" at .*atlas.hcl:4`,
		},
		{
			name:    "a skip block",
			blocks:  "diff \"clickhouse\" {\n  skip {\n    drop_table = true\n  }\n}\n",
			wantErr: `unsupported atlas.hcl construct "skip" at .*atlas.hcl:2: Ptah reads a skip block only in an unlabeled diff block, .*; move it there`,
		},
		{
			name:    "a concurrent_index block",
			blocks:  "diff \"clickhouse\" {\n  allow_recreate = true\n  concurrent_index {\n    create = true\n  }\n}\n",
			wantErr: `unsupported atlas.hcl construct "concurrent_index" at .*atlas.hcl:3: Ptah reads a concurrent_index block only in an unlabeled diff block, .*`,
		},
		{
			name:    "an env block for a dialect with no table rebuild",
			env:     "  diff \"mysql\" {\n    allow_recreate = true\n  }\n",
			wantErr: `unsupported atlas.hcl construct "diff" at .*atlas.hcl:4`,
		},
		{
			name:    "an env block with two labels",
			env:     "  diff \"clickhouse\" \"extra\" {\n    allow_recreate = true\n  }\n",
			wantErr: `unsupported atlas.hcl construct "diff" at .*atlas.hcl:4`,
		},
		{
			name:    "an env block whose value is not a bool",
			env:     "  diff \"clickhouse\" {\n    allow_recreate = \"yes\"\n  }\n",
			wantErr: `(?s).*allow_recreate.*`,
		},
		{
			name:    "an env block written twice",
			env:     "  diff \"clickhouse\" {\n    allow_recreate = true\n  }\n  diff \"clickhouse\" {\n    allow_recreate = false\n  }\n",
			wantErr: `duplicate diff "clickhouse" at .*atlas.hcl:7`,
		},
		{
			name:    "an env block written again under the dialect's alias",
			env:     "  diff \"clickhouse\" {\n    allow_recreate = true\n  }\n  diff \"ch\" {\n    allow_recreate = false\n  }\n",
			wantErr: `duplicate diff "ch" at .*atlas.hcl:7`,
		},
		{
			name:    "a skip block in an env block",
			env:     "  diff \"clickhouse\" {\n    skip {\n      drop_table = true\n    }\n  }\n",
			wantErr: `unsupported atlas.hcl construct "skip" at .*atlas.hcl:5: Ptah reads a skip block only in an unlabeled diff block, .*`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			cfg, err := dialectDiffProject(c, test.blocks, test.env, projectconfig.AtlasLoadOptions{})

			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(cfg.Diff.AllowRecreate, qt.IsNil)
			c.Assert(cfg.Diff.Skip.DropTable.Set, qt.IsFalse)
		})
	}
}

// The community binary plans no table rebuild, so a strict policy reads every
// labeled diff block as ignored, at the top level and in an env, the label no
// dialect rebuilds for included.
func TestParseAtlas_IgnoreDialectDiffBlocksAnswersAsCommunityEdition(t *testing.T) {
	tests := []struct {
		name   string
		blocks string
		env    string
	}{
		{name: "a rebuild dialect", blocks: "diff \"clickhouse\" {\n  allow_recreate = true\n}\n"},
		{name: "a dialect with no table rebuild", blocks: "diff \"mysql\" {\n  allow_recreate = true\n}\n"},
		{name: "a rebuild dialect in an env", env: "  diff \"clickhouse\" {\n    allow_recreate = true\n  }\n"},
		{name: "a dialect with no table rebuild in an env", env: "  diff \"mysql\" {\n    allow_recreate = true\n  }\n"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			cfg, err := dialectDiffProject(c, test.blocks, test.env, projectconfig.AtlasLoadOptions{IgnoreDialectDiffBlocks: true})

			c.Assert(err, qt.IsNil)
			c.Assert(cfg.Diff.TableRebuildDialects(), qt.HasLen, 0)
			c.Assert(cfg.IgnoredConstructs, qt.HasLen, 1)
			c.Assert(cfg.IgnoredConstructs[0].Kind, qt.Equals, "block")
			c.Assert(cfg.IgnoredConstructs[0].Name, qt.Equals, "diff")
		})
	}
}

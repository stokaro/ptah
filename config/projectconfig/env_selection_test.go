package projectconfig_test

import (
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/config/projectconfig"
)

// loneEnvWithTopLevelLint is the shape stokaro/ptah#5999 measured: one env
// naming a database, beside a top-level block that applies to every env.
const loneEnvWithTopLevelLint = `lint {
  latest = 3
}

env "prod" {
  url = "sqlite://prod.db"
}
`

// loneEnvWithTopLevelMigration is the same shape in ptah.yaml.
const loneEnvWithTopLevelMigration = `migration:
  exec_order: linear
env:
  prod:
    url: sqlite://prod.db
`

// TestParseAtlasWithOptionsEnvSelection_HappyPath holds what an EnvName
// selects. An empty name leaves the lone env unselected and reads only the
// top-level lint block, so a database the env names is reached only by a run
// that names the env.
func TestParseAtlasWithOptionsEnvSelection_HappyPath(t *testing.T) {
	tests := []struct {
		name       string
		envName    string
		wantEnv    string
		wantURL    projectconfig.Value[string]
		wantLatest projectconfig.Value[int]
	}{
		{
			name:       "an empty env name selects no env",
			wantURL:    projectconfig.Value[string]{},
			wantLatest: projectconfig.Value[int]{Value: 3, Present: true},
		},
		{
			name:       "a named env is selected",
			envName:    "prod",
			wantEnv:    "prod",
			wantURL:    projectconfig.Value[string]{Value: "sqlite://prod.db", Present: true},
			wantLatest: projectconfig.Value[int]{Value: 3, Present: true},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			cfg, err := projectconfig.ParseAtlasWithOptions(
				[]byte(loneEnvWithTopLevelLint),
				projectconfig.AtlasFileName,
				projectconfig.AtlasLoadOptions{EnvName: test.envName},
			)

			c.Assert(err, qt.IsNil)
			c.Assert(cfg.EnvName, qt.Equals, test.wantEnv)
			c.Assert(cfg.StringValue(projectconfig.StringDatabaseURL), qt.Equals, test.wantURL)
			c.Assert(cfg.LintLatestValue(), qt.Equals, test.wantLatest)
			c.Assert(cfg.DeclaredEnvs, qt.DeepEquals, []projectconfig.DeclaredEnv{
				{File: projectconfig.AtlasFileName, Name: "prod"},
			})
		})
	}
}

// TestParseAtlasWithOptionsEvaluatesNoUnnamedEnv_HappyPath holds that an env
// no name selects is not evaluated, so neither an expression that cannot
// evaluate nor a second env block stops the top-level read. The pinned
// community binary v1.3.0 reads a file named with --config and no --env this
// way: `env "prod" { url = nosuch() }` there exits 0.
func TestParseAtlasWithOptionsEvaluatesNoUnnamedEnv_HappyPath(t *testing.T) {
	tests := []struct {
		name         string
		data         string
		wantDeclared []projectconfig.DeclaredEnv
	}{
		{
			name:         "an env that cannot evaluate",
			data:         "lint {\n  latest = 3\n}\n\nenv \"prod\" {\n  url = nosuch()\n}\n",
			wantDeclared: []projectconfig.DeclaredEnv{{File: projectconfig.AtlasFileName, Name: "prod"}},
		},
		{
			name: "two envs",
			data: "lint {\n  latest = 3\n}\n\nenv \"b\" {\n  url = \"sqlite://b.db\"\n}\n\nenv \"a\" {\n  url = \"sqlite://a.db\"\n}\n",
			wantDeclared: []projectconfig.DeclaredEnv{
				{File: projectconfig.AtlasFileName, Name: "b"},
				{File: projectconfig.AtlasFileName, Name: "a"},
			},
		},
		{
			name: "an unlabeled env beside a labeled one",
			data: "lint {\n  latest = 3\n}\n\nenv {\n  name = atlas.env\n  url = \"sqlite://${atlas.env}.db\"\n}\n\nenv \"prod\" {\n  url = \"sqlite://prod.db\"\n}\n",
			wantDeclared: []projectconfig.DeclaredEnv{
				{File: projectconfig.AtlasFileName},
				{File: projectconfig.AtlasFileName, Name: "prod"},
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			cfg, err := projectconfig.ParseAtlasWithOptions(
				[]byte(test.data),
				projectconfig.AtlasFileName,
				projectconfig.AtlasLoadOptions{},
			)

			c.Assert(err, qt.IsNil)
			c.Assert(cfg.EnvName, qt.Equals, "")
			c.Assert(cfg.StringValue(projectconfig.StringDatabaseURL), qt.Equals, projectconfig.Value[string]{})
			c.Assert(cfg.LintLatestValue(), qt.Equals, projectconfig.Value[int]{Value: 3, Present: true})
			c.Assert(cfg.DeclaredEnvs, qt.DeepEquals, test.wantDeclared)
		})
	}
}

// TestParseAtlasWithOptionsNamedEnv_FailurePath is the control for the test
// above: naming the env evaluates it, so the passing rows there come from
// leaving it unnamed and not from a fixture nothing would refuse.
func TestParseAtlasWithOptionsNamedEnv_FailurePath(t *testing.T) {
	c := qt.New(t)

	cfg, err := projectconfig.ParseAtlasWithOptions(
		[]byte("env \"prod\" {\n  url = nosuch()\n}\n"),
		projectconfig.AtlasFileName,
		projectconfig.AtlasLoadOptions{EnvName: "prod"},
	)

	c.Assert(err, qt.ErrorMatches, `(?s).*Call to unknown function.*`)
	c.Assert(cfg.StringValue(projectconfig.StringDatabaseURL), qt.Equals, projectconfig.Value[string]{})
}

// TestParsePtahEnvSelection_HappyPath is the ptah.yaml half of
// stokaro/ptah#5999: an empty env name selects no env, however many the file
// declares, and keeps the top-level settings.
func TestParsePtahEnvSelection_HappyPath(t *testing.T) {
	tests := []struct {
		name    string
		envName string
		wantEnv string
		wantURL projectconfig.Value[string]
	}{
		{
			name:    "an empty env name selects no env",
			wantURL: projectconfig.Value[string]{},
		},
		{
			name:    "a named env is selected",
			envName: "prod",
			wantEnv: "prod",
			wantURL: projectconfig.Value[string]{Value: "sqlite://prod.db", Present: true},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			cfg, err := projectconfig.ParsePtah([]byte(loneEnvWithTopLevelMigration), "", test.envName)

			c.Assert(err, qt.IsNil)
			c.Assert(cfg.EnvName, qt.Equals, test.wantEnv)
			c.Assert(cfg.StringValue(projectconfig.StringDatabaseURL), qt.Equals, test.wantURL)
			c.Assert(cfg.Migration.ExecOrder, qt.Equals, "linear")
			c.Assert(cfg.DeclaredEnvs, qt.DeepEquals, []projectconfig.DeclaredEnv{
				{File: projectconfig.PtahFileName, Name: "prod"},
			})
		})
	}
}

// TestParsePtahDeclaredEnvsWithoutSelection_HappyPath holds the list a caller
// names when it refuses for want of an env: every env of the file in name
// order, and none for a file that declares no env.
func TestParsePtahDeclaredEnvsWithoutSelection_HappyPath(t *testing.T) {
	tests := []struct {
		name         string
		data         string
		wantURL      projectconfig.Value[string]
		wantDeclared []projectconfig.DeclaredEnv
	}{
		{
			name: "two envs",
			data: "env:\n  prod:\n    url: sqlite://prod.db\n  dev:\n    url: sqlite://dev.db\n",
			wantDeclared: []projectconfig.DeclaredEnv{
				{File: "conf/ptah.yaml", Name: "dev"},
				{File: "conf/ptah.yaml", Name: "prod"},
			},
		},
		{
			name:    "a top-level url and no env",
			data:    "url: sqlite://app.db\n",
			wantURL: projectconfig.Value[string]{Value: "sqlite://app.db", Present: true},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			cfg, err := projectconfig.ParsePtah([]byte(test.data), "conf/ptah.yaml", "")

			c.Assert(err, qt.IsNil)
			c.Assert(cfg.EnvName, qt.Equals, "")
			c.Assert(cfg.StringValue(projectconfig.StringDatabaseURL), qt.Equals, test.wantURL)
			c.Assert(cfg.DeclaredEnvs, qt.DeepEquals, test.wantDeclared)
		})
	}
}

// TestLoadEnvSelection_HappyPath drives both files through Load, the entry the
// native commands take: with no env name neither file's lone env applies, and
// the declared envs of both are kept, ptah.yaml first.
func TestLoadEnvSelection_HappyPath(t *testing.T) {
	c := qt.New(t)
	dir := c.TempDir()
	ptahPath := filepath.Join(dir, projectconfig.PtahFileName)
	atlasPath := filepath.Join(dir, projectconfig.AtlasFileName)
	c.Assert(os.WriteFile(ptahPath, []byte("env:\n  dev:\n    url: sqlite://dev.db\n"), 0o600), qt.IsNil)
	c.Assert(os.WriteFile(atlasPath, []byte(loneEnvWithTopLevelLint), 0o600), qt.IsNil)

	cfg, err := projectconfig.Load(projectconfig.LoadOptions{PtahPath: ptahPath, AtlasPath: atlasPath})

	c.Assert(err, qt.IsNil)
	c.Assert(cfg.EnvName, qt.Equals, "")
	c.Assert(cfg.StringValue(projectconfig.StringDatabaseURL), qt.Equals, projectconfig.Value[string]{})
	c.Assert(cfg.DeclaredEnvs, qt.DeepEquals, []projectconfig.DeclaredEnv{
		{File: ptahPath, Name: "dev"},
		{File: atlasPath, Name: "prod"},
	})
}

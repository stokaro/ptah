package projectconfig_test

import (
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

// TestParseAtlasWithOptionsNoImplicitEnv_HappyPath holds what an empty EnvName
// selects. With NoImplicitEnv the lone env stays unselected and only the
// top-level lint block is read; without it the lone env is selected, which is
// the contract the native loaders keep.
func TestParseAtlasWithOptionsNoImplicitEnv_HappyPath(t *testing.T) {
	tests := []struct {
		name       string
		opts       projectconfig.AtlasLoadOptions
		wantURL    projectconfig.Value[string]
		wantLatest projectconfig.Value[int]
	}{
		{
			name:       "an empty env name selects no env",
			opts:       projectconfig.AtlasLoadOptions{NoImplicitEnv: true},
			wantURL:    projectconfig.Value[string]{},
			wantLatest: projectconfig.Value[int]{Value: 3, Present: true},
		},
		{
			name:       "a named env is selected",
			opts:       projectconfig.AtlasLoadOptions{NoImplicitEnv: true, EnvName: "prod"},
			wantURL:    projectconfig.Value[string]{Value: "sqlite://prod.db", Present: true},
			wantLatest: projectconfig.Value[int]{Value: 3, Present: true},
		},
		{
			name:       "the zero value selects the lone env",
			opts:       projectconfig.AtlasLoadOptions{},
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
				test.opts,
			)

			c.Assert(err, qt.IsNil)
			c.Assert(cfg.StringValue(projectconfig.StringDatabaseURL), qt.Equals, test.wantURL)
			c.Assert(cfg.LintLatestValue(), qt.Equals, test.wantLatest)
		})
	}
}

// TestParseAtlasWithOptionsNoImplicitEnvEvaluatesNoEnv_HappyPath holds that
// an unselected env is not evaluated, so neither an expression that cannot
// evaluate nor a second env block stops the top-level read. The pinned
// community binary v1.3.0 reads a file named with --config and no --env this
// way: `env "prod" { url = nosuch() }` there exits 0.
func TestParseAtlasWithOptionsNoImplicitEnvEvaluatesNoEnv_HappyPath(t *testing.T) {
	tests := []struct {
		name string
		data string
	}{
		{
			name: "an env that cannot evaluate",
			data: "lint {\n  latest = 3\n}\n\nenv \"prod\" {\n  url = nosuch()\n}\n",
		},
		{
			name: "two envs",
			data: "lint {\n  latest = 3\n}\n\nenv \"a\" {\n  url = \"sqlite://a.db\"\n}\n\nenv \"b\" {\n  url = \"sqlite://b.db\"\n}\n",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			cfg, err := projectconfig.ParseAtlasWithOptions(
				[]byte(test.data),
				projectconfig.AtlasFileName,
				projectconfig.AtlasLoadOptions{NoImplicitEnv: true},
			)

			c.Assert(err, qt.IsNil)
			c.Assert(cfg.StringValue(projectconfig.StringDatabaseURL), qt.Equals, projectconfig.Value[string]{})
			c.Assert(cfg.LintLatestValue(), qt.Equals, projectconfig.Value[int]{Value: 3, Present: true})
		})
	}
}

// TestParseAtlasWithOptionsImplicitEnv_FailurePath is the control for the test
// above: without NoImplicitEnv the same files are refused, so the passing rows
// there come from the option and not from a fixture nothing would refuse.
func TestParseAtlasWithOptionsImplicitEnv_FailurePath(t *testing.T) {
	tests := []struct {
		name    string
		data    string
		wantErr string
	}{
		{
			name:    "an env that cannot evaluate",
			data:    "env \"prod\" {\n  url = nosuch()\n}\n",
			wantErr: `(?s).*Call to unknown function.*`,
		},
		{
			name:    "two envs",
			data:    "env \"a\" {\n  url = \"sqlite://a.db\"\n}\n\nenv \"b\" {\n  url = \"sqlite://b.db\"\n}\n",
			wantErr: `atlas.hcl contains multiple env blocks; pass --env`,
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

			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(cfg.StringValue(projectconfig.StringDatabaseURL), qt.Equals, projectconfig.Value[string]{})
		})
	}
}

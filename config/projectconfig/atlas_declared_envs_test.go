package projectconfig_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/config/projectconfig"
)

// TestDeclaredAtlasEnvs_HappyPath lists the envs of each source and compares
// the list with the one a parse that selects no env records, so a block the
// parse reads as an env and the listing skips fails here. The data source in
// the last source names a program that does not exist: listing reads none of
// it.
func TestDeclaredAtlasEnvs_HappyPath(t *testing.T) {
	tests := []struct {
		name string
		data string
		want []projectconfig.DeclaredEnv
	}{
		{
			name: "two labeled envs in file order",
			data: "env \"prod\" {\n  url = \"sqlite://prod.db\"\n}\n\nenv \"dev\" {\n  url = \"sqlite://dev.db\"\n}\n",
			want: []projectconfig.DeclaredEnv{
				{File: "conf/atlas.hcl", Name: "prod"},
				{File: "conf/atlas.hcl", Name: "dev"},
			},
		},
		{
			name: "an env block with no label",
			data: "env {\n  name = atlas.env\n  url  = \"sqlite://${atlas.env}.db\"\n}\n",
			want: []projectconfig.DeclaredEnv{{File: "conf/atlas.hcl"}},
		},
		{
			name: "no env",
			data: "lint {\n  latest = 1\n}\n",
		},
		{
			name: "an env that reads a data source",
			data: "data \"external\" \"url\" {\n  program = [\"/no/such/program\"]\n}\n\nenv \"prod\" {\n  url = data.external.url\n}\n",
			want: []projectconfig.DeclaredEnv{{File: "conf/atlas.hcl", Name: "prod"}},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			got, err := projectconfig.DeclaredAtlasEnvs([]byte(test.data), "conf/atlas.hcl")

			c.Assert(err, qt.IsNil)
			c.Assert(got, qt.DeepEquals, test.want)
			parsed, err := projectconfig.ParseAtlas([]byte(test.data), "conf/atlas.hcl", "")
			c.Assert(err, qt.IsNil)
			c.Assert(got, qt.DeepEquals, parsed.DeclaredEnvs)
		})
	}

	t.Run("an empty file name stands for atlas.hcl", func(t *testing.T) {
		c := qt.New(t)

		got, err := projectconfig.DeclaredAtlasEnvs([]byte("env \"prod\" {}\n"), "")

		c.Assert(err, qt.IsNil)
		c.Assert(got, qt.DeepEquals, []projectconfig.DeclaredEnv{{File: projectconfig.AtlasFileName, Name: "prod"}})
	})
}

func TestDeclaredAtlasEnvs_FailurePath(t *testing.T) {
	tests := []struct {
		name    string
		data    string
		wantErr string
	}{
		{
			name:    "a source that is not HCL",
			data:    "env \"prod\" {\n",
			wantErr: `(?s)parse atlas project config: conf/atlas.hcl:1,.*`,
		},
		{
			name:    "an env block with two labels",
			data:    "env \"prod\" \"eu\" {}\n",
			wantErr: `unsupported atlas.hcl construct "env" at conf/atlas.hcl:1`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			got, err := projectconfig.DeclaredAtlasEnvs([]byte(test.data), "conf/atlas.hcl")

			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(got, qt.IsNil)
		})
	}
}

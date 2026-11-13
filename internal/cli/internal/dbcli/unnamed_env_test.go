package dbcli_test

import (
	"errors"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/config/projectconfig"
	"ptah.run/internal/cli/internal/dbcli"
)

// errDatabaseURLRequired stands in for the refusal a command builds when it
// has no database URL.
var errDatabaseURLRequired = errors.New("database URL is required")

// TestMissingProjectValue_NamesTheDeclaredEnvs holds the sentence a refusal
// gains when the run named no env and the project files declare some: it says
// that no --env was given and names every env each file declares, so the
// operator learns from the failure that an env applies only when named.
func TestMissingProjectValue_NamesTheDeclaredEnvs(t *testing.T) {
	tests := []struct {
		name     string
		declared []projectconfig.DeclaredEnv
		want     string
	}{
		{
			name:     "one env",
			declared: []projectconfig.DeclaredEnv{{File: "atlas.hcl", Name: "prod"}},
			want:     `database URL is required: no --env given; atlas.hcl declares env "prod"`,
		},
		{
			name: "three envs in one file",
			declared: []projectconfig.DeclaredEnv{
				{File: "ptah.yaml", Name: "dev"},
				{File: "ptah.yaml", Name: "prod"},
				{File: "ptah.yaml", Name: "staging"},
			},
			want: `database URL is required: no --env given; ptah.yaml declares envs "dev", "prod" and "staging"`,
		},
		{
			name: "envs in both files",
			declared: []projectconfig.DeclaredEnv{
				{File: "ptah.yaml", Name: "dev"},
				{File: "conf/staging.hcl", Name: "staging"},
			},
			want: `database URL is required: no --env given; ptah.yaml declares env "dev"; conf/staging.hcl declares env "staging"`,
		},
		{
			name: "an unlabeled env beside a labeled one",
			declared: []projectconfig.DeclaredEnv{
				{File: "atlas.hcl"},
				{File: "atlas.hcl", Name: "prod"},
			},
			want: `database URL is required: no --env given; atlas.hcl declares env "prod" and an env block with no label`,
		},
		{
			name:     "an unlabeled env alone",
			declared: []projectconfig.DeclaredEnv{{File: "atlas.hcl"}},
			want:     `database URL is required: no --env given; atlas.hcl declares an env block with no label`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			err := dbcli.MissingProjectValue(projectconfig.Config{DeclaredEnvs: test.declared}, errDatabaseURLRequired)

			c.Assert(err, qt.ErrorIs, errDatabaseURLRequired)
			c.Assert(err, qt.ErrorMatches, test.want)
		})
	}
}

// TestMissingProjectValue_LeavesTheRefusalAlone holds the cases with nothing to
// add: a run that named an env, and files that declare none. Either way the
// refusal is the one the command built, unchanged.
func TestMissingProjectValue_LeavesTheRefusalAlone(t *testing.T) {
	tests := []struct {
		name   string
		config projectconfig.Config
	}{
		{
			name: "an env was named",
			config: projectconfig.Config{
				EnvName:      "prod",
				DeclaredEnvs: []projectconfig.DeclaredEnv{{File: "ptah.yaml", Name: "prod"}},
			},
		},
		{name: "no env is declared"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			err := dbcli.MissingProjectValue(test.config, errDatabaseURLRequired)

			c.Assert(err, qt.Equals, errDatabaseURLRequired)
		})
	}
}

package dbcli_test

import (
	"cmp"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/config/projectconfig"
	"ptah.run/core/selectext"
	"ptah.run/internal/builtintest"
	"ptah.run/internal/cli/internal/dbcli"
)

// excludeProject is a ptah.yaml with a top-level exclude list and three envs:
// one that names its own list, one that empties it, and one that names none.
const excludeProject = `
exclude: [audit_log, "legacy_*"]
env:
  reporting:
    exclude: [reporting_cache]
  everything:
    exclude: []
  plain:
    url: sqlite://app.db
`

// parseExcludeProject reads project with envName selected, as the commands
// read ptah.yaml.
func parseExcludeProject(c *qt.C, project, envName string) projectconfig.Config {
	c.Helper()
	cfg, err := projectconfig.ParsePtah([]byte(project), projectconfig.PtahFileName, envName)
	c.Assert(err, qt.IsNil)
	return cfg
}

func TestEffectiveExclude_HappyPath(t *testing.T) {
	tests := []struct {
		name    string
		project string
		env     string
		flag    []string
		want    []string
	}{
		{
			name: "the top-level list, no env and no flag",
			want: []string{"audit_log", "legacy_*"},
		},
		{
			name: "the flag adds to the top-level list",
			flag: []string{"tmp_*"},
			want: []string{"audit_log", "legacy_*", "tmp_*"},
		},
		{
			name: "a selector both name is kept once",
			flag: []string{"tmp_*", "audit_log"},
			want: []string{"audit_log", "legacy_*", "tmp_*"},
		},
		{
			name: "the selected env's list replaces the top-level list",
			env:  "reporting",
			want: []string{"reporting_cache"},
		},
		{
			name: "the flag adds to the selected env's list",
			env:  "reporting",
			flag: []string{"tmp_*"},
			want: []string{"reporting_cache", "tmp_*"},
		},
		{
			name: "an env with an empty list leaves the flag alone",
			env:  "everything",
			flag: []string{"tmp_*"},
			want: []string{"tmp_*"},
		},
		{
			name: "an env that names no list keeps the top-level list",
			env:  "plain",
			want: []string{"audit_log", "legacy_*"},
		},
		{
			name:    "no project list returns the flag unchanged",
			project: "url: sqlite://app.db\n",
			flag:    []string{"tmp_*", "tmp_*"},
			want:    []string{"tmp_*", "tmp_*"},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			project := cmp.Or(test.project, excludeProject)
			cfg := parseExcludeProject(c, project, test.env)

			got, err := dbcli.EffectiveExclude(cfg, test.flag, builtintest.Runtime().Selections())

			c.Assert(err, qt.IsNil)
			c.Assert(got, qt.DeepEquals, test.want)
		})
	}
}

// TestEffectiveExclude_NothingToAdd returns the flag's own slice when the
// project names no selector, so a run with no project list passes the command
// exactly what it passed before, nil included.
func TestEffectiveExclude_NothingToAdd(t *testing.T) {
	tests := []struct {
		name    string
		project string
		flag    []string
	}{
		{name: "no file, no flag"},
		{name: "an empty list, no flag", project: "exclude: []\n"},
		{name: "only blanks and commas", project: "exclude: [\"\", \" , \"]\n", flag: []string{"tmp_*"}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			cfg := parseExcludeProject(c, test.project, "")

			got, err := dbcli.EffectiveExclude(cfg, test.flag, selectext.Set{})

			c.Assert(err, qt.IsNil)
			c.Assert(got, qt.DeepEquals, test.flag)
		})
	}
}

func TestEffectiveExclude_FailurePath(t *testing.T) {
	t.Run("a malformed selector names the file that holds it", func(t *testing.T) {
		c := qt.New(t)
		cfg := parseExcludeProject(c, "exclude: [\"us[ers\"]\n", "")

		got, err := dbcli.EffectiveExclude(cfg, []string{"tmp_*"}, builtintest.Runtime().Selections())

		c.Assert(err, qt.ErrorMatches, `invalid exclude in ptah\.yaml: invalid Atlas exclude glob "us\[ers": syntax error in pattern`)
		c.Assert(got, qt.IsNil)
	})
	t.Run("a list with no recorded file names the project config", func(t *testing.T) {
		c := qt.New(t)

		got, err := dbcli.EffectiveExclude(
			projectconfig.Config{Exclude: []string{"us[ers"}}, nil, builtintest.Runtime().Selections(),
		)

		c.Assert(err, qt.ErrorMatches, `invalid exclude in the project config: invalid Atlas exclude glob .*`)
		c.Assert(got, qt.IsNil)
	})
	t.Run("a runtime that serves no selections", func(t *testing.T) {
		c := qt.New(t)

		got, err := dbcli.EffectiveExclude(parseExcludeProject(c, excludeProject, ""), nil, selectext.Set{})

		c.Assert(err, qt.ErrorIs, selectext.ErrUnselected)
		c.Assert(got, qt.IsNil)
	})
}

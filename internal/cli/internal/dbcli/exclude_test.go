package dbcli_test

import (
	"cmp"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/spf13/cobra"

	"ptah.run/config/projectconfig"
	"ptah.run/core/selectext"
	"ptah.run/internal/atlasfilter"
	"ptah.run/internal/builtintest"
	"ptah.run/internal/cli/internal/cmdflags"
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

			got, _, err := dbcli.EffectiveExclude(nil, "exclude", test.flag, cfg, builtintest.Runtime().Selections())

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

			got, _, err := dbcli.EffectiveExclude(nil, "exclude", test.flag, cfg, selectext.Set{})

			c.Assert(err, qt.IsNil)
			c.Assert(got, qt.DeepEquals, test.flag)
		})
	}
}

func TestEffectiveExclude_FailurePath(t *testing.T) {
	t.Run("a malformed selector names the file that holds it", func(t *testing.T) {
		c := qt.New(t)
		cfg := parseExcludeProject(c, "exclude: [\"us[ers\"]\n", "")

		got, sources, err := dbcli.EffectiveExclude(nil, "exclude", []string{"tmp_*"}, cfg, builtintest.Runtime().Selections())

		c.Assert(err, qt.ErrorMatches, `invalid exclude in ptah\.yaml: invalid Atlas exclude glob "us\[ers": syntax error in pattern`)
		c.Assert(got, qt.IsNil)
		c.Assert(sources, qt.IsNil)
	})
	t.Run("a list with no recorded file names the project config", func(t *testing.T) {
		c := qt.New(t)

		got, _, err := dbcli.EffectiveExclude(
			nil, "", nil, projectconfig.Config{Exclude: []string{"us[ers"}}, builtintest.Runtime().Selections(),
		)

		c.Assert(err, qt.ErrorMatches, `invalid exclude in the project config: invalid Atlas exclude glob .*`)
		c.Assert(got, qt.IsNil)
	})
	t.Run("a runtime that serves no selections", func(t *testing.T) {
		c := qt.New(t)

		got, _, err := dbcli.EffectiveExclude(nil, "", nil, parseExcludeProject(c, excludeProject, ""), selectext.Set{})

		c.Assert(err, qt.ErrorIs, selectext.ErrUnselected)
		c.Assert(got, qt.IsNil)
	})
}

// excludeCommand is a command with one exclude flag called flag, its
// arguments parsed and its PTAH_* binding applied, as a native verb sees them
// when it runs.
func excludeCommand(c *qt.C, flag string, args ...string) *cobra.Command {
	c.Helper()
	cmd := &cobra.Command{Use: "verb"}
	cmd.Flags().StringArray(flag, nil, "Schema objects to exclude")
	c.Assert(cmd.ParseFlags(args), qt.IsNil)
	c.Assert(cmdflags.InitializeEnv("PTAH", cmd), qt.IsNil)
	return cmd
}

// TestEffectiveExclude_NamesTheSourceOfEachSelector records where each
// selector came from, so a warning about one that matched nothing names the
// file or the flag the operator wrote it in rather than always --exclude.
func TestEffectiveExclude_NamesTheSourceOfEachSelector(t *testing.T) {
	tests := []struct {
		name    string
		project string
		flag    string
		args    []string
		want    atlasfilter.ExcludeSources
	}{
		{
			name: "the project file, and the typed flag for what only it adds",
			flag: "exclude",
			args: []string{"--exclude", "tmp_*, audit_log"},
			want: atlasfilter.ExcludeSources{"audit_log": "ptah.yaml", "legacy_*": "ptah.yaml", "tmp_*": "--exclude"},
		},
		{
			name:    "a typed flag beside no project list",
			project: "url: sqlite://app.db\n",
			flag:    "exclude",
			args:    []string{"--exclude", "tmp_*"},
			want:    atlasfilter.ExcludeSources{"tmp_*": "--exclude"},
		},
		{
			name: "a flag of another name",
			flag: "drift-exclude",
			args: []string{"--drift-exclude", "tmp_*"},
			want: atlasfilter.ExcludeSources{"audit_log": "ptah.yaml", "legacy_*": "ptah.yaml", "tmp_*": "--drift-exclude"},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			cfg := parseExcludeProject(c, cmp.Or(test.project, excludeProject), "")
			cmd := excludeCommand(c, test.flag, test.args...)
			values, err := cmd.Flags().GetStringArray(test.flag)
			c.Assert(err, qt.IsNil)

			_, sources, err := dbcli.EffectiveExclude(cmd, test.flag, values, cfg, builtintest.Runtime().Selections())

			c.Assert(err, qt.IsNil)
			c.Assert(sources, qt.DeepEquals, test.want)
		})
	}
}

// TestEffectiveExclude_NamesTheVariableThatSetTheFlag attributes a value the
// flag's PTAH_* variable supplied to that variable: an operator who exported
// it in a CI job finds no --exclude on any command line.
func TestEffectiveExclude_NamesTheVariableThatSetTheFlag(t *testing.T) {
	c := qt.New(t)
	t.Setenv("PTAH_EXCLUDE", "tmp_*")
	cfg := parseExcludeProject(c, excludeProject, "")
	cmd := excludeCommand(c, "exclude")
	values, err := cmd.Flags().GetStringArray("exclude")
	c.Assert(err, qt.IsNil)

	got, sources, err := dbcli.EffectiveExclude(cmd, "exclude", values, cfg, builtintest.Runtime().Selections())

	c.Assert(err, qt.IsNil)
	c.Assert(got, qt.DeepEquals, []string{"audit_log", "legacy_*", "tmp_*"})
	c.Assert(sources, qt.DeepEquals, atlasfilter.ExcludeSources{"audit_log": "ptah.yaml", "legacy_*": "ptah.yaml", "tmp_*": "PTAH_EXCLUDE"})
}

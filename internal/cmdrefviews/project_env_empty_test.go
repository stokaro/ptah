package cmdrefviews_test

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"

	"ptah.run/internal/atlascompatpolicy"
	"ptah.run/internal/cli/root"
	"ptah.run/internal/cmdrefviews"
	"ptah.run/internal/envbinding"
)

// The tests in this file run every command that reads PTAH_ENV, on both
// binaries, beside a project file that declares an env. The refusal of an
// empty PTAH_ENV is decided by the files a run finds rather than by the flag,
// so no declaration on the flag can be checked the way the other variables
// are: each command is run instead.

// projectEnvExempt are the commands whose --env reads PTAH_ENV and selects no
// project env. `ptah seed --env` names the seed environment, which it
// requires, and `ptah-compat security` prints the help of its group.
var projectEnvExempt = []string{"ptah seed", "ptah-compat security"}

// projectEnvArgs are the arguments a command needs before it reads anything
// that could depend on an env.
var projectEnvArgs = map[string][]string{
	"ptah schema validate": {"--dialect", "postgres", "--root-dir", "."},
}

// The floors on the commands each walk finds, so a walk that stopped matching
// cannot pass by running nothing. `ptah` has 23 commands that read PTAH_ENV
// outside projectEnvExempt, and `ptah-compat` has 38 with the security scan
// enabled.
const (
	minNativeProjectEnvCommands = 23
	minCompatProjectEnvCommands = 38
)

// projectEnvTree is one binary's tree, the project file that declares an env
// for it, and the floor on its commands.
type projectEnvTree struct {
	name     string
	tree     func() *cobra.Command
	file     string
	contents string
	floor    int
}

var projectEnvTrees = []projectEnvTree{
	{
		name:     "ptah",
		tree:     root.NewRootCommand,
		file:     "ptah.yaml",
		contents: "env:\n  dev:\n    url: sqlite://app.db\n",
		floor:    minNativeProjectEnvCommands,
	},
	{
		name:     "ptah-compat",
		tree:     func() *cobra.Command { return cmdrefviews.Compat(atlascompatpolicy.Full()) },
		file:     "atlas.hcl",
		contents: "env \"dev\" {\n  url = \"sqlite://app.db\"\n}\n",
		floor:    minCompatProjectEnvCommands,
	},
}

// projectEnvCommands returns the argument path of every runnable command of
// tree that reads PTAH_ENV, outside projectEnvExempt.
func projectEnvCommands(tree *cobra.Command) [][]string {
	var found [][]string
	var walk func(cmd *cobra.Command, path []string)
	walk = func(cmd *cobra.Command, path []string) {
		reads := false
		visit := func(flag *pflag.Flag) {
			variable, bound := envbinding.Of("PTAH", flag)
			reads = reads || (bound && variable == "PTAH_ENV")
		}
		cmd.LocalFlags().VisitAll(visit)
		cmd.InheritedFlags().VisitAll(visit)
		if reads && cmd.Runnable() && !slices.Contains(projectEnvExempt, cmd.CommandPath()) {
			found = append(found, slices.Clone(path))
		}
		for _, child := range cmd.Commands() {
			walk(child, append(slices.Clone(path), child.Name()))
		}
	}
	walk(tree, nil)
	return found
}

// projectEnvRun runs the command at path of a fresh tree with args after it,
// in the working directory, with no input, and returns its error.
func projectEnvRun(c *qt.C, tree func() *cobra.Command, path, args []string) error {
	c.Helper()
	cmd := tree()
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetIn(strings.NewReader(""))
	cmd.SetArgs(slices.Concat(path, projectEnvArgs[strings.Join(slices.Concat([]string{cmd.Name()}, path), " ")], args))
	ctx, cancel := context.WithTimeout(c.Context(), 30*time.Second)
	defer cancel()
	return cmd.ExecuteContext(ctx)
}

// projectEnvDir makes a working directory for the test, holding the project
// file of tree, or nothing when contents is empty, and enters it.
func projectEnvDir(c *qt.C, file, contents string) {
	c.Helper()
	dir := c.TempDir()
	c.Assert(os.WriteFile(filepath.Join(dir, file), []byte(contents), 0o600), qt.IsNil)
	c.Chdir(dir)
}

// TestEveryProjectEnvCommandRefusesAnEmptyVariable runs each command with an
// empty PTAH_ENV beside a project file that declares the env dev. Read as
// unset, the variable selects no env, and a run meant for dev reads what the
// file says outside every env: over a history dev names, `migrations up` runs
// every migration again.
func TestEveryProjectEnvCommandRefusesAnEmptyVariable(t *testing.T) {
	for _, tree := range projectEnvTrees {
		t.Run(tree.name, func(t *testing.T) {
			c := qt.New(t)
			projectEnvDir(c, tree.file, tree.contents)
			c.Setenv("PTAH_ENV", "")
			c.Setenv("PTAH_ATLAS_SECURITY_SCAN", "1")
			commands := projectEnvCommands(tree.tree())

			c.Assert(len(commands) >= tree.floor, qt.IsTrue, qt.Commentf("found %d commands", len(commands)))
			for _, path := range commands {
				err := projectEnvRun(c, tree.tree, path, nil)

				comment := qt.Commentf("%s %s", tree.name, strings.Join(path, " "))
				c.Check(err, qt.ErrorIs, envbinding.ErrEmptyValue, comment)
				c.Check(err, qt.ErrorMatches, `PTAH_ENV is set but empty, and `+tree.file+` declares env "dev": name the env `+
					`to read, or unset it and leave --env out to select no env`, comment)
			}
		})
	}
}

// TestProjectEnvCommandsReadAnEmptyVariableAsUnsetOtherwise is the control
// for the refusal: a typed --env decides the run however the variable is set,
// and where no file declares an env, the empty value selects what any value
// would. Each command goes on to fail or succeed for its own reasons.
func TestProjectEnvCommandsReadAnEmptyVariableAsUnsetOtherwise(t *testing.T) {
	tests := []struct {
		name     string
		contents func(tree projectEnvTree) string
		args     []string
	}{
		{
			name:     "beside a typed --env",
			contents: func(tree projectEnvTree) string { return tree.contents },
			args:     []string{"--env", "dev"},
		},
		{
			name:     "beside a project file that declares no env",
			contents: func(projectEnvTree) string { return "" },
		},
	}
	for _, test := range tests {
		for _, tree := range projectEnvTrees {
			t.Run(test.name+"/"+tree.name, func(t *testing.T) {
				c := qt.New(t)
				projectEnvDir(c, tree.file, test.contents(tree))
				c.Setenv("PTAH_ENV", "")
				c.Setenv("PTAH_ATLAS_SECURITY_SCAN", "1")
				commands := projectEnvCommands(tree.tree())

				c.Assert(len(commands) >= tree.floor, qt.IsTrue, qt.Commentf("found %d commands", len(commands)))
				for _, path := range commands {
					err := projectEnvRun(c, tree.tree, path, test.args)

					c.Check(err, qt.Not(qt.ErrorIs), envbinding.ErrEmptyValue, qt.Commentf("%s %s", tree.name, strings.Join(path, " ")))
				}
			})
		}
	}
}

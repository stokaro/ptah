package root_test

import (
	"sort"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/spf13/cobra"

	"go.5x5.cz/ptah/cmd/internal/dbcli"
	"go.5x5.cz/ptah/cmd/root"
)

// projectEnvCommandPaths lists every native command that selects a project env
// and can therefore reach the atlas.hcl evaluator's
// `requires a default or --var name=value` diagnostic.
//
// The set is pinned rather than merely counted because the defect this test
// guards was per-command: eleven of the fourteen printed advice naming a flag
// they rejected, so a test exercising only the command in the bug report would
// have passed with thirteen still broken. A new entry here is a new command
// that must also answer --var.
var projectEnvCommandPaths = []string{
	"ptah migrations down",
	"ptah migrations generate",
	"ptah migrations lint",
	"ptah migrations plan",
	"ptah migrations set",
	"ptah migrations status",
	"ptah migrations up",
	"ptah schema apply",
	"ptah schema compare",
	"ptah schema diff",
	"ptah schema drift",
	"ptah schema inspect",
	"ptah schema plan",
	"ptah schema render",
}

// TestEveryProjectEnvCommandAcceptsVar holds every project-env command to the
// rule that it also accepts the flag its own diagnostic advises.
func TestEveryProjectEnvCommandAcceptsVar(t *testing.T) {
	c := qt.New(t)

	found := projectEnvCommands(root.NewRootCommand())
	paths := make([]string, 0, len(found))
	for path := range found {
		paths = append(paths, path)
	}
	sort.Strings(paths)

	// Reported before the per-command assertions so an added or removed
	// command is named rather than hidden behind a count.
	c.Assert(paths, qt.DeepEquals, projectEnvCommandPaths)

	for _, path := range paths {
		cmd := found[path]
		flag := cmd.Flags().Lookup(dbcli.ProjectVarFlagName)
		c.Assert(flag, qt.IsNotNil, qt.Commentf(
			"%s selects a project env but does not register --%s", path, dbcli.ProjectVarFlagName,
		))
		c.Assert(flag.Hidden, qt.IsFalse, qt.Commentf(
			"%s hides --%s, so --help does not list the flag the diagnostic advises", path, dbcli.ProjectVarFlagName,
		))
		c.Assert(flag.Value.Type(), qt.Equals, "stringArray", qt.Commentf(
			"%s must accept a repeatable --%s", path, dbcli.ProjectVarFlagName,
		))
	}
}

// TestSeedEnvIsNotAProjectEnv is the negative control for the rule above.
// `ptah seed --env` names the seed environment to apply, reads no project
// config, and can never print the variable diagnostic — so it must stay out of
// the annotated set and must not grow a --var it would ignore. Without this the
// rule could be satisfied by annotating every flag spelled --env.
func TestSeedEnvIsNotAProjectEnv(t *testing.T) {
	c := qt.New(t)

	seed := findCommand(root.NewRootCommand(), "ptah seed")
	c.Assert(seed, qt.IsNotNil)

	envFlag := seed.Flags().Lookup(dbcli.EnvFlagName)
	c.Assert(envFlag, qt.IsNotNil)
	c.Assert(envFlag.Annotations[dbcli.ProjectConfigEnvAnnotation], qt.IsNil)
	c.Assert(seed.Flags().Lookup(dbcli.ProjectVarFlagName), qt.IsNil)
}

// projectEnvCommands maps the full path of every command whose --env selects a
// project env to the command itself.
func projectEnvCommands(rootCmd *cobra.Command) map[string]*cobra.Command {
	found := make(map[string]*cobra.Command)
	walkCommands(rootCmd, func(cmd *cobra.Command) {
		flag := cmd.Flags().Lookup(dbcli.EnvFlagName)
		if flag == nil || flag.Annotations[dbcli.ProjectConfigEnvAnnotation] == nil {
			return
		}
		found[commandPath(cmd)] = cmd
	})
	return found
}

func findCommand(rootCmd *cobra.Command, path string) *cobra.Command {
	var match *cobra.Command
	walkCommands(rootCmd, func(cmd *cobra.Command) {
		if commandPath(cmd) == path {
			match = cmd
		}
	})
	return match
}

func walkCommands(cmd *cobra.Command, visit func(*cobra.Command)) {
	visit(cmd)
	for _, child := range cmd.Commands() {
		walkCommands(child, visit)
	}
}

func commandPath(cmd *cobra.Command) string {
	var parts []string
	for current := cmd; current != nil; current = current.Parent() {
		parts = append([]string{strings.Fields(current.Use)[0]}, parts...)
	}
	return strings.Join(parts, " ")
}
